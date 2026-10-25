"""Collector v2: single SQLite writer, OTLP endpoints, transcript sweeper, spool ingesters.

Concurrency policy (spec v3.1 §11): hooks and OTLP exporters never touch SQLite; this
process owns the only write connection (WAL + busy_timeout) and serializes all work
through one writer thread. OTLP payloads are persisted to a bounded spool FIRST
(at-least-once), then inserted; on spool overflow new payloads are refused and the loss
is recorded in telemetry_loss — nothing spooled is ever deleted (explicit prune only).
"""
import argparse
import hashlib
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import __version__
from .config import (DATA_DIR, DB_PATH, HOST, LOG_PATH, PID_PATH, PORT, PROJECTS_DIR,
                     SPOOL_MAX_BYTES, SWEEP_INTERVAL)
from .db import connect, migrate, now_iso
from .hookspool import commit_state, read_events
from .identity import gateway_from_base_url, jira_from
from .ingest.otel import insert_rows, normalize
from .ingest.transcripts import ingest_file

OTLP_SPOOL = DATA_DIR / "spool" / "otel"
OTLP_DONE = OTLP_SPOOL / "done"
REQUESTS_DIR = DATA_DIR / "spool" / "requests"
REQUESTS_DONE = REQUESTS_DIR / "done"


def log(msg):
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write("%s %s\n" % (now_iso(), msg))
    except OSError:
        pass


def git_branch(cwd):
    try:
        return subprocess.check_output(["git", "-C", cwd, "branch", "--show-current"],
                                       stderr=subprocess.DEVNULL, text=True,
                                       timeout=1).strip()
    except Exception:
        return ""


class Writer(threading.Thread):
    """Owns the only SQLite connection; executes submitted callables serially."""

    def __init__(self, db_path):
        super().__init__(daemon=True, name="cc-usage-writer")
        self.q = queue.Queue()
        self.db_path = db_path
        self.errors = 0
        self.ready = threading.Event()

    def run(self):
        db = connect(self.db_path)
        migrate(db)
        self.ready.set()
        while True:
            fn = self.q.get()
            if fn is None:
                break
            try:
                fn(db)
            except Exception as e:  # noqa: BLE001 — isolate every job; the writer survives
                self.errors += 1
                log("writer error: %s" % e)
        db.close()

    def submit(self, fn):
        self.q.put(fn)


# ---------------------------------------------------------------------------
# telemetry_loss accounting (spec §11: drops must be visible, never silent)
# ---------------------------------------------------------------------------

def record_loss(writer, reason):
    def job(db):
        row = db.execute("SELECT id FROM telemetry_loss WHERE recovered_at IS NULL "
                         "AND reason=? ORDER BY id DESC LIMIT 1", (reason,)).fetchone()
        ts = now_iso()
        if row:
            db.execute("UPDATE telemetry_loss SET dropped=dropped+1,last_ts=? WHERE id=?",
                       (ts, row["id"]))
        else:
            db.execute("INSERT INTO telemetry_loss(first_ts,last_ts,dropped,reason) "
                       "VALUES(?,?,1,?)", (ts, ts, reason))
        db.commit()
    writer.submit(job)


def mark_recovered(writer, reason):
    def job(db):
        db.execute("UPDATE telemetry_loss SET recovered_at=? WHERE recovered_at IS NULL "
                   "AND reason=?", (now_iso(), reason))
        db.commit()
    writer.submit(job)


# ---------------------------------------------------------------------------
# OTLP ingestion: spool-first, insert-second, replay on restart
# ---------------------------------------------------------------------------

def spool_pressure():
    OTLP_SPOOL.mkdir(parents=True, exist_ok=True)
    return _dir_size(str(OTLP_SPOOL))


def _dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        if "/done" in root.replace("\\", "/"):
            continue
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def enqueue_otel(writer, raw_bytes, state):
    """Persist raw OTLP payload to the spool, then queue insertion. On overflow: refuse
    (availability first), account the loss, never delete existing spool data."""
    if spool_pressure() > SPOOL_MAX_BYTES:
        record_loss(writer, "spool_full")
        return False
    OTLP_SPOOL.mkdir(parents=True, exist_ok=True)
    path = OTLP_SPOOL / ("p-%d-%s.json" % (time.time_ns(), state["seq"]))
    tmp = str(path) + ".tmp"
    with open(tmp, "wb") as f:
        f.write(raw_bytes)
    os.replace(tmp, str(path))

    def job(db):
        try:
            with open(str(path), "rb") as f:
                payload = json.loads(f.read().decode("utf-8"))
            insert_rows(db, normalize(payload))
            OTLP_DONE.mkdir(parents=True, exist_ok=True)
            os.replace(str(path), str(OTLP_DONE / path.name))
        except Exception as e:  # noqa: BLE001 — leave file for replay on restart
            log("otel insert failed (%s); kept for replay: %s" % (e, path.name))
            raise
    writer.submit(job)
    return True


def replay_otel(writer):
    """Replay any spooled-but-uninserted payloads (startup + post-recovery)."""
    OTLP_SPOOL.mkdir(parents=True, exist_ok=True)

    def job(db):
        replayed = 0
        for p in sorted(OTLP_SPOOL.glob("p-*.json")):
            try:
                payload = json.loads(p.read_bytes().decode("utf-8"))
                insert_rows(db, normalize(payload))
                OTLP_DONE.mkdir(parents=True, exist_ok=True)
                os.replace(str(p), str(OTLP_DONE / p.name))
                replayed += 1
            except Exception as e:  # noqa: BLE001
                log("otel replay failed for %s: %s" % (p.name, e))
        if replayed:
            log("otel replayed %d payloads" % replayed)
            mark_recovered_now(db)
    writer.submit(job)


def mark_recovered_now(db):
    db.execute("UPDATE telemetry_loss SET recovered_at=? WHERE recovered_at IS NULL",
               (now_iso(),))
    db.commit()


# ---------------------------------------------------------------------------
# Hook spool application (legacy v1 semantics, preserved exactly) + config snapshots
# ---------------------------------------------------------------------------

def _event_hash(payload):
    basis = "|".join(str(payload.get(k) or "") for k in
                     ("captured_at", "hook_event_name", "session_id", "prompt_id",
                      "command_name"))
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def apply_hook_event(db, payload):
    ev = payload.get("hook_event_name")
    sid = payload.get("session_id")
    pid = payload.get("prompt_id")
    cwd = payload.get("cwd") or os.getcwd()
    env = payload.get("env_snapshot") or {}
    ts = payload.get("captured_at") or now_iso()
    branch = git_branch(cwd)
    ehash = _event_hash(payload)

    if ev == "SessionStart":
        launcher = env.get("CC_LAUNCHER", "unknown")
        provider = env.get("CC_PROVIDER", "unknown")
        model = payload.get("model") or env.get("ANTHROPIC_MODEL") or ""
        db.execute(
            "INSERT INTO sessions(session_id,launcher,provider,model,cwd,git_branch,"
            "started_at,source) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET "
            "launcher=excluded.launcher,provider=excluded.provider,"
            "model=COALESCE(NULLIF(excluded.model,''),sessions.model),cwd=excluded.cwd,"
            "git_branch=excluded.git_branch",
            (sid, launcher, provider, model, cwd, branch, ts, payload.get("source")))
    elif ev == "UserPromptExpansion":
        cmd = payload.get("command_name")
        jira = (payload.get("command_args_jira") or payload.get("prompt_jira")
                or jira_from(branch))
        db.execute("INSERT OR IGNORE INTO command_events(session_id,prompt_id,ts,command_name,"
                   "command_args,command_source,jira_ticket,event_hash) VALUES(?,?,?,?,?,?,?,?)",
                   (sid, pid, ts, cmd, None, payload.get("command_source"), jira, ehash))
        if pid:
            db.execute("INSERT OR REPLACE INTO prompt_context(session_id,prompt_id,ts,"
                       "jira_ticket) VALUES(?,?,?,?)", (sid, pid, ts, jira))
    elif ev == "UserPromptSubmit":
        jira = payload.get("prompt_jira") or jira_from(branch)
        if pid:
            db.execute("INSERT OR REPLACE INTO prompt_context(session_id,prompt_id,ts,"
                       "jira_ticket) VALUES(?,?,?,?)", (sid, pid, ts, jira))
    elif ev == "SessionEnd":
        db.execute("UPDATE sessions SET ended_at=? WHERE session_id=?", (ts, sid))
        return  # no config snapshot on session end
    insert_config_snapshot(db, payload, env, ts, sid, ehash)


def insert_config_snapshot(db, payload, env, ts, sid, ehash=None):
    gw, base_url = gateway_from_base_url(env.get("ANTHROPIC_BASE_URL"))
    db.execute(
        "INSERT OR IGNORE INTO config_snapshots(session_id,ts,cli_version,launcher,provider,"
        "gateway,base_url,model_requested,effort,compact_window,subagent_model,"
        "permission_mode,orca_pane_key_present,load_avg,source,event_hash) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, ts, payload.get("version"), env.get("CC_LAUNCHER"), env.get("CC_PROVIDER"),
         gw, base_url, env.get("ANTHROPIC_MODEL"),
         env.get("CLAUDE_CODE_EFFORT_LEVEL") or env.get("CLAUDE_EFFORT"),
         env.get("CLAUDE_CODE_AUTO_COMPACT_WINDOW"), env.get("CLAUDE_CODE_SUBAGENT_MODEL"),
         payload.get("permission_mode"),
         1 if env.get("orca_pane_key_present") else 0, payload.get("load_avg"),
         payload.get("hook_event_name"), ehash))


def consume_hook_spool(writer):
    def job(db):
        state, payloads = read_events(DATA_DIR)
        if not payloads:
            return
        for p in payloads:
            try:
                apply_hook_event(db, p)
            except Exception as e:  # noqa: BLE001 — one bad payload must not stop the rest
                log("hook event apply failed: %s" % e)
        db.commit()
        commit_state(state)
    writer.submit(job)


# ---------------------------------------------------------------------------
# Transcript sweeper
# ---------------------------------------------------------------------------

def transcript_files(projects_dir):
    root = Path(projects_dir)
    if not root.exists():
        return []
    return [str(p) for p in root.rglob("*.jsonl")]


def submit_request(action, ts_window=None):
    """CLI ingest/backfill enter as REQUEST MARKERS consumed by the collector's sweeper —
    the CLI never opens a second SQLite write connection (critic P1-2)."""
    REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    req = {"action": action, "requested_at": now_iso()}
    if ts_window:
        req["since"] = ts_window[0]
        req["until"] = ts_window[1]
    path = REQUESTS_DIR / ("req-%d.json" % time.time_ns())
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(req, f)
    os.replace(tmp, str(path))
    return str(path)


def consume_requests(writer):
    """Drain request markers (runs inside the sweeper)."""
    REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    REQUESTS_DONE.mkdir(parents=True, exist_ok=True)

    def job(db):
        for p in sorted(REQUESTS_DIR.glob("req-*.json")):
            try:
                req = json.loads(p.read_text(encoding="utf-8"))
                if req.get("action") == "ingest":
                    for path in transcript_files(PROJECTS_DIR):
                        ingest_file(db, path)
                elif req.get("action") == "backfill":
                    w = (req.get("since"), req.get("until"))
                    for path in transcript_files(PROJECTS_DIR):
                        ingest_file(db, path, ts_window=w, backfill=True)
                db.commit()
                os.replace(str(p), str(REQUESTS_DONE / p.name))
                log("request %s processed" % req.get("action"))
            except Exception as e:  # noqa: BLE001
                log("request %s failed: %s" % (p.name, e))
        db.commit()
    writer.submit(job)


class Sweeper(threading.Thread):
    def __init__(self, writer, projects_dir=PROJECTS_DIR, interval=SWEEP_INTERVAL):
        super().__init__(daemon=True, name="cc-usage-sweeper")
        self.writer = writer
        self.projects_dir = str(projects_dir)
        self.interval = max(1.0, float(interval))
        self.in_flight = set()
        self.stop_evt = threading.Event()

    def run(self):
        while not self.stop_evt.wait(self.interval):
            self.sweep()

    def sweep(self):
        for path in transcript_files(self.projects_dir):
            if path in self.in_flight:
                continue
            self.in_flight.add(path)

            def job(db, p=path):
                try:
                    ingest_file(db, p)
                finally:
                    self.in_flight.discard(p)
            self.writer.submit(job)
        consume_hook_spool(self.writer)
        consume_requests(self.writer)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def make_handler(writer, state):
    seq_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def _read_body(self):
            """Read a request body supporting both Content-Length and chunked
            transfer-encoding. ROOT CAUSE FIX (2026-10-25): Claude Code's OTLP exporter
            sends chunked bodies; reading only Content-Length silently discarded every
            payload (bytes=0) since v1 — the entire 'dead pipeline' symptom."""
            te = (self.headers.get("Transfer-Encoding") or "").lower()
            if "chunked" in te:
                chunks = []
                while True:
                    line = self.rfile.readline(65536).strip()
                    if not line:
                        break
                    if b";" in line:
                        line = line.split(b";", 1)[0]
                    try:
                        size = int(line, 16)
                    except ValueError:
                        break
                    if size == 0:
                        self.rfile.readline()  # trailing CRLF after last chunk
                        break
                    chunks.append(self.rfile.read(size))
                    self.rfile.readline()  # CRLF after each chunk
                return b"".join(chunks)
            try:
                n = int(self.headers.get("Content-Length", "0") or 0)
            except ValueError:
                n = 0
            return self.rfile.read(n) if n > 0 else b""

        def do_POST(self):
            try:
                raw = self._read_body()
            except Exception:  # noqa: BLE001
                raw = b""
            with seq_lock:
                state["seq"] = state.get("seq", 0) + 1
            if self.path.endswith("/v1/logs") and raw:
                try:
                    enqueue_otel(writer, raw, state)
                except Exception as e:  # noqa: BLE001
                    log("otel spool failed: %s" % e)
            elif not self.path.endswith("/v1/logs"):
                # /v1/metrics and /v1/traces: accepted, counted, NOT parsed in Phase A
                # (pre-2.1.214 aggregation risk). Logged so probes can distinguish
                # "endpoint reached" from "nothing arrived".
                log("OTLP POST %s bytes=%d seq=%d (accepted, not parsed)"
                    % (self.path, len(raw), state.get("seq", 0)))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, fmt, *args):
            return
    return Handler


def collector_foreground(host=HOST, port=PORT):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OTLP_DONE.mkdir(parents=True, exist_ok=True)
    boot = connect(DB_PATH)
    migrate(boot)
    boot.close()

    writer = Writer(DB_PATH)
    writer.start()
    writer.ready.wait(10)
    state = {"seq": 0}

    replay_otel(writer)  # recover anything spooled before a crash/restart
    sweeper = Sweeper(writer)
    sweeper.start()
    sweeper.sweep()  # immediate first sweep (this is also the historical backfill)

    server = ThreadingHTTPServer((host, port), make_handler(writer, state))
    server.daemon_threads = True

    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print("cc-usage collector v2 listening on http://%s:%d" % (host, port), flush=True)
    try:
        server.serve_forever()
    finally:
        sweeper.stop_evt.set()
        writer.submit(None)
        server.server_close()


# ---------------------------------------------------------------------------
# start/stop/status (pid-file managed, same CLI shape as v1)
# ---------------------------------------------------------------------------

def collector_alive():
    if not PID_PATH.exists():
        return False
    try:
        pid = int(PID_PATH.read_text().strip())
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def port_open(port=PORT, host=HOST):
    try:
        with socket.create_connection((host, port), timeout=0.15):
            return True
    except OSError:
        return False


def start_collector():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if collector_alive() and port_open():
        return
    lib = Path(__file__).resolve()
    with open(LOG_PATH, "a") as logf:
        p = subprocess.Popen(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, %r); "
             "from cc_usage.collector import collector_foreground; "
             "collector_foreground()" % str(lib.parent.parent)],
            stdout=logf, stderr=logf, start_new_session=True,
            env=dict(os.environ, CC_USAGE_HOME=str(DATA_DIR)))
    PID_PATH.write_text(str(p.pid))
    for _ in range(40):
        if port_open():
            return
        time.sleep(0.05)
    raise SystemExit("collector failed to start; see " + str(LOG_PATH))


def stop_collector():
    if not PID_PATH.exists():
        return
    try:
        pid = int(PID_PATH.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        for _ in range(60):  # wait for exit so the port is free before a restart (P2-7)
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.05)
    except Exception:
        pass
    try:
        PID_PATH.unlink()
    except OSError:
        pass


def spool_prune():
    """EXPLICIT maintenance only (never automatic): remove replayed OTLP payloads and
    processed request markers. Spec §11: removal of spooled data happens only here."""
    removed = 0
    for d in (OTLP_DONE, REQUESTS_DONE):
        if d.exists():
            for p in d.glob("*"):
                try:
                    p.unlink()
                    removed += 1
                except OSError:
                    pass
    print("pruned %d replayed payload(s) from done/ directories" % removed)
    return 0


def dispatch(action):
    if action == "start":
        start_collector()
        print("collector running")
    elif action == "stop":
        stop_collector()
        print("collector stopped")
    elif action == "restart":
        stop_collector()
        time.sleep(0.2)
        start_collector()
        print("collector running")
    elif action == "status":
        print("running" if collector_alive() and port_open() else "stopped")
    else:
        collector_foreground()
    return 0


def main():
    p = argparse.ArgumentParser(prog="cc-usage collector")
    p.add_argument("action", choices=["start", "stop", "restart", "status", "foreground"])
    a = p.parse_args()
    return dispatch(a.action)


if __name__ == "__main__":
    main()
