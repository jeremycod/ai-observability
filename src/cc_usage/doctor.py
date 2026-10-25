"""doctor v2: legacy installation checks + pipeline health + isolated self-check.

The self-check runs a throwaway collector in an isolated CC_USAGE_HOME on a random
port, posts a synthetic OTLP log, and verifies the row in the TEMP database — synthetic
records can never mix into real data (spec §11).
"""
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

from . import __version__
from .config import DATA_DIR, DB_PATH
from .collector import OTLP_SPOOL, collector_alive, port_open
from .db import connect, migrate


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def self_check():
    """Isolated end-to-end check. Returns (ok, detail)."""
    tmp = tempfile.mkdtemp(prefix="cc-usage-doctor-")
    port = _free_port()
    pkg_parent = Path(__file__).resolve().parent.parent
    code = ("import sys; sys.path.insert(0, %r); "
            "from cc_usage.collector import collector_foreground; "
            "collector_foreground('127.0.0.1', %d)" % (str(pkg_parent), port))
    env = dict(os.environ, CC_USAGE_HOME=tmp, CC_USAGE_PORT=str(port))
    proc = subprocess.Popen([sys.executable, "-c", code], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)
    ok, detail = False, "no response"
    try:
        for _ in range(100):  # generous: the child may be slow to bind under load
            if port_open(port):
                break
            time.sleep(0.1)
            if proc.poll() is not None:
                break  # child died; fall through to the failure detail
        tdb = os.path.join(tmp, "usage.db")
        payload = json.dumps({"resourceLogs": [{"resource": {"attributes": []},
            "scopeLogs": [{"scope": {"name": "doctor"}, "logRecords": [{
                "timeUnixNano": "1761370000000000000", "severityText": "INFO",
                "body": {"stringValue": "api_request"},
                "attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
                    {"key": "session.id", "value": {"stringValue": "doctor-selfcheck"}},
                    {"key": "model", "value": {"stringValue": "doctor-model"}},
                    {"key": "input_tokens", "value": {"intValue": "1"}},
                    {"key": "output_tokens", "value": {"intValue": "2"}},
                    {"key": "cache_read_tokens", "value": {"intValue": "3"}},
                    {"key": "cache_creation_tokens", "value": {"intValue": "4"}},
                    {"key": "duration_ms", "value": {"intValue": "5"}}]}]}]}]}).encode()
        req = urllib.request.Request("http://127.0.0.1:%d/v1/logs" % port, data=payload,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        for _ in range(100):  # generous: ingestion may lag under load
            tdb = os.path.join(tmp, "usage.db")
            try:
                con = sqlite3.connect(tdb, timeout=2)
                n = con.execute("SELECT COUNT(*) FROM otel_api_requests "
                                "WHERE session_id='doctor-selfcheck'").fetchone()[0]
                con.close()
                if n:
                    ok, detail = True, "synthetic event ingested in isolated DB"
                    break
            except sqlite3.Error:
                pass
            time.sleep(0.1)
    except Exception as e:  # noqa: BLE001
        detail = "self-check failed: %s" % e
    finally:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)
    return ok, detail


def run():
    checks = []
    checks.append(("cc-usage version", __version__, True))
    checks.append(("Python", sys.version.split()[0], sys.version_info >= (3, 9)))
    claude = shutil.which("claude")
    checks.append(("Claude Code", claude or "not found", bool(claude)))
    running = collector_alive() and port_open()
    checks.append(("Collector v2", "running" if running else "stopped", running))

    migrated = False
    if DB_PATH.exists():
        try:
            db = connect(DB_PATH, timeout=5)
            row = db.execute("SELECT COALESCE(MAX(version),0) v FROM schema_migrations").fetchone()
            migrated = bool(row and row["v"] >= 1)
            n_state = db.execute("SELECT COUNT(*) c FROM ingest_state").fetchone()["c"]
            last = db.execute("SELECT MAX(last_ok) l FROM ingest_state").fetchone()["l"]
            n_req = db.execute("SELECT COUNT(*) c FROM v_model_responses").fetchone()["c"]
            db.close()
            checks.append(("DB migrations", "v%s applied" % row["v"] if migrated
                           else "NOT migrated", migrated))
            checks.append(("Transcript ingest", "%d files, %d responses, last %s"
                           % (n_state, n_req, (last or "-")[:19]), True))
        except Exception as e:  # noqa: BLE001
            checks.append(("DB health", "error: %s" % e, False))
    else:
        checks.append(("SQLite DB", str(DB_PATH), False))

    # Spool + loss accounting (spec §11: loss must be visible)
    try:
        otel_bytes = 0
        if OTLP_SPOOL.exists():
            for p in OTLP_SPOOL.rglob("*.json"):
                otel_bytes += p.stat().st_size
        checks.append(("OTLP spool", "%.1f KB" % (otel_bytes / 1024.0), True))
        if DB_PATH.exists():
            db = connect(DB_PATH, timeout=5)
            losses = db.execute("SELECT COUNT(*) c, COALESCE(SUM(dropped),0) d, "
                                "COALESCE(SUM(CASE WHEN recovered_at IS NULL THEN 1 ELSE 0 END),0) open "
                                "FROM telemetry_loss").fetchone()
            db.close()
            checks.append(("Telemetry loss", "%d events, %d dropped, %d unresolved"
                           % (losses["c"], losses["d"], losses["open"]),
                           losses["open"] == 0))
    except Exception as e:  # noqa: BLE001
        checks.append(("Spool/loss", "error: %s" % e, False))

    ok_sc, detail_sc = self_check()
    checks.append(("Isolated self-check", detail_sc, ok_sc))

    width = max(len(c[0]) for c in checks)
    ok_all = True
    for name, val, ok in checks:
        ok_all &= bool(ok)
        print("%s  %s  %s" % ("OK" if ok else "!!", name.ljust(width), val))
    print("\nData stays local at %s" % DATA_DIR)
    return 0 if ok_all else 1
