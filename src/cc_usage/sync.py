"""Phase G: multi-device sync — local-first, push-to-aggregator (spec §17).

- Each machine keeps its full local database (nothing is removed by syncing).
- `push` exports new/changed rows since per-table watermarks as NDJSON batches and
  POSTs them to the staging receiver with a bearer token. Pushes are IDEMPOTENT:
  watermark tables use INSERT OR IGNORE, mutable tables INSERT OR REPLACE, derived
  tables (workflow phases/lineages) are snapshot-replaced.
- `serve-sync` is the receiver for the staging server: token-authed, same schema,
  single small SQLite database that AGGREGATES devices. Query side filters by
  device_id: personal / work / everything.
- No prompt or assistant text is in any synced table (spec §11 holds).
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import device as devicemod
from .db import connect, migrate, now_iso

# table -> (mode, id_column). watermark = push rows with id > watermark (append-only);
# upsert = full-table INSERT OR REPLACE (small, mutable); snapshot = derived data,
# receiver deletes then inserts (workflow phases rebuild on the source machine).
WATERMARK = ["api_requests", "api_errors", "otel_api_requests", "command_events",
             "tool_events", "provider_events", "config_snapshots", "pr_contributions",
             "api_request_occurrences"]
UPSERT = ["sessions", "prompt_context", "pull_requests", "pr_reviews", "pr_revisions",
          "pr_files", "subagent_dispatches", "jira_issues", "quota_windows",
          "critic_findings", "review_artifacts", "work_lineages"]
PHASE_TABLES = ["workflow_phases", "phase_transitions"]  # deterministic per-device ids
BATCH = 4000


def device_num(device):
    """Stable small int from the device id (< 100000)."""
    return int(hashlib.sha1(device.encode()).hexdigest()[:8], 16) % 100000


def sync_phase_id(device, local_id):
    """Deterministic globally-unique phase id: device_num * 10^7 + local_id.
    Stable across pushes -> phases upsert (never renumbered), api_requests.phase_id
    references stay valid forever, and two devices cannot collide (spec §17)."""
    return device_num(device) * 10**7 + int(local_id)


def _columns(db, table):
    return [r[1] for r in db.execute("PRAGMA table_info(%s)" % table)]


def ensure_device_backfill(db):
    """One-time: every pre-sync row was captured on THIS machine. Derived tables
    inherit the device of their session."""
    did = devicemod.device_id()
    for t in ("sessions", "api_requests", "config_snapshots", "pr_contributions",
              "command_events", "prompt_context", "api_request_occurrences",
              "api_errors", "otel_api_requests", "tool_events"):
        db.execute("UPDATE %s SET device_id=? WHERE device_id IS NULL" % t, (did,))
    for t in ("workflow_phases", "phase_transitions"):
        db.execute("UPDATE %s SET device_id=(SELECT s.device_id FROM sessions s "
                   "WHERE s.session_id=%s.session_id) WHERE device_id IS NULL" % (t, t))
    db.execute("UPDATE work_lineages SET device_id=? WHERE device_id IS NULL", (did,))
    db.commit()
    return did


def _rows_since(db, table, last_id):
    cols = _columns(db, table)
    q = "SELECT rowid AS _rid, %s FROM %s WHERE rowid > ? ORDER BY rowid LIMIT %d" % (
        ",".join(cols), table, BATCH)
    while True:
        rows = db.execute(q, (last_id,)).fetchall()
        if not rows:
            return
        for r in rows:
            yield table, "watermark", {c: r[c] for c in cols}, r["_rid"]
        last_id = rows[-1]["_rid"]
        if len(rows) < BATCH:
            return


def _rows_all(db, table, mode, device=None):
    cols = _columns(db, table)
    q = "SELECT %s FROM %s" % (",".join(cols), table)
    params = ()
    if device and "device_id" in cols:
        q += " WHERE device_id = ?"
        params = (device,)
    for r in db.execute(q, params):
        yield table, mode, {c: r[c] for c in cols}


def _iter_batches(db, device):
    """Yield NDJSON lines: one JSON object per row. Snapshot tables force a full
    replace on the receiver; upsert tables are always sent whole (small)."""
    yield json.dumps({"meta": {"device": device, "pushed_at": now_iso()}}) + "\n"
    for table in WATERMARK:
        st = db.execute("SELECT last_id FROM sync_state WHERE table_name=?",
                        (table,)).fetchone()
        last = st["last_id"] if st else 0
        maxrow = db.execute("SELECT COALESCE(MAX(rowid),0) m FROM %s" % table).fetchone()["m"]
        if maxrow <= last:
            continue
        count = 0
        for (t, mode, row, _rid) in _rows_since(db, table, last):
            if table == "api_requests" and row.get("phase_id") is not None \
                    and row.get("device_id"):
                # phase stamps become deterministic sync ids on the aggregate
                row["phase_id"] = device_num(row["device_id"]) * 10**7 + int(row["phase_id"])
            row["_src_table"] = t
            row["_mode"] = mode
            yield json.dumps(row, default=str) + "\n"
            count += 1
        yield json.dumps({"watermark": {"table": table, "last_id": maxrow}}) + "\n"
    for table in UPSERT:
        for (t, mode, row) in _rows_all(db, table, "upsert"):
            row["_src_table"] = t
            row["_mode"] = mode
            yield json.dumps(row, default=str) + "\n"
        yield json.dumps({"upsert_done": {"table": table}}) + "\n"
    dnum = device_num(device)
    for table in PHASE_TABLES:
        # deterministic per-device ids: no renumbering, no remapping, no clobbering
        kept = []
        for (t, mode, row) in _rows_all(db, table, "upsert"):
            if row.get("id") is not None:
                row["id"] = dnum * 10**7 + int(row["id"])
            if table == "workflow_phases" and not row.get("device_id"):
                row["device_id"] = device
            kept.append(row["id"])
            row["_src_table"] = t
            row["_mode"] = "upsert"
            yield json.dumps(row, default=str) + "\n"
        yield json.dumps({"phase_done": {"table": table, "device": device,
                                         "kept": kept}}) + "\n"


def push(db, url, token, device=None):
    """Push local data to the staging aggregator. Returns (lines_sent, applied)."""
    device = device or os.environ.get("CC_USAGE_DEVICE") or devicemod.device_id()
    ensure_device_backfill(db)
    body_iter = _iter_batches(db, device)
    sent = 0
    req = urllib.request.Request(
        url.rstrip("/") + "/sync", data=b"".join(line.encode() for line in body_iter),
        headers={"Content-Type": "application/x-ndjson",
                 "Authorization": "Bearer " + token}, method="POST")
    with urllib.request.urlopen(req, timeout=600) as resp:
        result = json.loads(resp.read().decode())
    applied = result.get("applied", {})
    sent = sum(applied.values()) if isinstance(applied, dict) else result.get("applied", 0)
    # persist watermarks only after receiver acknowledged
    for table in WATERMARK:
        maxrow = db.execute("SELECT COALESCE(MAX(rowid),0) m FROM %s" % table).fetchone()["m"]
        db.execute("INSERT INTO sync_state(table_name,last_id,last_push) VALUES(?,?,?) "
                   "ON CONFLICT(table_name) DO UPDATE SET last_id=excluded.last_id,"
                   "last_push=excluded.last_push", (table, maxrow, now_iso()))
    db.commit()
    return sent, applied


def _apply_line(db, line, applied, batch_device=None):
    rec = json.loads(line)
    if "meta" in rec:
        return
    if "phase_done" in rec:
        info = rec["phase_done"]
        # per-device prune: drop THIS device's stale phases (rebuilds renumber
        # locally); other devices' rows are untouched
        t = info["table"]
        kept = info.get("kept") or [0]
        db.execute("CREATE TEMP TABLE IF NOT EXISTS _kept_ids (id INTEGER)")
        db.execute("DELETE FROM _kept_ids")
        db.executemany("INSERT INTO _kept_ids VALUES(?)", [(i,) for i in kept])
        db.execute("DELETE FROM %s WHERE device_id=? AND id NOT IN "
                   "(SELECT id FROM _kept_ids)" % t, (info["device"],))
        return
    if not rec.get("_src_table"):
        return
    table = rec["_src_table"]
    mode = rec["_mode"]
    row = {k: v for k, v in rec.items() if not k.startswith("_")}
    if table == "work_lineages" and mode == "upsert":
        _merge_lineage(db, row)
        applied[table] = applied.get(table, 0) + 1
        return
    cols = list(row.keys())
    if table in ("workflow_phases", "phase_transitions", "work_lineages"):
        if not row.get("device_id") and batch_device:
            row["device_id"] = batch_device
            cols = list(row.keys())
    sql = "INSERT OR IGNORE INTO %s(%s) VALUES(%s)" % (table, ",".join(cols),
                                                       ",".join("?" * len(cols))) \
        if table in WATERMARK else \
        "INSERT OR REPLACE INTO %s(%s) VALUES(%s)" % (table, ",".join(cols),
                                                      ",".join("?" * len(cols)))
    db.execute(sql, [row.get(c) for c in cols])
    applied[table] = applied.get(table, 0) + 1


def _merge_lineage(db, row):
    """work_lineages: SAME ticket lineage exists on every device — merge session_ids
    (union) instead of replace, so neither laptop's sessions drop out of rollups."""
    cols = [r[1] for r in db.execute("PRAGMA table_info(work_lineages)")]
    existing = db.execute("SELECT session_ids FROM work_lineages WHERE lineage_id=?",
                          (row["lineage_id"],)).fetchone()
    row = dict(row)
    if existing:
        try:
            ours = set(json.loads(existing["session_ids"] or "[]"))
        except (ValueError, TypeError):
            ours = set()
        try:
            theirs = set(json.loads(row.get("session_ids") or "[]"))
        except (ValueError, TypeError):
            theirs = set()
        row["session_ids"] = json.dumps(sorted(ours | theirs))
    for c in cols:
        row.setdefault(c, None)
    db.execute("INSERT OR REPLACE INTO work_lineages(%s) VALUES(%s)" % (
        ",".join(cols), ",".join("?" * len(cols))), [row[c] for c in cols])


def make_receiver_handler(db_path, token):
    """token=None/empty -> OPEN mode: meant for a Tailscale/private network where
    WireGuard provides encryption + device identity. A configured token enforces
    bearer auth (use it if the receiver is ever reachable beyond your tailnet)."""
    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/sync":
                return self._send(404, b'{"error":"not found"}')
            if token:
                auth = self.headers.get("Authorization", "")
                if auth != "Bearer " + token:
                    return self._send(403, b'{"error":"forbidden"}')
            n = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(n)
            db = connect(db_path)
            migrate(db)
            applied = {}
            batch_device = None
            try:
                for line in raw.decode("utf-8").splitlines():
                    if line.strip():
                        first = json.loads(line)
                        if "meta" in first:
                            batch_device = first["meta"].get("device")
                        _apply_line(db, line, applied, batch_device)
                db.commit()
            except Exception as e:  # noqa: BLE001 — batch fails loud, connection kept
                db.rollback()
                return self._send(500, json.dumps({"error": str(e)}).encode(),
                                  "application/json")
            finally:
                db.close()
            self._send(200, json.dumps({"ok": True, "applied": applied}).encode(),
                       "application/json")

        def do_GET(self):
            if self.path == "/health":
                return self._send(200, b'{"ok":true}', "application/json")
            self._send(404, b"not found", "text/plain")

        def _send(self, code, body, ctype="application/json"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return
    return Receiver


def serve_sync(port=4398, db_path=None, token=None):
    token = token or os.environ.get("CC_USAGE_SYNC_TOKEN") or ""
    db_path = str(db_path or os.environ.get("CC_USAGE_SYNC_DB") or
                  Path.home() / ".local/share/cc-usage/aggregate.db")
    server = ThreadingHTTPServer(("0.0.0.0", port), make_receiver_handler(db_path, token))
    server.daemon_threads = True
    mode = "token-authed" if token else "OPEN (no token — protect via Tailscale/private net)"
    print("cc-usage sync receiver on :%d -> %s [%s]" % (port, db_path, mode), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def receiver_main():
    p = __import__("argparse").ArgumentParser(prog="cc-usage serve-sync")
    p.add_argument("--port", type=int, default=4398)
    p.add_argument("--db", default=None)
    p.add_argument("--token", default=None)
    a = p.parse_args()
    return serve_sync(a.port, a.db, a.token)


def sync_main():
    import argparse
    p = argparse.ArgumentParser(prog="cc-usage sync")
    p.add_argument("action", choices=["push", "status"])
    p.add_argument("--url", default=os.environ.get("CC_USAGE_SYNC_URL", ""))
    p.add_argument("--token-env", default="CC_USAGE_SYNC_TOKEN")
    a = p.parse_args()
    db = connect(_local_db())
    migrate(db)
    if a.action == "status":
        rows = db.execute("SELECT * FROM sync_state ORDER BY table_name").fetchall()
        for r in rows:
            print("%-28s last_id=%d pushed=%s" % (r["table_name"], r["last_id"],
                                                  r["last_push"] or "never"))
        if not rows:
            print("never pushed")
        db.close()
        return 0
    token = os.environ.get(a.token_env, "")
    if not a.url or not token:
        print("set CC_USAGE_SYNC_URL and %s (or pass --url / --token-env)" % a.token_env)
        db.close()
        return 1
    sent, applied = push(db, a.url, token)
    print("pushed %d rows; receiver applied: %s" % (sent, applied))
    db.close()
    return 0


def _local_db():
    from .config import DB_PATH
    return DB_PATH


if __name__ == "__main__":
    sys.exit(receiver_main())
