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
SNAPSHOT = ["workflow_phases", "phase_transitions"]
BATCH = 4000


def _columns(db, table):
    return [r[1] for r in db.execute("PRAGMA table_info(%s)" % table)]


def ensure_device_backfill(db):
    """One-time: every pre-sync row was captured on THIS machine."""
    did = devicemod.device_id()
    for t in ("sessions", "api_requests", "config_snapshots", "pr_contributions",
              "command_events", "prompt_context"):
        db.execute("UPDATE %s SET device_id=? WHERE device_id IS NULL" % t, (did,))
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


def _rows_all(db, table, mode):
    cols = _columns(db, table)
    for r in db.execute("SELECT %s FROM %s" % (",".join(cols), table)):
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
    for table in SNAPSHOT:
        yield json.dumps({"snapshot_begin": {"table": table}}) + "\n"
        for (t, mode, row) in _rows_all(db, table, "snapshot"):
            row["_src_table"] = t
            row["_mode"] = mode
            yield json.dumps(row, default=str) + "\n"
        yield json.dumps({"snapshot_end": {"table": table}}) + "\n"


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


def _apply_line(db, line, applied):
    rec = json.loads(line)
    if "meta" in rec or "watermark" in rec or "upsert_done" in rec:
        return
    if "snapshot_begin" in rec:
        t = rec["snapshot_begin"]["table"]
        db.execute("DELETE FROM %s" % t)
        return
    if "snapshot_end" in rec or not rec.get("_src_table"):
        return
    table = rec["_src_table"]
    mode = rec["_mode"]
    row = {k: v for k, v in rec.items() if not k.startswith("_")}
    cols = list(row.keys())
    if mode == "snapshot":
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (table, ",".join(cols),
                                                 ",".join("?" * len(cols)))
    else:
        sql = "INSERT OR IGNORE INTO %s(%s) VALUES(%s)" % (table, ",".join(cols),
                                                           ",".join("?" * len(cols))) \
            if table in WATERMARK else \
            "INSERT OR REPLACE INTO %s(%s) VALUES(%s)" % (table, ",".join(cols),
                                                          ",".join("?" * len(cols)))
    db.execute(sql, [row[c] for c in cols])
    applied[table] = applied.get(table, 0) + 1


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
            try:
                for line in raw.decode("utf-8").splitlines():
                    if line.strip():
                        _apply_line(db, line, applied)
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
