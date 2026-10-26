"""Dashboard server: read-only SQLite, localhost-only, zero dependencies (spec §10).
Query logic lives in queries.py (unit-tested); this module is transport only."""
import argparse
import json
import re
import sqlite3
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from . import queries

STATIC = Path(__file__).resolve().parent / "static"
SESSION_RE = re.compile(r"^/api/session/([0-9a-fA-F-]+)$")


def open_ro(db_path):
    return sqlite3.connect("file:%s?mode=ro" % db_path, uri=True, timeout=10)


def make_handler(db_path):
    class Handler(BaseHTTPRequestHandler):
        def _db(self):
            db = open_ro(db_path)  # per-request connection: ThreadingHTTPServer-safe
            db.row_factory = sqlite3.Row
            return db

        def do_GET(self):
            u = urlparse(self.path)
            try:
                if u.path == "/" or u.path == "/index.html":
                    body = (STATIC / "index.html").read_bytes()
                    return self._send(200, body, "text/html; charset=utf-8")
                if u.path.startswith("/api/"):
                    q = {k: v[0] for k, v in parse_qs(u.query).items()}
                    days = int(q.get("days", 30))
                    device = q.get("device") or "all"
                    db = self._db()
                    try:
                        m = SESSION_RE.match(u.path)
                        if m:
                            data = queries.session_timeline(db, m.group(1))
                        elif u.path == "/api/summary":
                            data = queries.summary(db, days, device)
                        elif u.path == "/api/providers":
                            data = queries.providers(db, days, device)
                        elif u.path == "/api/phases":
                            data = queries.phases(db, days, ticket=q.get("ticket"), device=device)
                        elif u.path == "/api/concurrency":
                            data = queries.concurrency(db, days, device)
                        elif u.path == "/api/trends":
                            data = queries.trends(db, days, device)
                        elif u.path == "/api/quota":
                            data = queries.quota(db)
                        elif u.path == "/api/prs":
                            data = queries.prs(db, days, device)
                        elif u.path == "/api/value":
                            from .. import value
                            data = value.value_report(db, days, device=device)
                        elif u.path == "/api/devices":
                            data = queries.devices(db)
                        else:
                            return self._send(404, b'{"error":"unknown endpoint"}',
                                              "application/json")
                    finally:
                        db.close()
                    return self._send(200, json.dumps(data, default=str).encode(),
                                      "application/json")
                return self._send(404, b"not found", "text/plain")
            except Exception as e:  # noqa: BLE001 — dashboard must never crash hard
                try:
                    return self._send(500, json.dumps({"error": str(e)}).encode(),
                                      "application/json")
                except Exception:  # noqa: BLE001
                    pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    return Handler


def run(port=4399, db_path=None, open_browser=False):
    from ..config import DB_PATH as DEFAULT_DB
    db_path = str(db_path or DEFAULT_DB)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(db_path))
    server.daemon_threads = True
    url = "http://127.0.0.1:%d" % port
    print("cc-usage observatory: %s  (read-only; Ctrl-C to stop)" % url)
    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def main():
    p = argparse.ArgumentParser(prog="cc-usage dashboard")
    p.add_argument("--port", type=int, default=4399)
    p.add_argument("--db", default=None)
    p.add_argument("--open", action="store_true")
    a = p.parse_args()
    return run(a.port, a.db, a.open)


if __name__ == "__main__":
    main()
