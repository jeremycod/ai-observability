"""A9 (isolated): end-to-end pipeline in a temp home — collector subprocess, OTLP posts,
concurrent-session transcript ingestion, zero double-count between sources."""
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
import unittest
from pathlib import Path

import common  # noqa: F401
from common import REPO, assistant, write_transcript

SRC = str(REPO / "src")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def otlp_log(session, model, seq):
    return json.dumps({"resourceLogs": [{"resource": {"attributes": []}, "scopeLogs": [{
        "scope": {"name": "e2e"}, "logRecords": [{
            "timeUnixNano": str(1761370000000000000 + seq), "severityText": "INFO",
            "body": {"stringValue": "api_request"},
            "attributes": [
                {"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
                {"key": "session.id", "value": {"stringValue": session}},
                {"key": "model", "value": {"stringValue": model}},
                {"key": "input_tokens", "value": {"intValue": str(10 + seq)}},
                {"key": "output_tokens", "value": {"intValue": "5"}},
                {"key": "cache_read_tokens", "value": {"intValue": "100"}},
                {"key": "cache_creation_tokens", "value": {"intValue": "0"}},
                {"key": "duration_ms", "value": {"intValue": "50"}}]}]}]}]}).encode()


class TestEndToEndIsolated(unittest.TestCase):
    def test_collector_ingest_idempotence_and_source_separation(self):
        tmp = Path(tempfile.mkdtemp(prefix="cc-usage-e2e-"))
        home = tmp / "home"
        projects = home / "projects" / "p1"
        port = free_port()
        env = dict(os.environ, CC_USAGE_HOME=str(home), CC_USAGE_PORT=str(port),
                   CC_USAGE_PROJECTS_DIR=str(projects),
                   CC_USAGE_SWEEP_INTERVAL="1", PYTHONPATH=SRC)
        # three concurrent "sessions" as transcript files (GLM + DeepSeek mix)
        projects.mkdir(parents=True)
        sessions = []
        for i, (sid, model) in enumerate([("sessA", "glm-5.3-flash"),
                                          ("sessB", "deepseek-flash"),
                                          ("sessC", "glm-5.3-flash")]):
            recs = [assistant(sid, "%s-m%d" % (sid, j),
                              "2026-10-24T10:0%d:%02dZ" % (i, j), model=model)
                    for j in range(5)]
            sessions.append(write_transcript(projects / ("%s.jsonl" % sid), recs))
        # duplicate one record across two "concurrent" sessions files (rewrite case)
        with open(sessions[0], "a") as f:
            f.write(json.dumps(assistant("sessA", "sessA-m0", "2026-10-24T10:00:00Z")) + "\n")

        proc = subprocess.Popen(
            [sys.executable, "-c",
             "from cc_usage.collector import collector_foreground; collector_foreground()"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        try:
            up = False
            for _ in range(80):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        up = True
                        break
                except OSError:
                    time.sleep(0.05)
            self.assertTrue(up, "collector did not come up")
            # three OTLP posts (would be live Claude Code exports)
            for i in range(3):
                req = urllib.request.Request(
                    "http://127.0.0.1:%d/v1/logs" % port, data=otlp_log("otel-s%d" % i, "m", i),
                    headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=5)
        finally:
            proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()
        # transcript ingest via a separate process (the `cc-usage ingest` code path)
        subprocess.run([sys.executable, "-c",
                        "import sys; sys.path.insert(0, %r);"
                        "from cc_usage.app import main; sys.exit(main(['ingest',"
                        "'--projects-dir', %r]))" % (SRC, str(projects))],
                       env=env, check=True)
        db = sqlite3.connect(str(home / "usage.db"))
        db.row_factory = sqlite3.Row
        n_req = db.execute("SELECT COUNT(*) c FROM v_model_responses").fetchone()["c"]
        self.assertGreaterEqual(n_req, 15)  # 3 sessions x 5 responses
        n_otel = db.execute("SELECT COUNT(*) c FROM otel_api_requests").fetchone()["c"]
        self.assertEqual(n_otel, 3)
        # source separation: transcript rows never merged with OTLP rows
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_requests "
                                    "WHERE source='otel'").fetchone()["c"], 0)
        # idempotence: second ingest adds nothing
        subprocess.run([sys.executable, "-c",
                        "import sys; sys.path.insert(0, %r);"
                        "from cc_usage.app import main; sys.exit(main(['ingest',"
                        "'--projects-dir', %r]))" % (SRC, str(projects))],
                       env=env, check=True)
        n_req2 = db.execute("SELECT COUNT(*) c FROM v_model_responses").fetchone()["c"]
        self.assertEqual(n_req, n_req2)
        # zero dedup violations: identity invariant holds
        viol = db.execute("SELECT COUNT(*) c FROM (SELECT session_id, identity_key "
                          "FROM api_requests WHERE identity_key IS NOT NULL "
                          "GROUP BY session_id, identity_key HAVING COUNT(*)>1)").fetchone()["c"]
        self.assertEqual(viol, 0)
        # identity basis populated for all native rows
        bare = db.execute("SELECT COUNT(*) c FROM v_model_responses "
                          "WHERE identity_key IS NULL").fetchone()["c"]
        self.assertEqual(bare, 0)
        db.close()


if __name__ == "__main__":
    unittest.main()
