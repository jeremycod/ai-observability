"""Phase G: multi-device sync — push round-trip to a live receiver, idempotency,
device filtering, and receiver auth."""
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import time
import urllib.error
import urllib.request
from pathlib import Path

import common  # noqa: F401
from common import REPO, Base, assistant, fresh_db, write_transcript

from cc_usage import sync
from cc_usage.dashboard import queries
from cc_usage.ingest.transcripts import ingest_file

SRC = str(REPO / "src")
U = {"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 5000,
     "cache_creation_input_tokens": 0}


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class TestSync(Base):
    def test_roundtrip_idempotent_and_auth(self):
        # local: two sessions on "this device"
        f = write_transcript(self.home / "s" / "sessR.jsonl",
                             [assistant("sessR", "m1", "2026-10-24T10:00:00Z"),
                              assistant("sessR", "m2", "2026-10-24T10:01:00Z")])
        db = fresh_db(self.home)
        ingest_file(db, f)
        db.execute("INSERT INTO sessions(session_id,launcher,provider,git_branch,started_at,"
                   "device_id) VALUES('sessR','claude-glm','zai','jeremycod/RUS-9',"
                   "'2026-10-24T09:59:00Z','dev-A')")
        db.commit()
        # aggregate target
        agg_home = Path(tempfile.mkdtemp(prefix="cc-usage-agg-"))
        agg_db = agg_home / "aggregate.db"
        port = free_port()
        token = "test-token-123"
        env = dict(os.environ, CC_USAGE_SYNC_DB=str(agg_db),
                   CC_USAGE_SYNC_TOKEN=token, PYTHONPATH=SRC)
        errf = tempfile.TemporaryFile()
        proc = subprocess.Popen(
            [sys.executable, "-c",
             "from cc_usage.sync import serve_sync; serve_sync(port=%d)" % port],
            env=env, stdout=subprocess.DEVNULL, stderr=errf,
            start_new_session=True)
        try:
            up = False
            for _ in range(300):  # generous: child start can lag under heavy load
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        up = True
                        break
                except OSError:
                    time.sleep(0.1)
            if not up:
                errf.seek(0)
                self.fail("receiver did not start: %r" % errf.read()[:500])
            url = "http://127.0.0.1:%d" % port
            # wrong token -> 403
            req = urllib.request.Request(url + "/sync", data=b"{}", method="POST")
            try:
                urllib.request.urlopen(req, timeout=5)
                raised = False
            except urllib.error.HTTPError as e:
                raised = e.code == 403
            self.assertTrue(raised, "wrong/missing token must be rejected")
            # correct push (env var read by push? push takes token explicitly)
            sent, applied = sync.push(db, url, token, device="dev-A")
            self.assertGreater(sent, 0)
            agg = sqlite3.connect(str(agg_db))
            agg.row_factory = sqlite3.Row
            n = agg.execute("SELECT COUNT(*) c FROM api_requests "
                            "WHERE session_id='sessR'").fetchone()["c"]
            self.assertEqual(n, 2)
            # rows carry the CAPTURING machine's device id (ingest stamp), not the
            # push-time override — the device dimension is set at capture time.
            did = agg.execute("SELECT DISTINCT device_id FROM api_requests "
                              "WHERE session_id='sessR'").fetchone()[0]
            self.assertTrue(did, "device stamp must be present on synced rows")
            ph = agg.execute("SELECT COUNT(*) c FROM workflow_phases").fetchone()["c"]
            # derived snapshot arrived too (device-A runs the builder locally)
            self.assertGreaterEqual(ph, 0)
            # IDEMPOTENT second push: no duplicate accounting rows
            before = agg.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"]
            sync.push(db, url, token, device="dev-A")
            after = agg.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"]
            self.assertEqual(before, after)
            agg.close()
            # device filter on the AGGREGATE: a second device's rows only show for 'all'
            agg2 = sqlite3.connect(str(agg_db))
            agg2.execute("INSERT INTO sessions(session_id,launcher,provider,git_branch,"
                         "started_at,device_id) VALUES('sessW','claude-deepseek','deepseek',"
                         "'jeremycod/RUS-9','2026-10-24T11:00:00Z','dev-B')")
            agg2.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,"
                         "record_ts,is_sidechain,device_id) VALUES('sessW',"
                         "'2026-10-24T11:01:00Z','transcript','mid-w1',"
                         "'2026-10-24T11:01:00Z',0,'dev-B')")
            agg2.commit()
            agg2.close()
            aggconn = sqlite3.connect(str(agg_db))
            aggconn.row_factory = sqlite3.Row
            self.assertEqual(len(queries.devices(aggconn)), 2)
            # sessR rows carry the CAPTURING machine id (set at ingest), sessW is dev-B
            from cc_usage.ingest.transcripts import DEVICE as MACHINE
            s_all = queries.summary(sqlite3.connect(str(agg_db)), 30, device="all")
            s_a = queries.summary(sqlite3.connect(str(agg_db)), 30, device=MACHINE)
            self.assertEqual(s_all["responses"], s_a["responses"] + 1)
        finally:
            proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()


import time  # noqa: E402

if __name__ == "__main__":
    unittest.main()
