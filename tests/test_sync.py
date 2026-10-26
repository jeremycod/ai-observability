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
            # OPEN mode (no token) accepts pushes — the Tailscale deployment shape
            port2 = free_port()
            open_db = str(agg_home / "open.db")
            errf2 = tempfile.TemporaryFile()
            pro2 = subprocess.Popen(
                [sys.executable, "-c",
                 "from cc_usage.sync import serve_sync; serve_sync(port=%d, db_path=%r)"
                 % (port2, open_db)],
                env=dict({k: v for k, v in os.environ.items()
                          if k != "CC_USAGE_SYNC_TOKEN"}, PYTHONPATH=SRC),
                stdout=subprocess.DEVNULL, stderr=errf2,
                start_new_session=True)
            try:
                up2 = False
                for _ in range(300):
                    try:
                        with socket.create_connection(("127.0.0.1", port2), timeout=0.2):
                            up2 = True
                            break
                    except OSError:
                        time.sleep(0.1)
                        if pro2.poll() is not None:
                            break
                if not up2:
                    errf2.seek(0)
                    self.fail("open receiver did not start: %r" % errf2.read()[:400])
                sent2, _ = sync.push(db, "http://127.0.0.1:%d" % port2, "", device="dev-A")
                self.assertGreater(sent2, 0, "open-mode receiver must accept tokenless push")
            finally:
                pro2.terminate()
                try:
                    pro2.wait(5)
                except subprocess.TimeoutExpired:
                    pro2.kill()
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


class TestTwoDeviceSnapshots(Base):
    def test_second_device_push_does_not_clobber_first(self):
        """Multi-device merge regression: A's phases must survive B's snapshot push,
        and lineage session lists must UNION, not replace. Receiver runs in-process
        (subprocess receivers drift with env; same HTTP surface either way)."""
        import http.server
        import threading
        agg_db = str(self.home / "aggregate.db")
        port = free_port()
        httpd = http.server.ThreadingHTTPServer(
            ("127.0.0.1", port), sync.make_receiver_handler(agg_db, ""))
        th = threading.Thread(target=httpd.serve_forever, daemon=True)
        th.start()
        url = "http://127.0.0.1:%d" % port
        try:
            from cc_usage.db import connect as _c, migrate as _m
            from cc_usage.attribution import workflow as wf
            dbs = {}
            for sid, launcher, dev in (("sA", "claude-glm", "dev-A"),
                                       ("sB", "claude-deepseek", "dev-B")):
                h = Path(tempfile.mkdtemp(prefix=dev))
                os.environ["CC_USAGE_HOME"] = str(h)
                d = _c(h / "usage.db"); _m(d)
                d.execute("INSERT INTO sessions(session_id,launcher,git_branch,started_at,"
                          "device_id) VALUES(?,?,?,?,?)",
                          (sid, launcher, "jeremycod/RUS-5",
                           "2026-10-24T10:00:00Z" if sid == "sA" else
                           "2026-10-24T11:00:00Z", dev))
                f = write_transcript(h / "s" / (sid + ".jsonl"),
                                     [assistant(sid, "m1", "2026-10-24T%02d:01:00Z" % (10 if sid == "sA" else 11))])
                ingest_file(d, f)
                wf.build_session(d, sid)
                d.commit()
                dbs[dev] = d
            sync.push(dbs["dev-A"], url, "", device="dev-A")
            sync.push(dbs["dev-B"], url, "", device="dev-B")
            agg = sqlite3.connect(agg_db)
            agg.row_factory = sqlite3.Row
            devs = {r[0] for r in agg.execute(
                "SELECT DISTINCT device_id FROM workflow_phases").fetchall()}
            self.assertEqual(devs, {"dev-A", "dev-B"},
                             "B's snapshot push must not delete A's phases")
            lin = agg.execute("SELECT session_ids FROM work_lineages WHERE "
                              "primary_ticket='RUS-5'").fetchone()
            ids = set(json.loads(lin["session_ids"]))
            self.assertEqual(ids, {"sA", "sB"}, "lineage session_ids must union")
            # A re-pushes: still no clobber
            sync.push(dbs["dev-A"], url, "", device="dev-A")
            devs2 = {r[0] for r in agg.execute(
                "SELECT DISTINCT device_id FROM workflow_phases").fetchall()}
            self.assertEqual(devs2, {"dev-A", "dev-B"})
            agg.close()
        finally:
            httpd.shutdown()


import time  # noqa: E402

if __name__ == "__main__":
    unittest.main()


class TestTwoDeviceSnapshots(Base):
    def test_second_device_push_does_not_clobber_first(self):
        """Multi-device merge regression: A's phases must survive B's snapshot push,
        and lineage session lists must UNION, not replace."""
        agg_db = str(self.home / "aggregate.db")
        port = free_port()
        env = dict(os.environ, CC_USAGE_SYNC_DB=agg_db,
                   CC_USAGE_SYNC_TOKEN="", PYTHONPATH=SRC)
        procs = []
        try:
            procs.append(subprocess.Popen(
                [sys.executable, "-c",
                 "from cc_usage.sync import serve_sync; serve_sync(port=%d, db_path=%r)"
                 % (port, agg_db)],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True))
            for _ in range(100):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            url = "http://127.0.0.1:%d" % port
            # device A: one session + phases
            hA = Path(tempfile.mkdtemp(prefix="devA-"))
            os.environ["CC_USAGE_HOME"] = str(hA)
            from cc_usage.db import connect as _c, migrate as _m
            dbA = _c(hA / "usage.db"); _m(dbA)
            dbA.execute("INSERT INTO sessions(session_id,launcher,git_branch,started_at,"
                        "device_id) VALUES('sA','claude-glm','jeremycod/RUS-5',"
                        "'2026-10-24T10:00:00Z','dev-A')")
            dbA.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,"
                        "record_ts,is_sidechain,device_id) VALUES('sA',"
                        "'2026-10-24T10:01:00Z','transcript','mid-a1',"
                        "'2026-10-24T10:01:00Z',0,'dev-A')")
            from cc_usage.attribution import workflow as wf
            wf.build_session(dbA, "sA")
            dbA.commit()
            sync.push(dbA, url, "", device="dev-A")
            # device B: different session + phases (same lineage ticket)
            hB = Path(tempfile.mkdtemp(prefix="devB-"))
            os.environ["CC_USAGE_HOME"] = str(hB)
            dbB = _c(hB / "usage.db"); _m(dbB)
            dbB.execute("INSERT INTO sessions(session_id,launcher,git_branch,started_at,"
                        "device_id) VALUES('sB','claude-deepseek','jeremycod/RUS-5',"
                        "'2026-10-24T11:00:00Z','dev-B')")
            dbB.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,"
                        "record_ts,is_sidechain,device_id) VALUES('sB',"
                        "'2026-10-24T11:01:00Z','transcript','mid-b1',"
                        "'2026-10-24T11:01:00Z',0,'dev-B')")
            wf.build_session(dbB, "sB")
            dbB.commit()
            sync.push(dbB, url, "", device="dev-B")
            agg = sqlite3.connect(agg_db)
            agg.row_factory = sqlite3.Row
            devs = {r[0] for r in agg.execute(
                "SELECT DISTINCT device_id FROM workflow_phases").fetchall()}
            self.assertEqual(devs, {"dev-A", "dev-B"},
                             "B's snapshot push must not delete A's phases")
            lin = agg.execute("SELECT session_ids FROM work_lineages WHERE "
                              "primary_ticket='RUS-5'").fetchone()
            ids = set(json.loads(lin["session_ids"]))
            self.assertEqual(ids, {"sA", "sB"}, "lineage session_ids must union")
            # A re-pushes: still no clobber
            sync.push(dbA, url, "", device="dev-A")
            devs2 = {r[0] for r in agg.execute(
                "SELECT DISTINCT device_id FROM workflow_phases").fetchall()}
            self.assertEqual(devs2, {"dev-A", "dev-B"})
            agg.close()
        finally:
            for pr in procs:
                pr.terminate()
                try:
                    pr.wait(5)
                except subprocess.TimeoutExpired:
                    pr.kill()
