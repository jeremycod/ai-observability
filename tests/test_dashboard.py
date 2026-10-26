"""Phase E: dashboard queries against a seeded read-only DB + HTTP smoke test."""
import json
import os
import sqlite3
import tempfile
import unittest
import urllib.request
from pathlib import Path

import common  # noqa: F401
from common import Base, assistant, fresh_db, write_transcript

from cc_usage.dashboard import queries, server
from cc_usage.ingest.transcripts import ingest_file
from cc_usage.attribution import workflow

U = {"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 5000,
     "cache_creation_input_tokens": 0}


def seed_session(db, home, sid, branch, launcher, provider, t0, t1, responses):
    db.execute("INSERT OR REPLACE INTO sessions(session_id,launcher,provider,model,cwd,"
               "git_branch,started_at,ended_at,source) VALUES(?,?,?,?,?,?,?,?,?)",
               (sid, launcher, provider, "glm-5.3-flash", str(home), branch, t0, t1, "startup"))
    recs = []
    for i, ts in enumerate(responses):
        recs.append(assistant(sid, "mid-%s-%d" % (sid[:6], i), ts, usage=U))
    return write_transcript(home / "d" / (sid + ".jsonl"), recs)


class TestDashboardQueries(Base):
    def setUp(self):
        super().setUp()
        self.db = fresh_db(self.home)
        f1 = seed_session(self.db, self.home, "sessD1", "jeremycod/RUS-800",
                          "claude-glm", "zai", "2026-10-24T10:00:00Z",
                          "2026-10-24T11:00:00Z",
                          ["2026-10-24T10:05:00Z", "2026-10-24T10:20:00Z"])
        f2 = seed_session(self.db, self.home, "sessD2", "jeremycod/RUS-800",
                          "claude-deepseek", "deepseek", "2026-10-24T10:10:00Z",
                          "2026-10-24T10:40:00Z", ["2026-10-24T10:15:00Z"])
        for f in (f1, f2):
            ingest_file(self.db, f)
        # workflow.build groups both sessions into lineage ticket:RUS-800 (shared branch)
        workflow.build(self.db, ["sessD1", "sessD2"])
        self.db.commit()

    def test_summary_and_coverage(self):
        s = queries.summary(self.db, 30)
        self.assertEqual(s["responses"], 3)
        self.assertEqual(s["sessions"], 2)
        self.assertEqual(s["coverage"]["phase_attribution"], 100.0)
        self.assertGreaterEqual(s["coverage"]["provider_identity"], 100.0)
        self.assertIn("basis", s)

    def test_providers_shared_bucket(self):
        # one PR for RUS-800 with both providers contributing -> bucket 'shared'
        self.db.execute("INSERT INTO pull_requests(number,repo,title,branch,base_branch,"
                        "state,merged,created_at,ticket_keys) VALUES(501,'o/r','[RUS-800] x',"
                        "'jeremycod/RUS-800','main','MERGED',1,'2026-10-24T12:00:00Z',"
                        "'[\"RUS-800\"]')")
        self.db.commit()
        from cc_usage.correlate import github
        github.link_contributions(self.db)
        rows = queries.providers(self.db, 30)
        tok = [r for r in rows if r.get("kind") is None]
        self.assertEqual({r["provider"] for r in tok}, {"zai", "deepseek"})
        pr_rows = [r for r in rows if r.get("kind") == "pr_outcome"]
        self.assertEqual(len(pr_rows), 1)
        self.assertEqual(pr_rows[0]["provider"], "shared")  # never double-counted

    def test_phases_filter_by_ticket(self):
        ph = queries.phases(self.db, 30, ticket="RUS-800")
        self.assertTrue(all(p["ticket"] == "RUS-800" for p in ph))
        self.assertTrue(all("timeline" in p["basis"] for p in ph))

    def test_session_timeline_has_no_text(self):
        d = queries.session_timeline(self.db, "sessD1")
        # command-less fixture: gap + cluster + tail spans, all honestly unclassified
        self.assertGreaterEqual(len(d["phases"]), 1)
        self.assertTrue(all(p["phase"] == "unclassified" for p in d["phases"]))
        blob = json.dumps(d)
        self.assertNotIn('"content"', blob)
        self.assertIn("basis", d)

    def test_concurrency_buckets(self):
        c = queries.concurrency(self.db, 30)
        # sessD1+sessD2 overlap 10:10-10:40 -> at least one hour in bucket 1-2
        self.assertGreaterEqual(c["buckets"]["1-2"]["hours"], 1)
        self.assertIn("basis", c)

    def test_quota_isolated_from_accounting(self):
        self.db.execute("INSERT INTO quota_windows(provider,kind,window_start,window_end,"
                        "consumed_pct_or_units,source,imported_at) VALUES('zai','5h',"
                        "'2026-10-24T02:08:00Z','2026-10-24T03:36:00Z',100,'snapshot','now')")
        self.db.commit()
        q = queries.quota(self.db)
        self.assertEqual(len(q["windows"]), 1)
        self.assertIn("SHARED", q["caption"])
        blob = json.dumps(q)
        self.assertNotIn("sessD1", blob)  # quota payload never mixes in session accounting


class TestDashboardHTTP(Base):
    def test_smoke_over_http(self):
        tmp = Path(tempfile.mkdtemp(prefix="cc-usage-dash-"))
        # copy the seeded DB to an isolated file for the server
        src = Path(os.environ["CC_USAGE_HOME"]) / "usage.db"
        if not src.exists():
            self.skipTest("no seeded db")
        dst = tmp / "usage.db"
        s = sqlite3.connect(str(src))
        d = sqlite3.connect(str(dst))
        s.backup(d)
        s.close()
        d.close()
        import threading
        import time as _t
        import urllib.error
        port = 4399 + (os.getpid() % 200)
        httpd = None
        try:
            httpd = __import__("http.server", fromlist=["ThreadingHTTPServer"]).ThreadingHTTPServer(
                ("127.0.0.1", port), server.make_handler(str(dst)))
            th = threading.Thread(target=httpd.serve_forever, daemon=True)
            th.start()
            _t.sleep(0.3)
            with urllib.request.urlopen(
                    "http://127.0.0.1:%d/api/summary?days=30" % port, timeout=5) as r:
                data = json.loads(r.read())
                self.assertIn("coverage", data)
            with urllib.request.urlopen(
                    "http://127.0.0.1:%d/" % port, timeout=5) as r:
                self.assertIn(b"ai-observability", r.read())
        finally:
            if httpd:
                httpd.shutdown()


if __name__ == "__main__":
    unittest.main()
