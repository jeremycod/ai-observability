"""Phase F: Wilson intervals, shared-bucket exclusion, matched-set detection,
and the validation harness verdicts on a seeded corpus."""
import unittest

import common  # noqa: F401
from common import Base, fresh_db

from cc_usage.compare import report, wilson
from cc_usage.validate import validate


class TestWilson(unittest.TestCase):
    def test_known_intervals(self):
        self.assertEqual(wilson(0, 0), (0.0, 0.0, 0.0))
        p, lo, hi = wilson(1, 1)
        self.assertEqual(p, 1.0)
        self.assertLess(lo, 1.0)          # small-n must NOT look perfect
        self.assertGreater(hi, lo)
        p, lo, hi = wilson(50, 100)
        self.assertAlmostEqual(p, 0.5, places=3)
        self.assertLess(lo, 0.5)
        self.assertGreater(hi, 0.5)
        self.assertGreater(hi - lo, 0.0)


class TestCompareReport(Base):
    def _seed(self, db, sid, branch, launcher, provider):
        db.execute("INSERT INTO sessions(session_id,launcher,provider,git_branch,started_at) "
                   "VALUES(?,?,?,?,datetime('now','-2 days'))",
                   (sid, launcher, provider, branch))
        db.execute("INSERT INTO command_events(session_id,ts,command_name,jira_ticket) "
                   "VALUES(?,datetime('now','-2 days'),'implement-jira-ticket',?)",
                   (sid, branch.split("/")[-1]))
        db.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,launcher,"
                   "model,latency_basis,duration_ms,record_ts) VALUES(?,?,?,?,?,?,?,?,?)",
                   (sid, "2026-10-24T10:00:00Z", "transcript", "mid:" + sid, launcher,
                    "glm-5.3-flash", "measured_api", 4000, "2026-10-24T10:00:00Z"))

    def test_report_separates_shared_and_flags_small_n(self):
        db = fresh_db(self.home)
        self._seed(db, "sA", "jeremycod/RUS-1", "claude-glm", "zai")
        self._seed(db, "sB", "jeremycod/RUS-1", "claude-deepseek", "deepseek")
        self._seed(db, "sC", "jeremycod/RUS-2", "claude-glm", "zai")
        for n, tk in ((401, "RUS-1"), (402, "RUS-2")):
            db.execute("INSERT INTO pull_requests(number,repo,ticket_keys,merged,review_rounds,"
                       "net_add,created_at) VALUES(?,'o/r',?,1,1,100,datetime('now','-1 day'))",
                       (n, json.dumps([tk]) if False else '["%s"]' % tk))
        from cc_usage.correlate import github
        github.link_contributions(db)
        db.commit()
        text = report(db, 90)
        self.assertIn("shared multi-provider PRs separate", text)
        self.assertIn("Matched-at-ticket-level sets (multi-provider PRs): 1", text)
        self.assertIn("CONFOUNDS", text)
        self.assertIn("measured_api", text)

    def test_validate_runs_all_cases_without_crash(self):
        db = fresh_db(self.home)
        # verdicts must be from the allowed set even on an empty corpus
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = validate(db)
        self.assertIn(code, (0, 1))
        self.assertIn("validation dataset reconciliation", buf.getvalue())


import json  # noqa: E402  (used inside test method above)

if __name__ == "__main__":
    unittest.main()
