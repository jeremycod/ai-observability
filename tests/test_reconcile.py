"""A12 (fixture-level): independent reconciliation must produce EXACT totals."""
import io
import contextlib
import unittest

import common  # noqa: F401
from common import Base, assistant, fresh_db, make_subagent_dir, write_transcript

from cc_usage.ingest.transcripts import ingest_file
from cc_usage.reconcile import reconcile_session

U = {"input_tokens": 100, "output_tokens": 10, "cache_read_input_tokens": 900,
     "cache_creation_input_tokens": 0}


class TestReconcile(Base):
    def test_exact_for_complete_records(self):
        sid = "s-rec"
        recs = [assistant(sid, "m1", "2026-10-24T10:00:00Z", usage=U),
                assistant(sid, "m1", "2026-10-24T10:00:00Z", usage=U),   # rewrite
                assistant(sid, "m2", "2026-10-24T10:00:05Z", usage=U),
                assistant(sid, "m3", "2026-10-24T10:00:09Z", model="<synthetic>",
                          usage={"input_tokens": 0, "output_tokens": 0,
                                 "cache_read_input_tokens": None,
                                 "cache_creation_input_tokens": None},
                          extra={"isApiErrorMessage": True})]
        f = write_transcript(self.home / "r" / "s.jsonl", recs)
        db = fresh_db(self.home)
        ingest_file(db, f)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = reconcile_session(db, sid)
        self.assertEqual(code, 0, out.getvalue())
        self.assertIn("EXACT", out.getvalue())

    def test_sidechain_bucket_reconciles(self):
        sid = "s-rec2"
        f = write_transcript(self.home / "r2" / "s.jsonl",
                             [assistant(sid, "m1", "2026-10-24T10:00:00Z", usage=U)])
        sub = make_subagent_dir(self.home, sid, "aa11", [
            assistant(sid, "c1", "2026-10-24T10:00:02Z", sidechain=True,
                      usage={"input_tokens": 50, "output_tokens": 5,
                             "cache_read_input_tokens": 45, "cache_creation_input_tokens": 0}),
            assistant(sid, "c1", "2026-10-24T10:00:02Z", sidechain=True,
                      usage={"input_tokens": 50, "output_tokens": 5,
                             "cache_read_input_tokens": 45, "cache_creation_input_tokens": 0})],
            meta={"agentType": "Explore", "spawnDepth": 1})
        db = fresh_db(self.home)
        ingest_file(db, f)
        ingest_file(db, sub)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = reconcile_session(db, sid)
        self.assertEqual(code, 0, out.getvalue())
        self.assertIn("sidechain", out.getvalue())


if __name__ == "__main__":
    unittest.main()
