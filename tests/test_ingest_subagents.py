"""A5: subagent accounting — depth-0 files, meta enrichment, exactly-once rollup."""
import unittest

import common  # noqa: F401
from common import FIXTURES, Base, assistant, fresh_db, make_subagent_dir

from cc_usage.ingest.transcripts import ingest_file


class TestSubagents(Base):
    def test_depth0_real_fixture_ingests_as_sidechain(self):
        src = FIXTURES / "subagents" / "agent-depth0real.jsonl"
        db = fresh_db(self.home)
        ingest_file(db, src)
        rows = db.execute("SELECT * FROM api_requests WHERE is_sidechain=1").fetchall()
        self.assertGreater(len(rows), 0)
        first = rows[0]
        self.assertTrue(first["agent_id"])                       # from filename
        self.assertTrue(first["parent_session_id"])              # record sessionId
        # depth-0 file must still be treated as a subagent (spawnDepth is descriptive only)
        self.assertIsNotNone(first["agent_id"])

    def test_meta_enrichment_and_exactly_once_rollup(self):
        sid = "s-parent"
        recs = [assistant(sid, "m1", "2026-10-24T10:00:00Z", sidechain=True,
                          usage={"input_tokens": 10, "output_tokens": 1,
                                 "cache_read_input_tokens": 0,
                                 "cache_creation_input_tokens": 0}),
                assistant(sid, "m2", "2026-10-24T10:00:01Z", sidechain=True,
                          usage={"input_tokens": 20, "output_tokens": 2,
                                 "cache_read_input_tokens": 0,
                                 "cache_creation_input_tokens": 0})]
        meta = {"agentType": "builder", "description": "d", "name": "n",
                "toolUseId": "call_x", "spawnDepth": 0}
        f = make_subagent_dir(self.home, sid, "abc123", recs, meta)
        db = fresh_db(self.home)
        ingest_file(db, f)
        ingest_file(db, f)  # idempotent re-ingest
        tot = db.execute("SELECT * FROM v_subagent_totals "
                         "WHERE parent_session_id=?", (sid,)).fetchone()
        self.assertIsNotNone(tot)
        self.assertEqual(tot["responses"], 2)               # exactly once, not 4
        self.assertEqual(tot["input_tokens"], 30)
        self.assertEqual(tot["agent_type"], "builder")
        self.assertEqual(tot["agent_name"], "n")

    def test_orphan_without_meta_is_counted_state(self):
        sid = "s-orphan"
        recs = [assistant(sid, "m1", "2026-10-24T10:00:00Z", sidechain=True)]
        f = make_subagent_dir(self.home, sid, "noheartb", recs, meta=None)
        db = fresh_db(self.home)
        r = ingest_file(db, f)  # must not raise; tokens still counted (spec §6)
        self.assertIsNotNone(r)
        row = db.execute("SELECT * FROM api_requests WHERE is_sidechain=1").fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["agent_type"])


if __name__ == "__main__":
    unittest.main()
