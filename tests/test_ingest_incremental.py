"""A6: incremental ingestion — offset resume, appends, re-ingest = 0 new rows."""
import json
import unittest

import common  # noqa: F401
from common import Base, assistant, fresh_db, write_transcript

from cc_usage.ingest.transcripts import ingest_file


class TestIncremental(Base):
    def test_append_then_resume(self):
        f = write_transcript(self.home / "inc" / "s.jsonl",
                             [assistant("s1", "m1", "2026-10-24T10:00:00Z")])
        db = fresh_db(self.home)
        r1 = ingest_file(db, f)
        self.assertEqual(r1["records"], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"], 1)
        # append two records: one new response, one exact rewrite of m1
        with open(f, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(assistant("s1", "m2", "2026-10-24T10:00:05Z")) + "\n")
            fh.write(json.dumps(assistant("s1", "m1", "2026-10-24T10:00:00Z")) + "\n")
        r2 = ingest_file(db, f)  # resumes from stored offset
        self.assertEqual(r2["records"], 2)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"], 2)
        occ = db.execute("SELECT occurrences FROM api_requests WHERE identity_key='mid:m1'").fetchone()
        self.assertEqual(occ["occurrences"], 2)
        # full re-ingest at offset 0 (simulating state loss) must add nothing new
        db.execute("DELETE FROM ingest_state WHERE source_file=?", (f,))
        db.commit()
        ingest_file(db, f)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"], 2)
        self.assertEqual(db.execute("SELECT occurrences FROM api_requests "
                                    "WHERE identity_key='mid:m1'").fetchone()["occurrences"], 2)

    def test_event_time_window(self):
        f = write_transcript(self.home / "w" / "s.jsonl",
                             [assistant("s2", "m1", "2026-10-01T10:00:00Z"),
                              assistant("s2", "m2", "2026-10-24T10:00:00Z")])
        db = fresh_db(self.home)
        ingest_file(db, f, ts_window=("2026-10-20T00:00:00Z", None))
        models = [r["model"] for r in db.execute("SELECT model FROM api_requests").fetchall()]
        self.assertEqual(models, ["glm-5.3-flash"])  # only the in-window record
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_request_occurrences")
                         .fetchone()["c"], 1)

    def test_truncated_file_restarts_cleanly(self):
        f = write_transcript(self.home / "tr" / "s.jsonl",
                             [assistant("s3", "m1", "2026-10-24T10:00:00Z")])
        db = fresh_db(self.home)
        ingest_file(db, f)
        with open(f, "w", encoding="utf-8") as fh:  # truncate/rewrite with the same record
            fh.write(json.dumps(assistant("s3", "m1", "2026-10-24T10:00:00Z")) + "\n")
        ingest_file(db, f)
        row = db.execute("SELECT occurrences FROM api_requests").fetchone()
        self.assertEqual(row["occurrences"], 1)  # occurrence log keeps it idempotent

    def test_backfill_window_uses_separate_offsets(self):
        """Critic P1-1 regression: a windowed backfill must never poison live offsets."""
        f = write_transcript(self.home / "bf" / "s.jsonl",
                             [assistant("s5", "m1", "2026-10-01T10:00:00Z"),
                              assistant("s5", "m2", "2026-10-24T10:00:00Z")])
        db = fresh_db(self.home)
        ingest_file(db, f, ts_window=("2026-10-20T00:00:00Z", None), backfill=True)
        # live ingest_state untouched
        self.assertIsNone(db.execute("SELECT byte_offset FROM ingest_state "
                                     "WHERE source_file=?", (f,)).fetchone())
        # a later live sweep still sees the out-of-window record
        ingest_file(db, f)
        n = db.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"]
        self.assertEqual(n, 2)

    def test_null_timestamp_in_conflict_never_crashes(self):
        """Critic P0-2 regression: type-mixed ordering must not stall ingest."""
        sid = "s6"
        recs = [assistant(sid, "mX", "2026-10-24T10:00:00Z",
                          usage={"input_tokens": 5, "output_tokens": 1,
                                 "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}),
                assistant(sid, "mX", None,
                          usage={"input_tokens": 7, "output_tokens": 2,
                                 "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0})]
        f = write_transcript(self.home / "nt" / "s.jsonl", recs)
        db = fresh_db(self.home)
        r = ingest_file(db, f)  # must not raise
        self.assertEqual(r["records"], 2)
        row = db.execute("SELECT * FROM api_requests").fetchone()
        self.assertEqual(row["identity_conflict"], 1)
        # the file must NOT be stuck: re-ingest advances cleanly (idempotent)
        db.execute("DELETE FROM ingest_state WHERE source_file=?", (f,))
        db.commit()
        ingest_file(db, f)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_request_occurrences")
                         .fetchone()["c"], 2)


if __name__ == "__main__":
    unittest.main()
