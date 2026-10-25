"""A3/A4: request identity — rewrites, real conflicting ids, <synthetic>, cross-file."""
import json
import sqlite3
import unittest

import common  # noqa: F401
from common import FIXTURES, Base, assistant, fresh_db, write_transcript

from cc_usage.ingest.transcripts import ingest_file


def rows(db, sql, p=()):
    return db.execute(sql, p).fetchall()


class TestRewrites(Base):
    def test_identical_rewrites_collapse_to_one_response(self):
        src = FIXTURES / "transcripts" / "rewrite_group_real.jsonl"
        db = fresh_db(self.home)
        r = ingest_file(db, src)
        self.assertGreaterEqual(r["records"], 5)
        one = rows(db, "SELECT * FROM api_requests")
        self.assertEqual(len(one), 1)
        self.assertEqual(one[0]["occurrences"], 5)
        self.assertEqual(one[0]["identity_conflict"], 0)
        # idempotent re-ingest (A6)
        ingest_file(db, src)
        self.assertEqual(len(rows(db, "SELECT * FROM api_requests")), 1)
        occ = db.execute("SELECT COUNT(*) c FROM api_request_occurrences").fetchone()["c"]
        self.assertEqual(occ, 5)


class TestRealConflicts(Base):
    def test_real_conflicting_group_resolves_with_audit(self):
        src = FIXTURES / "transcripts" / "conflict_group_real.jsonl"
        db = fresh_db(self.home)
        ingest_file(db, src)
        one = rows(db, "SELECT * FROM api_requests")
        self.assertEqual(len(one), 1)
        self.assertEqual(one[0]["occurrences"], 3)
        self.assertEqual(one[0]["identity_conflict"], 1)
        # both losing occurrences audited; exactly one chosen
        conf = rows(db, "SELECT * FROM api_request_conflicts")
        self.assertEqual(len(conf), 2)
        self.assertEqual(sum(1 for c in conf if c["chosen"]), 1)
        # reconciliation guard consistent: flag set iff winner != max-usage occurrence
        total = (one[0]["input_tokens"] + one[0]["output_tokens"]
                 + one[0]["cache_read_tokens"] + one[0]["cache_creation_tokens"])
        mx = db.execute("SELECT MAX(input_tokens+output_tokens+cache_read_tokens"
                        "+cache_creation_tokens) m FROM api_request_occurrences").fetchone()["m"]
        expected_flag = None if total == mx else "order_usage_disagree"
        self.assertEqual(one[0]["reconciliation_flag"], expected_flag)


class TestSynthetic(Base):
    def test_synthetic_records_become_api_errors_never_responses(self):
        src = FIXTURES / "transcripts" / "synthetic_real.jsonl"
        db = fresh_db(self.home)
        ingest_file(db, src)
        self.assertEqual(len(rows(db, "SELECT * FROM api_requests")), 0)
        errs = rows(db, "SELECT * FROM api_errors")
        self.assertGreaterEqual(len(errs), 1)
        self.assertTrue(all(e["is_synthetic"] == 1 for e in errs))
        self.assertEqual(len(rows(db, "SELECT * FROM v_model_responses")), 0)


class TestCrossFile(Base):
    def test_same_id_across_two_files_is_one_row(self):
        sid = "s-cross"
        rec = assistant(sid, "msg_x", "2026-10-24T10:00:00Z")
        f1 = write_transcript(self.home / "t" / "a.jsonl", [rec, rec])
        f2 = write_transcript(self.home / "t" / "b.jsonl", [rec])
        db = fresh_db(self.home)
        ingest_file(db, f1)
        ingest_file(db, f2)
        one = rows(db, "SELECT * FROM api_requests")
        self.assertEqual(len(one), 1)
        self.assertEqual(one[0]["occurrences"], 3)

    def test_cross_file_winner_uses_timestamp_not_path_order(self):
        # file_a sorts before file_b but carries the LATER timestamp: it must win
        sid = "s-order"
        late = assistant(sid, "msg_o", "2026-10-24T11:00:00Z",
                         usage={"input_tokens": 111, "output_tokens": 1,
                                "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0})
        early = assistant(sid, "msg_o", "2026-10-24T09:00:00Z",
                          usage={"input_tokens": 222, "output_tokens": 2,
                                 "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0})
        fa = write_transcript(self.home / "o" / "a.jsonl", [late])
        fb = write_transcript(self.home / "o" / "b.jsonl", [early])
        db = fresh_db(self.home)
        ingest_file(db, fa)
        ingest_file(db, fb)
        one = rows(db, "SELECT * FROM api_requests")[0]
        self.assertEqual(one["input_tokens"], 111)  # later event ts wins, not later path
        self.assertEqual(one["identity_conflict"], 1)


class TestSurrogate(Base):
    def test_missing_message_id_gets_stable_surrogate(self):
        sid = "s-sur"
        rec = assistant(sid, None, "2026-10-24T10:00:00Z")
        f = write_transcript(self.home / "s" / "x.jsonl", [rec])
        db = fresh_db(self.home)
        ingest_file(db, f)
        one = rows(db, "SELECT * FROM api_requests")[0]
        self.assertTrue(one["identity_key"].startswith("sur:"))
        ingest_file(db, f)  # re-ingest must not duplicate
        self.assertEqual(len(rows(db, "SELECT * FROM api_requests")), 1)


if __name__ == "__main__":
    unittest.main()
