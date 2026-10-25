"""Phase D: quota windows (shared-window rule, snapshot import, quota-code extraction)
and the four-measure cost model (per-provider availability, off-peak, amortization)."""
import datetime as dt
import json
import os
import unittest

import common  # noqa: F401
from common import Base, fresh_db

from cc_usage import cost, quota


class TestOffPeak(unittest.TestCase):
    def test_window_boundaries(self):
        def at(h, m):
            return dt.datetime(2026, 10, 24, h, m, tzinfo=dt.timezone.utc).timestamp()
        self.assertEqual(cost.offpeak_factor(at(17, 0)), 0.5)   # inside window
        self.assertEqual(cost.offpeak_factor(at(23, 59)), 0.5)
        self.assertEqual(cost.offpeak_factor(at(0, 0)), 0.5)    # wraps midnight
        self.assertEqual(cost.offpeak_factor(at(0, 30)), 0.5)   # inclusive end
        self.assertEqual(cost.offpeak_factor(at(0, 31)), 1.0)
        self.assertEqual(cost.offpeak_factor(at(12, 0)), 1.0)
        self.assertEqual(cost.offpeak_factor(at(16, 29)), 1.0)
        self.assertEqual(cost.offpeak_factor(at(16, 30)), 0.5)


class TestCostEstimate(Base):
    def test_zai_never_priced_deepseek_priced(self):
        db = fresh_db(self.home)
        t = dt.datetime(2026, 10, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        rows = [
            ("zai", "glm-5.3-flash", 1_000_000, 100_000, 5_000_000, 0, t),
            ("deepseek", "deepseek-flash", 1_000_000, 100_000, 5_000_000, 0, t),
            ("unknown-provider", "mystery", 1, 1, 0, 0, t),
        ]
        est, priced, unpriced = cost.estimate_provider_usd(db, rows)
        # deepseek at peak: 1M*0.15 + 5M*0.003 + 0.1M*0.60 = 0.15+0.015+0.06 = $0.225
        self.assertAlmostEqual(est, 0.225, places=3)
        self.assertEqual(priced, 1)
        self.assertEqual(unpriced, 1)  # unknown provider counted, not guessed

    def test_offpeak_halves_deepseek(self):
        db = fresh_db(self.home)
        peak = dt.datetime(2026, 10, 24, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        off = dt.datetime(2026, 10, 24, 20, 0, tzinfo=dt.timezone.utc).timestamp()
        e1, _, _ = cost.estimate_provider_usd(
            db, [("deepseek", "deepseek-flash", 1_000_000, 0, 0, 0, peak)])
        e2, _, _ = cost.estimate_provider_usd(
            db, [("deepseek", "deepseek-flash", 1_000_000, 0, 0, 0, off)])
        self.assertAlmostEqual(e2, e1 * 0.5, places=6)

    def test_amortization_requires_invoice_and_stays_period_grain(self):
        db = fresh_db(self.home)
        amt, inv, method = cost.amortized_usd(db, {"zai": 100}, invoice=None)
        self.assertIsNone(amt)  # no invoice on file: never invent one
        amt, inv, method = cost.amortized_usd(db, {"zai": 100}, invoice=18)
        self.assertEqual(amt, 18.0)
        self.assertIn("period grain", method)
        self.assertNotIn("ticket", method)


class TestQuota(Base):
    def test_import_json_and_kv(self):
        db = fresh_db(self.home)
        j = self.home / "q.json"
        j.write_text(json.dumps([
            {"kind": "5h", "consumed_pct": 87, "window_start": "2026-10-24T02:08:00Z",
             "reset_at": "2026-10-24T03:36:00Z", "captured_at": "2026-10-24T02:30:00Z"},
            {"kind": "weekly", "consumed_pct": 100, "window_start": "2026-10-21T06:08:00Z",
             "reset_at": "2026-10-23T02:43:00Z"}]), encoding="utf-8")
        quota.import_snapshot(db, str(j))
        kv = self.home / "q.txt"
        kv.write_text("kind=5h\nconsumed_pct=40\nwindow_start=2026-10-24T18:00:00Z\n",
                      encoding="utf-8")
        quota.import_snapshot(db, str(kv))
        rows = db.execute("SELECT kind, consumed_pct_or_units, source, captured_at "
                          "FROM quota_windows ORDER BY imported_at").fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[2]["source"], "snapshot")
        self.assertIsNotNone(rows[0]["captured_at"])  # capture time kept separate

    def test_quota_code_extraction_no_raw_text(self):
        db = fresh_db(self.home)
        code = quota.extract_quota_event(
            db, "s1", "Error 1310: weekly limit exhausted, resets 2026-10-23T02:43:00",
            "2026-10-21T06:08:00Z")
        self.assertEqual(code, "1310")
        ev = db.execute("SELECT * FROM provider_events").fetchone()
        self.assertEqual(ev["kind"], "quota_weekly_exhausted")
        self.assertIn("2026-10-23", ev["reset_at"] or "")
        self.assertIsNone(ev["detail"])  # raw error text never stored
        self.assertIsNone(quota.extract_quota_event(db, "s1", "plain failure", "t"))

    def test_shared_window_caption_counts_active_sessions(self):
        db = fresh_db(self.home)
        w0 = dt.datetime(2026, 10, 24, 18, 0, tzinfo=dt.timezone.utc)
        for i, (sh, eh) in enumerate([("17:30", "19:00"), ("18:30", "20:00"), ("23:30", "23:45")]):
            s = (w0 + dt.timedelta(hours=int(sh[:2]) - 18, minutes=int(sh[3:]))).isoformat()
            e = (w0 + dt.timedelta(hours=int(eh[:2]) - 18, minutes=int(eh[3:]))).isoformat()
            db.execute("INSERT INTO sessions(session_id,started_at,ended_at) VALUES(?,?,?)",
                       ("s%d" % i, s, e))
        db.commit()
        act = quota.active_sessions(db, w0.timestamp(), (w0 + dt.timedelta(hours=5)).timestamp())
        self.assertEqual(len(act), 2)  # third session is outside the window

    def test_no_per_ticket_quota_view_exists(self):
        """Structural guarantee (spec §8): no view joins quota_windows to accounting —
        per-ticket quota attribution is impossible by construction."""
        db = fresh_db(self.home)
        views = db.execute("SELECT name, sql FROM sqlite_master WHERE type='view'").fetchall()
        for v in views:
            self.assertNotIn("quota_windows", (v["sql"] or ""),
                             "view %s joins quota to accounting" % v["name"])


if __name__ == "__main__":
    unittest.main()
