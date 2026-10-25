"""A7/A8: hook spool enqueue (non-blocking, redacted), legacy-semantics parity,
bounded OTLP spool with visible loss accounting + replay."""
import json
import os
import threading
import time
import unittest

import common  # noqa: F401
from common import Base, fresh_db

from cc_usage import collector
from cc_usage.db import connect
from cc_usage.hookspool import commit_state, enqueue, events_path, read_events


def rebind_collector_paths(home):
    """Collector binds its paths at import; rebind them to this test's home."""
    collector.DATA_DIR = home
    collector.OTLP_SPOOL = home / "spool" / "otel"
    collector.OTLP_DONE = home / "spool" / "otel" / "done"


class TestHookSpool(Base):
    def setUp(self):
        super().setUp()
        rebind_collector_paths(self.home)

    def test_enqueue_fast_and_redacted(self):
        os.environ["ANTHROPIC_AUTH_TOKEN"] = "sk-secret-value-123"
        try:
            t0 = time.perf_counter()
            for i in range(100):
                self.assertTrue(enqueue(self.home, {
                    "hook_event_name": "UserPromptSubmit", "session_id": "s%d" % i,
                    "prompt_id": "p%d" % i, "prompt": "hello"}))
            per_ms = (time.perf_counter() - t0) * 1000 / 100.0
            self.assertLess(per_ms, 50.0, "hook enqueue must stay far under 50 ms")
        finally:
            os.environ.pop("ANTHROPIC_AUTH_TOKEN", None)
        path, _ = events_path(self.home)
        lines = open(path, encoding="utf-8").read().splitlines()
        self.assertEqual(len(lines), 100)
        blob = "\n".join(lines)
        self.assertNotIn("sk-secret-value-123", blob)
        snap = json.loads(lines[0])["env_snapshot"]
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", snap)

    def test_concurrent_enqueue_no_loss(self):
        N = 20
        def worker(k):
            for i in range(N):
                enqueue(self.home, {"hook_event_name": "UserPromptSubmit",
                                    "session_id": "s-%d-%d" % (k, i)})
        threads = [threading.Thread(target=worker, args=(k,)) for k in range(7)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        path, _ = events_path(self.home)
        self.assertEqual(len(open(path, encoding="utf-8").read().splitlines()), 7 * N)

    def test_state_offset_advance(self):
        enqueue(self.home, {"hook_event_name": "SessionStart", "session_id": "s1"})
        state, payloads = read_events(self.home)
        self.assertEqual(len(payloads), 1)
        commit_state(state)
        _state2, payloads2 = read_events(self.home)
        self.assertEqual(payloads2, [])


class TestHookApplyParity(Base):
    """Legacy v1 handle_hook semantics, preserved exactly by the collector."""

    def _apply(self, payload):
        db = fresh_db(self.home)
        collector.apply_hook_event(db, payload)
        db.commit()
        return db

    def test_session_start_upsert(self):
        p = {"hook_event_name": "SessionStart", "session_id": "sA", "cwd": "/tmp",
             "source": "startup", "captured_at": "2026-10-24T10:00:00-07:00",
             "env_snapshot": {"CC_LAUNCHER": "claude-glm", "CC_PROVIDER": "zai",
                              "ANTHROPIC_MODEL": "glm-5.3-flash[1m]", "gateway": "zai",
                              "base_url": "api.z.ai/api/anthropic",
                              "ANTHROPIC_BASE_URL": "https://api.z.ai/api/anthropic"}}
        db = self._apply(p)
        row = db.execute("SELECT * FROM sessions WHERE session_id='sA'").fetchone()
        self.assertEqual(row["launcher"], "claude-glm")
        self.assertEqual(row["provider"], "zai")
        snap = db.execute("SELECT * FROM config_snapshots WHERE session_id='sA'").fetchone()
        self.assertEqual(snap["gateway"], "zai")
        self.assertEqual(snap["model_requested"], "glm-5.3-flash[1m]")

    def test_prompt_expansion_records_command_and_jira(self):
        p = {"hook_event_name": "UserPromptExpansion", "session_id": "sB",
             "prompt_id": "p1", "command_name": "implement-jira-ticket",
             "command_args": "RUS-6225", "prompt": "/implement-jira-ticket RUS-6225",
             "cwd": "/tmp", "captured_at": "2026-10-24T10:01:00-07:00", "env_snapshot": {}}
        db = self._apply(p)
        ce = db.execute("SELECT * FROM command_events").fetchone()
        self.assertEqual(ce["command_name"], "implement-jira-ticket")
        self.assertEqual(ce["jira_ticket"], "RUS-6225")
        pc = db.execute("SELECT * FROM prompt_context").fetchone()
        self.assertEqual(pc["jira_ticket"], "RUS-6225")

    def test_session_end(self):
        self._apply({"hook_event_name": "SessionStart", "session_id": "sC", "cwd": "/tmp",
                     "env_snapshot": {}})
        self._apply({"hook_event_name": "SessionEnd", "session_id": "sC", "cwd": "/tmp",
                     "captured_at": "2026-10-24T11:00:00-07:00", "env_snapshot": {}})
        db = fresh_db(self.home)
        row = db.execute("SELECT ended_at FROM sessions WHERE session_id='sC'").fetchone()
        self.assertIsNotNone(row["ended_at"])


class TestOtel(Base):
    def setUp(self):
        super().setUp()
        rebind_collector_paths(self.home)

    def test_otel_rows_never_touch_api_requests(self):
        from cc_usage.ingest.otel import insert_rows, normalize
        payload = {"resourceLogs": [{"resource": {"attributes": []}, "scopeLogs": [{
            "scope": {"name": "t"}, "logRecords": [{
                "timeUnixNano": "1761370000000000000", "severityText": "INFO",
                "body": {"stringValue": "api_request"},
                "attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
                    {"key": "session.id", "value": {"stringValue": "s1"}},
                    {"key": "model", "value": {"stringValue": "glm-5.3-flash"}},
                    {"key": "input_tokens", "value": {"intValue": "7"}},
                    {"key": "output_tokens", "value": {"intValue": "8"}},
                    {"key": "cache_read_tokens", "value": {"intValue": "9"}},
                    {"key": "cache_creation_tokens", "value": {"intValue": "10"}},
                    {"key": "duration_ms", "value": {"intValue": "120"}}]}]}]}]}
        db = fresh_db(self.home)
        rows = normalize(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "api_request")
        self.assertEqual(rows[0]["duration_ms"], 120)  # measured_api duration only from OTLP
        insert_rows(db, rows)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM api_requests").fetchone()["c"], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM otel_api_requests").fetchone()["c"], 1)


class TestOtelSpool(Base):
    def setUp(self):
        super().setUp()
        rebind_collector_paths(self.home)
        collector.OTLP_SPOOL.mkdir(parents=True, exist_ok=True)

    def _writer(self):
        from cc_usage.collector import Writer
        w = Writer(self.home / "usage.db")
        w.start()
        w.ready.wait(5)
        return w

    def test_overflow_records_loss_never_deletes(self):
        w = self._writer()
        keep = collector.OTLP_SPOOL / "p-keep.json"
        keep.write_bytes(b'{"resourceLogs":[]}')  # existing spooled data
        old = collector.spool_pressure
        collector.spool_pressure = lambda: collector.SPOOL_MAX_BYTES + 1  # force overflow
        try:
            self.assertFalse(collector.enqueue_otel(w, b'{}', {"seq": 1}))
        finally:
            collector.spool_pressure = old
        time.sleep(0.3)
        db = connect(self.home / "usage.db")
        loss = db.execute("SELECT * FROM telemetry_loss WHERE reason='spool_full'").fetchone()
        self.assertIsNotNone(loss)
        self.assertGreaterEqual(loss["dropped"], 1)
        self.assertIsNotNone(loss["first_ts"])
        self.assertIsNotNone(loss["last_ts"])
        self.assertTrue(keep.exists())  # nothing spooled was ever deleted
        db.close()

    def test_replay_inserts_and_marks_recovered(self):
        w = self._writer()
        raw = json.dumps({"resourceLogs": [{"resource": {"attributes": []}, "scopeLogs": [{
            "scope": {"name": "t"}, "logRecords": [{
                "timeUnixNano": "1761370000000000000", "severityText": "INFO",
                "body": {"stringValue": "api_request"},
                "attributes": [
                    {"key": "event.name", "value": {"stringValue": "claude_code.api_request"}},
                    {"key": "session.id", "value": {"stringValue": "replay-1"}},
                    {"key": "model", "value": {"stringValue": "m"}},
                    {"key": "input_tokens", "value": {"intValue": "1"}}]}]}]}]}).encode()
        (collector.OTLP_SPOOL / "p-replay.json").write_bytes(raw)

        def seed(db):
            db.execute("INSERT INTO telemetry_loss(first_ts,last_ts,dropped,reason) "
                       "VALUES('t0','t1',3,'spool_full')")
        w.submit(seed)
        time.sleep(0.2)
        collector.replay_otel(w)
        time.sleep(0.6)
        db = connect(self.home / "usage.db")
        n = db.execute("SELECT COUNT(*) c FROM otel_api_requests "
                       "WHERE session_id='replay-1'").fetchone()["c"]
        self.assertEqual(n, 1)
        open_loss = db.execute("SELECT COUNT(*) c FROM telemetry_loss "
                               "WHERE recovered_at IS NULL").fetchone()["c"]
        self.assertEqual(open_loss, 0)
        self.assertTrue((collector.OTLP_DONE / "p-replay.json").exists())  # moved, not deleted
        db.close()


if __name__ == "__main__":
    unittest.main()
