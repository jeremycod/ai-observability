"""Phase B: workflow phases, prompt-chain linkage, lineage stitching, subagent
dispatch attribution, and OTLP measured-duration join."""
import json
import os
import pathlib
import unittest

import common  # noqa: F401
from common import Base, assistant, make_subagent_dir, write_transcript

from cc_usage.attribution import subagents, workflow
from cc_usage.db import connect, migrate
from cc_usage.ingest.transcripts import ingest_file

U = {"input_tokens": 100, "output_tokens": 10, "cache_read_input_tokens": 900,
     "cache_creation_input_tokens": 0}


def user_rec(uuid, parent, ts, prompt_id=None):
    return {"type": "user", "uuid": uuid, "parentUuid": parent, "sessionId": None,
            "timestamp": ts, "isSidechain": False, "promptId": prompt_id,
            "userType": "external"}


def chain_session(sid, prompts):
    """Transcript with a parentUuid chain: each prompt (user record) followed by its
    assistant responses; the next prompt chains off the last response."""
    recs = []
    prev = "root"
    for (u_uuid, u_parent, ts, pid, responses) in prompts:
        recs.append(dict(user_rec(u_uuid, u_parent, ts, pid), sessionId=sid))
        prev = u_uuid
        for (a_uuid, a_ts, usage) in responses:
            recs.append(dict(assistant(sid, "mid-" + a_uuid, a_ts, usage=usage),
                             uuid=a_uuid, parentUuid=prev))
            prev = a_uuid
    return recs


class PhaseBase(Base):
    def db(self):
        db = connect(pathlib.Path(os.environ["CC_USAGE_HOME"]) / "usage.db")
        migrate(db)
        return db

    def seed(self, db, sid, branch, commands, recs, t0="2026-10-24T10:00:00Z"):
        """Persist sessions + command_events rows (hook-equivalent evidence)."""
        db.execute("INSERT OR REPLACE INTO sessions(session_id,launcher,provider,model,cwd,"
                   "git_branch,started_at,source) VALUES(?,?,?,?,?,?,?,?)",
                   (sid, "claude-glm", "zai", "glm-5.3-flash", str(self.home), branch,
                    t0, "startup"))
        for (ts, cmd, pid) in commands:
            db.execute("INSERT INTO command_events(session_id,prompt_id,ts,command_name,"
                       "command_source) VALUES(?,?,?,?,?)", (sid, pid, ts, cmd, "user"))
        db.commit()

    def write_session_transcript(self, sid, recs):
        return write_transcript(self.home / "ph" / (sid + ".jsonl"), recs)


class TestPhases(PhaseBase):
    def test_multi_command_session_phases_and_prompt_linkage(self):
        sid = "sess-multi"
        db = self.db()
        recs = chain_session(sid, [
            ("U1", "root", "2026-10-24T10:00:30Z", "p1",
             [("A1", "2026-10-24T10:01:00Z", U), ("A2", "2026-10-24T10:02:00Z", U)]),
            ("U2", "A2", "2026-10-24T10:10:00Z", "p2",
             [("A3", "2026-10-24T10:11:00Z", U)]),
        ])
        self.seed(db, sid, "jeremycod/RUS-6225",
                  [("2026-10-24T10:00:00Z", "implement-jira-ticket", "p1"),
                   ("2026-10-24T10:09:00Z", "challenge-artifact", "p2")], recs)
        f = self.write_session_transcript(sid, recs)
        ingest_file(db, f)
        workflow.build(db, [sid])
        names = [p["phase"] for p in db.execute(
            "SELECT phase FROM workflow_phases WHERE session_id=? ORDER BY started_at",
            (sid,)).fetchall()]
        # session starts exactly at cmd1: no pre-gap; challenge ends the implement span
        self.assertEqual(names, ["implement", "critic_review"])
        link = {r["k"]: r["prompt_uuid"] for r in db.execute(
            "SELECT substr(identity_key,5) k, prompt_uuid FROM api_requests "
            "WHERE session_id=?", (sid,)).fetchall()}
        self.assertEqual(link.get("mid-A1"), "U1")
        self.assertEqual(link.get("mid-A2"), "U1")   # walks A2 -> A1 -> U1
        self.assertEqual(link.get("mid-A3"), "U2")
        split = {r["phase"]: r["n"] for r in db.execute(
            "SELECT p.phase, COUNT(*) n FROM api_requests r JOIN workflow_phases p "
            "ON p.id=r.phase_id WHERE r.session_id=? GROUP BY 1", (sid,)).fetchall()}
        self.assertEqual(split.get("implement"), 2)
        self.assertEqual(split.get("critic_review"), 1)
        lin = db.execute("SELECT * FROM work_lineages WHERE primary_ticket='RUS-6225'").fetchone()
        self.assertIsNotNone(lin)
        self.assertIn(sid, json.loads(lin["session_ids"]))

    def test_commandless_session_uses_skill_clusters(self):
        sid = "sess-skill"
        db = self.db()
        recs = [
            dict(assistant(sid, "mid-s1", "2026-10-24T10:00:10Z", usage=U,
                           extra={"attributionSkill": "analyze-problem"}), uuid="S1",
                 parentUuid="root"),
            dict(assistant(sid, "mid-s2", "2026-10-24T10:05:10Z", usage=U,
                           extra={"attributionSkill": "revise-from-review"}), uuid="S2",
                 parentUuid="S1"),
        ]
        self.seed(db, sid, "main", [], recs)
        f = self.write_session_transcript(sid, recs)
        ingest_file(db, f)
        workflow.build(db, [sid])
        phases = {r["phase"] for r in db.execute(
            "SELECT phase FROM workflow_phases WHERE session_id=?", (sid,)).fetchall()}
        self.assertIn("investigation", phases)
        self.assertIn("revision", phases)
        med = db.execute("SELECT COUNT(*) c FROM workflow_phases WHERE session_id=? "
                         "AND confidence='medium' AND basis LIKE 'skill%'", (sid,)).fetchone()
        self.assertGreaterEqual(med["c"], 2)  # skill clusters are medium confidence

    def test_subagent_attributes_to_dispatch_phase(self):
        sid = "sess-disp"
        db = self.db()
        recs = chain_session(sid, [
            ("U1", "root", "2026-10-24T10:00:30Z", "p1",
             [("A1", "2026-10-24T10:01:00Z", U), ("A2", "2026-10-24T10:02:00Z", U)]),
        ])
        # Agent dispatch record inside the implement span (toolUseId = call_XYZ)
        recs.append({"type": "assistant", "uuid": "DISPATCH", "parentUuid": "A1",
                     "sessionId": sid, "timestamp": "2026-10-24T10:01:30Z",
                     "isSidechain": False,
                     "message": {"content": [{"type": "tool_use", "id": "call_XYZ",
                                              "name": "Agent"}]}})
        self.seed(db, sid, "jeremycod/RUS-6225",
                  [("2026-10-24T10:00:00Z", "implement-jira-ticket", "p1")], recs)
        f = self.write_session_transcript(sid, recs)
        sub = make_subagent_dir(self.home, sid, "agent9", [
            assistant(sid, "mid-c1", "2026-10-24T10:01:40Z", sidechain=True, usage=U)],
            meta={"agentType": "Explore", "name": "explore", "toolUseId": "call_XYZ",
                  "spawnDepth": 1})
        ingest_file(db, f)
        ingest_file(db, sub)
        workflow.build(db, [sid])
        subagents.build_dispatches(db, [sid])
        d = db.execute("SELECT * FROM subagent_dispatches WHERE agent_id='agent9'").fetchone()
        self.assertIsNotNone(d)
        self.assertEqual(d["tool_use_id"], "call_XYZ")
        self.assertEqual(d["dispatch_ts"], "2026-10-24T10:01:30Z")
        ph = db.execute("SELECT phase FROM workflow_phases WHERE id=?",
                        (d["phase_id"],)).fetchone()
        self.assertEqual(ph["phase"], "implement")
        stamped = db.execute("SELECT COUNT(*) c FROM api_requests WHERE agent_id='agent9' "
                             "AND phase_id IS NOT NULL").fetchone()["c"]
        self.assertEqual(stamped, 1)

    def test_otel_duration_join_is_one_to_one(self):
        sid = "sess-otel"
        db = self.db()
        recs = chain_session(sid, [
            ("U1", "root", "2026-10-24T10:00:30Z", "p1",
             [("A1", "2026-10-24T10:01:00Z", U), ("A2", "2026-10-24T10:02:00Z", U)]),
        ])
        self.seed(db, sid, "main", [], recs)
        f = self.write_session_transcript(sid, recs)
        ingest_file(db, f)
        db.execute("INSERT INTO otel_api_requests(session_id,ts,model,input_tokens,"
                   "output_tokens,cache_read_tokens,cache_creation_tokens,duration_ms) "
                   "VALUES(?,?,?,?,?,?,?,?)",
                   (sid, "2026-10-24T10:01:01Z", "glm-5.3-flash", 100, 10, 900, 0, 4321))
        db.execute("INSERT INTO otel_api_requests(session_id,ts,model,input_tokens,"
                   "output_tokens,cache_read_tokens,cache_creation_tokens,duration_ms) "
                   "VALUES(?,?,?,?,?,?,?,?)",
                   (sid, "2026-10-24T10:06:00Z", "glm-5.3-flash", 1, 1, 0, 0, 999))
        db.commit()
        workflow.build(db, [sid])
        n = subagents.join_otel_durations(db, [sid])
        self.assertEqual(n, 1)  # second OTLP row has no response within ±5 s
        r1 = db.execute("SELECT duration_ms, latency_basis FROM api_requests "
                        "WHERE identity_key='mid:mid-A1'").fetchone()
        self.assertEqual(r1["duration_ms"], 4321)
        self.assertEqual(r1["latency_basis"], "measured_api")
        r2 = db.execute("SELECT latency_basis FROM api_requests "
                        "WHERE identity_key='mid:mid-A2'").fetchone()
        self.assertEqual(r2["latency_basis"], "unavailable")

    def test_idle_gap_ends_span_honestly(self):
        """A command span closes at >=4h inactivity; post-gap work is unclassified,
        never misattributed to the stale command (spec §5)."""
        sid = "sess-idle"
        db = self.db()
        recs = chain_session(sid, [
            ("U1", "root", "2026-10-24T10:00:30Z", "p1",
             [("A1", "2026-10-24T10:01:00Z", U), ("A2", "2026-10-24T10:02:00Z", U)]),
            ("U2", "A2", "2026-10-24T20:00:00Z", "p2",   # 10h later, no new command
             [("A3", "2026-10-24T20:01:00Z", U)]),
        ])
        self.seed(db, sid, "main",
                  [("2026-10-24T10:00:00Z", "implement-jira-ticket", "p1")], recs)
        f = self.write_session_transcript(sid, recs)
        ingest_file(db, f)
        workflow.build(db, [sid])
        split = {r["phase"]: r["n"] for r in db.execute(
            "SELECT p.phase, COUNT(*) n FROM api_requests r JOIN workflow_phases p "
            "ON p.id=r.phase_id WHERE r.session_id=? GROUP BY 1", (sid,)).fetchall()}
        self.assertEqual(split.get("implement"), 2)          # pre-gap activity
        self.assertEqual(split.get("unclassified"), 1)       # post-gap tail: honest
        spans = [(r["phase"], r["trigger_kind"]) for r in db.execute(
            "SELECT phase, trigger_kind FROM workflow_phases WHERE session_id=? "
            "ORDER BY started_at", (sid,)).fetchall()]
        self.assertIn(("unclassified", "idle_gap"), spans)

    def test_lineage_groups_sessions_by_ticket(self):
        db = self.db()
        for sid, cmd, t in (("sessA", "implement-jira-ticket", "2026-10-24T10:00:00Z"),
                            ("sessB", "create-pr", "2026-10-24T12:00:00Z")):
            recs = chain_session(sid, [(uuid := "U-" + sid, "root", t, "p-" + sid,
                                        [("X-" + sid, t.replace("00:00", "01"), U)])])
            self.seed(db, sid, "jeremycod/RUS-777", [(t, cmd, "p-" + sid)], recs)
            ingest_file(db, self.write_session_transcript(sid, recs))
        workflow.build(db, ["sessA", "sessB"])
        lin = db.execute("SELECT * FROM work_lineages WHERE primary_ticket='RUS-777'").fetchone()
        ids = json.loads(lin["session_ids"])
        self.assertIn("sessA", ids)
        self.assertIn("sessB", ids)


if __name__ == "__main__":
    unittest.main()
