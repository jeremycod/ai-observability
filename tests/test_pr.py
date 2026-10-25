"""Phase C: gh PR correlation — stacks, tickets, rounds, generated-file exclusion,
shared-bucket rule, timeline refinement, reviews scan, jira import. No network:
`gh` is stubbed."""
import json
import os
import pathlib
import unittest

import common  # noqa: F401
from common import Base, assistant, fresh_db, write_transcript

from cc_usage.attribution import workflow
from cc_usage.correlate import github, jira, reviews as rvmod
from cc_usage.ingest.transcripts import ingest_file

U = {"input_tokens": 100, "output_tokens": 10, "cache_read_input_tokens": 900,
     "cache_creation_input_tokens": 0}

PRS = [
    {"number": 101, "title": "[RUS-900] Feature A", "headRefName": "jeremycod/RUS-900",
     "baseRefName": "main", "state": "MERGED", "mergedAt": "2026-10-20T12:00:00Z",
     "createdAt": "2026-10-20T10:00:00Z", "updatedAt": "2026-10-20T12:00:00Z",
     "author": {"login": "jeremycod"}, "additions": 250, "deletions": 40,
     "changedFiles": 3},
    {"number": 102, "title": "[RUS-901] Feature B on top of A", "headRefName": "jeremycod/RUS-901",
     "baseRefName": "jeremycod/RUS-900", "state": "OPEN", "mergedAt": None,
     "createdAt": "2026-10-21T10:00:00Z", "updatedAt": "2026-10-21T10:00:00Z",
     "author": {"login": "jeremycod"}, "additions": 90, "deletions": 5,
     "changedFiles": 2},
]
DETAILS = {
    "101": {"reviews": [
        {"id": "r1", "author": {"login": "reviewer"}, "state": "CHANGES_REQUESTED",
         "submittedAt": "2026-10-20T11:00:00Z"},
        {"id": "r2", "author": {"login": "reviewer"}, "state": "APPROVED",
         "submittedAt": "2026-10-20T11:30:00Z"}],
        "commits": [{"oid": "a" * 40, "committedDate": "2026-10-20T10:30:00Z"},
                    {"oid": "b" * 40, "committedDate": "2026-10-20T11:15:00Z"}],
        "files": [{"path": "src/lib.rs", "additions": 200, "deletions": 30},
                  {"path": "Cargo.lock", "additions": 50, "deletions": 10},
                  {"path": "backend/migrations/20260101000000_x.sql", "additions": 0,
                   "deletions": 0}]},
    "102": {"reviews": [], "commits": [{"oid": "c" * 40, "committedDate": "2026-10-21T10:30:00Z"}],
            "files": [{"path": "src/b.rs", "additions": 90, "deletions": 5}]},
}


def fake_gh(args):
    if args[:2] == ["pr", "list"]:
        return json.dumps(PRS)
    if args[:2] == ["pr", "view"]:
        number, field = args[2], args[-1]
        if field == "body":
            return json.dumps({"body": "Refs RUS-9002 and RUS-7777"})
        if field == "reviews,commits,files":
            return json.dumps(DETAILS[number])
    raise AssertionError("unexpected gh call: %r" % (args,))


class TestPrSync(Base):
    def test_sync_stacks_tickets_rounds_generated(self):
        db = fresh_db(self.home)
        # RUS-9002 appears only in the PR body: admitted because a lineage corroborates
        # it; an uncorroborated body id (RUS-7777, seeded with no lineage) must NOT be.
        db.execute("INSERT INTO work_lineages(lineage_id,primary_ticket,created_at) "
                   "VALUES('ticket:RUS-9002','RUS-9002','2026-10-01T00:00:00Z')")
        orig = github._gh
        github._gh = fake_gh
        try:
            synced, _ = github.sync(db, days=90, repo="o/r")
        finally:
            github._gh = orig
        self.assertEqual(synced, 2)
        p101 = db.execute("SELECT * FROM pull_requests WHERE number=101").fetchone()
        p102 = db.execute("SELECT * FROM pull_requests WHERE number=102").fetchone()
        # branch+title keys always; body keys only when lineage-corroborated
        self.assertEqual(json.loads(p101["ticket_keys"]), ["RUS-900", "RUS-9002"])
        self.assertNotIn("RUS-7777", p101["ticket_keys"])
        # stacked graph: 102 bases on 101's head; 101 is not a leaf
        self.assertEqual(p102["base_pr_number"], 101)
        self.assertEqual(p101["is_stacked_leaf"], 0)
        self.assertEqual(p102["is_stacked_leaf"], 1)
        # review rounds: pushes after first review (commit b at 11:15 after 11:00 review)
        self.assertEqual(p101["review_rounds"], 1)
        self.assertEqual(p102["review_rounds"], 0)
        # generated files excluded from net; migrations matched too
        self.assertEqual(p101["net_add"], 200)
        self.assertEqual(p101["gen_add"], 50)
        self.assertEqual(p102["net_add"], 90)
        # review bodies are never stored (only author/state/at)
        blob = repr(list(db.execute("SELECT * FROM pr_reviews")))
        self.assertNotIn("body", blob.lower().replace("body'", "").replace("review_id", ""))

    def test_contributions_and_shared_bucket(self):
        db = fresh_db(self.home)
        for sid, branch, launcher, provider in (
                ("sA", "jeremycod/RUS-900", "claude-glm", "zai"),
                ("sB", "jeremycod/RUS-900", "claude-deepseek", "deepseek")):
            db.execute("INSERT INTO sessions(session_id,launcher,provider,model,cwd,"
                       "git_branch,started_at) VALUES(?,?,?,?,?,?,?)",
                       (sid, launcher, provider, "m", "/t", branch, "2026-10-20T09:00:00Z"))
            db.execute("INSERT INTO command_events(session_id,ts,command_name,jira_ticket) "
                       "VALUES(?,?,?,?)", (sid, "2026-10-20T09:01:00Z",
                                           "implement-jira-ticket", "RUS-900"))
            db.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,model,"
                       "input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens) "
                       "VALUES(?,?,?,?,?,?,?,?,?)",
                       (sid, "2026-10-20T09:05:00Z", "transcript", "mid:%s" % sid, "m",
                        1000, 100, 500, 0))
        db.execute("INSERT INTO pull_requests(number,repo,ticket_keys) VALUES(201,'o/r','[\"RUS-900\"]')")
        db.execute("INSERT INTO pull_requests(number,repo,ticket_keys) VALUES(202,'o/r','[\"RUS-999\"]')")
        # RUS-999 has only session sA
        db.execute("UPDATE sessions SET git_branch='jeremycod/RUS-999' WHERE session_id='sA'")
        github.link_contributions(db)
        rows = github.outcomes(db)
        by = {r["number"]: r for r in rows}
        # 201 has both providers -> shared; 202 has only zai -> zai
        self.assertEqual(by[201]["bucket"], "shared")
        self.assertEqual(by[202]["bucket"], "zai")
        contrib = db.execute("SELECT COUNT(*) c FROM pr_contributions "
                             "WHERE pr_number=201").fetchone()["c"]
        self.assertGreaterEqual(contrib, 2)

    def test_timeline_refinement_splits_pr_creation(self):
        db = fresh_db(self.home)
        sid = "s-tl"
        db.execute("INSERT INTO sessions(session_id,git_branch,started_at) "
                   "VALUES(?,'jeremycod/RUS-910','2026-10-20T10:00:00Z')", (sid,))
        # pr_creation span 10:00 -> 14:00 (epochs); first review at 12:00
        e0, e1, tr = 1761000000, 1761014400 - 3600 * 0, 0
        import datetime as dt
        e0 = dt.datetime(2026, 10, 20, 10, 0, tzinfo=dt.timezone.utc).timestamp()
        e1 = dt.datetime(2026, 10, 20, 14, 0, tzinfo=dt.timezone.utc).timestamp()
        tr = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        pid = db.execute(
            "INSERT INTO workflow_phases(session_id,phase,started_at,ended_at,trigger_kind,"
            "evidence_json) VALUES(?,?,?,?,?,?)",
            (sid, "pr_creation", "2026-10-20T10:00:00Z", "2026-10-20T14:00:00Z", "command",
             json.dumps({"start_epoch": e0, "end_epoch": e1}))).lastrowid
        for name, ts in (("early", "2026-10-20T11:00:00Z"), ("late", "2026-10-20T13:00:00Z")):
            db.execute("INSERT INTO api_requests(session_id,ts,source,identity_key,record_ts,"
                       "phase_id,is_sidechain) VALUES(?,?,?,?,?,?,0)",
                       (sid, ts, "transcript", "mid:" + name, ts, pid))
        db.execute("INSERT INTO pr_reviews(pr_number,review_id,author,state,at) "
                   "VALUES(301,'r1','rev','CHANGES_REQUESTED','2026-10-20T12:00:00Z')")
        db.execute("INSERT INTO pull_requests(number,repo) VALUES(301,'o/r')")
        db.commit()
        n = github.apply_timeline_refinements(db)
        self.assertEqual(n, 1)
        spans = {r["phase"]: r["id"] for r in db.execute(
            "SELECT phase, id FROM workflow_phases WHERE session_id=?", (sid,)).fetchall()}
        self.assertEqual(set(spans), {"pr_creation", "revision"})
        tail = db.execute("SELECT basis, confidence FROM workflow_phases WHERE id=?",
                          (spans["revision"],)).fetchone()
        self.assertEqual(tail["basis"], "post_pr_timeline")
        self.assertEqual(tail["confidence"], "medium")
        early = db.execute("SELECT p.phase ph FROM api_requests r JOIN workflow_phases p "
                           "ON p.id=r.phase_id WHERE r.identity_key='mid:early'").fetchone()
        late = db.execute("SELECT p.phase ph, p.basis b FROM api_requests r "
                          "JOIN workflow_phases p ON p.id=r.phase_id "
                          "WHERE r.identity_key='mid:late'").fetchone()
        self.assertEqual(early["ph"], "pr_creation")
        self.assertEqual((late["ph"], late["b"]), ("revision", "post_pr_timeline"))
        # no dangling sidechain refs
        dang = db.execute("SELECT COUNT(*) c FROM api_requests r LEFT JOIN workflow_phases p "
                          "ON p.id=r.phase_id WHERE r.phase_id IS NOT NULL AND p.id IS NULL"
                          ).fetchone()["c"]
        self.assertEqual(dang, 0)


class TestReviewsScan(Base):
    def test_severity_counts_and_disposition(self):
        db = fresh_db(self.home)
        root = self.home / "reviews-root"
        t = root / "topic-x"
        t.mkdir(parents=True)
        (t / "findings.md").write_text("P0 missing auth check\nP1 minor leak\np1 dup\n", encoding="utf-8")
        (t / "final-decision.md").write_text("Verdict: accepted, no blockers.\n", encoding="utf-8")
        topics, files = rvmod.scan_root(db, str(root))
        self.assertEqual((topics, files), (1, 2))
        f = db.execute("SELECT * FROM critic_findings WHERE topic='topic-x' AND severity='p1'"
                       ).fetchone()
        self.assertEqual(f["hits"], 2)
        art = db.execute("SELECT * FROM review_artifacts WHERE root=?",
                         (str(root),)).fetchone()
        self.assertEqual(art["disposition"], "accepted")


class TestJiraImport(Base):
    def test_import_snapshot(self):
        db = fresh_db(self.home)
        snap = self.home / "snap.json"
        snap.write_text(json.dumps({"issues": [
            {"key": "RUS-900", "status": "Done", "summary": "Feature A",
             "fields": {"issuetype": {"name": "Story"}}}]}), encoding="utf-8")
        jira.import_snapshot(db, str(snap))
        row = db.execute("SELECT * FROM jira_issues WHERE key='RUS-900'").fetchone()
        self.assertEqual(row["status"], "Done")
        self.assertEqual(row["issuetype"], "Story")


if __name__ == "__main__":
    unittest.main()
