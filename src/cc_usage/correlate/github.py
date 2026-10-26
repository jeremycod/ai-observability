"""Phase C: GitHub PR correlation via `gh` (spec v3.1 §7).

Rules encoded here (reviewer-mandated):
- PR outcome is counted ONCE; provider PR-count attribution uses a `shared` bucket when
  more than one provider contributed (never per-provider PR counts).
- Stacked PRs are linked via base_pr_number; with base-relative diff numbers each PR
  already carries only its own incremental LOC.
- Multi-ticket PRs link per-ticket; merged status never upgrades quality.
- Review/comment BODIES are never stored (author/state/timestamp only, spec §11).

C carried over from Phase B: post-create-pr span tails are relabelled `revision`
(basis='post_pr_timeline', confidence medium) once the PR review timeline is known.
"""
import json
import os
import re
import subprocess
from datetime import datetime as _dt, timezone as _tz

from ..db import now_iso
from ..attribution.workflow import ts_epoch, _iso, SQL_INS_PHASE

TICKET_RE = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")
REPO_RE = re.compile(r"github\.com[:/](?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git)?$")
TRUNK_BRANCHES = ("main", "master", "develop")
GENERATED_RE = re.compile(
    r"(^|/)(Cargo\.lock|package-lock\.json|yarn\.lock|pnpm-lock\.yaml|.*\.snap|"
    r".*\.snapshot|.*\.min\.(js|css)|.*-lock\..*)$|/migrations/|/snapshots/|/__snapshots__/")

ROLE_MAP = {
    "implement": "first_impl", "investigation": "first_impl",
    "critic_review": "review", "revision": "revision",
    "pr_creation": "pr_work", "merge_gate": "merge",
}


def _gh(args, timeout=60):
    out = subprocess.run(["gh"] + args, capture_output=True, text=True, timeout=timeout)
    if out.returncode != 0:
        raise RuntimeError("gh %s failed: %s" % (" ".join(args[:2]), out.stderr.strip()[:200]))
    return out.stdout


def default_repo():
    env = os.environ.get("CC_USAGE_REPO")
    if env:
        return env
    candidates = [os.path.expanduser("~/Sources/RustLearning/rustfolio"),
                  os.path.expanduser("~/orca/workspaces/rustfolio")]
    for c in candidates:
        if not os.path.exists(c):
            continue
        try:
            url = subprocess.check_output(["git", "-C", c, "remote", "get-url", "origin"],
                                          stderr=subprocess.DEVNULL, text=True).strip()
            m = REPO_RE.search(url)
            if m:
                return "%s/%s" % (m.group("owner"), m.group("repo"))
        except Exception:  # noqa: BLE001
            continue
    return None


def ticket_keys(*texts):
    keys = []
    for t in texts:
        if not t:
            continue
        for m in TICKET_RE.finditer(str(t).upper()):
            if m.group(1) not in keys:
                keys.append(m.group(1))
    return keys


def sync(db, days=90, repo=None, limit=100):
    """Sync PR truth from gh: pull_requests, pr_files, pr_reviews, pr_revisions.
    Returns (synced_count, linked_contributions)."""
    repo = repo or default_repo()
    if not repo:
        raise SystemExit("no repo resolved; pass --repo OWNER/NAME or set CC_USAGE_REPO")
    synced = 0
    listed = json.loads(_gh(["pr", "list", "--repo", repo, "--state", "all",
                             "--limit", str(limit),
                             "--json", "number,title,headRefName,baseRefName,state,"
                             "mergedAt,createdAt,updatedAt,author,additions,deletions,"
                             "changedFiles"]))
    for p in listed:
        number = p["number"]
        created = p.get("createdAt")
        # snapshot-only window filter (event-time semantics)
        if days is not None and created:
            age_s = (_dt.now(_tz.utc) - _dt.fromisoformat(created.replace("Z", "+00:00"))
                     ).total_seconds()
            if age_s > days * 86400:
                continue
        body_keys = []
        try:
            body = json.loads(_gh(["pr", "view", str(number), "--repo", repo,
                                   "--json", "body"])).get("body") or ""
            body_keys = ticket_keys(body)
            del body  # marker extraction only; PR bodies are never stored (spec §11)
        except Exception:  # noqa: BLE001
            pass
        tks = ticket_keys(p.get("headRefName"), p.get("title"))
        for k in body_keys:
            # Body markers are WEAK evidence (PR descriptions cite related tickets and
            # plan-doc ids like REV-1/CELL-4 that are not Jira keys). Admit a body key
            # only when a work lineage exists for it — corroboration over regex (live-
            # data fix: one PR falsely claimed 10 tickets).
            if k in tks:
                continue
            has_lineage = db.execute("SELECT 1 FROM work_lineages WHERE primary_ticket=?",
                                     (k,)).fetchone()
            if has_lineage:
                tks.append(k)
        base_branch = p.get("baseRefName")
        db.execute(
            "INSERT INTO pull_requests(number,repo,title,branch,base_branch,state,merged,"
            "merged_at,created_at,updated_at,author,ticket_keys,gross_add,gross_del,"
            "changed_files,synced_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(number) DO UPDATE SET repo=excluded.repo,title=excluded.title,"
            "branch=excluded.branch,base_branch=excluded.base_branch,state=excluded.state,"
            "merged=excluded.merged,merged_at=excluded.merged_at,"
            "created_at=excluded.created_at,updated_at=excluded.updated_at,"
            "author=excluded.author,ticket_keys=excluded.ticket_keys,"
            "gross_add=excluded.gross_add,gross_del=excluded.gross_del,"
            "changed_files=excluded.changed_files,synced_at=excluded.synced_at",
            (number, repo, p.get("title"), p.get("headRefName"), base_branch,
             p.get("state"), 1 if p.get("mergedAt") else 0, p.get("mergedAt"),
             created, p.get("updatedAt"), (p.get("author") or {}).get("login"),
             json.dumps(tks), p.get("additions") or 0, p.get("deletions") or 0,
             p.get("changedFiles") or 0, now_iso()))
        _sync_detail(db, repo, number)
        synced += 1
    _link_stack(db)
    n = link_contributions(db)
    db.commit()
    return synced, n


def _sync_detail(db, repo, number):
    """Reviews + commits timeline + per-file language/generated stats."""
    try:
        detail = json.loads(_gh(["pr", "view", str(number), "--repo", repo, "--json",
                                 "reviews,commits,files"]))
    except Exception as e:  # noqa: BLE001
        from ..collector import log
        log("pr %s detail failed: %s" % (number, e))
        return
    for r in detail.get("reviews") or []:
        db.execute("INSERT OR REPLACE INTO pr_reviews(pr_number,review_id,author,state,at) "
                   "VALUES(?,?,?,?,?)",
                   (number, str(r.get("id") or r.get("submittedAt")),
                    (r.get("author") or {}).get("login"), r.get("state"), r.get("submittedAt")))
    for c in detail.get("commits") or []:
        db.execute("INSERT OR REPLACE INTO pr_revisions(pr_number,kind,at,ref) "
                   "VALUES(?,?,?,?)",
                   (number, "push", (c.get("committedDate") or "")[:19], (c.get("oid") or "")[:12]))
    db.execute("DELETE FROM pr_files WHERE pr_number=?", (number,))
    gen_a = gen_d = 0
    for f in detail.get("files") or []:
        path = f.get("path") or ""
        generated = 1 if GENERATED_RE.search(path) else 0
        added, removed = f.get("additions") or 0, f.get("deletions") or 0
        ext = os.path.splitext(path)[1].lstrip(".").lower() or "none"
        db.execute("INSERT OR REPLACE INTO pr_files(pr_number,path,ext,generated,added,removed) "
                   "VALUES(?,?,?,?,?,?)", (number, path, ext, generated, added, removed))
        if generated:
            gen_a += added
            gen_d += removed
    # review rounds: pushes after the first substantive review (spec: timeline vs pushes)
    first_review = db.execute(
        "SELECT MIN(at) m FROM pr_reviews WHERE pr_number=? AND state IN "
        "('CHANGES_REQUESTED','COMMENTED','APPROVED')", (number,)).fetchone()["m"]
    rounds = 0
    if first_review:
        rounds = db.execute("SELECT COUNT(*) c FROM pr_revisions WHERE pr_number=? "
                            "AND kind='push' AND at > ?",
                            (number, first_review[:19])).fetchone()["c"]
    net_a = (db.execute("SELECT COALESCE(SUM(added),0) s FROM pr_files WHERE pr_number=? "
                        "AND generated=0", (number,)).fetchone()["s"])
    net_d = (db.execute("SELECT COALESCE(SUM(removed),0) s FROM pr_files WHERE pr_number=? "
                        "AND generated=0", (number,)).fetchone()["s"])
    db.execute("UPDATE pull_requests SET review_rounds=?, gen_add=?, gen_del=?, "
               "net_add=?, net_del=? WHERE number=?",
               (rounds, gen_a, gen_d, net_a, net_d, number))


def _link_stack(db):
    rows = db.execute("SELECT number, branch, base_branch FROM pull_requests").fetchall()
    heads = {r["branch"]: r["number"] for r in rows}
    for r in rows:
        base = r["base_branch"]
        base_pr = heads.get(base) if base and base not in TRUNK_BRANCHES else None
        leaf = 0 if any(x["base_branch"] == r["branch"] for x in rows) else 1
        db.execute("UPDATE pull_requests SET base_pr_number=?, is_stacked_leaf=? "
                   "WHERE number=?", (base_pr, leaf, r["number"]))


def link_contributions(db, session_ids=None):
    """Attach lineage sessions to PRs by ticket, with per-role token attribution from
    Phase B phase stamps. Provider dims come from the session row."""
    prs = db.execute("SELECT number, ticket_keys FROM pull_requests "
                     "WHERE ticket_keys IS NOT NULL AND ticket_keys != '[]'").fetchall()
    n = 0
    for pr in prs:
        keys = json.loads(pr["ticket_keys"])
        sess = set()
        for k in keys:
            lin = db.execute("SELECT lineage_id, session_ids FROM work_lineages "
                             "WHERE primary_ticket=?", (k,)).fetchone()
            if lin:
                try:
                    sess |= set(json.loads(lin["session_ids"] or "[]"))
                except (ValueError, TypeError):
                    pass
            for r in db.execute(
                    "SELECT DISTINCT s.session_id FROM sessions s "
                    "WHERE s.git_branch LIKE ? OR EXISTS (SELECT 1 FROM command_events c "
                    "WHERE c.session_id=s.session_id AND c.jira_ticket=?)",
                    ("%" + k + "%", k)).fetchall():
                sess.add(r[0])
        for sid in sorted(sess):
            if session_ids and sid not in session_ids:
                continue
            srow = db.execute("SELECT launcher, provider FROM sessions WHERE session_id=?",
                              (sid,)).fetchone()
            launcher = srow["launcher"] if srow else "unknown"
            provider = srow["provider"] if srow else "unknown"
            roles = db.execute(
                "SELECT COALESCE(p.phase,'unclassified') phase, COUNT(*) n, "
                "SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_creation_tokens) tok, "
                "MIN(r.record_ts) f, MAX(r.record_ts) l "
                "FROM api_requests r LEFT JOIN workflow_phases p ON p.id=r.phase_id "
                "WHERE r.session_id=? GROUP BY 1", (sid,)).fetchall()
            for row in roles:
                role = ROLE_MAP.get(row["phase"], "unclassified")
                db.execute(
                    "INSERT INTO pr_contributions(pr_number,session_id,role,lineage_id,launcher,"
                    "provider,gateway,billing,tokens,responses,first_ts,last_ts) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pr_number,session_id,role) "
                    "DO UPDATE SET tokens=excluded.tokens,responses=excluded.responses,"
                    "first_ts=excluded.first_ts,last_ts=excluded.last_ts,"
                    "launcher=excluded.launcher,provider=excluded.provider",
                    (pr["number"], sid, role, "ticket:%s" % (keys[0] if keys else ""), launcher,
                     provider, None, None, row["tok"] or 0, row["n"], row["f"], row["l"]))
                n += 1
    db.commit()
    return n


def apply_timeline_refinements(db, session_ids=None):
    """Split pr_creation spans at the first review/comment time; the tail becomes
    revision (basis='post_pr_timeline', confidence medium). Re-runnable: after the
    split, the head span no longer contains the review instant."""
    n = 0
    prs = db.execute("SELECT number FROM pull_requests").fetchall()
    for pr in prs:
        first_review = db.execute(
            "SELECT MIN(at) m FROM pr_reviews WHERE pr_number=? AND state IN "
            "('CHANGES_REQUESTED','COMMENTED','APPROVED')", (pr["number"],)).fetchone()["m"]
        if not first_review:
            continue
        t_review = ts_epoch(first_review)
        spans = db.execute(
            "SELECT id, session_id, started_at, ended_at, trigger_kind, prompt_id, "
            "ticket_keys, evidence_json, device_id FROM workflow_phases "
            "WHERE phase='pr_creation' AND started_at IS NOT NULL").fetchall()
        for s in spans:
            if session_ids and s["session_id"] not in session_ids:
                continue
            e0 = ts_epoch(s["started_at"])
            e1 = ts_epoch(s["ended_at"])
            if e0 is None or e1 is None or not (e0 < t_review < e1):
                continue
            _split_phase(db, s, t_review)
            n += 1
    db.commit()
    return n


def _split_phase(db, span_row, t_split):
    """Split one workflow_phases row at t_split (epoch): keep the head as pr_creation,
    insert a revision tail (post_pr_timeline), re-stamp affected responses."""
    ev = json.loads(span_row["evidence_json"] or "{}")
    db.execute("DELETE FROM workflow_phases WHERE id=?", (span_row["id"],))
    head = db.execute(SQL_INS_PHASE, (
        None, span_row["session_id"], "pr_creation", span_row["started_at"],
        _iso(t_split), span_row["trigger_kind"], span_row["prompt_id"],
        span_row["ticket_keys"], None, "high", "command",
        json.dumps({"start_epoch": ev.get("start_epoch"), "end_epoch": t_split},
                   separators=(",", ":")), span_row["device_id"])).lastrowid
    tail = db.execute(SQL_INS_PHASE, (
        None, span_row["session_id"], "revision", _iso(t_split), span_row["ended_at"],
        "post_pr_timeline", span_row["prompt_id"], span_row["ticket_keys"], None,
        "medium", "post_pr_timeline",
        json.dumps({"start_epoch": t_split, "end_epoch": ev.get("end_epoch")},
                   separators=(",", ":")), span_row["device_id"])).lastrowid
    rows = db.execute(
        "SELECT id, record_ts FROM api_requests WHERE session_id=? AND phase_id=? "
        "AND is_sidechain=0", (span_row["session_id"], span_row["id"])).fetchall()
    for r in rows:
        new_id = head if ts_epoch(r["record_ts"]) < t_split else tail
        db.execute("UPDATE api_requests SET phase_id=? WHERE id=?", (new_id, r["id"]))
    db.execute("UPDATE api_requests SET phase_id=? WHERE phase_id=? AND is_sidechain=1",
               (head, span_row["id"]))


def outcomes(db, days=None):
    """Provider outcome table with the shared-bucket rule enforced (spec §7)."""
    where, params = "", []
    if days:
        where = "WHERE pr.created_at >= datetime('now', ?)"
        params = ["-%d days" % days]
    rows = db.execute(
        "SELECT pr.number, pr.merged, pr.review_rounds, pr.net_add, pr.net_del, "
        "pr.ticket_keys FROM pull_requests pr %s ORDER BY pr.number" % where, params).fetchall()
    out = []
    for pr in rows:
        contrib = db.execute(
            "SELECT provider, role, SUM(tokens) tok FROM pr_contributions "
            "WHERE pr_number=? GROUP BY provider, role", (pr["number"],)).fetchall()
        providers = sorted({c["provider"] for c in contrib if c["provider"] != "unknown"})
        bucket = providers[0] if len(providers) == 1 else ("shared" if providers else "none")
        out.append({"number": pr["number"], "merged": pr["merged"],
                    "rounds": pr["review_rounds"], "net_add": pr["net_add"],
                    "net_del": pr["net_del"], "bucket": bucket,
                    "providers": providers, "contrib": [dict(c) for c in contrib]})
    return out
