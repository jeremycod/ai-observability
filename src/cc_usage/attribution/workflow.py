"""Phase B attribution: workflow phases, prompt-chain linkage, lineage stitching.

Spec v3.1 §5. Design points carried from review:
- Classification cascade: (1) UserPromptExpansion command names, (2) transcript
  attributionSkill clusters (command-less sessions only), (3) structural hooks.
  ChatGPT-follow-up is multi-signal only; anything ambiguous stays unclassified.
- Spans may nest; token attribution joins each main-chain model_response to the
  innermost enclosing phase of its session at that row's timestamp.
- Phase durations are never presented as disjoint wall-clock (timeline, not partition).
- Phases/lineages are DERIVED data: per-session rebuild is DELETE+INSERT; evidence
  tables are untouched.

Prompt linkage (the 0/32,099 gap): assistant records carry no promptId (critic
finding #7); the triggering prompt is resolved by walking the parentUuid chain to the
nearest-preceding user record; `last-prompt`/leafUuid records terminate chains.
"""
import datetime as dt
import json
import re

from ..db import now_iso
from ..ingest.transcripts import SUBAGENT_RE, ts_epoch

# Command -> phase map (documented; unknown commands never guessed).
COMMAND_PHASES = {
    "implement-jira-ticket": "implement", "implement-ticket": "implement",
    "implement-from-problem": "implement", "implement-from-design": "implement",
    "fix-ticket": "implement",
    "challenge-artifact": "critic_review", "review-pr": "critic_review",
    "revise-from-review": "revision", "address-comments": "revision",
    "create-pr": "pr_creation", "merge-pr": "merge_gate",
    "analyze-problem": "investigation", "investigate-problem": "investigation",
    "mark-problem": "investigation", "design-solution": "investigation",
    "draft-implementation-plan": "investigation", "draft-jira-ticket": "investigation",
    "create-tickets": "investigation", "plan": "investigation", "plan-task": "investigation",
    "rework-epic": "investigation", "validate-ticket": "investigation",
    "run-from-worktree": "maintenance", "disk-cleanup": "maintenance",
}
SKILL_PHASES = {
    "revise-from-review": "revision", "analyze-problem": "investigation",
    "challenge-artifact": "critic_review", "implement-jira-ticket": "implement",
}
TICKET_RE = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")

SQL_SESSIONS = "SELECT session_id, cwd, git_branch, started_at, ended_at FROM sessions"
SQL_RESP = ("SELECT id, record_ts, source_file, byte_offset, is_sidechain, skill_name "
            "FROM api_requests WHERE session_id=? AND source='transcript' "
            "ORDER BY record_ts")
SQL_CMDS = ("SELECT ts, command_name, prompt_id, jira_ticket FROM command_events "
            "WHERE session_id=? AND command_name IS NOT NULL ORDER BY ts")
SQL_DEL_PHASES = "DELETE FROM workflow_phases WHERE session_id=?"
SQL_DEL_TRANS = "DELETE FROM phase_transitions WHERE session_id=?"
SQL_INS_PHASE = ("INSERT INTO workflow_phases(lineage_id,session_id,phase,started_at,ended_at,"
                 "trigger_kind,prompt_id,ticket_keys,pr_number,confidence,basis,evidence_json) "
                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
SQL_INS_TRANS = ("INSERT INTO phase_transitions(lineage_id,session_id,from_phase,to_phase,at,"
                 "trigger_kind,evidence_json) VALUES(?,?,?,?,?,?,?)")
SQL_UNSTAMP = ("UPDATE api_requests SET phase_id=NULL, prompt_uuid=NULL "
               "WHERE session_id=? AND is_sidechain=0")
SQL_UNSTAMP_SIDEC = ("UPDATE api_requests SET phase_id=NULL "
                     "WHERE parent_session_id=? AND is_sidechain=1")
SQL_STAMP = "UPDATE api_requests SET phase_id=?, prompt_uuid=COALESCE(?, prompt_uuid), " \
            "attribution_skill=COALESCE(attribution_skill, ?) WHERE id=?"
SQL_LINEAGE_UPSERT = ("INSERT INTO work_lineages(lineage_id,primary_ticket,created_at,"
                      "session_ids,continuation_of,note) VALUES(?,?,?,?,?,?) "
                      "ON CONFLICT(lineage_id) DO UPDATE SET primary_ticket=excluded.primary_ticket,"
                      "session_ids=excluded.session_ids")


def phase_for_command(cmd):
    return COMMAND_PHASES.get((cmd or "").strip().lstrip("/").lower())


def phase_for_skill(skill):
    return SKILL_PHASES.get(skill or "")


def scan_session_file(path):
    """One pass over a session transcript retaining ONLY derived, non-sensitive node
    data: uuid/parentUuid graph, user-record prompt ids and timestamps, per-assistant
    attributionSkill. No prompt or assistant text is retained (spec §11)."""
    nodes = {}      # uuid -> {type, parent_uuid, ts, prompt_id, skill, is_sidechain}
    order = []      # (ts_epoch, uuid) for nearest-preceding lookups
    try:
        with open(path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(rec, dict):
                    continue
                uid = rec.get("uuid")
                rtype = rec.get("type")
                if not uid or rtype not in ("user", "assistant", "last-prompt"):
                    continue
                ts = rec.get("timestamp")
                node = {"type": rtype, "parent": rec.get("parentUuid"), "ts": ts,
                        "prompt_id": rec.get("promptId"),
                        "skill": rec.get("attributionSkill"),
                        "side": 1 if rec.get("isSidechain") else 0}
                nodes[uid] = node
                order.append((ts_epoch(ts), uid))
                if rtype == "last-prompt":
                    nodes[uid]["leaf"] = rec.get("leafUuid")
    except OSError:
        pass
    order.sort()
    return nodes, order


def resolve_prompt_uuid(nodes, order, parent_uuid):
    """Walk the parentUuid chain to the nearest-preceding user record (main chain only);
    `last-prompt` records terminate the walk (chain ends). Returns (uuid, prompt_id)."""
    seen = 0
    cur = parent_uuid
    while cur and seen < 10000:  # bound: pathological chains cannot hang the builder
        node = nodes.get(cur)
        if node is None:
            return (None, None)
        if node.get("leaf"):
            return (cur, node.get("prompt_id"))  # last-prompt terminator
        if node["type"] == "user" and not node.get("side"):
            return (cur, node.get("prompt_id"))
        cur = node.get("parent")
        seen += 1
    return (None, None)


def nearest_prompt(order, nodes, ts_e):
    """Fallback: the nearest-preceding user record by time (chain broken)."""
    best = None
    for e, uid in order:
        if e > ts_e:
            break
        n = nodes.get(uid)
        if n and n["type"] == "user" and not n.get("side"):
            best = (uid, n.get("prompt_id"))
    return best or (None, None)


def build_session(db, session_id):
    """Rebuild phase spans + prompt linkage for one session. Returns span count."""
    sess = db.execute("SELECT cwd, git_branch, started_at, ended_at FROM sessions "
                      "WHERE session_id=?", (session_id,)).fetchone()
    resps = db.execute(SQL_RESP, (session_id,)).fetchall()
    if sess is None or not resps:
        return 0
    main_files = sorted({r["source_file"] for r in resps
                         if r["source_file"] and not SUBAGENT_RE.search(r["source_file"])})
    nodes, order = {}, []
    for p in main_files:
        n, o = scan_session_file(p)
        nodes.update(n)
        order = sorted(order + o)

    # --- prompt linkage + skill enrichment (per response, main chain only) ---
    span_resp = []  # (response_row, ts_epoch, prompt_uuid, skill)
    for r in resps:
        if r["is_sidechain"]:
            continue
        skill = r["skill_name"]
        puuid = None
        line = _read_line(r["source_file"], r["byte_offset"])
        if line:
            try:
                rec = json.loads(line)
                node = nodes.get(rec.get("uuid"))
                if node and node.get("skill") and not skill:
                    skill = node["skill"]
                puuid, _ = resolve_prompt_uuid(nodes, order, rec.get("parentUuid"))
                if puuid is None:
                    puuid, _ = nearest_prompt(order, nodes, ts_epoch(r["record_ts"]))
            except ValueError:
                pass
        span_resp.append((r, ts_epoch(r["record_ts"]), puuid, skill))

    # --- phase spans from command events (time-scoped; commands END a phase) ---
    cmds = db.execute(SQL_CMDS, (session_id,)).fetchall()
    start_bound = ts_epoch(sess["started_at"]) if sess["started_at"] else None
    end_bound = ts_epoch(sess["ended_at"]) if sess["ended_at"] else None
    if end_bound is None and span_resp:
        end_bound = max(t for _, t, _, _ in span_resp)
    spans = []  # (start_e, end_e, phase, trigger, prompt_id, confidence, basis)
    if cmds:
        prev_e = start_bound
        for c in cmds:
            c_e = ts_epoch(c["ts"])
            phase = phase_for_command(c["command_name"]) or "unclassified"
            # A command ENDS the previous phase: the interval since the last command
            # belongs to that phase. Only activity before the FIRST command is a gap.
            if prev_e is not None and c_e > prev_e and not spans:
                spans.append((prev_e, c_e, "unclassified", "pre_command_gap", None,
                              "high", "gap"))
            spans.append((c_e, None, phase, "command:" + (c["command_name"] or ""),
                          c["prompt_id"], "high", "command"))
            prev_e = c_e
        if prev_e is not None and end_bound is not None and end_bound > prev_e:
            spans[-1] = (spans[-1][0], end_bound, spans[-1][2], spans[-1][3],
                         spans[-1][4], spans[-1][5], spans[-1][6])
    else:
        # command-less sessions: cluster by attributionSkill (cascade step 2)
        clusters = []
        for r, t, _pu, skill in span_resp:
            ph = phase_for_skill(skill)
            if clusters and clusters[-1][2] == ph:
                clusters[-1][1] = t
                clusters[-1][3].append(skill)
            else:
                clusters.append([t, t, ph, [skill]])
        if not clusters:
            if span_resp:
                spans.append((min(t for _, t, _, _ in span_resp),
                              max(t for _, t, _, _ in span_resp),
                              "unclassified", "no_evidence", None, "low", "no_commands"))
        else:
            prev_e = start_bound
            for c0, c1, ph, skills in clusters:
                if prev_e is not None and c0 > prev_e:
                    spans.append((prev_e, c0, "unclassified", "pre_cluster_gap", None,
                                  "low", "gap"))
                spans.append((c0, c1, ph or "unclassified", "skill_cluster",
                              None, "medium", "skill:" + str(skills[:1])))
                prev_e = c1
            if end_bound is not None and end_bound > prev_e:
                spans.append((prev_e, end_bound, "unclassified", "tail_gap", None,
                              "low", "gap"))

    # --- idle-gap split: a span ends at >=4h of inactivity; the tail is honestly
    # unclassified rather than misattributed to the last command (spec §5: uncertain
    # intervals remain unclassified). ---
    IDLE_GAP_S = 4 * 3600
    split_spans = []
    for (s0, s1, ph, trig, pid, conf, basis) in spans:
        if ph == "unclassified":
            split_spans.append((s0, s1, ph, trig, pid, conf, basis))
            continue
        ts_list = sorted(t for (_r, t, _p, _s) in span_resp
                         if (s0 is None or t >= s0) and (s1 is None or t <= s1))
        if not ts_list:
            split_spans.append((s0, s1, ph, trig, pid, conf, basis))
            continue
        cur0, prev_t = s0, None
        for t in ts_list:
            if prev_t is not None and t - prev_t > IDLE_GAP_S:
                split_spans.append((cur0, prev_t, ph, trig, pid, conf, basis))
                split_spans.append((prev_t, t, "unclassified", "idle_gap", None,
                                    "low", "gap"))
                cur0 = t
            prev_t = t
        split_spans.append((cur0, s1, ph, trig, pid, conf, basis))
    spans = split_spans

    # --- materialize (derived data; per-session rebuild is DELETE+INSERT) ---
    lineage_id = lineage_for_session(db, session_id, sess)
    ticket_keys = None
    if sess["git_branch"]:
        m = TICKET_RE.search(sess["git_branch"].upper())
        ticket_keys = m.group(1) if m else None
    db.execute(SQL_DEL_PHASES, (session_id,))
    db.execute(SQL_DEL_TRANS, (session_id,))
    db.execute(SQL_UNSTAMP, (session_id,))
    # Sidechain rows must also re-stamp: the rebuild re-creates span rows with NEW ids,
    # and stale phase_id references would silently drop subagent tokens from rollups.
    db.execute(SQL_UNSTAMP_SIDEC, (session_id,))
    phase_ids = []
    prev_phase = None
    for (s0, s1, ph, trig, pid, conf, basis) in spans:
        cur = db.execute(SQL_INS_PHASE, (
            lineage_id, session_id, ph, _iso(s0), _iso(s1), trig, pid,
            ticket_keys, None, conf, basis,
            json.dumps({"start_epoch": s0, "end_epoch": s1}, separators=(",", ":")))).lastrowid
        phase_ids.append((cur, s0, s1, ph))
        if prev_phase is not None and prev_phase != ph:
            db.execute(SQL_INS_TRANS, (lineage_id, session_id, prev_phase, ph,
                                       _iso(s0), trig, None))
        prev_phase = ph

    # --- stamp responses: innermost enclosing span (sequential spans -> the container) ---
    for (r, t, puuid, skill) in span_resp:
        ph_id, ph_uuid = None, None
        for (pid_, s0, s1, _ph) in phase_ids:
            if (s0 is None or t >= s0) and (s1 is None or t <= s1):
                ph_id = pid_
                ph_uuid = puuid
        db.execute(SQL_STAMP, (ph_id, ph_uuid, skill, r["id"]))
    db.commit()
    return len(spans)


def _read_line(path, offset):
    try:
        with open(path, "rb") as f:
            f.seek(int(offset))
            return f.readline()
    except (OSError, TypeError, ValueError):
        return None


def _iso(e):
    if e is None:
        return None
    return dt.datetime.fromtimestamp(e, dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def lineage_for_session(db, session_id, sess):
    """Lineage = ticket-grouped work: all sessions sharing a ticket (branch or command
    extraction) form ONE lineage; ticket-less sessions group by cwd. Resume attachment
    (hook source=resume) merges explicitly when encountered (spec §5)."""
    ticket = None
    if sess["git_branch"]:
        m = TICKET_RE.search(sess["git_branch"].upper())
        ticket = m.group(1) if m else None
    if not ticket:
        row = db.execute("SELECT jira_ticket FROM command_events WHERE session_id=? "
                         "AND jira_ticket IS NOT NULL ORDER BY id DESC LIMIT 1",
                         (session_id,)).fetchone()
        ticket = row["jira_ticket"] if row else None
    if ticket:
        lineage_id = "ticket:" + ticket
        rows = db.execute(
            "SELECT DISTINCT s.session_id FROM sessions s WHERE s.session_id=? "
            "OR s.git_branch LIKE ? OR EXISTS (SELECT 1 FROM command_events c "
            "WHERE c.session_id=s.session_id AND c.jira_ticket=?)",
            (session_id, "%" + ticket + "%", ticket)).fetchall()
        note = "ticket-grouped"
    else:
        lineage_id = "cwd:" + (sess["cwd"] or "unknown")
        rows = db.execute("SELECT session_id FROM sessions WHERE cwd=? OR session_id=?",
                          (sess["cwd"] or "", session_id)).fetchall()
        note = "cwd-grouped"
    sib_ids = sorted({r[0] for r in rows} | {session_id})
    db.execute(SQL_LINEAGE_UPSERT, (lineage_id, ticket, now_iso(), json.dumps(sib_ids),
                                    None, note))
    return lineage_id


def build(db, session_ids=None):
    """Build phases for the given sessions (default: every session with responses)."""
    if session_ids is None:
        session_ids = [r[0] for r in db.execute(
            "SELECT DISTINCT session_id FROM api_requests WHERE source='transcript'")]
    n = 0
    for sid in session_ids:
        try:
            n += build_session(db, sid)
        except Exception as e:  # noqa: BLE001 — one bad session never stops the build
            from ..collector import log
            log("phase build failed for %s: %s" % (sid, e))
    return n
