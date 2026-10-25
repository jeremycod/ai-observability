"""Phase B: subagent→phase dispatch join and OTLP measured-duration enrichment.

- Subagent usage attributes to the phase active at its DISPATCH timestamp (spec §6):
  meta.toolUseId is the primary join key into the parent transcript's Agent tool_use
  record; the prose agentId in the parent's tool result is the fallback. Orphans
  (no parent-side record) remain counted states with phase_id NULL.
- OTLP `api_request` events carry measured `duration_ms`; each unjoined OTLP row is
  matched to at most one same-session transcript response within ±5 s (evidence:
  delivery-matrix samples arrive within ~1 s of the response). The transcript row
  gains duration_ms + latency_basis='measured_api'; the OTLP row is marked consumed.
"""
import json
import os

from .workflow import ts_epoch

SQL_SIDECROWS = ("SELECT id, parent_session_id, agent_id, source_file FROM api_requests "
                 "WHERE is_sidechain=1 AND phase_id IS NULL AND agent_id IS NOT NULL")
SQL_UPSERT_DISPATCH = ("INSERT INTO subagent_dispatches(parent_session_id,agent_id,tool_use_id,"
                       "dispatch_ts,phase_id,agent_type,description) VALUES(?,?,?,?,?,?,?) "
                       "ON CONFLICT(parent_session_id,agent_id) DO UPDATE SET "
                       "tool_use_id=excluded.tool_use_id,dispatch_ts=excluded.dispatch_ts,"
                       "phase_id=excluded.phase_id,agent_type=excluded.agent_type,"
                       "description=excluded.description")
SQL_STAMP_SIDEC = ("UPDATE api_requests SET phase_id=? WHERE id=? AND is_sidechain=1")


def _phase_id_at(db, session_id, ts_e):
    """Phase containing ts_e by EPOCH comparison (ISO strings mix Z and +offset formats
    and must never be compared lexically — spans store epochs in evidence_json).
    A dispatch record can postdate the last main-chain response (the dispatch IS the
    last event before the agent runs), so with no containing span the nearest
    preceding phase is the attribution (spec §6: dispatch-phase rule)."""
    best, best_start = None, None
    for r in db.execute("SELECT id, evidence_json FROM workflow_phases WHERE session_id=?",
                        (session_id,)).fetchall():
        try:
            ev = json.loads(r["evidence_json"] or "{}")
        except ValueError:
            continue
        s0, s1 = ev.get("start_epoch"), ev.get("end_epoch")
        if (s0 is None or ts_e >= s0) and (s1 is None or ts_e <= s1):
            return r["id"]  # sequential spans: the container wins outright
        if s0 is not None and ts_e >= s0 and (best_start is None or s0 >= best_start):
            best, best_start = r["id"], s0
    return best


def _find_dispatch_ts(parent_file, tool_use_id, agent_id):
    """Scan the parent transcript for the Agent tool_use record (primary: toolUseId in
    the call; fallback: agentId in the result). Returns its timestamp or None."""
    if not parent_file or not os.path.exists(parent_file):
        return None
    needle_a = ('"%s"' % tool_use_id).encode() if tool_use_id else None
    needle_b = ('"agentId":"%s"' % agent_id).encode() if agent_id else None
    fallback_ts = None
    try:
        with open(parent_file, "rb") as f:
            for line in f:
                ts = None
                if needle_a and needle_a in line:
                    try:
                        ts = json.loads(line).get("timestamp")
                    except ValueError:
                        continue
                    if ts:
                        return ts
                if fallback_ts is None and needle_b and needle_b in line:
                    try:
                        fallback_ts = json.loads(line).get("timestamp")
                    except ValueError:
                        continue
    except OSError:
        return None
    return fallback_ts


def _meta_for(source_file):
    meta_path = str(source_file)[: -len(".jsonl")] + ".meta.json"
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def build_dispatches(db, session_ids=None):
    q = ("SELECT DISTINCT parent_session_id, agent_id, source_file FROM api_requests "
         "WHERE is_sidechain=1 AND agent_id IS NOT NULL")
    params = ()
    if session_ids:
        q += " AND parent_session_id IN (%s)" % ",".join("?" * len(session_ids))
        params = tuple(session_ids)
    joined = 0
    for row in db.execute(q, params).fetchall():
        parent, agent_id, source_file = row["parent_session_id"], row["agent_id"], row["source_file"]
        meta = _meta_for(source_file) if source_file else {}
        tool_use_id = meta.get("toolUseId")
        # Dispatch candidates: the root session's main transcript PLUS sibling subagent
        # files — a depth-2 agent's dispatch record lives in its depth-1 parent's file,
        # not the root main transcript (verified on a real specimen, Phase B gate #4).
        candidates = [_parent_main_file(db, parent)] + [
            str(p) for p in _sibling_agent_files(source_file)]
        dispatch_ts = None
        for cand in candidates:
            if not cand:
                continue
            dispatch_ts = _find_dispatch_ts(cand, tool_use_id, agent_id)
            if dispatch_ts:
                break
        phase_id = None
        if dispatch_ts:
            phase_id = _phase_id_at(db, parent, ts_epoch(dispatch_ts))
        db.execute(SQL_UPSERT_DISPATCH, (parent, agent_id, tool_use_id, dispatch_ts,
                                         phase_id, meta.get("agentType"),
                                         meta.get("description")))
        if phase_id:
            for r in db.execute("SELECT id FROM api_requests WHERE parent_session_id=? "
                                "AND agent_id=? AND is_sidechain=1 AND phase_id IS NULL",
                                (parent, agent_id)).fetchall():
                db.execute(SQL_STAMP_SIDEC, (phase_id, r["id"]))
                joined += 1
    db.commit()
    return joined


def _parent_main_file(db, parent_session_id):
    row = db.execute("SELECT source_file FROM api_requests WHERE session_id=? "
                     "AND is_sidechain=0 AND source_file IS NOT NULL "
                     "AND source_file NOT LIKE '%/subagents/%' LIMIT 1",
                     (parent_session_id,)).fetchone()
    return row["source_file"] if row else None


def _sibling_agent_files(source_file):
    if not source_file:
        return []
    d = os.path.dirname(source_file)
    try:
        return [os.path.join(d, p) for p in os.listdir(d)
                if p.endswith(".jsonl") and p.startswith("agent-")]
    except OSError:
        return []


def join_otel_durations(db, session_ids=None, tolerance_s=5.0):
    """Measured-API enrichment: pair unused OTLP rows with same-session transcript
    responses within tolerance. One-to-one; re-runnable (both sides marked)."""
    q = "SELECT DISTINCT session_id FROM otel_api_requests WHERE joined_api_request_id IS NULL"
    params = ()
    if session_ids:
        q += " AND session_id IN (%s)" % ",".join("?" * len(session_ids))
        params = tuple(session_ids)
    pairs = 0
    for row in db.execute(q, params).fetchall():
        sid = row["session_id"]
        otel = db.execute("SELECT id, ts, duration_ms FROM otel_api_requests "
                          "WHERE session_id=? AND joined_api_request_id IS NULL "
                          "ORDER BY ts", (sid,)).fetchall()
        resp = db.execute("SELECT id, record_ts FROM api_requests WHERE session_id=? "
                          "AND source='transcript' AND is_sidechain=0 "
                          "AND latency_basis='unavailable' ORDER BY record_ts",
                          (sid,)).fetchall()
        if not otel or not resp:
            continue
        ri = 0
        used = set()
        for o in otel:
            o_e = ts_epoch(o["ts"])
            best, best_diff = None, None
            for r in resp:
                if r["id"] in used:
                    continue
                diff = abs(ts_epoch(r["record_ts"]) - o_e)
                if diff <= tolerance_s and (best_diff is None or diff < best_diff):
                    best, best_diff = r, diff
            if best is not None:
                used.add(best["id"])
                db.execute("UPDATE api_requests SET duration_ms=?, latency_basis='measured_api' "
                           "WHERE id=?", (o["duration_ms"], best["id"]))
                db.execute("UPDATE otel_api_requests SET joined_api_request_id=? WHERE id=?",
                           (best["id"], o["id"]))
                pairs += 1
    db.commit()
    return pairs
