"""Transcript JSONL ingestion — the primary per-request source (spec v3.1 §3, §6).

Identity contract:
- counted object = one model_response per (session_id, message_id);
- every occurrence is logged in api_request_occurrences (PRIMARY KEY over
  session/identity/file/offset) so re-ingest is idempotent and fully auditable;
- differing usage fingerprints -> deterministic ordered-last wins (order valid across
  files), all conflicting occurrences audited in api_request_conflicts, and a
  reconciliation guard flags any winner that is not the max-usage occurrence;
- <synthetic>/isApiErrorMessage records -> api_errors, never model_response rows;
- subagent files (subagents/agent-*.jsonl) -> is_sidechain rows with meta enrichment;
  orphan agents (no meta / no parent record) are counted, expected states (spec §6).
"""
import datetime as dt
import json
import os
import re

from ..db import now_iso
from ..device import device_id as _device_id
from ..identity import (billing_for, canonical_model, identity_key, order_key,
                        provider_inferred, usage_fp)

SUBAGENT_RE = re.compile(r"/subagents/agent-([A-Za-z0-9_.-]+)\.jsonl$")
DEVICE = _device_id()

_SQL_OCC = ("INSERT OR IGNORE INTO api_request_occurrences"
            "(session_id,identity_key,source_file,byte_offset,record_ts,"
            "input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,usage_fp,"
            "seen_at,device_id)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
_SQL_SEL = ("SELECT id,record_ts,source_file,byte_offset,input_tokens,output_tokens,"
            "cache_read_tokens,cache_creation_tokens,usage_fp,occurrences,first_ts,last_ts "
            "FROM api_requests WHERE session_id=? AND identity_key=?")
_SQL_UPD_WINNER = ("UPDATE api_requests SET record_ts=?,source_file=?,byte_offset=?,"
                   "input_tokens=?,output_tokens=?,cache_read_tokens=?,cache_creation_tokens=?,"
                   "usage_fp=?,occurrences=?,first_ts=?,last_ts=?,identity_conflict=1 WHERE id=?")
_SQL_UPD_OCC = "UPDATE api_requests SET occurrences=?,last_ts=? WHERE id=?"
_SQL_FLAG = "UPDATE api_requests SET identity_conflict=1 WHERE id=?"
_SQL_CONFLICT = ("INSERT INTO api_request_conflicts"
                 "(session_id,identity_key,chosen,record_ts,source_file,byte_offset,"
                 "input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,usage_fp,note)"
                 " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
_SQL_MAX = ("SELECT MAX(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) m "
            "FROM api_request_occurrences WHERE session_id=? AND identity_key=?")
_SQL_SESSION = "SELECT launcher,provider FROM sessions WHERE session_id=?"
_SQL_CFG = ("SELECT gateway,model_requested FROM config_snapshots "
            "WHERE session_id=? ORDER BY id DESC LIMIT 1")
_SQL_INSERT_REQ = (
    "INSERT INTO api_requests(session_id,prompt_id,ts,launcher,provider,model,"
    "input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,"
    "identity_key,identity_basis,source,record_ts,source_file,byte_offset,occurrences,usage_fp,"
    "is_sidechain,parent_session_id,agent_id,agent_type,agent_display_name,spawn_depth,"
    "gateway,billing_provider,model_requested,ctx_variant,provider_inferred,"
    "latency_basis,first_ts,last_ts,device_id) "
    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
_SQL_ERR = ("INSERT OR IGNORE INTO api_errors(session_id,ts,message_id,model_raw,"
            "is_synthetic,error_kind,source,source_file,byte_offset,raw_json)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)")
_SQL_STATE = ("INSERT INTO {state_table}(source_file,byte_offset,size,mtime,records,last_ok) "
              "VALUES(?,?,?,?,?,?) ON CONFLICT(source_file) DO UPDATE SET "
              "byte_offset=excluded.byte_offset,size=excluded.size,mtime=excluded.mtime,"
              "records=records+excluded.records,last_ok=excluded.last_ok")
_SQL_GET_OFFSET = "SELECT byte_offset FROM {state_table} WHERE source_file=?"


def ts_epoch(ts):
    """Parse an RFC3339 timestamp to epoch seconds for cross-file-safe ordering.
    Naive timestamps are interpreted as UTC (corpus is Z-anchored). Unparseable or
    missing timestamps map to 0.0 — they order FIRST, never crash, and remain visible
    in raw_json for audit (critic P0-2/P2-1)."""
    if not ts:
        return 0.0
    s = str(ts).strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return 0.0


def _tokens(usage):
    def n(k):
        v = usage.get(k)
        try:
            return int(v) if v is not None and v != "" else 0
        except (TypeError, ValueError):
            return 0
    return tuple(n(k) for k in ("input_tokens", "output_tokens",
                                "cache_read_input_tokens", "cache_creation_input_tokens"))


def _total(t):
    return t[0] + t[1] + t[2] + t[3]


def _min_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if ts_epoch(a) <= ts_epoch(b) else b


def _max_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if ts_epoch(a) >= ts_epoch(b) else b


def _bump_occ(db, row, ts):
    db.execute(_SQL_UPD_OCC, (row["occurrences"] + 1, _max_ts(row["last_ts"], ts), row["id"]))


def _subagent_meta(path):
    m = SUBAGENT_RE.search(str(path).replace("\\", "/"))
    if not m:
        return None
    agent_id = m.group(1)
    meta_path = str(path)[: -len(".jsonl")] + ".meta.json"
    data = {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        pass  # orphan-without-meta is a counted, expected state (spec §6)
    return {"agent_id": agent_id, "agent_type": data.get("agentType"),
            "display_name": data.get("name"), "description": data.get("description"),
            "tool_use_id": data.get("toolUseId"), "spawn_depth": data.get("spawnDepth")}


def process_line(db, line, source_file, byte_offset, ts_window=None):
    """Process one complete JSONL record. Returns (kind, session_id) where kind is
    'response' | 'error' | None (kind None = not an accounting record).
    ts_window=(start_epoch, end_epoch) filters by EVENT timestamp (spec §12): records
    outside the window are skipped for accounting but still consumed for offset
    bookkeeping of the backfill_state table (never the live ingest_state — critic P1-1)."""
    try:
        rec = json.loads(line)
    except ValueError:
        return (None, None)
    if not isinstance(rec, dict) or rec.get("type") != "assistant":
        return (None, None)
    msg = rec.get("message")
    if not isinstance(msg, dict) or not isinstance(msg.get("usage"), dict):
        return (None, None)

    session_id = rec.get("sessionId") or rec.get("session_id")  # canonical: sessionId (F16)
    ts = rec.get("timestamp")
    if ts_window:
        w0, w1 = ts_window
        e = ts_epoch(ts)
        if w0 is not None and e < w0:
            return (None, session_id)
        if w1 is not None and e > w1:
            return (None, session_id)
    model_raw = msg.get("model")

    if model_raw == "<synthetic>" or rec.get("isApiErrorMessage"):
        db.execute(_SQL_ERR, (session_id, ts, msg.get("id"), model_raw,
                              1 if model_raw == "<synthetic>" else 0, "api_error",
                              "transcript", source_file, byte_offset,
                              json.dumps({"model": model_raw, "usage_fp": usage_fp(msg["usage"])},
                                         separators=(",", ":"))))
        # Z.ai quota/rate-limit codes: extract code + reset hint only — the raw error
        # text is never stored (Phase D, spec §8).
        try:
            content = msg.get("content")
            err_text = content if isinstance(content, str) else \
                (content[0].get("text", "") if isinstance(content, list) and content else "")
            if err_text:
                from ..quota import extract_quota_event
                extract_quota_event(db, session_id, err_text, ts)
        except Exception:  # noqa: BLE001 — never let extraction break ingest
            pass
        return ("error", session_id)

    sub = _subagent_meta(source_file)
    model, ctx_variant = canonical_model(model_raw)
    key = identity_key(msg.get("id"), session_id, source_file, byte_offset)
    tokens = _tokens(msg["usage"])
    fp = usage_fp(msg["usage"])

    new_occ = db.execute(_SQL_OCC, (session_id, key, source_file, byte_offset, ts,
                                    tokens[0], tokens[1], tokens[2], tokens[3], fp,
                                    now_iso(), DEVICE)).rowcount
    if not new_occ:
        return ("response", session_id)  # already accounted; keeps re-ingest idempotent

    row = db.execute(_SQL_SEL, (session_id, key)).fetchone()
    if row is None:
        srow = db.execute(_SQL_SESSION, (session_id,)).fetchone()
        launcher = srow["launcher"] if srow else None
        provider = srow["provider"] if srow else None
        crow = db.execute(_SQL_CFG, (session_id,)).fetchone()
        gw = crow["gateway"] if crow else None
        model_req = crow["model_requested"] if crow else None
        db.execute(_SQL_INSERT_REQ, (
            session_id, rec.get("promptId"), ts, launcher, provider, model,
            tokens[0], tokens[1], tokens[2], tokens[3],
            key, "surrogate" if key.startswith("sur:") else "native",
            "transcript", ts, source_file, byte_offset, 1, fp,
            1 if sub else 0, session_id if sub else None,
            sub["agent_id"] if sub else None, sub["agent_type"] if sub else None,
            sub["display_name"] if sub else None, sub["spawn_depth"] if sub else None,
            gw, billing_for(gw) if gw else None, model_req, ctx_variant,
            provider_inferred(model), "unavailable", ts, ts, DEVICE))
        return ("response", session_id)

    if row["usage_fp"] == fp:
        _bump_occ(db, row, ts)
        return ("response", session_id)

    # Differing usage fingerprints: deterministic ordered-last winner (cross-file-safe order).
    new_k = order_key(ts_epoch(ts), source_file, byte_offset)
    cur_k = order_key(ts_epoch(row["record_ts"]), row["source_file"], row["byte_offset"])
    if new_k > cur_k:
        db.execute(_SQL_CONFLICT, (session_id, key, 0, row["record_ts"], row["source_file"],
                                   row["byte_offset"], row["input_tokens"], row["output_tokens"],
                                   row["cache_read_tokens"], row["cache_creation_tokens"],
                                   row["usage_fp"], "displaced_by_order"))
        db.execute(_SQL_CONFLICT, (session_id, key, 1, ts, source_file, byte_offset,
                                   tokens[0], tokens[1], tokens[2], tokens[3], fp,
                                   "ordered_last_wins"))
        db.execute(_SQL_UPD_WINNER, (ts, source_file, byte_offset,
                                     tokens[0], tokens[1], tokens[2], tokens[3], fp,
                                     row["occurrences"] + 1,
                                     _min_ts(row["first_ts"], ts), _max_ts(row["last_ts"], ts),
                                     row["id"]))
    else:
        db.execute(_SQL_CONFLICT, (session_id, key, 0, ts, source_file, byte_offset,
                                   tokens[0], tokens[1], tokens[2], tokens[3], fp,
                                   "ordered_last_unchanged"))
        _bump_occ(db, row, ts)
        db.execute(_SQL_FLAG, (row["id"],))

    # Reconciliation guard: the ordered winner must also be the max-usage occurrence.
    # Keep-last == max-usage was empirical (732/732), never an invariant (spec §3.2).
    agg = db.execute(_SQL_SEL, (session_id, key)).fetchone()
    mx = db.execute(_SQL_MAX, (session_id, key)).fetchone()
    flag = None
    if mx and mx["m"] is not None and agg is not None:
        if _total((agg["input_tokens"], agg["output_tokens"],
                   agg["cache_read_tokens"], agg["cache_creation_tokens"])) != mx["m"]:
            flag = "order_usage_disagree"
    db.execute("UPDATE api_requests SET reconciliation_flag=?, identity_basis='conflict-resolved' "
               "WHERE id=?", (flag, agg["id"]))
    return ("response", session_id)


def ingest_file(db, path, ts_window=None, backfill=False):
    """Incrementally ingest one transcript file from its stored byte offset; only
    complete lines are consumed. Windowed (backfill) ingest keeps its OWN offsets in
    backfill_state so it can never poison the live ingest_state (critic P1-1).
    Returns dict(records, new_offset) or None."""
    path = str(path)
    state_table = "backfill_state" if (backfill or ts_window) else "ingest_state"
    state = db.execute(_SQL_GET_OFFSET.format(state_table=state_table),
                       (path,)).fetchone()
    offset = int(state["byte_offset"]) if state else 0
    try:
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    if size < offset:  # truncated/rewritten file: restart; occurrence log keeps idempotence
        offset = 0
    window = None
    if ts_window:
        w0 = ts_epoch(ts_window[0]) if ts_window[0] else None
        w1 = ts_epoch(ts_window[1]) if ts_window[1] else None
        window = (w0, w1)
    records = 0
    sessions = set()
    with open(path, "rb") as f:
        f.seek(offset)
        chunk = f.read()
    pos = 0
    while True:
        nl = chunk.find(b"\n", pos)
        if nl < 0:
            break
        line = chunk[pos:nl]
        line_start_offset = offset + pos
        pos = nl + 1
        if line.strip():
            kind, sid = process_line(db, line, path, line_start_offset, window)
            if sid:
                sessions.add(sid)
            records += 1
    new_offset = offset + pos
    db.execute(_SQL_STATE.format(state_table=state_table),
               (path, new_offset, size, mtime, records, now_iso()))
    db.commit()
    return {"records": records, "new_offset": new_offset, "sessions": sessions}
