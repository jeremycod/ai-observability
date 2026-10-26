"""Dashboard queries — pure functions over a read-only DB connection (spec v3.1 §10,
§12). Every derived/estimated figure carries its basis so the UI can tag it. No view
may ever join quota_windows to accounting (shared-window rule, §8)."""
import datetime as dt
import json
import sqlite3

from ..ingest.transcripts import ts_epoch


def _rowfactory(db):
    """Queries index rows by name; enforce Row regardless of the caller's connection
    settings (raw sqlite3.connect in tests/receivers defaults to tuples)."""
    if not isinstance(db.row_factory, type(sqlite3.Row)):
        db.row_factory = sqlite3.Row
    return db

DAYS = 30
CONCURRENCY_BUCKETS = ((1, 2), (3, 4), (5, 7), (8, 999))
BUCKET_LABELS = {(1, 2): "1-2", (3, 4): "3-4", (5, 7): "5-7", (8, 999): "8+"}


def _since(days):
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).isoformat()



def _dev(device):
    """SQL fragment + params for an optional device filter."""
    if device and device != "all":
        return (" AND device_id = ?", [device])
    return "", []


def devices(db):
    _rowfactory(db)
    return [r["device_id"] for r in db.execute(
        "SELECT DISTINCT device_id FROM sessions WHERE device_id IS NOT NULL "
        "ORDER BY device_id")]


def summary(db, days=DAYS, device=None):
    _rowfactory(db)
    start = _since(days)
    df, dp = _dev(device)
    row = db.execute(
        "SELECT COUNT(*) responses, COALESCE(SUM(input_tokens),0) i, "
        "COALESCE(SUM(output_tokens),0) o, COALESCE(SUM(cache_read_tokens),0) cr, "
        "COALESCE(SUM(cache_creation_tokens),0) cc, "
        "SUM(CASE WHEN provider_reported IS NOT NULL AND provider_reported != 'unknown' "
        "THEN 1 ELSE 0 END) with_provider, "
        "SUM(CASE WHEN prompt_uuid IS NOT NULL THEN 1 ELSE 0 END) with_prompt, "
        "SUM(CASE WHEN phase_id IS NOT NULL THEN 1 ELSE 0 END) with_phase, "
        "SUM(CASE WHEN latency_basis='measured_api' THEN 1 ELSE 0 END) measured "
        "FROM (SELECT *, provider AS provider_reported FROM api_requests "
        "WHERE source='transcript' AND record_ts >= ?" + df + ")",
        [start] + dp).fetchone()
    total = row["responses"] or 0
    cov = lambda n: round(100.0 * (n or 0) / total, 1) if total else 0.0
    otel = db.execute("SELECT COUNT(*) n, AVG(duration_ms) d FROM otel_api_requests "
                      "WHERE ts >= ?" + df, [start] + dp).fetchone()
    return {
        "window_days": days,
        "responses": total,
        "tokens": {"input": row["i"], "output": row["o"], "cache_read": row["cr"],
                   "cache_creation": row["cc"],
                   "total": row["i"] + row["o"] + row["cr"] + row["cc"]},
                "sessions": db.execute("SELECT COUNT(*) n FROM sessions WHERE started_at >= ?"
                               + df,
                               [start] + dp).fetchone()["n"],
        "coverage": {  # §12 provenance eras: what fraction carries each attribution
            "provider_identity": cov(row["with_provider"]),
            "prompt_linkage": cov(row["with_prompt"]),
            "phase_attribution": cov(row["with_phase"]),
            "measured_latency": cov(row["measured"]),
        },
        "otlp_crosscheck": {"rows": otel["n"],
                            "avg_duration_ms": round(otel["d"] or 0)},
        "basis": "transcript rows are the source of record; OTLP is a separate cross-check series",
    }


def providers(db, days=DAYS, device=None):
    _rowfactory(db)
    start = _since(days)
    df, dp = _dev(device)
    rows = db.execute(
        "SELECT COALESCE(NULLIF(launcher,''),'(pre-instrumentation)') launcher, "
        "COALESCE(NULLIF(provider,''),'unknown') provider, COUNT(*) responses, "
        "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) tokens, "
        "SUM(CASE WHEN latency_basis='measured_api' THEN 1 ELSE 0 END) measured "
        "FROM api_requests WHERE source='transcript' AND record_ts >= ?" + df +
        " GROUP BY 1,2 ORDER BY tokens DESC", [start] + dp).fetchall()
    out = []
    for r in rows:
        out.append({"launcher": r["launcher"], "provider": r["provider"],
                    "responses": r["responses"], "tokens": r["tokens"] or 0,
                    "measured_responses": r["measured"] or 0,
                    "basis": "observed (transcript usage)"})
    # PR outcomes with the shared-bucket rule (spec §7)
    prs = db.execute("SELECT number, merged, review_rounds FROM pull_requests "
                     "WHERE created_at >= ? AND ticket_keys != '[]'", (start,)).fetchall()
    buckets = {}
    for pr in prs:
        dfc, dpc = _dev(device)
        provs = {c["provider"] for c in db.execute(
            "SELECT DISTINCT provider FROM pr_contributions WHERE pr_number=? "
            "AND provider != 'unknown'" + dfc, [pr["number"]] + dpc).fetchall()}
        key = "shared" if len(provs) > 1 else (provs.pop() if provs else "none")
        b = buckets.setdefault(key, {"prs": 0, "merged": 0, "rounds": 0})
        b["prs"] += 1
        b["merged"] += pr["merged"]
        b["rounds"] += pr["review_rounds"]
    for k, v in buckets.items():
        v["avg_rounds"] = round(v["rounds"] / v["prs"], 2) if v["prs"] else 0
        v["basis"] = "PR outcome counted once; multi-provider PRs land in 'shared'"
        out_bucket = {"launcher": "PRs: %s" % k, "provider": k}
        out.append(dict(out_bucket, responses=v["prs"], tokens=None,
                        merged=v["merged"], avg_rounds=v["avg_rounds"], basis=v["basis"],
                        kind="pr_outcome"))
    return out


def phases(db, days=DAYS, ticket=None, device=None):
    _rowfactory(db)
    params = [_since(days)]
    df, dp = _dev(device)
    q = ("SELECT COALESCE(l.primary_ticket,'(no ticket)') ticket, p.phase, "
         "COUNT(r.id) responses, "
         "SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_creation_tokens) tokens "
         "FROM api_requests r JOIN workflow_phases p ON p.id=r.phase_id "
         "LEFT JOIN work_lineages l ON l.lineage_id=p.lineage_id "
         "WHERE r.source='transcript' AND r.record_ts >= ?" + df)
    params += dp
    if ticket:
        q += " AND l.primary_ticket=?"
        params.append(ticket)
    q += " GROUP BY 1,2 ORDER BY tokens DESC"
    rows = db.execute(q, params).fetchall()
    return [{"ticket": r["ticket"], "phase": r["phase"], "responses": r["responses"],
             "tokens": r["tokens"] or 0,
             "basis": "phase spans close at next command or >=4h idle gap; durations are "
                      "a timeline, not disjoint wall-clock"} for r in rows]


def session_timeline(db, session_id):
    _rowfactory(db)
    srow = db.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
    if not srow:
        return None
    phases = db.execute(
        "SELECT id, phase, started_at, ended_at, trigger_kind, confidence, basis "
        "FROM workflow_phases WHERE session_id=? ORDER BY started_at", (session_id,)).fetchall()
    responses = db.execute(
        "SELECT record_ts, model, input_tokens i, output_tokens o, cache_read_tokens cr, "
        "cache_creation_tokens cc, occurrences, identity_conflict, is_sidechain, agent_id, "
        "phase_id, latency_basis FROM api_requests WHERE session_id=? AND source='transcript' "
        "ORDER BY record_ts", (session_id,)).fetchall()
    ph_by_id = {p["id"]: p["phase"] for p in phases}
    subs = db.execute(
        "SELECT agent_id, agent_type, COUNT(*) responses, "
        "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) tokens "
        "FROM api_requests WHERE parent_session_id=? AND is_sidechain=1 GROUP BY 1,2",
        (session_id,)).fetchall()
    errors = db.execute("SELECT COUNT(*) n FROM api_errors WHERE session_id=?",
                        (session_id,)).fetchone()["n"]
    return {
        "session": dict(srow),
        "phases": [dict(p) for p in phases],
        "responses": [{"ts": r["record_ts"], "model": r["model"],
                       "tokens": (r["i"] or 0) + (r["o"] or 0) + (r["cr"] or 0) + (r["cc"] or 0),
                       "occurrences": r["occurrences"], "conflict": r["identity_conflict"],
                       "sidechain": r["is_sidechain"], "agent_id": r["agent_id"],
                       "phase": ph_by_id.get(r["phase_id"]),
                       "latency_basis": r["latency_basis"]} for r in responses],
        "subagents": [dict(x) for x in subs],
        "api_errors": errors,
        "basis": "no prompt/assistant text exists in this database (spec §11)",
    }


def concurrency(db, days=DAYS, device=None):
    _rowfactory(db)
    """Hourly overlap counts bucketed 1-2 / 3-4 / 5-7 / 8+ (mission §7)."""
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    df, dp = _dev(device)
    rows = db.execute("SELECT session_id, started_at, ended_at FROM sessions "
                      "WHERE started_at >= ?" + df, [_since(days)] + dp).fetchall()
    intervals = []
    for r in rows:
        s = ts_epoch(r["started_at"])
        if not s:
            continue
        e = ts_epoch(r["ended_at"]) or dt.datetime.now(dt.timezone.utc).timestamp()
        intervals.append((s, max(e, s + 60)))
    buckets = {BUCKET_LABELS[b]: 0 for b in CONCURRENCY_BUCKETS}
    step = 3600
    t0 = int(start.timestamp()) // step * step
    t1 = int(dt.datetime.now(dt.timezone.utc).timestamp()) // step * step
    hours = 0
    for slot in range(t0, t1, step):
        n = sum(1 for (s, e) in intervals if s <= slot < e)
        if n == 0:
            continue
        hours += 1
        for b in CONCURRENCY_BUCKETS:
            if b[0] <= n <= b[1]:
                buckets[BUCKET_LABELS[b]] += 1
                break
    total = sum(buckets.values()) or 1
    return {"hours_active": hours,
            "buckets": {k: {"hours": v, "pct": round(100.0 * v / total, 1)}
                        for k, v in buckets.items()},
            "basis": "hourly overlap of session intervals; Orca journal join is Phase E+"}


def trends(db, days=DAYS, device=None):
    _rowfactory(db)
    df, dp = _dev(device)
    rows = db.execute(
        "SELECT substr(record_ts,1,10) day, COALESCE(NULLIF(provider,''),'unknown') provider, "
        "COUNT(*) responses, "
        "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) tokens "
        "FROM api_requests WHERE source='transcript' AND record_ts >= ?" + df +
        " GROUP BY 1,2 ORDER BY 1", [_since(days)] + dp).fetchall()
    lat = db.execute(
        "SELECT substr(ts,1,10) day, model, AVG(duration_ms) ms, COUNT(*) n "
        "FROM otel_api_requests WHERE ts >= ?" + df + " GROUP BY 1,2 ORDER BY 1",
        [_since(days)] + dp).fetchall()
    return {"daily_tokens": [dict(r) for r in rows],
            "daily_measured_latency": [{"day": r["day"], "model": r["model"],
                                        "avg_ms": round(r["ms"] or 0), "n": r["n"]}
                                       for r in lat],
            "basis": "token series observed; latency measured_api (OTLP) only accrues "
                     "since the 2026-10-25 chunked-delivery fix"}


def quota(db):
    _rowfactory(db)
    wins = db.execute("SELECT * FROM quota_windows ORDER BY imported_at DESC LIMIT 12").fetchall()
    evs = db.execute("SELECT ts, kind, code, reset_at FROM provider_events "
                     "WHERE kind LIKE 'quota%' OR kind='rate_limit' "
                     "ORDER BY ts DESC LIMIT 12").fetchall()
    return {"windows": [dict(r) for r in wins],
            "events": [dict(e) for e in evs],
            "caption": "NO token<->quota conversion exists; multi-session windows are "
                       "SHARED EVIDENCE (spec §8) — capacity from snapshots only"}


def prs(db, days=DAYS, device=None):
    _rowfactory(db)
    rows = db.execute(
        "SELECT number, title, branch, base_branch, base_pr_number, is_stacked_leaf, state, "
        "merged, merged_at, created_at, review_rounds, net_add, net_del, ticket_keys "
        "FROM pull_requests WHERE created_at >= ? ORDER BY number DESC", (_since(days),)).fetchall()
    out = []
    for r in rows:
        dfc, dpc = _dev(device)
        provs = {c["provider"] for c in db.execute(
            "SELECT DISTINCT provider FROM pr_contributions WHERE pr_number=? "
            "AND provider != 'unknown'" + dfc, [r["number"]] + dpc).fetchall()}
        d = dict(r)
        d["providers"] = sorted(provs)
        d["bucket"] = "shared" if len(provs) > 1 else (next(iter(provs), "none"))
        d["basis"] = "outcome counted once; net LOC excludes generated files; merged != correct"
        out.append(d)
    return out
