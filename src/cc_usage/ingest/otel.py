"""OTLP log normalization (secondary/cross-check source; spec v3.1 §1 F5, §16.9).

api_request log events -> otel_api_requests (NEVER api_requests: legacy transcript-only
report semantics must not double-count).
api_error log events   -> provider_events.
tool_result log events -> tool_events (legacy table, as v1 did).
Metrics/traces are accepted (HTTP 200) but not parsed in Phase A (pre-2.1.214 aggregation
risk); the decision is recorded in the delivery matrix (gate A10).
"""
import json


def _attr_value(v):
    if not isinstance(v, dict):
        return v
    for k in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
        if k in v:
            return v[k]
    if "arrayValue" in v:
        return [_attr_value(x) for x in v["arrayValue"].get("values", [])]
    if "kvlistValue" in v:
        return {x.get("key"): _attr_value(x.get("value", {}))
                for x in v["kvlistValue"].get("values", [])}
    return None


def _attrs(items):
    out = {}
    for item in items or []:
        k = item.get("key")
        if k:
            out[k] = _attr_value(item.get("value", {}))
    return out


def _num(v):
    try:
        return int(v) if v is not None and v != "" else 0
    except (TypeError, ValueError):
        return 0


def flatten_log_records(payload):
    """Yield (record, merged_attributes) for resourceLogs/scopeLogs/logRecords."""
    for rl in (payload or {}).get("resourceLogs", []) or []:
        rattrs = _attrs(rl.get("resource", {}).get("attributes"))
        scopes = rl.get("scopeLogs") or rl.get("instrumentationLibraryLogs") or []
        for sl in scopes:
            for rec in sl.get("logRecords", []) or []:
                a = dict(rattrs)
                a.update(_attrs(rec.get("attributes")))
                yield rec, a


def _event_name(rec, a):
    name = a.get("event.name")
    if not name:
        body = _attr_value(rec.get("body") or {})
        name = body if isinstance(body, str) else None
    return str(name or "").replace("claude_code.", "")


def normalize(payload):
    """Return list of typed row dicts: {'kind': 'api_request'|'api_error'|'tool_result', ...}."""
    rows = []
    for rec, a in flatten_log_records(payload):
        event = _event_name(rec, a)
        ts = a.get("event.timestamp")
        common = {"session_id": a.get("session.id") or a.get("session_id"),
                  "ts": ts, "attrs": a}
        if event == "api_request" or event.endswith(".api_request"):
            rows.append(dict(common, kind="api_request",
                             model=a.get("model"),
                             input_tokens=_num(a.get("input_tokens")),
                             output_tokens=_num(a.get("output_tokens")),
                             cache_read_tokens=_num(a.get("cache_read_tokens")),
                             cache_creation_tokens=_num(a.get("cache_creation_tokens")),
                             cost_usd=float(a.get("cost_usd") or 0),
                             duration_ms=_num(a.get("duration_ms")),
                             request_id=a.get("request_id"),
                             prompt_id=a.get("prompt.id") or a.get("prompt_id")))
        elif event == "api_error" or event.endswith(".api_error"):
            detail = a.get("error")
            rows.append(dict(common, kind="api_error",
                             model=a.get("model"),
                             code=str(a.get("status_code") or ""),
                             detail=(str(detail)[:500] if detail is not None else None),
                             duration_ms=_num(a.get("duration_ms")),
                             attempt=a.get("attempt")))
        elif event == "tool_result" or event.endswith(".tool_result"):
            tool = a.get("tool_name")
            if tool:
                rows.append(dict(common, kind="tool_result", tool_name=str(tool),
                                 success=str(a.get("success", "")),
                                 duration_ms=_num(a.get("duration_ms")),
                                 skill=a.get("skill.name") or a.get("skill_name")))
    return rows


def insert_rows(db, rows):
    """Write normalized rows through the single writer connection."""
    for r in rows:
        if r["kind"] == "api_request":
            db.execute(
                "INSERT INTO otel_api_requests(session_id,ts,model,input_tokens,output_tokens,"
                "cache_read_tokens,cache_creation_tokens,cost_usd,duration_ms,request_id,"
                "prompt_id,attrs_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (r["session_id"], r["ts"], r["model"], r["input_tokens"], r["output_tokens"],
                 r["cache_read_tokens"], r["cache_creation_tokens"], r["cost_usd"],
                 r["duration_ms"], r["request_id"], r["prompt_id"],
                 json.dumps(r["attrs"], separators=(",", ":"), default=str)))
        elif r["kind"] == "api_error":
            db.execute(
                "INSERT INTO provider_events(ts,provider,gateway,session_id,kind,code,detail,"
                "source,raw_json) VALUES(?,?,?,?,?,?,?,?,?)",
                (r["ts"], None, None, r["session_id"], "api_error", r.get("code") or None,
                 r.get("detail"), "otel",
                 json.dumps({"model": r.get("model"), "attempt": r.get("attempt"),
                             "duration_ms": r.get("duration_ms")},
                            separators=(",", ":"))))
        elif r["kind"] == "tool_result":
            db.execute(
                "INSERT INTO tool_events(session_id,prompt_id,ts,launcher,provider,tool_name,"
                "success,duration_ms,skill_name,jira_ticket) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (r["session_id"], None, r["ts"], None, None, r["tool_name"],
                 r.get("success", ""), r.get("duration_ms", 0), r.get("skill"), None))
    db.commit()
