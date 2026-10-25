"""Independent reconciliation (gate A12): recompute a session's token totals directly
from raw transcript records with a separate minimal implementation, and compare to the
database rollup. Deliberately does NOT import the ingest engine's aggregation logic —
only the identity notion (dedup by message id, keep ordered-last) is mirrored, so a
shared bug cannot hide. Exact equality is required wherever raw usage is complete;
records lacking message id or usage are reported separately as unverifiable.
"""
import json
import os
import sys

from .db import now_iso
from .ingest.transcripts import SUBAGENT_RE, ts_epoch


def _identity(rec):
    msg = rec.get("message") or {}
    return msg.get("id")


def recompute_from_files(files):
    """Independent recompute: {bucket: {responses, input, output, cache_read,
    cache_creation, unverifiable}} where bucket is 'main' or 'sidechain'."""
    agg = {"main": _zero(), "sidechain": _zero()}
    seen = {}  # message_id -> (order_key, tokens) keep ordered-last (cross-file-safe)
    for path, base_offset in files:
        try:
            with open(path, "rb") as f:
                f.seek(base_offset or 0)
                chunk = f.read()
        except OSError:
            continue
        pos = 0
        while True:
            nl = chunk.find(b"\n", pos)
            if nl < 0:
                break
            line = chunk[pos:nl]
            off = (base_offset or 0) + pos
            pos = nl + 1
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if not isinstance(rec, dict) or rec.get("type") != "assistant":
                continue
            msg = rec.get("message") or {}
            usage = msg.get("usage")
            if not isinstance(usage, dict):
                continue
            if msg.get("model") == "<synthetic>" or rec.get("isApiErrorMessage"):
                continue  # errors are never model responses
            mid = msg.get("id")
            side = 1 if SUBAGENT_RE.search(path.replace("\\", "/")) else 0
            bucket = "sidechain" if side else "main"

            def tok(k):
                v = usage.get(k)
                try:
                    return int(v) if v is not None and v != "" else 0
                except (TypeError, ValueError):
                    return 0
            tokens = (tok("input_tokens"), tok("output_tokens"),
                      tok("cache_read_input_tokens"), tok("cache_creation_input_tokens"))
            k = (ts_epoch(rec.get("timestamp")), path, off)
            if not mid:
                agg[bucket]["unverifiable"] += 1
                continue
            if mid not in seen or k > seen[mid][0]:
                seen[mid] = (k, tokens, bucket)
    for _mid, (_k, tokens, bucket) in seen.items():
        a = agg[bucket]
        a["responses"] += 1
        for i, name in enumerate(("input", "output", "cache_read", "cache_creation")):
            a[name] += tokens[i]
    return agg


def _zero():
    return {"responses": 0, "input": 0, "output": 0, "cache_read": 0,
            "cache_creation": 0, "unverifiable": 0}


def db_totals(db, session_id):
    out = {"main": _zero(), "sidechain": _zero()}
    # Only natively-identified rows participate in the EXACT comparison (counts AND
    # token sums); surrogate rows are reported separately as unverifiable (critic P2-6).
    rows = db.execute(
        "SELECT is_sidechain, COUNT(*) n, SUM(input_tokens) i, SUM(output_tokens) o, "
        "SUM(cache_read_tokens) cr, SUM(cache_creation_tokens) cc, "
        "SUM(CASE WHEN identity_key LIKE 'sur:%' THEN 1 ELSE 0 END) sur "
        "FROM v_model_responses WHERE session_id=? "
        "AND (identity_key IS NULL OR identity_key NOT LIKE 'sur:%') "
        "GROUP BY is_sidechain", (session_id,)).fetchall()
    for r in rows:
        b = "sidechain" if r["is_sidechain"] else "main"
        out[b]["responses"] = r["n"] or 0
        out[b]["unverifiable"] = r["sur"] or 0
        out[b]["input"] = r["i"] or 0
        out[b]["output"] = r["o"] or 0
        out[b]["cache_read"] = r["cr"] or 0
        out[b]["cache_creation"] = r["cc"] or 0
    return out


def reconcile_session(db, session_id):
    files = [(r["source_file"], 0) for r in db.execute(
        "SELECT DISTINCT source_file FROM api_requests WHERE session_id=? AND "
        "source_file IS NOT NULL", (session_id,)).fetchall()]
    if not files:
        print("reconcile: no ingested files for session %s (run ingest/backfill first)"
              % session_id)
        return 1
    raw = recompute_from_files(files)
    dbl = db_totals(db, session_id)
    ok = True
    print("reconcile session %s  (%d source files)" % (session_id, len(files)))
    print("%-10s %-12s %-12s %-12s %-12s %-12s" %
          ("bucket", "responses", "input", "output", "cache_read", "cache_create"))
    for bucket in ("main", "sidechain"):
        r = dict(raw[bucket])
        r_unver = r.pop("unverifiable", 0)
        d_unver = dbl[bucket].get("unverifiable", 0)
        d = {k: v for k, v in dbl[bucket].items() if k != "unverifiable"}
        exact = (r == d)
        ok &= exact
        print("%-10s %-12s %-12s %-12s %-12s %-12s  %s" %
              (bucket, r["responses"], r["input"], r["output"], r["cache_read"],
               r["cache_creation"], "EXACT" if exact else "MISMATCH"))
        if not exact:
            print("   raw: %r" % r)
            print("   db : %r" % d)
        unver = r_unver + d_unver
        if unver:
            print("   unverifiable records (missing message id; raw=%d db=%d) — "
                  "reported, excluded from EXACT comparison" % (r_unver, d_unver))
    print("verdict: %s" % ("EXACT — all complete-usage records reconcile" if ok
                           else "MISMATCH — investigate"))
    return 0 if ok else 1
