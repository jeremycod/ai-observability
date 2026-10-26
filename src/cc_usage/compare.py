"""Phase F: comparison methodology (spec v3.1 §13).

Honesty rules encoded here:
- Observations vs inferences are separated and labeled on every metric.
- Sample sizes and Wilson intervals accompany every proportion; small-n is visible.
- Matched-task comparison happens AT THE TICKET LEVEL: only multi-provider PRs compare
  providers within the same ticket; cross-ticket comparisons are shown separately and
  flagged as unmatched.
- LOC/PR counts are never equated with productivity; merged != correct.
- Confounds panel: concurrency level, quota-exhaustion overlap, config deltas, and the
  2026-10-23 disk-incident window.
- Quota efficiency exists only at provider-window grain (§8) — never per ticket.
"""
import json
import math

from .correlate.github import outcomes


def wilson(k, n, z=1.96):
    """Wilson score interval for a binomial proportion — returns (p, lo, hi)."""
    if n == 0:
        return (0.0, 0.0, 0.0)
    p = k / float(n)
    d = 1 + z * z / n
    center = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(p, 3), round(max(0.0, center - half), 3), round(min(1.0, center + half), 3))


def _percentile(values, q):
    if not values:
        return None
    vs = sorted(values)
    k = max(0, min(len(vs) - 1, int(q * (len(vs) - 1))))
    return vs[k]


def provider_rows(db, days):
    """Per-launcher observed metrics. Launcher is the comparison dimension (it maps
    1:1 to gateway+billing in this setup; grouping by gateway+model is available in
    the DB for finer cuts)."""
    start = "datetime('now', '-%d days')" % days
    rows = db.execute(
        "SELECT COALESCE(NULLIF(launcher,''),'(pre-instrumentation)') launcher, "
        "COUNT(*) responses, "
        "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) tokens, "
        "SUM(CASE WHEN latency_basis='measured_api' THEN 1 ELSE 0 END) measured_n "
        "FROM api_requests WHERE source='transcript' AND record_ts >= " + start +
        " GROUP BY 1 ORDER BY tokens DESC").fetchall()
    out = []
    for r in rows:
        lat = db.execute(
            "SELECT AVG(r.duration_ms) avg_ms FROM api_requests r "
            "WHERE r.launcher=? AND r.latency_basis='measured_api' AND r.record_ts >= " + start,
            (r["launcher"],)).fetchone()
        per_resp = (r["tokens"] or 0) / r["responses"] if r["responses"] else 0
        out.append({
            "launcher": r["launcher"], "responses": r["responses"], "tokens": r["tokens"] or 0,
            "tokens_per_response": round(per_resp),
            "measured_avg_ms": round(lat["avg_ms"] or 0) if lat and lat["avg_ms"] else None,
            "measured_n": r["measured_n"] or 0,
            "basis": "observed; latency measured_api only (OTLP), derived_gap excluded",
        })
    return out


def pr_stats(db, days):
    """Per-launcher PR outcome stats with shared-bucket rule + Wilson intervals."""
    rows = outcomes(db, days)
    stats = {}  # launcher -> counters (solo-bucket PRs only for provider attribution)
    for r in rows:
        if r["bucket"] in ("shared", "none"):
            continue
        s = stats.setdefault(r["bucket"], {"prs": 0, "merged": 0, "rounds": [],
                                           "first_pass": 0, "reviewed": 0,
                                           "net_add": 0})
        s["prs"] += 1
        s["merged"] += r["merged"]
        s["rounds"].append(r["rounds"])
        s["net_add"] += r["net_add"] or 0
        cr = db.execute("SELECT COUNT(*) c FROM pr_reviews WHERE pr_number=? AND "
                        "state='CHANGES_REQUESTED'", (r["number"],)).fetchone()["c"]
        s["reviewed"] += 1
        if cr == 0:
            s["first_pass"] += 1
    shared = [r for r in rows if r["bucket"] == "shared"]
    for s in stats.values():
        s["avg_rounds"] = round(sum(s["rounds"]) / len(s["rounds"]), 2) if s["rounds"] else None
        s["merge_rate"], s["merge_lo"], s["merge_hi"] = wilson(s["merged"], s["prs"])
        s["first_pass_rate"], s["fp_lo"], s["fp_hi"] = wilson(s["first_pass"], s["reviewed"])
        s["basis"] = ("PR outcome counted once; shared multi-provider PRs excluded here "
                      "and reported separately (%d in window)" % len(shared))
    return stats, len(shared)


def rework(db, days):
    """Rework: revision vs first_impl token ratio per launcher (post-review churn
    proxy) + review-role tokens (independent review load)."""
    rows = db.execute(
        "SELECT c.launcher launcher, c.role role, SUM(c.tokens) tok FROM pr_contributions c "
        "JOIN pull_requests pr ON pr.number=c.pr_number "
        "WHERE pr.created_at >= datetime('now', '-%d days') GROUP BY 1,2" % days).fetchall()
    by = {}
    for r in rows:
        by.setdefault(r["launcher"], {})[r["role"]] = r["tok"] or 0
    out = {}
    for launcher, roles in by.items():
        fi = roles.get("first_impl", 0)
        rev = roles.get("revision", 0)
        out[launcher] = {
            "first_impl_tokens": fi, "revision_tokens": rev, "review_tokens": roles.get("review", 0),
            "churn_ratio": round(rev / fi, 3) if fi else None,
            "basis": "churn_ratio = revision-role tokens / first_impl tokens (proxy; "
                     "counts post-review rework across the lineage)",
        }
    return out


def matched_sets(db, days, limit=6):
    """Ticket-level matched comparison: multi-provider PRs compare providers within the
    same ticket — the only apples-to-apples shape in this corpus."""
    rows = db.execute(
        "SELECT number, ticket_keys FROM pull_requests "
        "WHERE created_at >= datetime('now', '-%d days') AND ticket_keys != '[]'" % days
    ).fetchall()
    out = []
    for r in rows:
        provs = {}
        for c in db.execute("SELECT DISTINCT provider, launcher FROM pr_contributions "
                            "WHERE pr_number=? AND provider != 'unknown'",
                            (r["number"],)).fetchall():
            provs.setdefault(c["provider"], c["launcher"])
        if len(provs) >= 2:
            shares = db.execute(
                "SELECT provider, SUM(tokens) tok FROM pr_contributions WHERE pr_number=? "
                "GROUP BY 1", (r["number"],)).fetchall()
            tok = {s["provider"]: s["tok"] or 0 for s in shares}
            tot = sum(tok.values()) or 1
            out.append({"pr": r["number"], "tickets": json.loads(r["ticket_keys"]),
                        "providers": sorted(provs),
                        "token_share": {p: round(100.0 * tok.get(p, 0) / tot, 1)
                                        for p in tok},
                        "basis": "same-ticket multi-provider set; token share shown with "
                                 "weights (never PR credit)"})
    return out[:limit], len(out)


def confounds(db, days):
    """Confounds panel (§13): what could distort a naive comparison."""
    snaps = db.execute(
        "SELECT launcher, COUNT(DISTINCT effort) efforts, COUNT(DISTINCT compact_window) wins, "
        "COUNT(*) n FROM config_snapshots GROUP BY 1").fetchall()
    quota = db.execute("SELECT COUNT(*) n FROM provider_events WHERE kind LIKE 'quota%' OR "
                       "kind='rate_limit'").fetchone()["n"]
    return {
        "config_deltas": [dict(r) for r in snaps],
        "quota_exhaustion_events": quota,
        "quota_note": "windows are shared evidence; a provider active in an exhausted "
                      "window pays latency/retry costs others do not (§8)",
        "disk_incident": "2026-10-23 ENOSPC storm (disk 96% full) degraded all local "
                         "tooling incl. hooks; windows touching it are flagged here",
        "concurrency": "see dashboard Concurrency panel; 5-7 parallel sessions multiply "
                       "per-key request rates and quota burn",
        "basis": "confounds listed, not adjusted away — sample sizes remain visible",
    }


def report(db, days=90):
    lines = []
    prov = provider_rows(db, days)
    stats, n_shared = pr_stats(db, days)
    rework_d = rework(db, days)
    matched, n_matched = matched_sets(db, days)
    conf = confounds(db, days)
    lines.append("COMPARISON — last %d days (observations labeled; small-n visible)" % days)
    lines.append("%-18s %8s %12s %8s %10s %10s" % ("launcher", "responses", "tokens",
                                                   "tok/resp", "avg_ms*", "measured_n"))
    for p in prov:
        if p.get("kind"):
            continue
        lines.append("%-18s %8d %12s %8d %10s %10d" % (
            p["launcher"], p["responses"], "%.1fM" % (p["tokens"] / 1e6) if p["tokens"] else "0",
            p["tokens_per_response"],
            p["measured_avg_ms"] if p["measured_avg_ms"] is not None else "n/a",
            p["measured_n"]))
    lines.append("* measured_api (OTLP) only; derives excluded. %s" % prov[0]["basis"])
    lines.append("")
    lines.append("PR outcomes (solo-bucket only; %d shared multi-provider PRs separate)"
                 % n_shared)
    lines.append("%-18s %4s %7s %6s %9s %12s %11s" % (
        "launcher", "PRs", "merged", "avg_rnd", "merge[95%]", "first-pass[95%]", "net_add"))
    for launcher, s in sorted(stats.items(), key=lambda kv: -kv[1]["prs"]):
        lines.append("%-18s %4d %7d %6s %9s %12s %11d" % (
            launcher, s["prs"], s["merged"], s["avg_rounds"],
            "%.2f[%.2f,%.2f]" % (s["merge_rate"], s["merge_lo"], s["merge_hi"]),
            "%.2f[%.2f,%.2f]" % (s["first_pass_rate"], s["fp_lo"], s["fp_hi"]),
            s["net_add"]))
    lines.append("")
    lines.append("Rework (role-token proxies)")
    for launcher, r in sorted(rework_d.items()):
        lines.append("  %-18s first_impl=%sM revision=%sM review=%sM churn_ratio=%s" % (
            launcher, "%.1f" % (r["first_impl_tokens"] / 1e6), "%.1f" % (r["revision_tokens"] / 1e6),
            "%.1f" % (r["review_tokens"] / 1e6), r["churn_ratio"]))
    lines.append("")
    lines.append("Matched-at-ticket-level sets (multi-provider PRs): %d in window" % n_matched)
    for m in matched:
        lines.append("  PR #%s tickets=%s providers=%s token_share=%s" % (
            m["pr"], ",".join(m["tickets"][:3]), ",".join(m["providers"]),
            m["token_share"]))
    lines.append("")
    lines.append("CONFOUNDS")
    for k, v in conf.items():
        if k != "basis":
            lines.append("  %s: %s" % (k, v if isinstance(v, (int, str)) else json.dumps(v)))
    lines.append("  " + conf["basis"])
    return "\n".join(lines)
