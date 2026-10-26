"""VALUE panel: deliverable-denominated efficiency — the user's actual questions (§0).

Q-a  How many tokens do I get for the price?         -> tokens/$1k-lines + $/1k-lines
Q-b  How fast is a PR (e.g. ~1000 lines) written?    -> wall-clock per PR and per 1k lines
Q-c  How much does one ACTION burn?                  -> tokens & minutes per command run
Q-d  Which provider gives best value?                -> side-by-side, every figure labeled

Measurement contracts:
- "Lines" = PR net additions + deletions EXCLUDING generated files (Cargo.lock etc.).
- "Tokens" = contribution tokens (all roles) attributed to that PR's sessions.
- Solo-bucket merged PRs are the primary population (provider attribution is unambiguous);
  shared multi-provider PRs are reported separately with token-share weights.
- Speed = gh truth (created_at -> merged_at). Active-session time is shown separately
  (sum of session spans) — wall time includes review waiting, sessions include idle.
- GLM has NO per-token price: amortized $/line uses your invoice (labeled allocation);
  marginal cash while windows have spare capacity is $0 — and the quota-exhaustion
  events are the real cost of that (unpriced, §8). DeepSeek uses estimated rates
  (labeled approximate, off-peak applied).
"""
import datetime as dt

from . import cost
from .correlate.github import outcomes
from .ingest.transcripts import ts_epoch


def _blended_rates(db):
    """Provider blended $/M tokens from the actual cost estimate over all rows — used
    to price individual PRs (labeled estimate)."""
    rows = db.execute(
        "SELECT provider, model, input_tokens i, output_tokens o, cache_read_tokens cr, "
        "cache_creation_tokens cc, record_ts ts FROM v_model_responses").fetchall()
    est_rows = [(r["provider"], r["model"], r["i"] or 0, r["o"] or 0, r["cr"] or 0,
                 r["cc"] or 0, ts_epoch(r["ts"])) for r in rows]
    est, _, _ = cost.estimate_provider_usd(db, est_rows)
    by_provider = {}
    for r in est_rows:
        if r[0] not in cost.PLAN_PROVIDERS:
            by_provider[r[0]] = by_provider.get(r[0], 0) + (r[2] + r[3] + r[4] + r[5])
    rates = {p: (est / toks * 1e6) if toks else None for p, toks in by_provider.items()}
    return rates, est


def _invoice_rate(db, days):
    """GLM amortized $/M over the window (allocation, never marginal)."""
    inv = cost.get_setting(db, cost.INVOICE_KEY)
    try:
        inv = float(inv)
    except (TypeError, ValueError):
        return None, None
    row = db.execute(
        "SELECT COALESCE(SUM(input_tokens+output_tokens+cache_read_tokens"
        "+cache_creation_tokens),0) t FROM v_model_responses "
        "WHERE provider='zai' AND record_ts >= datetime('now', ?)",
        ("-%d days" % days,)).fetchone()
    toks = row["t"] or 0
    if toks <= 0:
        return None, inv
    daily = inv / 30.0
    return (daily * days) / toks * 1e6, inv


def value_report(db, days=30):
    since = "datetime('now', '-%d days')" % days
    rates, api_est_total = _blended_rates(db)
    glm_rate, invoice = _invoice_rate(db, days)
    out = {"window_days": days, "rates": {
        "deepseek_blended_usd_per_M": round(rates.get("deepseek"), 4) if rates.get("deepseek") else None,
        "glm_amortized_usd_per_M": round(glm_rate, 4) if glm_rate else None,
        "glm_invoice_on_file": invoice,
        "basis": "deepseek = estimated (approximate prices, off-peak applied); "
                 "glm = ALLOCATION of your invoice, never marginal cost"}}

    # ---- per-provider deliverable efficiency over solo merged PRs ----
    prs = db.execute(
        "SELECT number, merged, net_add, net_del, created_at, merged_at, ticket_keys "
        "FROM pull_requests WHERE created_at >= " + since +
        " AND ticket_keys != '[]'").fetchall()
    prov_stats = {}
    shared_prs = []
    for pr in prs:
        contrib = db.execute(
            "SELECT provider, SUM(tokens) tok, MIN(first_ts) f, MAX(last_ts) l "
            "FROM pr_contributions WHERE pr_number=? GROUP BY provider",
            (pr["number"],)).fetchall()
        provs = {c["provider"] for c in contrib if c["provider"] not in ("unknown", None)}
        if len(provs) > 1:
            shares = {c["provider"]: (c["tok"] or 0) for c in contrib}
            tot = sum(shares.values()) or 1
            shared_prs.append({"pr": pr["number"],
                               "share": {p: round(100.0 * t / tot, 1) for p, t in shares.items()}})
            continue
        if not provs:
            continue
        p = next(iter(provs))
        if not pr["merged"]:
            continue
        s = prov_stats.setdefault(p, {"prs": 0, "tokens": 0, "net_lines": 0,
                                      "wall_hours": [], "active_hours": []})
        s["prs"] += 1
        s["tokens"] += next(c["tok"] for c in contrib if c["provider"] == p)
        s["net_lines"] += (pr["net_add"] or 0) + (pr["net_del"] or 0)
        if pr["merged_at"] and pr["created_at"]:
            h = (ts_epoch(pr["merged_at"]) - ts_epoch(pr["created_at"])) / 3600.0
            if 0 < h < 24 * 30:  # sanity
                s["wall_hours"].append(round(h, 2))
        c_all = [c for c in contrib]
        if c_all:
            f = min((ts_epoch(c["f"]) for c in c_all if c["f"]), default=None)
            l = max((ts_epoch(c["l"]) for c in c_all if c["l"]), default=None)
            if f and l and l > f:
                s["active_hours"].append(round((l - f) / 3600.0, 2))

    def _avg(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else None

    table = []
    glm_list_usd, glm_list_rate, _mix = cost.glm_list_equivalent(db, days)
    for p, s in prov_stats.items():
        klines = s["net_lines"] / 1000.0
        list_rate = glm_list_rate if p in cost.PLAN_PROVIDERS else rates.get(p)
        list_basis = ("API list-rate equivalent [estimate]" if p in cost.PLAN_PROVIDERS
                      else "estimated API")
        cost_per_1k = (s["tokens"] / 1e6 * list_rate / klines) if (list_rate and klines) else None
        cost_per_pr = (s["tokens"] / 1e6 * list_rate / s["prs"]) if (list_rate and s["prs"]) else None
        tok_per_1k = (s["tokens"] / klines) if klines else None
        amort_per_1k = amort_per_pr = None
        if p in cost.PLAN_PROVIDERS and glm_rate:
            amort_per_1k = round(s["tokens"] / 1e6 * glm_rate / klines, 2) if klines else None
            amort_per_pr = round(s["tokens"] / 1e6 * glm_rate / s["prs"], 2) if s["prs"] else None
        table.append({
            "provider": p, "merged_prs": s["prs"], "tokens": s["tokens"],
            "net_lines": s["net_lines"],
            "tokens_per_1k_lines": round(tok_per_1k) if tok_per_1k else None,
            "cost_basis": list_basis,
            "cost_per_1k_lines": round(cost_per_1k, 2) if cost_per_1k is not None else None,
            "cost_per_pr": round(cost_per_pr, 2) if cost_per_pr is not None else None,
            "amortized_per_1k_lines": amort_per_1k,
            "amortized_per_pr": amort_per_pr,
            "wall_h_per_pr_median": _avg(s["wall_hours"]),
            "active_h_per_pr_median": _avg(s["active_hours"]),
            "basis": "solo merged PRs only (%d); lines exclude generated files" % s["prs"],
        })
    out["deliverables"] = table
    out["glm_list_equivalent"] = {"usd_30d_window": round(glm_list_usd, 2),
                                  "rate_per_M": round(glm_list_rate, 4) if glm_list_rate else None,
                                  "version": cost.GLM_LIST_VERSION,
                                  "note": "what GLM tokens would cost at Z.ai pay-per-token "
                                          "API list rates — NOT your plan bill"}
    out["shared_multi_provider_prs"] = {"count": len(shared_prs), "samples": shared_prs[:6]}

    # ---- per-action burn: one run of each command ----
    actions = db.execute(
        "SELECT p.trigger_kind trig, COUNT(*) spans, "
        "COUNT(DISTINCT p.session_id) sessions, "
        "SUM((SELECT COUNT(*) FROM api_requests r WHERE r.phase_id=p.id)) responses, "
        "SUM((SELECT COALESCE(SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens"
        "+r.cache_creation_tokens),0) FROM api_requests r WHERE r.phase_id=p.id)) tokens "
        "FROM workflow_phases p WHERE p.trigger_kind LIKE 'command:%' "
        "GROUP BY p.trigger_kind").fetchall()
    at = []
    for a in actions:
        cmd = a["trig"].replace("command:", "")
        runs = a["spans"]
        at.append({"command": cmd, "runs": runs, "sessions": a["sessions"],
                   "responses": a["responses"] or 0, "tokens": a["tokens"] or 0,
                   "tokens_per_run": round((a["tokens"] or 0) / runs) if runs else None,
                   "basis": "tokens of the phase span owned by each run (spans close at "
                            "next command or 4h idle)"})
    at.sort(key=lambda x: -(x["tokens"] or 0))
    out["actions"] = at
    out["actions_caption"] = ("per-RUN cost of each workflow action — e.g. 'one "
                              "challenge-artifact run costs about N tokens (M tokens "
                              "across R runs)'")
    return out


def print_report(db, days=30):
    import json
    v = value_report(db, days)
    print("VALUE — last %d days (your questions, in deliverable units)" % days)
    print()
    def _hm(n):
        if n is None:
            return "—"
        return ("%.1fM" % (n / 1e6)) if n >= 1e6 else (("%.1fK" % (n / 1e3)) if n >= 1e3 else str(int(n)))
    print("A. Tokens & cost per 1,000 delivered lines (solo merged PRs)")
    print("   (tokens = input + cache-read + output; on GLM cache-read is quota, on DS it is ~$0.003/M)")
    print("%-10s %5s %10s %9s %9s %30s %10s %9s %16s" % (
        "provider", "PRs", "tokens", "net lines", "tok/1kL", "cost basis",
        "$/1kL", "$/PR", "$/1kL amortized"))
    for t in v["deliverables"]:
        print("%-10s %5d %10s %9d %9s %30s %10s %9s %16s" % (
            t["provider"], t["merged_prs"], "%.1fM" % (t["tokens"] / 1e6),
            t["net_lines"], _hm(t["tokens_per_1k_lines"]), t["cost_basis"],
            ("$%.2f" % t["cost_per_1k_lines"]) if t["cost_per_1k_lines"] is not None else "—",
            ("$%.2f" % t["cost_per_pr"]) if t["cost_per_pr"] is not None else "—",
            ("$%.2f" % t["amortized_per_1k_lines"]) if t.get("amortized_per_1k_lines")
            is not None else "—"))
    gle = v.get("glm_list_equivalent") or {}
    if gle.get("usd_30d_window") is not None:
        print("   GLM list-rate equivalent: $%.2f for the window's GLM tokens (blended "
              "$%s/M) — what they WOULD cost at Z.ai pay-per-token API rates; your plan "
              "bill is the invoice, not this [estimate; 50%% promo not applied]" % (
                  gle["usd_30d_window"], gle["rate_per_M"]))
    if v["rates"].get("glm_invoice_on_file"):
        print("   amortized column uses your $%s invoice at period grain (allocation, "
              "never marginal)" % v["rates"]["glm_invoice_on_file"])
    else:
        print("   amortized column unlocks with: cc-usage config set zai_invoice_usd <amount>")
    r = v["rates"]
    print("   rates: deepseek blended $%s/M [estimate]; glm amortized $%s/M [allocation of "
          "your $%s invoice — marginal cash while windows are open is $0; the real price "
          "of that is quota exhaustion, see Q2/quota panel]" % (
              r["deepseek_blended_usd_per_M"], r["glm_amortized_usd_per_M"],
              r["glm_invoice_on_file"]))
    print("   shared multi-provider PRs in window: %d (credit split by token share, "
          "never double-counted)" % v["shared_multi_provider_prs"]["count"])
    print()
    print("B. Speed — PR wall-clock (created->merged, gh truth) and active-session time")
    for t in v["deliverables"]:
        print("   %-10s median wall %sh / PR · median active %sh / PR" % (
            t["provider"], t["wall_h_per_pr_median"] or "—",
            t["active_h_per_pr_median"] or "—"))
    print()
    print("C. Burn per ACTION (one run of each command)")
    print("%-26s %6s %8s %12s %12s" % ("command", "runs", "responses", "tokens",
                                       "tokens/run"))
    for a in v["actions"]:
        print("%-26s %6d %8d %12s %12s" % (a["command"], a["runs"], a["responses"],
                                           "%.1fM" % (a["tokens"] / 1e6),
                                           "%.2fM" % (a["tokens_per_run"] / 1e6)
                                           if a["tokens_per_run"] else "—"))
    print("   " + v["actions_caption"])
    return 0
