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
import json

from . import cost
from .correlate.github import outcomes
from .ingest.transcripts import SUBAGENT_RE, ts_epoch
from .correlate.github import TICKET_RE


def _blended_rates(db, device=None):
    """Provider blended $/M tokens from the actual cost estimate over all rows — used
    to price individual PRs (labeled estimate)."""
    df, dp = (("", []) if not device or device == "all"
              else (" WHERE device_id = ?", [device]))
    rows = db.execute(
        "SELECT provider, model, input_tokens i, output_tokens o, cache_read_tokens cr, "
        "cache_creation_tokens cc, record_ts ts FROM v_model_responses" + df, dp).fetchall()
    est_rows = [(r["provider"], r["model"], r["i"] or 0, r["o"] or 0, r["cr"] or 0,
                 r["cc"] or 0, ts_epoch(r["ts"])) for r in rows]
    # PER-PROVIDER rate: total-est / provider-tokens would contaminate every
    # provider's rate with every other provider's expensive tokens (live bug:
    # deepseek blended rate read 0.209/M instead of 0.0056/M because anthropic's
    # estimate leaked into the numerator)
    providers = {r[0] for r in est_rows if r[0] not in cost.PLAN_PROVIDERS}
    rates = {}
    est_by = {}
    for p in providers:
        prows = [r for r in est_rows if r[0] == p]
        est_p, _, _ = cost.estimate_provider_usd(db, prows)
        toks_p = sum(r[2] + r[3] + r[4] + r[5] for r in prows)
        rates[p] = (est_p / toks_p * 1e6) if toks_p else None
        est_by[p] = est_p
    return rates, sum(est_by.values())


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


def value_report(db, days=30, device=None):
    since = "datetime('now', '-%d days')" % days
    df, dp = (("", []) if not device or device == "all"
              else (" AND device_id = ?", [device]))
    rates, api_est_total = _blended_rates(db, device)
    glm_rate, invoice = _invoice_rate(db, days)
    # WINDOW usage per provider — the honest "what did this provider cost me" basis
    # (ALL work incl. analysis on non-ticket branches, not just PR-linked work)
    usage = db.execute(
        "SELECT COALESCE(provider,'unknown') p, COUNT(*) n, "
        "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) tok "
        "FROM api_requests WHERE source='transcript' AND record_ts >= " + since +
        df + " GROUP BY 1", dp).fetchall()
    window_tokens = {r["p"]: (r["tok"] or 0) for r in usage}
    window_responses = {r["p"]: r["n"] for r in usage}
    out = {"window_days": days, "rates": {
        "deepseek_blended_usd_per_M": round(rates.get("deepseek"), 4) if rates.get("deepseek") else None,
        "glm_amortized_usd_per_M": round(glm_rate, 4) if glm_rate else None,
        "glm_invoice_on_file": invoice,
        "basis": "deepseek = estimated (approximate prices, off-peak applied); "
                 "glm = ALLOCATION of your invoice, never marginal cost"}}

    # ---- per-provider deliverable efficiency ----
    # EVERY provider with window usage gets a row (claude/openrouter included even when
    # all their PR work landed on shared PRs). Lines on shared PRs are attributed
    # fractionally by token share (weights displayed) — PR COUNT is never split.
    prs = db.execute(
        "SELECT number, merged, net_add, net_del, created_at, merged_at, ticket_keys "
        "FROM pull_requests WHERE created_at >= " + since +
        " AND ticket_keys != '[]'").fetchall()
    window_providers = {r["provider"] for r in db.execute(
        "SELECT DISTINCT provider FROM api_requests WHERE source='transcript' "
        "AND record_ts >= " + since + " AND provider IS NOT NULL"
        + df.replace("device_id", "api_requests.device_id"), dp).fetchall()
        if r["provider"] not in ("unknown",)}
    prov_stats = {}
    shared_prs = []
    for pr in prs:
        contrib = db.execute(
            "SELECT provider, SUM(tokens) tok, MIN(first_ts) f, MAX(last_ts) l "
            "FROM pr_contributions WHERE pr_number=?" + df + " GROUP BY provider",
            [pr["number"]] + dp).fetchall()
        contrib = [c for c in contrib if c["provider"] not in ("unknown", None)
                   and (c["tok"] or 0) > 0]
        if not contrib:
            continue
        pr_lines = (pr["net_add"] or 0) + (pr["net_del"] or 0)
        tot = sum((c["tok"] or 0) for c in contrib) or 1
        provs = {c["provider"] for c in contrib}
        merged = bool(pr["merged"])
        if merged and pr["merged_at"] and pr["created_at"]:
            h = (ts_epoch(pr["merged_at"]) - ts_epoch(pr["created_at"])) / 3600.0
        else:
            h = None
        for c in contrib:
            p = c["provider"]
            s = prov_stats.setdefault(p, {
                "solo_prs": 0, "shared_prs": 0, "tokens": 0, "net_lines": 0.0,
                "wall_hours": [], "active_hours": []})
            s["tokens"] += c["tok"] or 0
            share = (c["tok"] or 0) / tot
            if len(provs) > 1:
                s["shared_prs"] += 1
                if merged:
                    s["net_lines"] += pr_lines * share  # fractional by token share
            else:
                if merged:
                    s["solo_prs"] += 1
                    s["net_lines"] += pr_lines
                    if h is not None and 0 < h < 24 * 30:
                        s["wall_hours"].append(round(h, 2))
                elif h is not None and 0 < h < 24 * 30:
                    pass
            if c["f"] and c["l"]:
                fh, lh = ts_epoch(c["f"]), ts_epoch(c["l"])
                if lh > fh and (lh - fh) / 3600.0 < 24 * 30:
                    s["active_hours"].append(round((lh - fh) / 3600.0, 2))
            if merged and h is not None and 0 < h < 24 * 30 and len(provs) > 1:
                s["wall_hours"].append(round(h, 2))

    def _avg(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else None

    table = []
    glm_list_usd, glm_list_rate, _mix = cost.glm_list_equivalent(db, days, device)
    # one row per provider with ANY window usage (claude/openrouter included even with
    # zero PR credit), sorted by window tokens. tokens = ALL window usage for the
    # provider (work + overhead incl. non-ticket analysis), NOT just PR-linked work.
    for p in window_tokens:
        if p == "unknown":
            continue  # unattributed rows stay in the providers panel, not VALUE
        if p not in prov_stats:
            prov_stats[p] = {"solo_prs": 0, "shared_prs": 0, "tokens": 0,
                             "net_lines": 0.0, "wall_hours": [], "active_hours": []}
    ordered = sorted(prov_stats.items(), key=lambda kv: -window_tokens.get(kv[0], 0))
    ordered = [(p, s) for p, s in ordered if p != "unknown"]
    for p, s in ordered:
        klines = s["net_lines"] / 1000.0
        list_rate = glm_list_rate if p in cost.PLAN_PROVIDERS else rates.get(p)
        list_basis = ("API list-rate equivalent [estimate]" if p in cost.PLAN_PROVIDERS
                      else "estimated API")
        pr_tokens = s["tokens"]  # PR-attributed contribution tokens (same scope as lines)
        wt = window_tokens.get(p, 0)
        cost_per_1k = (pr_tokens / 1e6 * list_rate / klines) if (list_rate and klines) else None
        pr_count = s["solo_prs"] + s["shared_prs"]
        cost_per_pr = (pr_tokens / 1e6 * list_rate / pr_count) if (list_rate and pr_count) else None
        tok_per_1k = (pr_tokens / klines) if klines else None
        tok_per_pr = (pr_tokens / pr_count) if pr_count else None
        amort_per_1k = amort_per_pr = None
        if p in cost.PLAN_PROVIDERS and glm_rate:
            amort_per_1k = round(pr_tokens / 1e6 * glm_rate / klines, 2) if klines else None
            amort_per_pr = round(pr_tokens / 1e6 * glm_rate / pr_count, 2) if pr_count else None
        table.append({
            "provider": p,
            "solo_merged_prs": s["solo_prs"], "shared_prs": s["shared_prs"],
            "merged_prs": s["solo_prs"],
            "tokens": pr_tokens,
            "window_tokens": wt,
            "window_responses": window_responses.get(p, 0),
            "net_lines": round(s["net_lines"]),
            "tokens_per_1k_lines": round(tok_per_1k) if tok_per_1k else None,
            "tokens_per_pr": round(tok_per_pr) if tok_per_pr else None,
            "cost_basis": list_basis,
            "cost_per_1k_lines": round(cost_per_1k, 2) if cost_per_1k is not None else None,
            "cost_per_pr": round(cost_per_pr, 2) if cost_per_pr is not None else None,
            "amortized_per_1k_lines": amort_per_1k,
            "amortized_per_pr": amort_per_pr,
            "wall_h_per_pr_median": _avg(s["wall_hours"]),
            "active_h_per_pr_median": _avg(s["active_hours"]),
            "basis": "tokens/1kL, $/1kL, $/PR use PR-ATTRIBUTED tokens only (same scope "
                     "as the lines: generated files excluded; shared PRs fractionally "
                     "by token share). window_tokens = provider's total burn incl. "
                     "non-PR work — context only, never divided by PR lines",
        })
    out["deliverables"] = table

    # ---- in-flight work: window tokens on tickets WITHOUT a merged PR ----
    # (OpenRouter's zero-credit explanation: its tickets 6257/6230/6261/6265 are
    # still open — the burn is real, the delivery hasn't happened yet.)
    merged_tickets = set()
    for r in db.execute("SELECT ticket_keys FROM pull_requests WHERE merged=1 AND "
                        "ticket_keys != '[]'").fetchall():
        try:
            merged_tickets |= set(json.loads(r["ticket_keys"]))
        except (ValueError, TypeError):
            pass
    in_flight = {}
    for sess in db.execute(
            "SELECT s.git_branch b, COALESCE(s.launcher,'?') launcher, "
            "SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_creation_tokens) tok, "
            "COUNT(r.id) n FROM sessions s JOIN api_requests r ON r.session_id=s.session_id "
            "WHERE r.source='transcript' AND r.record_ts >= " + since +
            " AND s.git_branch LIKE 'jeremycod/RUS-%' GROUP BY s.session_id, s.git_branch, s.launcher").fetchall():
        m = TICKET_RE.search((sess["b"] or "").upper())
        if not m or m.group(1) in merged_tickets:
            continue  # delivered or non-ticket branch
        key = (sess["launcher"], m.group(1))
        agg = in_flight.setdefault(key, {"tokens": 0, "responses": 0})
        agg["tokens"] += sess["tok"] or 0
        agg["responses"] += sess["n"]
    out["in_flight"] = [
        {"launcher": k[0], "ticket": k[1], "tokens": v["tokens"],
         "responses": v["responses"],
         "note": "work started on this ticket but its PR is not merged — no "
                 "delivered-line credit yet"} for k, v in
        sorted(in_flight.items(), key=lambda kv: -kv[1]["tokens"])[:12]]

    # ---- plan what-if: deliver the same merged-PR lines on each provider ----
    total_lines = sum(t["net_lines"] for t in table)
    what_if = []
    for t in table:
        rate = t["cost_per_1k_lines"]
        if rate is None:
            continue
        what_if.append({"provider": t["provider"], "usd_for_same_lines": round(
            rate * total_lines / 1000.0, 2)})
    out["plan_what_if"] = {
        "merged_lines_30d": round(total_lines),
        "scenarios": what_if,
        "note": "SAME delivered lines priced at each provider's measured $/1kL "
                "(zai = amortized plan allocation + its list-equiv; cross-model "
                "token-count differences not adjusted)"}

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
        "SUM((SELECT COUNT(*) FROM api_requests r WHERE r.phase_id=p.id" + df + ")) responses, "
        "SUM((SELECT COALESCE(SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens"
        "+r.cache_creation_tokens),0) FROM api_requests r WHERE r.phase_id=p.id" + df + ")) tokens "
        "FROM workflow_phases p WHERE p.trigger_kind LIKE 'command:%' "
        "GROUP BY p.trigger_kind", dp + dp).fetchall()  # df embedded in BOTH subqueries
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
    print("A. Tokens & cost per 1,000 delivered lines (every provider with usage)")
    print("   (tokens = input + cache-read + output; on GLM cache-read is quota, on DS it is ~$0.003/M)")
    print("%-12s %4s %4s %10s %9s %10s %10s %30s %10s %9s" % (
        "provider", "solo", "shar", "tokens", "net lines", "tok/1kL", "tok/PR",
        "cost basis", "$/1kL", "$/PR"))
    for t in v["deliverables"]:
        print("%-12s %4d %4d %10s %9d %10s %10s %30s %10s %9s" % (
            t["provider"], t["solo_merged_prs"], t["shared_prs"],
            "%.1fM" % (t["tokens"] / 1e6), t["net_lines"],
            _hm(t["tokens_per_1k_lines"]), _hm(t["tokens_per_pr"]), t["cost_basis"],
            ("$%.2f" % t["cost_per_1k_lines"]) if t["cost_per_1k_lines"] is not None else "—",
            ("$%.2f" % t["cost_per_pr"]) if t["cost_per_pr"] is not None else "—"))
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
        if t["wall_h_per_pr_median"] or t["active_h_per_pr_median"]:
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
