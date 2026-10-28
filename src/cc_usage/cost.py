"""Phase D: cost model — four measures with per-provider availability (spec v3.1 §9).

Availability table (reviewer-mandated):
- Z.ai plan      -> actual invoice + subscription amortized + marginal CASH only.
                    estimated_api_usd is NOT computed (no per-token statement exists).
- DeepSeek / Anthropic API / OpenRouter -> all four measures.
Quota OPPORTUNITY cost is a separate, unpriced dimension: consuming scarce plan quota can
displace future work, so marginal cash = 0 must never read as "costless".

Prices are versioned and labeled: the DeepSeek figures come from the measured 2026-10-23
diagnosis (glm-diagnosis REPORT §6) and are flagged approximate pending verification
against provider billing pages.
"""
import datetime as dt

PRICE_VERSION = "2025.10-diagnosis"
PRICES = {
    # (provider, model, metric) -> usd per million tokens
    ("deepseek", "deepseek-flash", "input_cache_miss"): 0.15,
    ("deepseek", "deepseek-flash", "input_cache_hit"): 0.003,
    ("deepseek", "deepseek-flash", "output"): 0.60,
    ("anthropic", "claude-sonnet", "input_cache_miss"): 3.00,
    ("anthropic", "claude-sonnet", "input_cache_hit"): 0.30,
    ("anthropic", "claude-sonnet", "output"): 15.00,
}
# DeepSeek off-peak discount window (UTC) — approximate, flagged in every report.
OFFPEAK = {"start": 16, "end": 30 + 24, "discount": 0.5, "note": "16:30-00:30 UTC, ~50% off"}
PLAN_PROVIDERS = {"zai", "zai-plan"}
INVOICE_KEY = "zai_invoice_usd"

# GLM-5.3-Flash PAY-PER-TOKEN API list rates (Z.ai announcement, verified 2026-10-25;
# a transient 50% promo exists and is NOT applied). Used ONLY for the clearly-labeled
# "list-rate equivalent" measure — NEVER as the plan's actual cost, which is
# invoice-amortized (spec §9). estimate_provider_usd still skips plan providers.
GLM_LIST_VERSION = "2025.10-zai-list"
GLM_LIST = {"input": 0.15, "cache_read": 0.03, "cache_creation": 0.15, "output": 0.50}


def glm_list_equivalent(db, days, device=None):
    """What the GLM token volume of the window would cost at Z.ai API list rates.
    Returns (usd, rate_per_M, tokens_by_kind). Labeled estimate everywhere."""
    df, dp = (("", []) if not device or device == "all"
              else (" AND device_id = ?", [device]))
    rows = db.execute(
        "SELECT input_tokens i, output_tokens o, cache_read_tokens cr, "
        "cache_creation_tokens cc FROM v_model_responses WHERE provider='zai' "
        "AND record_ts >= datetime('now', ?)" + df,
        ["-%d days" % days] + dp).fetchall()
    mix = {"input": 0, "cache_read": 0, "cache_creation": 0, "output": 0}
    for r in rows:
        mix["input"] += r["i"] or 0
        mix["output"] += r["o"] or 0
        mix["cache_read"] += r["cr"] or 0
        mix["cache_creation"] += r["cc"] or 0
    usd = (mix["input"] * GLM_LIST["input"] + mix["cache_read"] * GLM_LIST["cache_read"]
           + mix["cache_creation"] * GLM_LIST["cache_creation"]
           + mix["output"] * GLM_LIST["output"]) / 1e6
    total = sum(mix.values())
    rate = (usd / total * 1e6) if total else None
    return (usd, rate, mix)

SQL_PRICE = ("SELECT unit_price_usd FROM price_table WHERE version=? AND provider=? "
             "AND model=? AND metric=?")


def price_for(db, provider, model, metric):
    """Versioned table first (user-editable), built-in fallback."""
    row = db.execute(SQL_PRICE, (PRICE_VERSION, provider, model, metric)).fetchone()
    if row:
        return row["unit_price_usd"]
    for (p, m, met), v in PRICES.items():
        if p == provider and model and model.startswith(m) and met == metric:
            return v
    return None


def get_setting(db, key, default=None):
    row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(db, key, value):
    db.execute("INSERT INTO settings(key,value) VALUES(?,?) "
               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    db.commit()


def offpeak_factor(ts_epoch_val):
    """DeepSeek off-peak multiplier. Window 16:30-00:30 UTC (minutes-of-day; the window
    wraps midnight). Approximate — flagged in every report it appears in."""
    t = dt.datetime.fromtimestamp(ts_epoch_val, dt.timezone.utc)
    minutes = t.hour * 60 + t.minute
    start = 16 * 60 + 30
    end_wrap = 30  # 00:30 next day == minute-of-day 30
    in_window = minutes >= start or minutes <= end_wrap
    return OFFPEAK["discount"] if in_window else 1.0


def pricing_model(provider, model):
    """Map an (openrouter-routed) model to its underlying pricing table.
    OpenRouter pass-through: deepseek models price at DeepSeek list rates,
    labeled 'via OpenRouter [pass-through assumed]' in reports."""
    m = (model or "").lower()
    if provider == "openrouter":
        if "deepseek" in m:
            return "deepseek", "deepseek-flash"
    return provider, model


def estimate_provider_usd(db, rows):
    """rows: iterable of (provider, model, input, output, cache_read, cache_creation,
    ts_epoch). Returns (estimated_usd, n_priced, n_unpriced). Z.ai rows are NEVER
    priced (subscription, spec §9); they are skipped, not zeroed."""
    total = 0.0
    priced = unpriced = 0
    for (provider, model, i, o, cr, cc, tse) in rows:
        base = None
        for p in (provider,):
            base = p
        if base in PLAN_PROVIDERS:
            continue
        if base == "openrouter":
            base, model = pricing_model(base, model)
        pin_miss = price_for(db, base, model, "input_cache_miss")
        pin_hit = price_for(db, base, model, "input_cache_hit")
        pout = price_for(db, base, model, "output")
        if pin_miss is None or pout is None:
            unpriced += 1
            continue
        f = offpeak_factor(tse) if base == "deepseek" else 1.0
        cost = (i * pin_miss + cr * (pin_hit if pin_hit is not None else pin_miss)
                + o * pout) / 1e6
        total += cost * f
        priced += 1
    return (total, priced, unpriced)


def amortized_usd(db, period_tokens_by_provider, invoice=None):
    """Subscription amortization at PERIOD grain only (never ticket grain): invoice
    allocated by the provider's share of the period's plan tokens. Returns
    (amortized_zai_usd, invoice, method_string)."""
    inv = invoice or get_setting(db, INVOICE_KEY)
    try:
        inv = float(inv)
    except (TypeError, ValueError):
        return (None, None, "set %s first: cc-usage config set %s <amount>" %
                (INVOICE_KEY, INVOICE_KEY))
    tok = period_tokens_by_provider.get("zai", 0) or 0
    if tok <= 0:
        return (0.0, inv, "no zai usage in period")
    total_plan = tok  # only plan provider in settings today
    return (inv, inv, "invoice $%.2f allocated at period grain; this period carries 100%% "
                      "of the invoice while it spans the billing month (share=%.0f%%)"
            % (inv, 100.0 * tok / max(total_plan, 1)))
