# Phase F Report — Success Questions Answered (2026-10-25)

Sources: `cc-usage compare --days 30`, `cc-usage validate`, `cc-usage phases show`,
dashboard endpoints. All numbers observed in the local DB; caveats inline. Latency is
`measured_api` (OTLP) only, accruing since 2026-10-25; everything before that has no
measured latency. Quota answers are shared-window evidence by design (spec §8).

**Q1 — Tokens for RUS-6225 (all sessions, all phases).** 221 responses / **45.7M tokens**:
implement 14.6M (79 resp) · critic_review 7.1M (42) · pr_creation 21.4M (81) ·
revision 2.4M (18). Reconciles EXACTLY against raw transcripts (validation case #1).

**Q2 — Z.ai quota for PR #1419.** Honest answer: **shared-window evidence only**. PR #1419's
window overlaps the era of the observed 1310 weekly exhaustion (Oct 21, reset Oct 23) and
1308 5h exhaustion (Oct 24) — both imported with resets. 46–50 sessions were active in
those windows, so no per-PR quota figure may be claimed (structurally impossible, §8).

**Q3 — Which command consumes the most tokens.** `implement-jira-ticket` **4.21B tokens**
(14,790 responses), then `create-pr` 3.13B (7,865 — includes continuously-active
post-command spans; treat as upper bound), `revise-from-review` 553M,
`challenge-artifact` 290M, `analyze-problem` 270M, `merge-pr` 198M.

**Q4 — Does `challenge-artifact` improve acceptance?** Correlational only (n is small and
assignment is not random): lineages with critic_review phases (73 sessions, 283M tokens)
show review rounds of 0.78 (zai solo PRs) vs 1.08 (deepseek) — but provider, ticket mix,
and session style confound this. **Not yet answerable causally; needs Phase-F+ data
accumulation with matched sets.** The 21 matched multi-provider PRs are the substrate.

**Q5 — Is GLM getting slower?** Measured API latency (30d, n=589 GLM / 71 DS requests):
GLM avg **16.1 s** vs DeepSeek **7.8 s** per request (~2.1×), with GLM's tok/resp ≈ 2,937K
vs DS ≈ 2,915K — comparable request shapes. Month-over-month trend needs more accrued
measured data (only exists since 10-25); historical medians are derived_gap, labeled as
such. No evidence of GLM degradation *within* the measured window; the earlier diagnosis
(REPORT §2.1) found flat medians with a volatile p90 tail.

**Q6 — Does DeepSeek deliver more accepted work per dollar?** 30d solo-bucket PRs:
zai 36 PRs (100% merged, 33.3K net-add, avg 0.78 rounds) vs deepseek 13 PRs (100%
merged, 13.2K net-add, avg 1.08 rounds). Estimated API cost for DeepSeek's 2.50B tokens:
**≈ $11 (30d, off-peak-applied, labeled approximate)**; GLM's 7.04B tokens ride the flat
plan (marginal cash ≈ $0 while windows have spare capacity — but see Q2 for what that
spare capacity actually costs in exhaustion risk). DeepSeek: **$4.4/K net-added-lines
estimated**; GLM: no marginal cash but **2.2× measured latency** and the exhaustion
events. Both merge at 100% with overlapping Wilson intervals (zai [0.90,1.00],
deepseek [0.77,1.00]) — **differences in merge rate are not statistically
distinguishable at these sample sizes.**

**Q7 — Does 7-way concurrency help or hurt?** Concurrency panel shows the hourly overlap
distribution; the 1310/1308 events landed exactly in the high-concurrency era — the
binding constraint was the shared Z.ai key's quota windows, not per-request limits
(2026-10-23 diagnosis §2.3). DeepSeek showed clean 7-way walls vs GLM's non-monotonic
ones. Answer: **concurrency multiplies quota burn on one plan key; GLM fleets should
stay ≤2-3 concurrent or split keys** — consistent with the diagnosis routing.

**Q8 — Revision rounds per provider; implementation vs review efficiency.** Solo PRs:
zai avg **0.78 rounds**, deepseek **1.08** (small n: 36/13; intervals above). Rework
churn proxy: GLM revision/first_impl = **0.74**, DeepSeek = **0.60**. Review-role tokens:
GLM 204M, DS 150M. For implementation-vs-review efficiency per provider: within
matched-ticket sets (21 multi-provider PRs), token shares vary widely per ticket
(14%–86%) — the per-phase split (dashboard) is the instrument; formal per-role
efficiency needs more matched-set accumulation.

## Validation (spec §14)

5/5 PASS: single-ticket EXACT reconcile (#1423/RUS-6223) · multi-ticket single-credit
(#1386, 4 tickets) · multi-phase session (11 spans, 615 responses stamped) ·
multi-subagent with real depth-2 specimen, exactly-once rollups · ChatGPT follow-up
(#1392, 8 rounds, 51.4M post-review revision tokens).

## Known limitations

- Measured latency exists only from 2026-10-25 forward; trends need accrual.
- `create-pr` spans include continuously-active tails (upper bound; refinement splits
  only where reviews exist).
- 30 PRs in the window have no attributable sessions (pre-instrumentation or
  non-RUS work) — reported as `none` bucket, never guessed.
- Jira lifecycle needs snapshot imports (`cc-usage jira import`); statuses are not
  fetched automatically.
- Resume-attachment path ships validated-by-fixture only (no real resume in corpus yet).
