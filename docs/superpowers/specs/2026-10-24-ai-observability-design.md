# AI Coding Efficiency Observability — Design Spec v2

**Date:** 2026-10-24 · **Status:** revised per architecture review (conditional approval)
**Repo:** `~/Sources/RustLearning/ai-observability` (standalone; installed to `~/.local/share/cc-usage/`)
**Supersedes:** v1 architecture presented 2026-10-24; incorporates reviewer conditions P0-1…P0-5, P1-6…P1-12.

---

## 0. Purpose

Measure real engineering productivity of the multi-provider Claude Code workflow (Z.ai GLM coding plan,
DeepSeek direct, OpenRouter, Anthropic, Bedrock) across 6–7 concurrent Orca sessions, and answer:

1. Tokens/cost/quota consumed per Jira ticket and per PR, across all sessions and subagents.
2. Whether GLM throughput/quota efficiency is declining for comparable tasks.
3. Which provider produces more accepted work per dollar and per quota unit.
4. Whether concurrency level, workflow phase, or model config changes explain latency/usage shifts.

Non-goals: no provider routing changes, no AlphaFolio code changes, no cloud telemetry, no auto-deletion.

## 1. Verified environment facts (audited 2026-10-24, not assumed)

| Fact | Evidence |
|---|---|
| `cc-usage` v1.0.0 (509-line Python 3.9 stdlib tool) owns hooks (SessionStart/UserPromptSubmit/UserPromptExpansion/SessionEnd), statusline, OTLP log collector on 127.0.0.1:43182, SQLite `usage.db` | source read; `doctor` green |
| Sessions/command attribution already work: 622 sessions, 898 command events, 1,828 prompt contexts | `usage.db` counts |
| Per-request ingest is dead: `api_requests`=0, `tool_events`=0 | DB counts |
| The collector ingest path itself is healthy: a synthetic OTLP `claude_code.api_request` record ingested with all fields | live probe (row inserted, then deleted) |
| `claude_code.api_request` OTLP **log events are the documented per-request channel** (input/output/cache_read/cache_creation tokens, `duration_ms`, `request_id`, model, `prompt.id`, `session.id`) | Claude Code OTel docs (code.claude.com/docs/en/monitoring-usage); changelog 2.1.117/122/161/214/300 |
| Real sessions never deliver those records to the collector (0 rows, no ingest errors logged) ⇒ non-delivery upstream (endpoint-env handling or exporter behavior on CLI 2.1.206), root cause TBD in Phase A | collector.log (no entries since ENOSPC ended), DB counts |
| Transcript assistant records: `message.usage` complete; duplicate `message.id` rows are exact-usage rewrites (up to 5×); 0 null ids in sampled GLM+DS files | corpus probes 2026-10-24 |
| Transcript `ttftMs`/`durationMs`/`requestId`: undocumented, absent in 66,713 assistant records across 281 local files (all third-party providers) | research corpus scan |
| Subagent transcripts live at `<project>/<parentSessionId>/subagents/agent-<agentId>.jsonl` + `.meta.json`; records carry parent `sessionId`, `isSidechain=true`, `agentId`; meta has `agentType`, `description`, `name`, `toolUseId`, `spawnDepth` | file inspection |
| Parent transcript Agent tool_use results carry matching `agentId` (join key) | grep verified |
| `sessions.source` is always `startup` today; hook does capture `resume`/`clear`/`compact` when they occur | DB query; hook source |
| CLI 2.1.206: past the 2.1.161 drop fix; **predates 2.1.214 metrics double-count fix**; 2.1.300 adds cache_creation_5m/1h attrs | changelog |
| `gh` authenticated (repo scope); Atlassian MCP available interactively; Orca journal DB has session↔workspace mapping; `ORCA_PANE_KEY` env available inside Orca-spawned sessions | audit |
| TTFT is exported nowhere in OTel (no attr, no metric); on-demand `probe.py` remains the only TTFT instrument | docs + corpus scan |

## 2. Architecture

Standalone Python 3.9 **stdlib-only** package (no venv, matches existing deployment), installed by
`install.sh` into `~/.local/share/cc-usage/`. Existing `~/.local/bin/cc-usage` shim, hook block,
statusline, and `usage.db` are preserved. Single background process: the existing collector (extended).
Dashboard is an on-demand stdlib HTTP server bound to 127.0.0.1.

```
src/cc_usage/
  db.py                  # versioned, additive-only migrations + legacy-compat guard
  collector.py           # OTLP logs (+metrics) HTTP server; bounded durable spool; replay
  ingest/transcripts.py  # JSONL → request rows (primary source; identity contract §3)
  ingest/otel.py         # api_request/api_error/subagent_completed logs → cross-check series
  ingest/backfill.py     # event-time-windowed historical scan; idempotent incremental
  hooks.py               # SessionStart/Prompt*/SessionEnd; captures identity + config snapshot
  attribution/identity.py    # launcher/gateway/serving/billing model (§4)
  attribution/workflow.py    # phase state machine (§5)
  attribution/subagents.py   # subagent lineage + exactly-once rollups (§6)
  correlate/github.py    # gh: PRs, reviews, revisions, diffs, merges, stacked graph (§7)
  correlate/reviews.py   # .claude/reviews/ critic-artifact parsing (§7)
  correlate/jira.py      # optional snapshot import (§7)
  quota/snapshots.py     # manual Z.ai snapshot import; window math; shared-window rule (§8)
  cost.py                # four cost measures (§9)
  report/…  dashboard/{server.py, static/}  # CLI reports + local web dashboard (§10)
tests/                   # unittest; synthetic fixtures; no network
install.sh               # copy-test migration gate; backup; idempotent
```

CLI additions: `ingest`, `backfill`, `pr sync`, `quota import`, `jira import`, `dashboard`,
`compare`, `config snapshot`; all existing commands unchanged.

## 3. Request identity contract (P0-1) — verified semantics, tested fallbacks

**Verified on real transcripts (GLM + DeepSeek):** one assistant API response is written to the
session JSONL as one `message.id`; writers emit the **same record up to ~5×** with byte-identical
usage (rewrite artifacts, ms apart). No null ids observed in sampled files. `requestId` is null on
all third-party providers — provider request identity is *not observable* there and will never be
claimed.

Ingestion therefore distinguishes the objects it counts and never silently equates them:

- **model_response** (the unit stored in `api_requests`): identity =
  `(session_id, message_id)` **when usage fingerprints of all occurrences agree** (verified common
  case) → collapse to one row, store `occurrences=N`.
- If the same `message_id` appears with **differing usage fingerprints**: keep the **last**
  occurrence's usage (final cumulative), flag `identity_conflict=1`, keep all fingerprints in
  `raw_json` for audit. Unit-tested with crafted fixtures.
- If `message_id` is absent/unparseable: deterministic surrogate
  `sha256(session_id | file_path | byte_offset | timestamp)` with `identity_basis='surrogate'`;
  collision tests required.
- Every row preserves `source_file`, `byte_offset`, `occurrences`, `identity_basis`,
  `provider_request_id` (null unless observed), so any aggregate can be re-audited to source lines.
- **Retries** invisible to transcripts are a documented gap; only exhausted retries surface (as
  `isApiErrorMessage` records / OTLP `api_error` events). Streaming chunks, tool uses, and turns are
  separate derived objects (turn rollups), never conflated with model_responses.
- Dedup invariants enforced by unique index `(session_id, message_id, source)` + suite tests
  (`test_token_accounting.py`, `test_dedup.py`), including: re-ingest = 0 new rows; parent vs
  subagent files never double-count one response.

## 4. Provider identity — four dimensions (P0-2)

Per session (hooks, env) and per response (transcript/OTLP):

| Column | Source | Example |
|---|---|---|
| `launcher` | `CC_LAUNCHER` env | `claude-glm` |
| `gateway` | `ANTHROPIC_BASE_URL` at session start | `api.z.ai/api/anthropic` → `zai` |
| `model_requested` | `ANTHROPIC_MODEL` env | `glm-5.3-flash[1m]` |
| `model_observed` | per-response `message.model` | `glm-5.3-flash` |
| `serving_provider` | **NULL unless verified** (direct gateways = gateway itself; OpenRouter stays unknown unless routing evidence exists) | `zai` / NULL |
| `billing_provider` | launcher registry (plan vs API vs subscription vs org) | `zai-plan`, `deepseek-api` |

Comparisons group by `(gateway, model_observed, billing_provider)` — never by model name alone
(OpenRouter-hosted GLM ≠ Z.ai-hosted GLM). Historical sessions without instrumented env get
`gateway=NULL`, `serving_provider=NULL`, plus `provider_inferred` (model-id heuristic) kept **out of
authoritative comparisons** until explicitly blessed. Model changes mid-session are recorded
per-response; a session summary exposes the set.

## 5. Workflow phases — explicit state machine (P0-3)

Tables: `work_lineages` (lineage_id, primary_ticket, session_ids, created_at),
`workflow_phases` (phase, lineage_id, session_id, started_at, ended_at, trigger_command,
prompt_id, ticket_keys, pr_number, confidence, basis, evidence_json), `phase_transitions`.

- Phases: `implement`, `critic_review`, `revision`, `pr_creation`, `chatgpt_followup`,
  `merge_gate`, `investigation`, `maintenance`, `unclassified`.
- **Boundaries:** each phase has start/end timestamps, trigger (command name / skill attribution /
  structural event), evidence, and confidence. Multiple commands per session → multiple phases.
- Classification cascade: (1) `UserPromptExpansion` command names scoped by `prompt_id`;
  (2) transcript `attributionSkill`; (3) structural evidence (PR opened on ticket branch,
  `.claude/reviews/<topic>/` artifact created, commit windows). ChatGPT-follow-up is multi-signal
  only (never keyword). Anything ambiguous → `unclassified` interval, never guessed.
- **Resumes:** a resumed session attaches to the existing `work_lineage` when hook `source=resume`,
  or when ticket/branch/PR continuation matches with evidence; otherwise a new lineage is created
  with an explicit `continuation_of` link candidate flagged for review. (No resumes exist in the
  current corpus — 622/622 `startup` — so this path ships with fixtures + real-case validation in
  Phase B.)
- Lineage → ticket attribution: command args, branch, prompt_context, PR markers (RUS-#### regex
  is one *input*; multi-signal confirmation required for `confirmed` confidence).

## 6. Subagent accounting — exactly once (P0-4)

Verified layout (§1). Ingestion:

- Ingest `subagents/agent-<agentId>.jsonl` files as rows with `is_sidechain=1`,
  `parent_session_id` (=record sessionId), `agent_id`, and `agent_type`/`agent_name`/`description`
  from `meta.json`; `spawn_depth` supports nesting.
- Join parent side: Agent tool_use results carry `agentId` (+ `toolUseId` in meta) → contributes
  `dispatch_ts`, `subagent_type`, and the parent phase.
- **Exactly-once rule:** session/ticket rollups = main-chain rows + Σ(subagent rows). Guards:
  (a) audit query proving no subagent records appear inline in parent files (verified true today:
  parent files contain 0 sidechain records); (b) unique index on `(session_id, message_id,
  source_file)`; (c) fixture tests for depth-2 nesting (spawnDepth=2) summing each agent exactly
  once; (d) reconciliation report comparing DB rollups against raw-sum ground truth per session.
- OTLP `subagent_completed` events cross-check counts (not tokens; metrics are aggregates).

## 7. Jira / GitHub correlation (P1-6)

- **GitHub via `gh` (authoritative for outcomes):** PR list/detail incl. base branch (stacked PRs),
  additions/deletions/files per commit (gross vs net), review/comment timeline vs pushes (revision
  rounds), merge status, CI checks. PR↔ticket via branch/title/body markers; multi-ticket PRs get
  per-ticket links; follow-up bug tickets link to parent PR (e.g. RUS-6221 → #1418).
- `pr_contributions(pr_number, lineage_id, session_id, provider_dims…, role ∈ {first_impl, review,
  revision, followup}, tokens, duration)` — PR-level outcomes counted **once per PR**; provider
  comparisons aggregate contributions with weights shown. Unique constraints + tests prevent
  double-credit (multi-ticket PR, multi-provider PR, follow-up corrections).
- **Critic findings by severity** parsed from `.claude/reviews/<topic>/` artifacts; review
  disposition recorded (accepted/changes-requested/unresolved).
- **Tests-executed evidence** from transcript `toolUseResult` within phase windows.
- **Jira lifecycle:** snapshot import (`cc-usage jira import export.json`); no new credentials; a
  read-only Keychain token is a future opt-in. Merged ≠ correct: merge status never upgrades quality.
- Language distribution from changed-file extensions; generated files (lockfiles, migrations,
  snapshots) excluded from productivity comparisons by configurable pattern list.

## 8. Quota — observed windows, no invented conversions (P0-5)

- `quota_windows(provider, kind ∈ {5h, weekly}, window_start, window_end, consumed_pct_or_units,
  source ∈ {snapshot, derived_from_error}, imported_at)` — snapshots via `cc-usage quota import`
  (paste or file; schema-flexible parser, stores raw + normalized). Exhaustion/rate-limit events
  (1302/1308/1310) from `provider_events` carry reset timestamps when present.
- **Shared-window rule:** when >1 session is active in a window, consumption is "shared evidence";
  per-ticket quota attribution is **forbidden by construction** (no view computes it). Dashboard
  shows active-session lists per window instead. Correlations between token series and quota
  snapshots are shown as side-by-side series with uncertainty notes — never as conversion.
- No token→quota coefficient exists anywhere in code or docs. Marginal-capacity observations
  ("window had spare capacity") are the only quota-based marginal statement.

## 9. Cost — four separate measures (P1-9)

| Measure | Source | Label |
|---|---|---|
| `actual_billed_usd` | imported provider statements/exports (manual; DeepSeek/OpenRouter dashboards) | observed |
| `estimated_api_usd` | versioned price table × observed tokens (DeepSeek peak/off-peak + cache tiers; OpenRouter approximate) | estimate |
| `subscription_amortized_usd` | actual Z.ai invoice (settings) allocated by usage share | allocation |
| `marginal_cost_usd` | 0 for plan windows with spare capacity; estimated rate for API work | estimate |

Amortization is never presented as marginal cost; the dashboard keeps them in separate columns and
explains the allocation method inline.

## 10. Dashboard + reports (§7 of mission)

`cc-usage dashboard` → 127.0.0.1:4399, read-only SQLite, zero-dependency static HTML/JS.
Views: provider comparison (accepted PRs, tickets, tokens by type, wall/tool-active time, cost &
quota, tokens/PR, revision rounds, first-pass acceptance), workflow-phase breakdown, session
timeline drilldown (requests, tools, subagents, commits, errors, compactions), concurrency buckets
(1–2 / 3–4 / 5–7+ via Orca journal + session overlap), daily/weekly trends, quota panel with
shared-window labels, filters (provider/model/command/ticket/PR/date/complexity). Every derived or
estimated figure is visually tagged with its basis. CLI reports remain for terminal use.

## 11. Reliability, safety, migrations (P1-7, P1-8)

- **Migration contract:** every migration (a) runs first against a **copy of the live DB** in the
  test suite, (b) must keep the legacy `cc_usage.py` queries (statusline, hooks, reports) passing
  unchanged, (c) is additive-only (new tables/columns/views/indexes), (d) is preceded by an
  automatic `cc-usage backup`, with demonstrated restore. `install.sh` aborts if (a)/(b) fail.
- **Bounded spool:** failed OTLP persists go to a disk spool with max size (default 200 MB,
  configurable); when full: drop oldest with an incremented `dropped` counter surfaced in `doctor`
  and the dashboard ("telemetry loss" indicator). Ordered replay on restart. Transcript ingest is
  offset-resumable and likewise safe under ENOSPC (aborts cleanly, resumes later).
- **Hooks:** unchanged timeout (5 s), collector never in the request path; a dead collector must
  not affect Claude Code (fire-and-forget UDP-style OTLP export + spool).
- **Secrets:** no token values, raw prompts, assistant text, or API bodies stored (existing privacy
  posture preserved); redaction unit test over every captured attribute path; config snapshots
  (P1-8) record: CLI version, model/effort/compact-window env, subagent model, MCP server *names* +
  tool counts, permission mode, plugin names, `ORCA_PANE_KEY` presence, load-average sample —
  never values of credential-bearing vars.
- **Idempotency everywhere:** backfill rerun = 0 new rows; ingestion resumable; no auto-deletion
  of anything, ever.

## 12. Backfill & provenance eras (P1-10)

`cc-usage backfill --days 30` (or `--since/--until`): selects sessions by **event timestamps**
(record `timestamp` ∈ window), not file mtime; supports incremental refresh of still-growing files
(size+offset+hash re-check). Provenance eras: `instrumented` (CC_PROVIDER env known) vs `historical`
(provider unknown/inferred) — the dashboard separates them by default and every §8-mission report
shows coverage ("X% of requests have provider identity, Y% have phase attribution").

## 13. Comparison methodology (P1-12)

`cc-usage compare` and dashboard views: speed, token efficiency, quota efficiency, financial
efficiency, rework (revision rounds, post-review churn), quality (findings by severity, post-merge
fix tickets) — grouped by matched-task sets (same epic/ticket type; complexity from
pre-implementation signals and structural size only — never from provider-dependent outcomes),
with sample sizes, Wilson intervals, and a confounds panel (concurrency level, quota-exhaustion
overlap, disk incidents, config-snapshot deltas). LOC/PR counts are never equated with productivity;
observations vs inferences are separated in every view.

## 14. Validation dataset (P1-11)

Five real AlphaFolio cases reconciled manually against raw evidence, stored as fixtures + expected
values + explicit gap notes, gating Phases B/C/F:
1. Straightforward single-ticket implementation.
2. PR with multiple Jira issues.
3. Long session with several workflow commands (multi-phase).
4. Implementation with multiple subagents (incl. nesting).
5. PR with ChatGPT-review follow-up and multiple revisions.

## 15. Phases & gates

| Phase | Scope | Gate (must pass before next phase) |
|---|---|---|
| **A** | Telemetry repair + provider/session identity + migrations + spool | E2E events from ≥3 real concurrent sessions; safe-migration proof on DB copy + rollback; identity/dedup fixture suite; non-blocking proven; OTLP delivery verified or decision documented; **collection stays on permanently after A** |
| **B** | Workflow state machine + subagent rollups | Multi-phase session, resume, nested-subagent cases validated on real samples (incl. validation dataset #3, #4) |
| **C** | GitHub/Jira correlation | Multi-ticket + stacked PR reconciliation without duplicate credit (dataset #2, #5) |
| **D** | Quota + cost | Shared-window rule enforced; four cost measures separated (dataset evidence) |
| **E** | Dashboard + backfill | §10 views live; 30-day backfill with provenance eras; rerun = 0 duplicates |
| **F** | Comparative validation | §13 methodology on real data; validation dataset #1–#5 reconciled; mission's eight success questions answered with sample sizes + uncertainty |

Phase A is deliberately minimal so reliable capture begins accumulating history immediately after
its gate, while B–F are built.

## 16. Open assumptions & risks (unresolved)

1. **OTLP non-delivery root cause** (endpoint-env handling vs exporter behavior on 2.1.206) —
   diagnosed in Phase A with an env-var delivery matrix; transcripts are primary regardless, so
   Phase A does not depend on its outcome.
2. **CLI 2.1.206 predates 2.1.214 telemetry fixes** — upgrading Claude Code is recommended but is a
   user decision; capture design works on both.
3. **First-party-vs-third-party `ttftMs` hypothesis unverified** — TTFT treated as unavailable
   until a first-party session proves otherwise; probe.py remains the on-demand instrument.
4. **Resume behavior** unobserved in corpus (622/622 startup) — shipped behind fixtures + Phase B
   real-case gate.
5. **Z.ai quota semantics** (units, tier exacts) remain third-party-sourced until dashboard
   snapshots ground them; re-verify docs once in Phase D.
6. **Jira transitions** require either snapshot imports or a future user-supplied read-only token.
7. Metric-level OTLP aggregation on 2.1.206 may double-count (pre-2.1.214) — OTLP metrics stay a
   secondary, tolerance-checked series only; transcripts are the accounting source of record.
