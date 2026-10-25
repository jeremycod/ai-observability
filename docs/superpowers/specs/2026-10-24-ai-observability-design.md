# AI Coding Efficiency Observability — Design Spec v3

**Date:** 2026-10-24 · **Status:** critic-approved-with-changes; all findings incorporated
**Repo:** `~/Sources/RustLearning/ai-observability` (standalone; installed to `~/.local/share/cc-usage/`)
**History:** v1 architecture → v2 (architecture-review corrections) → v3 (independent critic findings 1–20 incorporated)

---

## 0. Purpose

Measure real engineering productivity of the multi-provider Claude Code workflow (Z.ai GLM coding
plan, DeepSeek direct, OpenRouter, Anthropic, Bedrock) across 6–7 concurrent Orca sessions, and
answer the eight success questions (Appendix A). Non-goals: no provider routing changes, no
AlphaFolio code changes, no cloud telemetry, no auto-deletion.

## 1. Verified environment facts (audited 2026-10-24; live DB counts drift ±, noted as such)

| # | Fact | Evidence |
|---|---|---|
| F1 | `cc-usage` v1.0.0 (509-line Python 3.9 stdlib tool) owns hooks (SessionStart/UserPromptSubmit/UserPromptExpansion/SessionEnd), statusline, OTLP log collector on 127.0.0.1:43182 (TCP), SQLite `usage.db` | source read; `doctor` green |
| F2 | Session/command attribution works: ~622 sessions, ~899 command events, ~1,832 prompt contexts (live-moving) | DB counts |
| F3 | Per-request ingest dead: `api_requests`=0, `tool_events`=0 | DB counts |
| F4 | Collector ingest path healthy: synthetic OTLP `claude_code.api_request` record ingested with all fields | live probe (inserted, then deleted) |
| F5 | `claude_code.api_request` OTLP **log events are the documented per-request channel** (input/output/cache_read/cache_creation tokens, `duration_ms`, `request_id`, model, `prompt.id`, `session.id`) | Claude Code OTel docs (code.claude.com/docs/en/monitoring-usage); changelog 2.1.117/122/161/214/300 (doc-verified by research agent, not independently re-fetched) |
| F6 | Real sessions never deliver those records (0 rows, no ingest errors) ⇒ non-delivery upstream; root cause TBD in Phase A | collector.log; DB counts |
| F7 | Transcript assistant records carry complete `message.usage`. Duplicate `message.id` rows: identical-usage rewrites collapse cleanly, **and 732 ids across 322 files carry ≥2 distinct usage fingerprints** (~2% of duplicated ids; occurrences to 16×). Keep-last = max-usage verified in all 732 observed conflicts | critic corpus scan (322 files / 69,430 assistant records) |
| F8 | `ttftMs`/`durationMs` absent (0 occurrences corpus-wide). `requestId` present in 3 records — all `<synthetic>` API-error records, Z.ai gateway format | critic scan |
| F9 | `<synthetic>` records: 10 in corpus, `model="<synthetic>"`, real UUID `message.id`, all-zero usage, some `isApiErrorMessage:true` | critic scan |
| F10 | Subagent transcripts at `<project>/<parentSessionId>/subagents/agent-<agentId>.jsonl` + `.meta.json` {agentType, description, name, toolUseId, spawnDepth}; records carry parent `sessionId`, `isSidechain=true`, `agentId`. Corpus: 24 subagent dirs; spawnDepth {0:7, 1:34} — **depth-0 subagents exist; depth-2 never observed** | file inspection; critic scan |
| F11 | Parent join: `meta.toolUseId` → parent tool_use `id` resolves 34/34; filename agentId appears in parent tool-result prose 33/34; **1 orphan agent file has no parent-side result record** (crash/kill/still-running class) | critic scan |
| F12 | 0 inline sidechain records in any parent file; 0 message.ids span >1 file (both latent risks only) | critic scan |
| F13 | `sessions.source` all `startup` (622/622); hook does capture `resume`/`clear`/`compact` when they occur | DB; hook source |
| F14 | CLI 2.1.206: past 2.1.161 drop fix; predates 2.1.214 metrics double-count fix; 2.1.300 adds cache_creation_5m/1h | changelog (research agent) |
| F15 | 1,061 `ai-title` records prove background/auxiliary calls run on the same model as work calls — per-ticket token totals include this overhead | critic scan |
| F16 | Assistant records carry both `sessionId` and `session_id` (0 mismatches in 25,212 records); canonical = `sessionId` | critic scan |
| F17 | `gh` authenticated (repo scope); Atlassian MCP available interactively; Orca journal DB exists (session↔workspace mapping, not yet opened); `ORCA_PANE_KEY` env available in Orca sessions | audit |
| F18 | TTFT is exported nowhere in OTel; on-demand `probe.py` remains the only TTFT instrument | docs + corpus scan |

## 2. Architecture

Standalone Python 3.9 **stdlib-only** package, installed by `install.sh` into `~/.local/share/cc-usage/`.
Existing `~/.local/bin/cc-usage` shim, hook block, statusline, and `usage.db` preserved. One background
process: the collector, **extended to be the single SQLite writer** (hooks enqueue to files; §11).
Dashboard: on-demand stdlib HTTP server on 127.0.0.1.

```
src/cc_usage/
  db.py                  # additive-only migrations; legacy-compat gate; backup/rollback
  collector.py           # OTLP logs(+metrics) server; bounded spool+replay; transcript sweeper; spool ingester; SINGLE DB WRITER
  ingest/transcripts.py  # JSONL → model_response/api_error rows (identity contract §3)
  ingest/otel.py         # api_request/api_error/subagent_completed logs → cross-check series
  ingest/backfill.py     # event-time-windowed scan; idempotent incremental
  hooks.py               # SessionStart/Prompt*/SessionEnd → identity+config snapshot ENQUEUED to spool file (never writes SQLite)
  attribution/identity.py    # gateway registry, canonicalization (§4)
  attribution/workflow.py    # phase state machine (§5)
  attribution/subagents.py   # lineage + exactly-once rollups (§6)
  correlate/{github,reviews,jira}.py   # §7
  quota/snapshots.py     # §8     cost.py  # §9
  report/…  dashboard/{server.py, static/}   # §10 (dashboard opens DB READ-ONLY)
tests/                   # unittest; synthetic + real-harvested fixtures; no network
install.sh               # copy-test migration gate; backup; idempotent
```

CLI additions: `ingest`, `backfill`, `pr sync`, `quota import`, `jira import`, `dashboard`,
`compare`, `config snapshot`. Existing commands unchanged.

## 3. Request identity contract (P0-1; critic findings 1, 2, 3, 16, 17)

**Counted object:** a *model_response* — one assistant API response. Transcript evidence (F7):
duplicate `message.id` rows are usually identical-usage rewrites; **732 ids corpus-wide carry ≥2
distinct usage fingerprints**, so conflict handling is a live path, not an edge case. Keep-last
(max byte offset) was the correct choice in all 732 observed conflicts; validation is documented as
**empirical**, guarded by a reconciliation check (below).

Rules:
1. **Identity grain:** `(session_id, message_id)` at the transcript layer (canonical field
   `sessionId`, F16). Invariant: **no (session_id, message_id) maps to >1 row** — enforced by
   unique index; `source_file`, `occurrences`, `byte_offsets` are non-key attributes. Cross-file
   duplicates (0 today, F12; possible after resume/compact re-serialization) resolve by a
   **deterministic total order that is valid across files** — byte offsets are file-local and must
   never be compared across files. Winner ordering: `(record_ts, source_file_path, byte_offset)`;
   fixture-tested.
2. **Conflict handling:** if usage fingerprints differ, the ordered-last occurrence wins; set
   `identity_conflict=1`. Keep-last being max-usage is **empirical (732/732), not an invariant**:
   when the ordered winner's usage differs from the max-usage occurrence, the row is flagged
   `reconciliation_flag='order_usage_disagree'` and **all** conflicting occurrences (usage
   fingerprints, timestamps, files, offsets) are preserved in the `api_request_conflicts` audit
   table. Nothing is silently resolved; `cc-usage reconcile` reports these cases.
3. **Usage fingerprint:** normalized integers with null≡0 (F-corpus has both `null` and `0` forms).
4. **Missing `message_id`:** deterministic surrogate `sha256(session_id | source_file |
   byte_offset)` (timestamp omitted so the surrogate is rewrite-stable); `identity_basis='surrogate'`;
   resumability keyed on `(source_file, byte_offset)` upserts. Collision tests required.
5. **Synthetic/error records (F9):** records with `model == "<synthetic>"` or
   `isApiErrorMessage == true` are stored as **`api_error` objects, never model_response**;
   `model_observed` stays NULL for them. (Prevents request-count/latency inflation.)
6. **Retries** invisible to transcripts are a documented gap; exhausted retries surface as the F9
   record class and OTLP `api_error` events. Streaming chunks, tool uses, and turns are derived
   objects, never conflated with model_responses.
7. Fixtures: real harvested (16× duplicates; conflicting-id class from
   `agent-abb09bfa173540b88.jsonl`; `<synthetic>` records; depth-0 subagent files; orphan agent
   file) **plus** synthetic edge cases (cross-file duplicate, null-id surrogate).

## 4. Provider identity — four dimensions + registry (P0-2; findings 3, 15, 18)

Per session and per response: `launcher`, `gateway`, `model_requested`, `model_observed`,
`serving_provider` (**NULL unless verified**; direct gateways = gateway itself; OpenRouter stays
unknown absent routing evidence), `billing_provider`.

- **Versioned gateway registry:** normalized mapping (URL trimmed of trailing `/`, path-insensitive
  match) `api.z.ai/api/anthropic → zai`, `openrouter.ai/api → openrouter`,
  `api.deepseek.com/anthropic → deepseek`, `api.anthropic.com → anthropic`,
  `bedrock-runtime.*.amazonaws.com → aws-bedrock`; unknown URLs stored verbatim with
  `gateway='unmapped'`.
- **Model canonicalization:** requested `[1m]` suffix is a context-window variant marker, not a
  distinct model: `model_requested="glm-5.3-flash[1m]"`, `model_observed="glm-5.3-flash"` →
  canonical model `glm-5.3-flash`, `ctx_variant='1m'`. Comparisons group by
  `(gateway, canonical model, billing_provider)` — never by model name alone.
- **Env captured at SessionStart *and* UserPromptSubmit** (mid-session gateway/model changes are
  detected, not missed).
- **Background-call caveat (F15):** per-ticket/per-session token totals include auxiliary calls
  (ai-title generation, etc.); identical phenomenon across providers, so comparisons remain valid;
  caveat printed on every affected report. A `call_purpose` field is added if/when OTLP delivery is
  repaired (OTel `query_source` attr).
- Historical sessions without instrumented env: `gateway=NULL`, `serving_provider=NULL`,
  `provider_inferred` kept **out of authoritative comparisons** until explicitly blessed.

## 5. Workflow phases — explicit state machine (P0-3; findings 6, 7)

Tables: `work_lineages`, `workflow_phases` (phase, lineage_id, session_id, started_at, ended_at,
trigger_command, prompt_id, ticket_keys, pr_number, confidence, basis, evidence_json),
`phase_transitions`. Phases: `implement`, `critic_review`, `revision`, `pr_creation`,
`chatgpt_followup`, `merge_gate`, `investigation`, `maintenance`, `unclassified`.

- **Boundaries:** every phase has start/end timestamps, trigger, evidence, confidence. Multiple
  commands per session → multiple phases.
- **Overlap/nesting semantics:** phase intervals **may nest within a session** (e.g. a critic
  subagent runs while the parent is in `implement`). Token/time attribution joins each
  model_response row to the **innermost enclosing phase of its session at that row's timestamp**.
  Phase durations are never summed as disjoint wall-clock; the dashboard shows a timeline, not a
  partition.
- **Prompt linkage:** `promptId` exists on user records only; assistant records link to their
  prompt via the `parentUuid` chain to the nearest-preceding user record; `last-prompt` /
  `leafUuid` records terminate chains. Subagent tokens attribute to the **phase active at their
  dispatch timestamp** (resolved via parent `toolUseId`), not per-timestamp.
- Classification cascade: (1) `UserPromptExpansion` command names scoped by resolved prompt;
  (2) transcript `attributionSkill`; (3) structural evidence (PR on ticket branch,
  `.claude/reviews/` artifact, commit windows). ChatGPT-follow-up is multi-signal only. Ambiguous →
  `unclassified` interval, never guessed.
- **Resumes:** attach to existing `work_lineage` when hook `source=resume` or ticket/branch/PR
  continuation matches with evidence; otherwise new lineage with flagged `continuation_of`
  candidate. (No resumes in corpus, F13 — ships behind fixtures + Phase B real-case gate.
  Resume/compact re-serialization behavior is untested and listed in §16.)

## 6. Subagent accounting — exactly once (P0-4; findings 4, 5, 19)

- **Subagent-ness is keyed on file location** (`subagents/agent-*.jsonl` / `is_sidechain=1`),
  **not** `spawnDepth` (F10: depth-0 subagents exist); `spawn_depth` stored descriptive-only.
- Ingest: rows with `is_sidechain=1`, `parent_session_id`, `agent_id`, `agent_type`/`agent_name`/
  `description` from meta.json.
- **Join:** primary = `meta.toolUseId` → parent tool_use `id` (34/34, F11); prose agentId is
  confirmation only.
- **Orphan case** (no parent-side result record): a counted, expected state — tokens ingested,
  `dispatch_ts`/phase NULL, surfaced in coverage stats; never an ingest error.
- **Exactly-once rule:** session/ticket rollups = main-chain rows + Σ(subagent rows). Guards:
  (a) corpus audit query (no inline sidechains — true today, F12); (b) unique index per §3 grain;
  (c) depth-0 and depth-2 fixture tests summing each agent exactly once; (d) per-session
  reconciliation report vs raw-sum ground truth.
- **Nesting (depth-2):** never observed (F10); will be **deliberately produced in a scratch session
  during Phase B** and stored as the fixture (validation dataset #4 amended accordingly).
- `SendMessage`-continuation appends to an existing agent file across parent phases: tokens
  attribute to the dispatch phase; continuation segments are visible in the timeline.

## 7. Jira / GitHub correlation (P1-6; findings 9, 10)

- **GitHub via `gh` (authoritative for outcomes):** PRs incl. base branch (stacked graph),
  per-commit additions/deletions/files (gross vs net), review/comment timeline vs pushes (revision
  rounds), merge status, CI checks. PR↔ticket via branch/title/body markers; multi-ticket PRs get
  per-ticket links; follow-up bug tickets link to parent PRs.
- **Multi-provider PR allocation rule (defined):** a PR's outcome is counted **once**. PR **count**
  attributes to a `shared` bucket when >1 provider contributed (never to each provider); provider
  **token/time** metrics attribute fractionally by observed token share, with the weights displayed
  next to every such number. No view may sum provider-attributed PR counts to >1 per PR.
- **Stacked PRs:** incremental additions/deletions attribute to the **leaf** PR of the stack only;
  base PRs carry zero incremental LOC for productivity comparisons.
- Critic findings by severity from `.claude/reviews/<topic>/` artifacts; review disposition
  recorded. Tests-executed evidence from transcript `toolUseResult` within phase windows. Language
  distribution from extensions; generated files excluded via configurable pattern list.
- **Jira lifecycle:** snapshot import (`cc-usage jira import export.json`); no new credentials;
  read-only Keychain token is a future opt-in. Merged ≠ correct — merge status never upgrades
  quality.

## 8. Quota — observed windows, no invented conversions (P0-5)

- `quota_windows(provider, kind ∈ {5h, weekly}, window_start, window_end, consumed_pct_or_units,
  source ∈ {snapshot, derived_from_error}, captured_at, imported_at)` — snapshots via
  `cc-usage quota import`; **both the window timestamps and the snapshot capture time are recorded
  separately** (user-supplied values may be pasted after the fact). Exhaustion/rate-limit events
  (1302/1308/1310) land in `provider_events` with reset timestamps when present.
- **Shared-window rule:** when >1 session is active in a window, consumption is "shared evidence";
  per-ticket quota attribution is **forbidden by construction** (no view computes it). Quota
  efficiency exists **only at provider-window grain** (tokens per window alongside active-session
  counts), and every quota/token juxtaposition carries the caption "shared window, N active
  sessions, no conversion". Side-by-side series with uncertainty notes only.

## 9. Cost — four measures with per-provider availability (P1-9; findings 11, 12)

| Measure | Source | Label |
|---|---|---|
| `actual_billed_usd` | imported statements/exports (manual) | observed |
| `estimated_api_usd` | versioned price table × observed tokens (DeepSeek peak/off-peak + cache tiers; OpenRouter approximate) | estimate |
| `subscription_amortized_usd` | actual Z.ai invoice (settings), allocated **at reporting-period grain (day/week) — never ticket grain** | allocation |
| `marginal_cost_usd` | 0 for plan windows with spare capacity; estimated rate for API work | estimate |

Per-provider availability: **Z.ai plan → invoice + amortized + marginal-cash only** (no per-token
statement exists; `estimated_api_usd` is not computed for it). **DeepSeek / OpenRouter / Anthropic
API / Bedrock → all four.** **Quota opportunity cost is tracked as a separate, unpriced dimension**
(unless real evidence prices it later): consuming scarce plan quota can displace future work or
force overflow to a paid provider, so "marginal cash = 0" must never be presented as "costless".
Cross-provider financial comparisons display which measure is being compared and never blend
amortization into marginal cash cost.

## 10. Dashboard + reports

`cc-usage dashboard` → 127.0.0.1:4399, read-only SQLite, zero-dependency static HTML/JS. Views:
provider comparison (accepted PRs, tickets, tokens by type, wall/tool-active time, cost & quota,
tokens/PR, revision rounds, first-pass acceptance), workflow-phase breakdown, session timeline
drilldown (requests, tools, subagents, commits, errors, compactions), concurrency buckets (1–2 /
3–4 / 5–7+ via session overlap + Orca journal), daily/weekly trends, quota panel with shared-window
captions, filters (provider/model/command/ticket/PR/date/complexity). Every derived/estimated
figure is visually tagged with its basis. CLI reports remain for terminal use.

## 11. Reliability, safety, migrations (P1-7, P1-8; findings 8, 20)

- **Single-writer concurrency policy:** hooks **never write SQLite** — they enqueue one JSON line
  to a per-session spool file (<1 ms); the collector is the sole DB writer (transcript sweeper +
  spool ingester + OTLP ingester serialized through one connection with explicit `busy_timeout`,
  WAL confirmed); dashboard opens read-only. Contention test with 7 simulated concurrent writers.
- **Migration contract:** each migration (a) runs first against a **copy of the live DB** in the
  test suite, (b) keeps the legacy `cc_usage.py` queries (statusline, hooks, reports) passing
  unchanged, (c) is additive-only, (d) is preceded by an automatic `cc-usage backup` with
  demonstrated restore. `install.sh` aborts if (a)/(b) fail.
- **Bounded spool:** failed OTLP persists → disk spool, max size (default 200 MB, configurable);
  when full, drop-oldest with an incremented `dropped` counter surfaced in `doctor` and dashboard.
  Ordered replay on restart (single-writer makes ordering well-defined). Transcript ingest is
  offset-resumable and aborts cleanly under ENOSPC.
- **Spool durability (reconciled with no-auto-deletion):** the spool **never deletes anything**.
  Exact overflow policy: when spool size reaches its cap, the collector **stops accepting new
  failed payloads** (Claude Code still gets 200 — availability first) and records a
  `telemetry_loss` row (first/last dropped timestamps, count, reason); recovery time is recorded
  when acceptance resumes. Coverage gaps derived from that table are surfaced in `doctor` and the
  dashboard. Removal of spooled data happens only via explicit user command.
- **Latency honesty — three classifications:** `measured_api` (`duration_ms` from OTLP
  `api_request` events only), `derived_gap` (transcript-gap timing — includes streaming, client
  processing, hidden retries; never labeled API latency), `unavailable` (TTFT always, and
  everything when OTLP is undelivered). Every stored timing row carries `latency_basis`; reports
  never present derived gaps as measured API duration.
- **Hooks:** unchanged 5 s budget, actual target <50 ms; dead collector never affects Claude Code
  (hook enqueues to spool file; export failure → spool, no retry loop in-process).
- **Test isolation:** synthetic telemetry tests and doctor's self-check run in an isolated
  `CC_USAGE_HOME` temp directory — synthetic records can never mix into real productivity data.
- **Secrets/privacy:** no token values, raw prompts, assistant text, or API bodies stored;
  redaction unit test over every captured attribute path. Config snapshots (P1-8) record: CLI
  version, model/effort/compact-window env, subagent model, MCP server *names* + tool counts,
  permission mode, plugin names, `ORCA_PANE_KEY` presence, load-average sample — never
  credential-bearing values.
- **Health:** `doctor` v2 adds `PRAGMA quick_check`, spool/loss indicators, and a weekly scheduled
  backup reminder with on-demand `cc-usage backup`.
- **Idempotency everywhere:** backfill rerun = 0 new rows; no auto-deletion of anything, ever.

## 12. Backfill & provenance eras (P1-10)

`cc-usage backfill --days 30 | --since/--until`: selects sessions by **event timestamps**
(record `timestamp` ∈ window), not file mtime; incremental refresh of growing files via
size+offset+hash re-check. Provenance eras: `instrumented` (CC_PROVIDER known) vs `historical`
(provider unknown/inferred) — dashboard separates them by default; coverage stats
("% requests with provider identity / phase attribution") shown on every summary.

## 13. Comparison methodology (P1-12; finding 12)

`cc-usage compare` + dashboard views: speed, token efficiency, **quota efficiency (provider-window
grain only, per §8)**, financial efficiency (measure-labeled per §9), rework (revision rounds,
post-review churn), quality (findings by severity, post-merge fix tickets) — grouped by
matched-task sets (same epic/ticket type; complexity from pre-implementation signals and structural
size only), with sample sizes, Wilson intervals, and a confounds panel (concurrency level,
quota-exhaustion overlap, disk incidents, config-snapshot deltas). LOC/PR counts are never equated
with productivity; observations vs inferences separated in every view.

## 14. Validation dataset (P1-11; findings 19, 9)

Five real cases reconciled manually against raw evidence (fixtures + expected values + explicit
gap notes), gating Phases B/C/F:
1. Straightforward single-ticket implementation.
2. PR with multiple Jira issues — gate includes the multi-provider allocation rule (§7).
3. Long session with several workflow commands (multi-phase).
4. Implementation with multiple subagents — nesting case **deliberately produced in a scratch
   session during Phase B** (none exists in the corpus, F10) and stored as the fixture.
5. PR with ChatGPT-review follow-up and multiple revisions.

## 15. Phases & gates

| Phase | Scope | Gate (testable as stated) |
|---|---|---|
| **A** | Telemetry repair + provider/session identity + migrations + spool + single-writer refactor | ≥3 real concurrent sessions yield ≥50 transcript-derived ingest rows with `identity_basis ∈ {native, conflict-resolved}` and **0 dedup violations**; migration proof on DB copy + rollback demo; identity/dedup fixture suite (incl. real harvested conflicting-id + `<synthetic>` + depth-0 + orphan fixtures) green; hook non-blocking proven (<50 ms, dead-collector survival); OTLP delivery matrix executed and outcome documented; **A12 real-data reconciliation**: several existing GLM sessions recomputed independently from raw transcripts — exact equality required wherever raw usage is complete, incomplete records reported separately; **no Claude Code upgrade, no launcher/routing changes** (baseline integrity); paid-API live testing minimal and reported; **collection left on permanently after A** |
| **B** | Workflow state machine + subagent rollups | Multi-phase, resume-attachment, and depth-2 nesting validated on real samples (incl. datasets #3, #4) |
| **C** | GitHub/Jira correlation | Multi-ticket + stacked PR reconciliation without duplicate credit (datasets #2, #5; multi-provider rule exercised) |
| **D** | Quota + cost | Shared-window rule enforced; per-provider measure table honored (§9) |
| **E** | Dashboard + backfill | §10 views live; 30-day event-time backfill with provenance eras; rerun = 0 duplicates |
| **F** | Comparative validation | §13 methodology on real data; datasets #1–#5 reconciled; Appendix A questions answered with sample sizes + uncertainty |

## 16. Open assumptions & risks (unresolved)

1. OTLP non-delivery root cause (endpoint-env handling vs exporter behavior on 2.1.206) — Phase A
   delivery matrix; transcripts are primary regardless.
2. CLI 2.1.206 predates 2.1.214 telemetry fixes — upgrade recommended, user decision.
3. First-party `ttftMs` hypothesis unverified — TTFT treated as unavailable.
4. **Resume/compact re-serialization unobserved** (F13) — cross-file id behavior untested until a
   real resume occurs; guarded by §3.1 invariant + fixture.
5. Z.ai quota semantics third-party-sourced until dashboard snapshots ground them (Phase D doc
   re-check).
6. Jira transitions need snapshot imports or a future read-only token.
7. OTLP metric aggregation on 2.1.206 may double-count (pre-2.1.214) — metrics stay secondary,
   tolerance-checked; transcripts are the source of record.
8. Orca journal DB not yet opened — session↔workspace join designed but unverified (Phase E).
9. OTLP-derived request rows are stored in a **separate** `otel_api_requests` table (never merged
   into `api_requests`) so legacy transcript-only report semantics cannot double-count; the same
   applies to any pre-2.1.214 metrics aggregation (kept out of accounting entirely in Phase A).
10. Claude Code is **not** upgraded during Phase A (baseline integrity); the delivery matrix
    verifies the installed version's actual behavior. Paid-API live tests are minimal and reported.

## Appendix A — Success questions (inlined from mission, for Phase F)

1. How many tokens did RUS-6225 consume across all implementation and review sessions?
2. How much Z.ai quota was consumed to produce PR #1419? *(answerable only as shared-window
   evidence per §8)*
3. Which command consumes the most tokens?
4. Does `challenge-artifact` materially improve PR acceptance?
5. Are GLM sessions becoming slower for comparable tasks?
6. Does DeepSeek produce more accepted work per dollar?
7. Do seven concurrent sessions increase or decrease total throughput?
8. How many additional revision rounds are required by each provider — and which provider is most
   efficient for implementation versus independent review?

## Changelog v3

Critic findings incorporated: #1 §1/§3 conflict facts corrected, real-fixture mandate; #2 synthetic
records routed to api_errors; #3 identity grain unified `(session_id, message_id)` + cross-file
fixture/invariant; #4 subagent-ness keyed on location not depth; #5 toolUseId primary join + orphan
state; #6 nested/overlapping phase semantics + innermost-phase attribution; #7 assistant→prompt
parentUuid-chain rule; #8 single-writer policy + busy_timeout + contention test; #9 multi-provider
PR allocation rule defined; #10 stacked-PR LOC to leaf; #11 per-provider cost measure table +
period-grain amortization; #12 quota efficiency window-grain + mandatory captions; #13 Phase A gate
made testable; #14 mission questions inlined (Appendix A); #15 requestId fact corrected + gateway
registry + model canonicalization + prompt-time env capture; #16 timestamp dropped from surrogate;
#17 fingerprint normalization null≡0; #18 background-call caveat; #19 depth-2 fixture produced in
Phase B scratch session; #20 sessionId canonical + doctor quick_check + snapshot capture-time.
