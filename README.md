# ai-observability

Local, private telemetry for AI-assisted development: per-request token accounting,
provider identity, subagent rollups, and (later phases) Jira/PR correlation, quota and
cost analysis across multi-provider Claude Code workflows (Z.ai GLM plan, DeepSeek,
OpenRouter, Anthropic, Bedrock).

Local-first: one SQLite database, one background collector, zero cloud services, zero
dependencies beyond Python 3.9 stdlib. Spec: `docs/superpowers/specs/2026-10-24-ai-observability-design.md` (v3.1).

## Install (new machine)

```bash
git clone git@github.com:jeremycod/ai-observability.git
cd ai-observability
./install/install.sh          # runs tests, migration proof on a DB copy, backs up,
                              # installs to ~/.local/share/cc-usage, starts collector
```

Requires an existing cc-usage v1 installation (hooks in `~/.claude/settings.json` and the
`~/.local/bin/cc-usage` shim are reused untouched; v1 is preserved as
`cc_usage_legacy_v1.py` for one-command rollback — see `install/rollback.sh`).

## What runs where

| Component | What it does | Started by |
|---|---|---|
| **Hooks** (`cc-usage hook`) | reduce every hook event to derived fields (no prompt text ever hits disk) and append to a spool file (<1 ms, never touches SQLite) | Claude Code, every prompt |
| **Collector** (`cc-usage collector`) | the single SQLite writer: consumes the hook spool, ingests transcript JSONLs incrementally, receives OTLP logs (chunked HTTP) with a durable bounded spool | auto-start from any launcher; `collector status` to check |
| **CLI** | reports, ingest/backfill requests, reconciliation, doctor | you |

## Daily commands

```bash
cc-usage doctor                        # full health: collector, migrations, ingest, loss, self-check
cc-usage collector status              # is capture running
cc-usage today | week | month          # token summaries (legacy-compatible reports)
cc-usage providers --period week       # tokens by launcher/provider
cc-usage matrix --period week          # skill × launcher token matrix
cc-usage sessions --period today       # per-session rollups
cc-usage ingest                        # force a transcript sweep now (request → collector)
cc-usage backfill --days 30            # event-time-windowed historical ingest (separate offsets)
cc-usage phases build                  # classify workflow phases + lineage + dispatch join
cc-usage phases show <session-id>      # phase timeline + per-phase token split
cc-usage phases show --lineage ticket:RUS-1234   # all sessions of one ticket, phase rollup
cc-usage reconcile <session-id>        # independently recompute a session from raw transcripts
cc-usage pr sync                       # sync PR truth from gh (tickets, rounds, stacks, files)
cc-usage pr outcomes --days 90         # provider outcome rollup (shared-bucket rule)
cc-usage pr show 1417                  # one PR: reviews, rounds, per-provider contributions
cc-usage reviews scan                  # critic-artifact severity counts from .claude/reviews
cc-usage jira import snapshot.json     # Jira lifecycle snapshots (no credentials)
cc-usage dashboard                     # local web observatory on 127.0.0.1:4399 (read-only)
cc-usage spool prune                   # EXPLICIT removal of replayed spool payloads (never automatic)
```

The dashboard (E) renders: attribution-coverage bars (§12 provenance), provider &
PR-outcome tables with the shared-bucket rule, workflow-phase split, daily token and
measured-latency trends, concurrency buckets, the quota panel with shared-window
captions, and a per-session timeline drilldown (phase spans, subagents, errors).
It opens **read-only**, localhost-only, with zero dependencies; every derived figure
carries its basis.

Phase attribution (B): each response is stamped with the workflow phase that encloses it
(implement / critic_review / revision / pr_creation / merge_gate / investigation /
unclassified). Evidence cascade: slash commands > attributionSkill clusters > nothing
(stays unclassified). Spans close at the next command or a ≥4 h idle gap — long tails are
never misattributed. All sessions sharing a Jira key form one lineage; subagent usage
attributes to the phase that dispatched it; OTLP `duration_ms` enriches matching
transcript responses as `measured_api` latency.

The database lives at `~/.local/share/cc-usage/usage.db`. Data model highlights:
`v_model_responses` (deduplicated per-request rows, transcript source of record),
`otel_api_requests` (OTLP cross-check series incl. measured `duration_ms`),
`api_request_conflicts` (full duplicate-usage audit trail), `telemetry_loss` (visible gaps).

## Multi-laptop aggregation (Phase G)

Each laptop keeps its full local database; pushes are incremental, token-authed, and
idempotent to a receiver on your staging server. Every row is tagged with a stable
`device_id`, so the dashboard and CLI can filter **personal laptop / work laptop /
everything**.

Transport: **Tailscale** (works from any network; WireGuard provides encryption and
device identity). On a private tailnet the receiver runs in **open mode — no token
needed**; set `CC_USAGE_SYNC_TOKEN` only if the receiver is ever reachable beyond your
tailnet.

One-time setup:
```bash
# staging server: install Tailscale, then:
sudo ./deploy/staging-install.sh                    # token omitted -> open mode
tailscale ip -4                                     # note the 100.x.y.z address

# each laptop:
export CC_USAGE_SYNC_URL=http://<staging-tailscale-ip-or-magicdns>:4398   # e.g. ~/.zshrc
cc-usage device                                     # shows this machine's device id
cc-usage sync push                                  # initial full push (then incremental)
```

View the aggregate from any tailnet device:
```bash
ssh staging 'python3 /opt/cc-usage/cc_usage.py dashboard \
    --db /var/lib/cc-usage/aggregate.db --port 4399 --host $(tailscale ip -4)'
# then open http://<staging-tailscale-ip>:4399 and pick the device selector
```

Ongoing: `cc-usage sync push` whenever you like (watermarks make it incremental), or
set `CC_USAGE_SYNC_URL` in the environment and the collector auto-pushes after every
sweep (token unnecessary in tailnet open mode).

Rebuilt or wiped the aggregate DB? Each device must re-send its full history once:
`cc-usage sync push --full` (resets watermarks; the push is idempotent, and the
local database is never modified by syncing). Point the dashboard at the aggregate:
`cc-usage dashboard --db /var/lib/cc-usage/aggregate.db` (on staging) and pick the
device in the header selector — `everything` merges both laptops; the shared-bucket
and matched-set rules already handle PRs touched from both.

## Guarantees (spec v3.1)

- No raw prompts, assistant text, API bodies, or secret env values are ever stored.
- Duplicate/rewritten transcript records collapse deterministically; conflicts keep an
  auditable ordered-last resolution with a max-usage reconciliation guard.
- Subagent usage (from `subagents/agent-*.jsonl`) is counted exactly once per ticket rollup.
- Spool overflow never deletes data: it refuses, records the loss, and recovers.
- All migrations are additive; rollback restores v1 without data loss.
