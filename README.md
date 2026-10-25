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
cc-usage reconcile <session-id>        # independently recompute a session from raw transcripts
cc-usage spool prune                   # EXPLICIT removal of replayed spool payloads (never automatic)
```

The database lives at `~/.local/share/cc-usage/usage.db`. Data model highlights:
`v_model_responses` (deduplicated per-request rows, transcript source of record),
`otel_api_requests` (OTLP cross-check series incl. measured `duration_ms`),
`api_request_conflicts` (full duplicate-usage audit trail), `telemetry_loss` (visible gaps).

## Guarantees (spec v3.1)

- No raw prompts, assistant text, API bodies, or secret env values are ever stored.
- Duplicate/rewritten transcript records collapse deterministically; conflicts keep an
  auditable ordered-last resolution with a max-usage reconciliation guard.
- Subagent usage (from `subagents/agent-*.jsonl`) is counted exactly once per ticket rollup.
- Spool overflow never deletes data: it refuses, records the loss, and recovers.
- All migrations are additive; rollback restores v1 without data loss.
