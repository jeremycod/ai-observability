# OTLP Delivery Matrix — Phase A gate A10 evidence

**Date:** 2026-10-25 · **CLI:** Claude Code 2.1.206 · **Collector:** cc-usage v2, 127.0.0.1:43182

## Verdict

**OTLP log delivery is OPERATIONAL.** The multi-month "dead pipeline" symptom
(`api_requests`/`otel_api_requests` empty despite telemetry-enabled sessions) was **not**
an exporter/configuration problem: it was a collector defect present in cc-usage v1 **and**
in the first v2 build — both read the request body via `Content-Length` only, while
Claude Code's OTLP exporter sends **chunked transfer-encoding**. Every payload arrived and
was silently discarded (`bytes=0` reads).

## Timeline of evidence

| Step | Configuration | Result |
|---|---|---|
| Pre-Phase-A audit (Oct 24) | v1 collector + signal-specific endpoint env | 0 rows; no ingest errors; synthetic (non-chunked) probe ingested fine |
| Matrix probe A (Oct 25 09:26) | baseline env (signal-specific logs endpoint) | 0 rows — **now explained**: payload arrived, body discarded (no POST logging existed yet) |
| Matrix probe B (09:26) | + generic `OTEL_EXPORTER_OTLP_ENDPOINT` | 0 rows — same reason |
| Matrix probe C (09:26) | + `OTEL_METRICS_EXPORTER=otlp` + metrics endpoint | 0 rows — same reason |
| V2 restart with POST logging (10:07) | unchanged env | **74 × `POST /v1/logs` observed** — all with `bytes=0` reads → chunked body confirmed as the discard point |
| Chunked-body fix deployed (10:2x) | unchanged env | `otel_api_requests` populated with real rows incl. `duration_ms` (measured API latency) and model/session identity |

## Post-fix live samples (real concurrent sessions, GLM gateway)

| session | model | in | out | cache_read | duration_ms |
|---|---|---|---|---|---|
| fe99fc97… | glm-5.3-flash | 563 | 317 | 220,160 | 11,399 |
| 2fc451d7… | glm-5.3-flash | 251 | 30 | 176,064 | 3,856 |

## Honest classification (per reviewer requirement)

- **Logs (`/v1/logs`): delivered and ingested** (at-least-once via spool; replay-safe).
- **Metrics/traces: accepted, counted, NOT parsed in Phase A** (pre-2.1.214 aggregation
  double-count risk on this CLI build; decision documented in spec §16.9).
- **Probe cost:** 6 minimal headless API calls total (~60k–102k input tokens each against
  the GLM plan), reported here as required by the Phase A authorization.
- **Attempt-proof note:** CLI `--debug` produced no stderr diagnostics in `-p` mode on this
  build, so exporter-side debug evidence was unobtainable; the chunked-encoding discovery
  superseded the need for it (delivery is now positively confirmed, not inferred).
- **Latency:** `duration_ms` from `api_request` log events is the ONLY measured-API latency
  source (`latency_basis='measured_api'` on OTLP rows). Transcript-derived gaps remain a
  separate labeled series (`derived_gap`); TTFT remains unavailable on all providers.

## Regression lock

`tests/test_e2e_tmp.py::TestChunkedOTLPDelivery` posts a chunked OTLP log payload through
a real collector and asserts ingestion — guards the root-cause fix permanently.
