"""Schema management: frozen base schema, additive-only migrations, legacy compatibility gate.

Design contract (spec v3.1 §11):
- migrations are additive-only (new tables/columns/views/indexes, never destructive);
- every migration must keep the legacy v1 queries passing unchanged (LEGACY_QUERIES);
- the collector is the single SQLite writer; all other components enqueue.
"""
import datetime as dt
import sqlite3

BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  launcher TEXT,
  provider TEXT,
  model TEXT,
  cwd TEXT,
  git_branch TEXT,
  started_at TEXT,
  ended_at TEXT,
  source TEXT
);
CREATE TABLE IF NOT EXISTS command_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  prompt_id TEXT,
  ts TEXT NOT NULL,
  command_name TEXT,
  command_args TEXT,
  command_source TEXT,
  jira_ticket TEXT
);
CREATE INDEX IF NOT EXISTS idx_command_session ON command_events(session_id, ts);
CREATE TABLE IF NOT EXISTS prompt_context (
  session_id TEXT NOT NULL,
  prompt_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  jira_ticket TEXT,
  PRIMARY KEY(session_id, prompt_id)
);
CREATE TABLE IF NOT EXISTS api_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT,
  prompt_id TEXT,
  ts TEXT,
  launcher TEXT,
  provider TEXT,
  model TEXT,
  skill_name TEXT,
  agent_name TEXT,
  query_source TEXT,
  input_tokens INTEGER DEFAULT 0,
  output_tokens INTEGER DEFAULT 0,
  cache_read_tokens INTEGER DEFAULT 0,
  cache_creation_tokens INTEGER DEFAULT 0,
  cost_usd REAL DEFAULT 0,
  duration_ms INTEGER DEFAULT 0,
  request_id TEXT,
  jira_ticket TEXT,
  raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_session ON api_requests(session_id, ts);
CREATE INDEX IF NOT EXISTS idx_api_skill ON api_requests(skill_name, ts);
CREATE INDEX IF NOT EXISTS idx_api_launcher ON api_requests(launcher, ts);
CREATE TABLE IF NOT EXISTS tool_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT,
  prompt_id TEXT,
  ts TEXT,
  launcher TEXT,
  provider TEXT,
  tool_name TEXT,
  success TEXT,
  duration_ms INTEGER DEFAULT 0,
  skill_name TEXT,
  jira_ticket TEXT
);
CREATE INDEX IF NOT EXISTS idx_tool_session ON tool_events(session_id, ts);
"""

# Migration 001 — Phase A identity, spool/loss accounting, and OTLP cross-check series.
# Note: OTLP-derived request rows go to otel_api_requests (NEVER api_requests) so legacy
# transcript-only report semantics can never double-count (spec §16.9).
M001 = """
ALTER TABLE api_requests ADD COLUMN identity_key TEXT;
ALTER TABLE api_requests ADD COLUMN source TEXT NOT NULL DEFAULT 'transcript';
ALTER TABLE api_requests ADD COLUMN record_ts TEXT;
ALTER TABLE api_requests ADD COLUMN source_file TEXT;
ALTER TABLE api_requests ADD COLUMN byte_offset INTEGER;
ALTER TABLE api_requests ADD COLUMN occurrences INTEGER NOT NULL DEFAULT 1;
ALTER TABLE api_requests ADD COLUMN usage_fp TEXT;
ALTER TABLE api_requests ADD COLUMN identity_conflict INTEGER NOT NULL DEFAULT 0;
ALTER TABLE api_requests ADD COLUMN reconciliation_flag TEXT;
ALTER TABLE api_requests ADD COLUMN is_sidechain INTEGER NOT NULL DEFAULT 0;
ALTER TABLE api_requests ADD COLUMN parent_session_id TEXT;
ALTER TABLE api_requests ADD COLUMN agent_id TEXT;
ALTER TABLE api_requests ADD COLUMN agent_type TEXT;
ALTER TABLE api_requests ADD COLUMN agent_display_name TEXT;
ALTER TABLE api_requests ADD COLUMN spawn_depth INTEGER;
ALTER TABLE api_requests ADD COLUMN gateway TEXT;
ALTER TABLE api_requests ADD COLUMN billing_provider TEXT;
ALTER TABLE api_requests ADD COLUMN model_requested TEXT;
ALTER TABLE api_requests ADD COLUMN ctx_variant TEXT;
ALTER TABLE api_requests ADD COLUMN provider_inferred TEXT;
ALTER TABLE api_requests ADD COLUMN provider_request_id TEXT;
ALTER TABLE api_requests ADD COLUMN latency_basis TEXT NOT NULL DEFAULT 'unavailable';
ALTER TABLE api_requests ADD COLUMN first_ts TEXT;
ALTER TABLE api_requests ADD COLUMN last_ts TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_api_identity ON api_requests(session_id, identity_key);
CREATE INDEX IF NOT EXISTS ix_api_parent ON api_requests(parent_session_id);
CREATE TABLE IF NOT EXISTS api_request_occurrences (
  session_id TEXT NOT NULL,
  identity_key TEXT NOT NULL,
  source_file TEXT NOT NULL,
  byte_offset INTEGER NOT NULL,
  record_ts TEXT,
  input_tokens INTEGER DEFAULT 0,
  output_tokens INTEGER DEFAULT 0,
  cache_read_tokens INTEGER DEFAULT 0,
  cache_creation_tokens INTEGER DEFAULT 0,
  usage_fp TEXT,
  seen_at TEXT NOT NULL,
  PRIMARY KEY (session_id, identity_key, source_file, byte_offset)
);
CREATE TABLE IF NOT EXISTS api_errors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT,
  ts TEXT,
  message_id TEXT,
  model_raw TEXT,
  is_synthetic INTEGER NOT NULL DEFAULT 0,
  error_kind TEXT,
  source TEXT NOT NULL DEFAULT 'transcript',
  source_file TEXT,
  byte_offset INTEGER,
  raw_json TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_api_error ON api_errors(session_id, message_id);
CREATE TABLE IF NOT EXISTS api_request_conflicts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  identity_key TEXT NOT NULL,
  chosen INTEGER NOT NULL DEFAULT 0,
  record_ts TEXT,
  source_file TEXT,
  byte_offset INTEGER,
  input_tokens INTEGER, output_tokens INTEGER,
  cache_read_tokens INTEGER, cache_creation_tokens INTEGER,
  usage_fp TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS otel_api_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT, ts TEXT, model TEXT,
  input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
  cache_read_tokens INTEGER DEFAULT 0, cache_creation_tokens INTEGER DEFAULT 0,
  cost_usd REAL DEFAULT 0, duration_ms INTEGER DEFAULT 0,
  request_id TEXT, prompt_id TEXT, attrs_json TEXT
);
CREATE TABLE IF NOT EXISTS provider_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT, provider TEXT, gateway TEXT, session_id TEXT,
  kind TEXT NOT NULL, code TEXT, detail TEXT, source TEXT, reset_at TEXT, raw_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_provider_events_ts ON provider_events(ts);
CREATE TABLE IF NOT EXISTS config_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT, ts TEXT, cli_version TEXT,
  launcher TEXT, provider TEXT, gateway TEXT, base_url TEXT,
  model_requested TEXT, effort TEXT, compact_window TEXT, subagent_model TEXT,
  permission_mode TEXT, orca_pane_key_present INTEGER NOT NULL DEFAULT 0,
  load_avg TEXT, source TEXT
);
CREATE TABLE IF NOT EXISTS telemetry_loss (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_ts TEXT, last_ts TEXT, dropped INTEGER NOT NULL DEFAULT 0, reason TEXT,
  recovered_at TEXT
);
CREATE TABLE IF NOT EXISTS ingest_state (
  source_file TEXT PRIMARY KEY,
  byte_offset INTEGER NOT NULL DEFAULT 0,
  size INTEGER,
  mtime REAL,
  records INTEGER NOT NULL DEFAULT 0,
  last_ok TEXT
);
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL
);
CREATE VIEW IF NOT EXISTS v_model_responses AS
  SELECT * FROM api_requests
  WHERE source = 'transcript'
    AND (model IS NULL OR model != '<synthetic>');
CREATE VIEW IF NOT EXISTS v_subagent_totals AS
  SELECT parent_session_id, agent_id, agent_type,
         COALESCE(agent_display_name, agent_name) AS agent_name,
         COUNT(*) AS responses,
         SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
         SUM(cache_read_tokens) AS cache_read_tokens,
         SUM(cache_creation_tokens) AS cache_creation_tokens
  FROM api_requests WHERE is_sidechain = 1
  GROUP BY parent_session_id, agent_id;
-- derived_gap_ms is TRANSCRIPT-DERIVED timing (streaming + client time + hidden retries).
-- It is never API latency; latency_basis stays 'derived_gap' for anything sourced from it.
CREATE VIEW IF NOT EXISTS v_request_gaps AS
  SELECT a.id AS row_id, a.session_id, a.record_ts, a.is_sidechain,
         (SELECT MIN((julianday(c.record_ts) - julianday(a.record_ts)) * 86400000.0)
            FROM api_requests c
           WHERE c.source = 'transcript' AND c.session_id = a.session_id
             AND c.is_sidechain = a.is_sidechain AND c.record_ts > a.record_ts) AS derived_gap_ms
  FROM api_requests a
  WHERE a.source = 'transcript';
"""

M002 = """
ALTER TABLE api_requests ADD COLUMN identity_basis TEXT;
UPDATE api_requests SET identity_basis =
  CASE WHEN identity_key LIKE 'sur:%' THEN 'surrogate' ELSE 'native' END
  WHERE identity_basis IS NULL;
UPDATE api_requests SET identity_basis = 'conflict-resolved'
  WHERE identity_conflict = 1 AND identity_basis IN ('native', NULL);
ALTER TABLE command_events ADD COLUMN event_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_command_event_hash ON command_events(event_hash)
  WHERE event_hash IS NOT NULL;
ALTER TABLE config_snapshots ADD COLUMN event_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_config_event_hash ON config_snapshots(event_hash)
  WHERE event_hash IS NOT NULL;
CREATE TABLE IF NOT EXISTS backfill_state (
  source_file TEXT PRIMARY KEY,
  byte_offset INTEGER NOT NULL DEFAULT 0,
  size INTEGER,
  mtime REAL,
  records INTEGER NOT NULL DEFAULT 0,
  last_ok TEXT
);
"""

M003 = """
-- Phase B: workflow attribution layer (spec v3.1 §5, §6).
-- workflow_phases/work_lineages/phase_transitions are DERIVED data: the builder
-- rebuilds them per session (idempotent DELETE+INSERT); raw evidence tables are
-- never touched and nothing is ever auto-deleted.
CREATE TABLE IF NOT EXISTS work_lineages (
  lineage_id TEXT PRIMARY KEY,
  primary_ticket TEXT,
  created_at TEXT,
  session_ids TEXT,
  continuation_of TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS workflow_phases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lineage_id TEXT,
  session_id TEXT NOT NULL,
  phase TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT,
  trigger_kind TEXT,
  prompt_id TEXT,
  ticket_keys TEXT,
  pr_number INTEGER,
  confidence TEXT,
  basis TEXT,
  evidence_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_phases_session ON workflow_phases(session_id, started_at);
CREATE INDEX IF NOT EXISTS ix_phases_lineage ON workflow_phases(lineage_id);
CREATE TABLE IF NOT EXISTS phase_transitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lineage_id TEXT,
  session_id TEXT,
  from_phase TEXT,
  to_phase TEXT,
  at TEXT,
  trigger_kind TEXT,
  evidence_json TEXT
);
CREATE TABLE IF NOT EXISTS subagent_dispatches (
  parent_session_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  tool_use_id TEXT,
  dispatch_ts TEXT,
  phase_id INTEGER,
  agent_type TEXT,
  description TEXT,
  PRIMARY KEY (parent_session_id, agent_id)
);
ALTER TABLE api_requests ADD COLUMN phase_id INTEGER;
ALTER TABLE api_requests ADD COLUMN attribution_skill TEXT;
ALTER TABLE api_requests ADD COLUMN prompt_uuid TEXT;
ALTER TABLE otel_api_requests ADD COLUMN joined_api_request_id INTEGER;
"""

MIGRATIONS = [(1, "phase_a_identity", M001), (2, "phase_a_critic_fixes", M002),
              (3, "phase_b_attribution", M003)]


def now_iso():
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def connect(db_path, timeout=10.0):
    db = sqlite3.connect(str(db_path), timeout=timeout)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript(BASE_SCHEMA)
    return db


def migrate(db):
    """Apply pending migrations. Returns list of applied (version, name)."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )
    applied = []
    for version, name, sql in MIGRATIONS:
        row = db.execute("SELECT version FROM schema_migrations WHERE version=?", (version,)).fetchone()
        if row:
            continue
        db.executescript("BEGIN;" + sql + "COMMIT;")  # atomic per migration (critic P2-3)
        db.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES(?,?,?)",
                   (version, name, now_iso()))
        applied.append((version, name))
    db.commit()
    return applied


# ---------------------------------------------------------------------------
# Legacy v1 compatibility gate. These are the exact query shapes the legacy
# cc_usage.py v1 issues against the database (statusline, reports, hooks).
# Every migration must keep this list executable. test_db_migrations enforces it.
# ---------------------------------------------------------------------------
LEGACY_QUERIES = [
    ("SELECT launcher,provider FROM sessions WHERE session_id=?", ("s",)),
    ("SELECT SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) x "
     "FROM api_requests WHERE session_id=?", ("s",)),
    ("SELECT COALESCE(NULLIF(skill_name,''),'interactive') s FROM api_requests "
     "WHERE session_id=? ORDER BY id DESC LIMIT 1", ("s",)),
    ("SELECT jira_ticket FROM command_events WHERE session_id=? AND jira_ticket IS NOT NULL "
     "ORDER BY id DESC LIMIT 1", ("s",)),
    ("SELECT COALESCE(NULLIF(launcher,''),'unknown') name, COUNT(*) requests, "
     "SUM(input_tokens) input, SUM(cache_read_tokens) cache_read, "
     "SUM(cache_creation_tokens) cache_create, SUM(output_tokens) output, "
     "SUM(input_tokens+cache_read_tokens+cache_creation_tokens+output_tokens) total, "
     "SUM(cost_usd) cost FROM api_requests WHERE ts >= ? GROUP BY 1 ORDER BY total DESC",
     ("2000-01-01T00:00:00+00:00",)),
    ("SELECT DISTINCT COALESCE(NULLIF(launcher,''),'unknown') FROM api_requests WHERE ts>=?",
     ("2000-01-01T00:00:00+00:00",)),
    ("SELECT COALESCE(NULLIF(skill_name,''),'interactive') skill, "
     "COALESCE(NULLIF(launcher,''),'unknown') launcher, "
     "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) total "
     "FROM api_requests WHERE ts>=? GROUP BY 1,2", ("2000-01-01T00:00:00+00:00",)),
    ("SELECT s.session_id,s.started_at,s.ended_at,COALESCE(s.launcher,'unknown') launcher, "
     "COALESCE(NULLIF(MAX(a.model),''),NULLIF(s.model,''),'unknown') model, s.git_branch, "
     "COUNT(a.id) req, COALESCE(SUM(a.input_tokens+a.output_tokens+a.cache_read_tokens"
     "+a.cache_creation_tokens),0) total FROM sessions s "
     "LEFT JOIN api_requests a ON a.session_id=s.session_id WHERE s.started_at>=? "
     "GROUP BY s.session_id ORDER BY s.started_at DESC LIMIT 20", ("2000-01-01T00:00:00+00:00",)),
    ("SELECT COUNT(*) req,COUNT(DISTINCT session_id) sessions,SUM(input_tokens) i,"
     "SUM(output_tokens) o,SUM(cache_read_tokens) cr,SUM(cache_creation_tokens) cc,"
     "SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens) total,"
     "SUM(cost_usd) cost FROM api_requests WHERE ts>=?", ("2000-01-01T00:00:00+00:00",)),
    # handle_hook statements (executed with representative params)
    ("INSERT INTO sessions(session_id,launcher,provider,model,cwd,git_branch,started_at,source) "
     "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET "
     "launcher=excluded.launcher,provider=excluded.provider,"
     "model=COALESCE(NULLIF(excluded.model,''),sessions.model),cwd=excluded.cwd,"
     "git_branch=excluded.git_branch",
     ("t", "l", "p", "m", "/tmp", "b", "2026-01-01T00:00:00+00:00", "startup")),
    ("INSERT INTO command_events(session_id,prompt_id,ts,command_name,command_args,"
     "command_source,jira_ticket) VALUES(?,?,?,?,?,?,?)",
     ("t", "p1", "2026-01-01T00:00:00+00:00", "cmd", None, "user", None)),
    ("INSERT OR REPLACE INTO prompt_context(session_id,prompt_id,ts,jira_ticket) VALUES(?,?,?,?)",
     ("t", "p1", "2026-01-01T00:00:00+00:00", None)),
    ("UPDATE sessions SET ended_at=? WHERE session_id=?",
     ("2026-01-01T00:00:00+00:00", "t")),
]
