"""A1/A2: migration safety on a copy of the live DB + legacy compatibility + rollback."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import common  # noqa: F401

from cc_usage.db import BASE_SCHEMA, LEGACY_QUERIES, connect, migrate

REAL_DB = Path.home() / ".local" / "share" / "cc-usage" / "usage.db"


def copy_live_db(dest):
    src = sqlite3.connect(str(REAL_DB))
    dst = sqlite3.connect(str(dest))
    src.backup(dst)
    src.close()
    dst.close()


def build_legacy_db(dest):
    db = sqlite3.connect(str(dest))
    db.executescript(BASE_SCHEMA)
    db.execute("INSERT INTO sessions VALUES('s1','claude-glm','zai','glm-5.3-flash','/tmp',"
               "'main','2026-10-24T10:00:00-07:00',NULL,'startup')")
    db.execute("INSERT INTO api_requests(session_id,ts,model,input_tokens,output_tokens,"
               "cache_read_tokens,cache_creation_tokens) VALUES('s1',"
               "'2026-10-24T10:00:01-07:00','glm-5.3-flash',10,20,30,40)")
    db.execute("INSERT INTO command_events(session_id,prompt_id,ts,command_name) "
               "VALUES('s1','p1','2026-10-24T10:00:00-07:00','implement-ticket')")
    db.execute("INSERT INTO prompt_context VALUES('s1','p1',"
               "'2026-10-24T10:00:00-07:00','RUS-6225')")
    db.commit()
    db.close()


def counts(path):
    db = sqlite3.connect(str(path))
    out = {}
    for t in ("sessions", "command_events", "prompt_context", "api_requests", "tool_events"):
        try:
            out[t] = db.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
        except sqlite3.OperationalError:
            out[t] = None
    db.close()
    return out


class TestMigrationOnCopy(unittest.TestCase):
    def test_migrate_copy_and_legacy_compat(self):
        tmp = Path(tempfile.mkdtemp(prefix="cc-usage-mig-"))
        target = tmp / "copy.db"
        if REAL_DB.exists():
            copy_live_db(target)
            source = "live"
        else:
            build_legacy_db(target)
            source = "synthetic"
        before = counts(target)
        db = connect(target)
        applied = migrate(db)
        self.assertTrue(any(v == 1 for v, _ in applied))
        # A2: legacy table counts unchanged by an additive migration
        self.assertEqual(counts(target), before)
        # legacy queries (statusline / reports / hooks) must all still execute
        for sql, params in LEGACY_QUERIES:
            list(db.execute(sql, params))
        db.close()

    def test_migration_idempotent(self):
        tmp = Path(tempfile.mkdtemp(prefix="cc-usage-mig2-"))
        target = tmp / "x.db"
        build_legacy_db(target)
        db = connect(target)
        migrate(db)
        migrate(db)  # second run applies nothing and must not raise
        row = db.execute("SELECT COUNT(*) c FROM schema_migrations").fetchone()
        self.assertEqual(row["c"], 1)
        db.close()

    def test_rollback_preserves_v1_behavior(self):
        # Rollback proof: a pristine (pre-migration) copy keeps serving v1 queries.
        tmp = Path(tempfile.mkdtemp(prefix="cc-usage-rb-"))
        pristine = tmp / "pristine.db"
        build_legacy_db(pristine)
        legacy = sqlite3.connect(str(pristine))
        legacy.executescript(BASE_SCHEMA)  # exactly what v1 connect() does on startup
        for sql, params in LEGACY_QUERIES:
            list(legacy.execute(sql, params))
        legacy.close()


if __name__ == "__main__":
    unittest.main()
