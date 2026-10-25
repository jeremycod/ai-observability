#!/usr/bin/env python3
"""Standalone migration + rollback proof against a COPY of the live database.
Used by install.sh (step 2) and runnable directly. Never touches the live DB.

Exit 0 = PASS. Steps:
  1. copy the live usage.db via the SQLite backup API (falls back to a synthetic
     legacy-shaped DB when no live DB exists);
  2. record legacy-table counts, run migrations, verify counts unchanged (A2);
  3. execute every legacy v1 query on the migrated copy (A1 compat gate);
  4. rollback proof: a pristine pre-migration copy keeps serving v1 queries.
"""
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cc_usage.db import BASE_SCHEMA, LEGACY_QUERIES, connect, migrate  # noqa: E402

REAL = Path.home() / ".local" / "share" / "cc-usage" / "usage.db"


def copy_live(dest):
    src = sqlite3.connect(str(REAL))
    dst = sqlite3.connect(str(dest))
    src.backup(dst)
    src.close()
    dst.close()


def build_synthetic(dest):
    db = sqlite3.connect(str(dest))
    db.executescript(BASE_SCHEMA)
    db.execute("INSERT INTO sessions VALUES('s1','claude-glm','zai','glm','/tmp','main',"
               "'2026-10-24T10:00:00-07:00',NULL,'startup')")
    db.execute("INSERT INTO api_requests(session_id,ts,model,input_tokens) "
               "VALUES('s1','2026-10-24T10:00:01-07:00','glm',10)")
    db.commit()
    db.close()


def counts(path):
    db = sqlite3.connect(str(path))
    out = []
    for t in ("sessions", "command_events", "prompt_context", "api_requests", "tool_events"):
        try:
            out.append(db.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0])
        except sqlite3.OperationalError:
            out.append(None)
    db.close()
    return out


def run_legacy(db, label):
    for i, (sql, params) in enumerate(LEGACY_QUERIES):
        try:
            list(db.execute(sql, params))
        except Exception as e:  # noqa: BLE001
            print("FAIL: legacy query %d (%s) raised %s" % (i, label, e))
            return False
    return True


def main():
    tmp = Path(tempfile.mkdtemp(prefix="cc-usage-proof-"))
    work = tmp / "work.db"
    pristine = tmp / "pristine.db"
    if REAL.exists():
        copy_live(work)
        copy_live(pristine)
        print("source: live copy of %s" % REAL)
    else:
        build_synthetic(work)
        build_synthetic(pristine)
        print("source: synthetic legacy-shaped DB (no live DB found)")
    before = counts(work)
    db = connect(work)
    applied = migrate(db)
    after = counts(work)
    print("applied migrations: %s" % (applied or "none pending"))
    if before != after:
        print("FAIL: legacy table counts changed by migration: %r -> %r" % (before, after))
        return 1
    if not run_legacy(db, "migrated copy"):
        return 1
    db.close()
    legacy = sqlite3.connect(str(pristine))
    legacy.executescript(BASE_SCHEMA)  # exactly what v1 connect() does at startup
    if not run_legacy(legacy, "pristine copy (rollback state)"):
        return 1
    legacy.close()
    print("PASS: migration additive (counts unchanged), legacy queries green on migrated "
          "copy, rollback state green on pristine copy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
