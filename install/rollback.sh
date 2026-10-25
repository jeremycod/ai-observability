#!/bin/bash
# Phase A rollback: restore the v1 collector + entrypoint. The database is left as-is
# (migrations are additive-only and the v1 queries are proven to work on the migrated
# schema); pass --restore-db <backup.db> to also restore a database backup.
set -euo pipefail
DEST="${CC_USAGE_HOME:-$HOME/.local/share/cc-usage}"
PY="${PYTHON:-python3}"

if [ "${1:-}" = "--restore-db" ]; then
  BACKUP="$2"
  [ -f "$BACKUP" ] || { echo "backup not found: $BACKUP"; exit 1; }
  "$PY" "$DEST/cc_usage.py" collector stop || true
  "$PY" - "$BACKUP" "$DEST/usage.db" <<'EOF'
import sqlite3, sys
src = sqlite3.connect(sys.argv[1]); dst = sqlite3.connect(sys.argv[2])
src.backup(dst); src.close(); dst.close()
EOF
  echo "database restored from $BACKUP"
fi

"$PY" "$DEST/cc_usage.py" collector stop || true
cp "$DEST/cc_usage_legacy_v1.py" "$DEST/cc_usage.py"
"$PY" "$DEST/cc_usage.py" collector start
"$PY" "$DEST/cc_usage.py" doctor || true
echo "rollback complete: v1 collector + entrypoint restored"
