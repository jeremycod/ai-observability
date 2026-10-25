#!/bin/bash
# Phase A installer. Safety contract (spec v3.1 §11, reviewer safeguards 5-7):
#   1. run the full test suite first — abort on any failure;
#   2. prove migration + rollback on a COPY of the live database before touching anything;
#   3. back up the live DB; preserve the v1 module as cc_usage_legacy_v1.py;
#   4. install the package + entry; restart the collector (v2); run doctor.
# Nothing here upgrades Claude Code, touches launchers, or changes provider routing.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${CC_USAGE_HOME:-$HOME/.local/share/cc-usage}"
PY="${PYTHON:-python3}"

echo "== 1/5 test suite =="
( cd "$REPO" && CC_USAGE_HOME="$(mktemp -d)" "$PY" -m unittest discover -s tests -v ) \
  || { echo "ABORT: tests failed"; exit 1; }

echo "== 2/5 migration + rollback proof on a live-DB copy =="
"$PY" "$REPO/tests/migration_proof.py" || { echo "ABORT: migration proof failed"; exit 1; }

echo "== 3/5 backup + preserve v1 =="
mkdir -p "$DEST/backups"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="$DEST/backups/pre-phase-a-$STAMP.db"
"$PY" - "$DEST/usage.db" "$BACKUP" <<'EOF'
import sqlite3, sys
src = sqlite3.connect(sys.argv[1]); dst = sqlite3.connect(sys.argv[2])
src.backup(dst); src.close(); dst.close()
print("backup:", sys.argv[2])
EOF
[ -f "$DEST/cc_usage_legacy_v1.py" ] || cp "$DEST/cc_usage.py" "$DEST/cc_usage_legacy_v1.py"
echo "v1 module preserved: $DEST/cc_usage_legacy_v1.py"

echo "== 4/5 install package + entry =="
rm -rf "$DEST/cc_usage"
mkdir -p "$DEST"
cp -R "$REPO/src/cc_usage" "$DEST/cc_usage"
find "$DEST/cc_usage" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
cp "$REPO/install/cc_usage_entry.py" "$DEST/cc_usage.py"
chmod +x "$DEST/cc_usage.py"

echo "== 5/5 restart collector (v2) + doctor =="
"$PY" "$DEST/cc_usage.py" collector restart
"$PY" "$DEST/cc_usage.py" doctor || true

cat <<EOF

Phase A installed. Rollback (no data loss — all migrations are additive):
  1. $PY "$DEST/cc_usage_legacy_v1.py" collector restart   # back to the v1 collector
  2. cp "$DEST/cc_usage_legacy_v1.py" "$DEST/cc_usage.py"  # restore the v1 entrypoint
  3. optional full DB rollback: restore "$BACKUP" over "$DEST/usage.db"
EOF
