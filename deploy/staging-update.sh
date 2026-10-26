#!/bin/bash
# Update the staging receiver + dashboard to the latest code and restart services.
# Run from the repo on the MAIN laptop:  ./deploy/staging-update.sh
# (uses `ssh staging`; services are user-level, no sudo needed)
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

echo "== sync code =="
ssh staging 'mkdir -p /tmp/ai-obs ~/cc-usage'
rsync -a "$REPO/src/cc_usage/" staging:/tmp/ai-obs/cc_usage/
ssh staging 'rm -rf ~/cc-usage/cc_usage && cp -r /tmp/ai-obs/cc_usage ~/cc-usage/ && find ~/cc-usage -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null; true'

echo "== restart services =="
ssh staging 'systemctl --user restart cc-usage-sync cc-usage-dash && sleep 1.5 && systemctl --user is-active cc-usage-sync cc-usage-dash'

echo "== health =="
ssh staging 'curl -s http://127.0.0.1:4398/health; echo; curl -s -o /dev/null -w "dashboard HTTP %{http_code}\n" http://127.0.0.1:4399/'
echo "update complete"
