#!/bin/bash
# Staging receiver install (run ON the staging server, e.g. 10.0.0.207).
# Requires: python3, a token. Installs a systemd service listening on :4398.
#   sudo CC_USAGE_SYNC_TOKEN='<generate-one>' ./deploy/staging-install.sh
# The token must match CC_USAGE_SYNC_TOKEN on every laptop. LAN-only exposure is
# expected (both laptops are on the same network); do not port-forward to the internet.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEST=/opt/cc-usage
TOKEN="${CC_USAGE_SYNC_TOKEN:-}"
[ -n "$TOKEN" ] || { echo "set CC_USAGE_SYNC_TOKEN first (openssl rand -hex 32)"; exit 1; }

mkdir -p "$DEST"
rm -rf "$DEST/cc_usage"
cp -R "$REPO/src/cc_usage" "$DEST/cc_usage"
find "$DEST/cc_usage" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
mkdir -p /var/lib/cc-usage

cat > /etc/systemd/system/cc-usage-sync.service <<EOF
[Unit]
Description=cc-usage sync receiver (AI coding telemetry aggregator)
After=network.target

[Service]
ExecStart=/usr/bin/python3 $DEST/cc_usage.py serve-sync --port 4398 --db /var/lib/cc-usage/aggregate.db
Environment=CC_USAGE_SYNC_TOKEN=$TOKEN
Restart=always
User=$USER
[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now cc-usage-sync
sleep 1
curl -s http://127.0.0.1:4398/health && echo " <- receiver healthy on :4398"
echo "aggregate DB: /var/lib/cc-usage/aggregate.db"
echo "dashboard against the AGGREGATE (run where convenient, e.g. on staging):"
echo "  python3 $DEST/cc_usage.py dashboard --db /var/lib/cc-usage/aggregate.db --port 4399"
