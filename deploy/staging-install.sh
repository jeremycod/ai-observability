#!/bin/bash
# Staging receiver install (run ON the staging server, e.g. 10.0.0.207).
# Requires: python3, a token. Installs a systemd service listening on :4398.
#   sudo CC_USAGE_SYNC_TOKEN='<generate-one>' ./deploy/staging-install.sh
# Intended to be reached over Tailscale (open mode, no token): only tailnet devices
# can connect. Set CC_USAGE_SYNC_TOKEN to enable bearer auth if it will ever be
# reachable beyond your tailnet.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEST=/opt/cc-usage

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
Restart=always
User=$USER
[Install]
WantedBy=multi-user.target
EOF
if [ -n "$TOKEN" ]; then
  mkdir -p /etc/systemd/system/cc-usage-sync.service.d
  printf '[Service]
Environment=CC_USAGE_SYNC_TOKEN=%s
' "$TOKEN" > /etc/systemd/system/cc-usage-sync.service.d/token.conf
fi

systemctl daemon-reload
systemctl enable --now cc-usage-sync
sleep 1
curl -s http://127.0.0.1:4398/health && echo " <- receiver healthy on :4398"
echo "aggregate DB: /var/lib/cc-usage/aggregate.db"
echo "dashboard against the AGGREGATE (run where convenient, e.g. on staging):"
echo "  python3 $DEST/cc_usage.py dashboard --db /var/lib/cc-usage/aggregate.db --port 4399"
