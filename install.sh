#!/usr/bin/env bash
# waba-relay installer — mirrors the flow ClawTerminal's quick setup expects:
# install deps, start the server, print the auth token at the end.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Installing dependencies"
npm install --no-audit --no-fund

echo "==> Starting waba-relay"
if command -v service >/dev/null 2>&1 && [ -f /etc/init.d/waba-relay ]; then
  sudo service waba-relay restart
else
  # fall back to foreground-ish start
  pkill -f "node src/server.js" 2>/dev/null || true
  mkdir -p logs
  nohup node src/server.js >> logs/relay.log 2>&1 &
fi
sleep 2

echo "==> Smoke test"
npm test --silent || echo "(!) smoke test had failures — check logs/relay.log"

TOKEN="$(curl -s http://localhost:8765/token | node -pe 'JSON.parse(require("fs").readFileSync(0)).token' 2>/dev/null || true)"
HOST="$(hostname)"
echo
echo "============================================================"
echo " waba-relay is running on ws://0.0.0.0:8765"
echo
echo " In ClawTerminal: Settings -> Relay Server"
echo "   Server:     ws://${HOST}:8765        (same LAN)"
echo "   or via Tailscale: ws://$(hostname -s):8765 or this machine's tailnet IP"
echo "   Auth Token: ${TOKEN:-<run: curl http://localhost:8765/token>}"
echo "============================================================"
