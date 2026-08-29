#!/usr/bin/env bash
# Deploy/màj du Market Memory MCP sur le VPS (idempotent).
# Prérequis : /root/mcp-srapper/.env (EVENTS_DSN mcp_ro, MCP_HTTP_PORT, MCP_HTTP_TOKEN).
set -euo pipefail
cd "$(dirname "$0")/.."

git pull --ff-only

# Fail-closed dès le deploy : sans token, le serveur refuserait de démarrer de toute façon.
grep -qE '^MCP_HTTP_TOKEN=.+' .env || { echo "ABORT: MCP_HTTP_TOKEN vide/absent dans .env"; exit 1; }

[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt

cp deploy/market-memory-mcp.service /etc/systemd/system/market-memory-mcp.service
systemctl daemon-reload
systemctl enable --now market-memory-mcp
systemctl restart market-memory-mcp

# `|| true` : sous `set -euo pipefail`, un grep sans match sort 1 et tuerait le script.
PORT="$(grep -E '^MCP_HTTP_PORT=' .env 2>/dev/null | cut -d= -f2 || true)"; PORT="${PORT:-8788}"

# Poll readiness (le service met ~2-3s à écouter + 1re connexion DB) : pas de course.
ok=""
for i in $(seq 1 15); do
  if curl -sf "http://127.0.0.1:${PORT}/healthz" >/dev/null; then ok=1; break; fi
  sleep 1
done
[ -n "$ok" ] && echo "healthz OK (port ${PORT})" || { echo "healthz KO après 15s"; journalctl -u market-memory-mcp -n 15 --no-pager; exit 1; }
echo "market-memory-mcp: $(systemctl is-active market-memory-mcp)"
