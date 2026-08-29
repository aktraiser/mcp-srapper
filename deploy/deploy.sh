#!/usr/bin/env bash
# Deploy/màj du Market Memory MCP sur le VPS (idempotent).
# Prérequis : /root/mcp-srapper/.env (EVENTS_DSN mcp_ro, MCP_HTTP_PORT, MCP_HTTP_TOKEN).
set -euo pipefail
cd "$(dirname "$0")/.."

git pull --ff-only
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt

cp deploy/market-memory-mcp.service /etc/systemd/system/market-memory-mcp.service
systemctl daemon-reload
systemctl enable --now market-memory-mcp
systemctl restart market-memory-mcp
sleep 2

PORT="$(grep -E '^MCP_HTTP_PORT=' .env | cut -d= -f2)"; PORT="${PORT:-8788}"
curl -sf "http://127.0.0.1:${PORT}/healthz" >/dev/null && echo "healthz OK (port ${PORT})" || { echo "healthz KO"; exit 1; }
echo "market-memory-mcp: $(systemctl is-active market-memory-mcp)"
