#!/usr/bin/env bash
set -euo pipefail

cd /root/task

echo "[run] installing dependencies..."
pip install -q -r requirements.txt

echo "[run] starting Redis..."
docker compose up -d

echo "[run] waiting for Redis health..."
ready=0
for i in $(seq 1 30); do
  if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "[run] Redis did not become ready in time" >&2
  exit 1
fi
echo "[run] Redis is ready."

echo "[run] performing Redis Stream readiness round-trip..."
docker compose exec -T redis sh -c '
  redis-cli XADD _readiness_probe "*" k v >/dev/null &&
  redis-cli XLEN _readiness_probe >/dev/null &&
  redis-cli DEL _readiness_probe >/dev/null
' >/dev/null
echo "[run] Redis Stream round-trip OK."

echo "[run] running package selfcheck..."
python -m agent_orchestrator --selfcheck

echo "[run] starter is ready."
exit 0
