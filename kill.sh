#!/usr/bin/env bash
set -euo pipefail

echo "[kill] starting cleanup..."

if [ -d /root/task ]; then
  echo "[kill] entering /root/task"
  cd /root/task
else
  echo "[kill] /root/task not found, cleaning from current directory"
fi

echo "[kill] stopping containers..."
docker compose down --remove-orphans || true

echo "[kill] removing named volumes (if any)..."
docker volume rm task_redis_data || true

echo "[kill] removing networks (if any)..."
docker network rm task_default || true

echo "[kill] removing task images (if any)..."
docker rmi -f playback-agent-stream-monitor || true

echo "[kill] pruning docker system..."
docker system prune -a --volumes -f || true

echo "[kill] removing task directory..."
rm -rf /root/task || true

echo "Cleanup completed successfully!"
