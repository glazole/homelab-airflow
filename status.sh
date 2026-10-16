#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker compose ps
echo
echo '=== Last API server logs ==='
docker compose logs --tail=40 airflow-api-server
