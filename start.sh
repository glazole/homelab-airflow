#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

[[ -f .env ]] || { echo 'Нет .env — сначала ./prepare.sh' >&2; exit 1; }
[[ -s certs/homelab-rootCA.crt ]] || { echo 'Нет локального корневого CA — сначала ./prepare.sh' >&2; exit 1; }
for key in KEYCLOAK_REALM KEYCLOAK_CLIENT_SECRET; do
  value="$(sed -n "s/^${key}=//p" .env | head -n 1)"
  if [[ -z "$value" || "$value" == CHANGE_ME ]]; then
    echo "Заполни ${key} в .env (из существующего Keycloak)." >&2
    exit 1
  fi
done
for network in homelab-db outline-network; do
  docker network inspect "$network" >/dev/null 2>&1 || {
    echo "Отсутствует Docker-сеть $network. Запусти PostgreSQL и Outline stack." >&2
    exit 1
  }
done
if ! docker inspect pg3 --format='{{.State.Health.Status}}' 2>/dev/null | grep -qx healthy; then
  echo 'pg3 не healthy — сначала проверь PostgreSQL.' >&2; exit 1
fi

docker compose config --quiet
# An image rebuild is necessary only on first start or after Dockerfile changes.
if ! docker image inspect handyman-airflow:3.1.8-keycloak >/dev/null 2>&1; then
  docker compose build
fi

echo '=== Airflow DB migration ==='
docker compose run --rm airflow-init

echo '=== Start Airflow ==='
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
printf '\nURL: https://airflow.handyman.moscow\n'
