#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker compose stop airflow-api-server airflow-scheduler airflow-dag-processor
# Does not stop pg3, Keycloak, Outline, or delete persistent logs.
