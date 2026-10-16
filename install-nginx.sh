#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
TARGET='../outline_wiki/nginx/conf.d/airflow.conf'
[[ -d "$(dirname "$TARGET")" ]] || { echo "Отсутствует $(dirname "$TARGET")" >&2; exit 1; }
if [[ -f "$TARGET" ]] && ! cmp -s nginx/airflow.conf "$TARGET"; then
  cp "$TARGET" "$TARGET.bak.$(date +%Y%m%d-%H%M%S)"
fi
cp nginx/airflow.conf "$TARGET"
if docker inspect homelab-nginx --format='{{.State.Running}}' 2>/dev/null | grep -qx true; then
  docker exec homelab-nginx nginx -t
  docker exec homelab-nginx nginx -s reload
else
  echo 'homelab-nginx сейчас не запущен. Конфиг установлен; запусти Outline stack.'
fi
