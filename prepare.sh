#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v docker >/dev/null; then
  echo 'Docker не найден.' >&2; exit 1
fi
if ! docker inspect pg3 --format='{{.State.Running}}' 2>/dev/null | grep -qx true; then
  echo 'Сначала запусти pg3 из ~/projects/homelab/postgres.' >&2; exit 1
fi

mkdir -p dags logs plugins certs

if [[ ! -f .env ]]; then
  python3 - <<'PY'
import base64, os, pathlib, secrets
p = pathlib.Path('.env.example').read_text()
values = {
    'AIRFLOW_UID=1000': f'AIRFLOW_UID={os.getuid()}',
    'AIRFLOW_DB_PASSWORD=CHANGE_ME': f'AIRFLOW_DB_PASSWORD={secrets.token_hex(32)}',
    'AIRFLOW_FERNET_KEY=CHANGE_ME': 'AIRFLOW_FERNET_KEY=' + base64.urlsafe_b64encode(os.urandom(32)).decode(),
    'AIRFLOW_API_SECRET_KEY=CHANGE_ME': 'AIRFLOW_API_SECRET_KEY=' + secrets.token_hex(32),
    'AIRFLOW_INTERNAL_API_SECRET_KEY=CHANGE_ME': 'AIRFLOW_INTERNAL_API_SECRET_KEY=' + secrets.token_hex(32),
    'AIRFLOW_JWT_SECRET=CHANGE_ME': 'AIRFLOW_JWT_SECRET=' + secrets.token_hex(32),
}
for old, new in values.items():
    assert old in p, old
    p = p.replace(old, new)
pathlib.Path('.env').write_text(p)
os.chmod('.env', 0o600)
print('Создан .env с индивидуальными секретами; дополни параметры Keycloak.')
PY
else
  echo 'Файл .env существует; секреты сохраняются.'
fi

# This is the ROOT certificate only; never copy a private CA key.
ROOT_CA="../outline_wiki/nginx/certs/rootCA.pem"
if [[ ! -s "$ROOT_CA" ]]; then
  echo "Не найден корневой сертификат: $ROOT_CA" >&2
  echo 'Проверь путь к rootCA.pem из работающего Outline или настрой доверенный публичный CA.' >&2
  exit 1
fi
cp "$ROOT_CA" certs/homelab-rootCA.crt
chmod 0644 certs/homelab-rootCA.crt

# Only generated hex passwords are accepted, avoiding SQL quoting problems.
DB_PASSWORD="$(sed -n 's/^AIRFLOW_DB_PASSWORD=//p' .env | head -n 1)"
if [[ ! "$DB_PASSWORD" =~ ^[a-f0-9]{64}$ ]]; then
  echo 'Некорректный AIRFLOW_DB_PASSWORD в .env' >&2
  exit 1
fi

echo 'Создаю/проверяю отдельную роль airflow_app в pg3 (без изменения pg1/pg2)...'
docker exec -i pg3 psql -U postgres -d airflow -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_app') THEN
    CREATE ROLE airflow_app LOGIN PASSWORD '$DB_PASSWORD';
  ELSE
    ALTER ROLE airflow_app WITH LOGIN PASSWORD '$DB_PASSWORD';
  END IF;
END
\$\$;
ALTER DATABASE airflow OWNER TO airflow_app;
GRANT CONNECT ON DATABASE airflow TO airflow_app;
GRANT USAGE, CREATE ON SCHEMA public TO airflow_app;
SQL

echo 'Готово. Следующее: настрой OIDC-клиент airflow в Keycloak и заполни KEYCLOAK_REALM/KEYCLOAK_CLIENT_SECRET в .env.'
