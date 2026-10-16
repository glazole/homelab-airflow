# Airflow 3 для handyman.moscow

Локальный Apache Airflow **3.1.8**, **LocalExecutor**, без Celery, Redis и отдельных Worker.

Использует существующие компоненты HP-сервера:

- **pg3** (PostgreSQL 15), БД `airflow`, доступ из Docker-сети `homelab-db` (`pg3:5432`);
- **Keycloak** существующего Outline-стека (`keycloak.handyman.moscow`), один realm, новый OIDC-клиент `airflow`;
- **homelab-nginx** и существующий локальный TLS CA (файл `outline_wiki/nginx/certs/rootCA.pem`);
- интерфейс `https://airflow.handyman.moscow`.

## Структура

```text
~/projects/homelab/
├── postgres/                # работающий pg1 / pg2 / pg3
├── outline_wiki/            # существующие Nginx + Keycloak + RustFS
└── airflow/                 # этот проект
    ├── docker-compose.yml
    ├── Dockerfile
    ├── .env.example
    ├── prepare.sh
    ├── start.sh
    ├── stop.sh
    ├── status.sh
    ├── install-nginx.sh
    ├── nginx/airflow.conf
    ├── dags/homelab_smoke_test.py
    ├── certs/               # локально копируется публичный rootCA сертификат
    ├── logs/                # логи Airflow
    └── plugins/
```

## 1. Разместить проект

Распаковать готовый архив в `~/projects/homelab/`:

```bash
cd ~/projects/homelab
# скопируйте сюда homelab-airflow.tar.gz
# далее:
tar -xzf homelab-airflow.tar.gz
cd airflow
chmod +x ./*.sh
```

**До запуска** проверь, что pg3 здоровый, Outline/Keycloak и homelab-nginx работают и что в Nginx SSL-сертификат покрывает `airflow.handyman.moscow` (wildcard или SAN).

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
docker network ls | grep -E 'homelab-db|outline-network'
```

Если Outline не запущен, запусти его штатным Compose из `../outline_wiki` перед Airflow. Запуск и остановка Airflow **не затрагивают** Outline, Keycloak, pg1 или pg2.

## 2. Подготовить pg3 и локальные секреты

```bash
./prepare.sh
```

Скрипт:

1. Создаёт `.env` с **новыми секретами**, если файл ещё не существует;
2. копирует **публичный корневой сертификат**, не ключ (!) из существующего Outline;
3. создаёт/обновляет прикладную роль PostgreSQL `airflow_app` с доступом к существующей БД `airflow` в **pg3**;
4. не выполняет миграций схемы БД (это сделает `start.sh`).

При повторном запуске `.env` **не перезаписывается**; пароль роли в `pg3` приводится в соответствие с уже существующим `.env`. Права на `.env` — `600`. Не публиковать файл, секреты из Docker и rootCA private key в Git.

## 3. Создать OIDC-клиент airflow в существующем Keycloak

Открой существующую консоль Keycloak: `https://keycloak.handyman.moscow`.

**Используй уже существующий realm Outline**. Realm не создавай, существующих пользователей и клиентов не меняй.

Создай новый клиент:

| Поле Keycloak | Значение |
|---|---|
| Client ID | `airflow` |
| Client type | `OpenID Connect` |
| Client authentication | `ON` |
| Authorization | `ON` (нужен Keycloak Auth Manager) |
| Standard flow | `ON` |
| Implicit flow | `OFF` |
| Root URL | `https://airflow.handyman.moscow` |
| Home URL | `https://airflow.handyman.moscow` |
| Valid redirect URIs | `https://airflow.handyman.moscow/*` |
| Valid post logout redirect URIs | `https://airflow.handyman.moscow/*` |
| Web origins | `https://airflow.handyman.moscow` |

Публичное имя Keycloak должно быть `https://keycloak.handyman.moscow` и доступно **изнутри контейнера API** через Nginx/Docker DNS.

На вкладке **Credentials** получи Client Secret. В `.env` замени:

```ini
KEYCLOAK_REALM=<точное_имя_существующего_realm>
KEYCLOAK_CLIENT_SECRET=<client_secret_клиента_airflow>
```

Не меняй автоматически сгенерированные `AIRFLOW_*_SECRET`, DB_PASSWORD или FERNET_KEY после инициализации. Не копируй секреты в чат.

Документация Apache: https://airflow.apache.org/docs/apache-airflow-providers-keycloak/stable/auth-manager/manage/login.html

## 4. Запустить Airflow

```bash
./start.sh
```

Последовательность: проверка pg3, Docker-сетей, OIDC env → сборка образа (если отсутствует) → `airflow db migrate` → запуск **API Server + Scheduler + DAG Processor**.

Загрузка образа и сборка на первом запуске могут занять время. Рабочих Celery Worker и Redis здесь нет. Triggerer не запускается (для будущих deferrable operators можно добавить отдельно).

Просмотр контейнеров и логов:

```bash
./status.sh
docker compose logs -f airflow-api-server
docker compose logs --tail=100 airflow-scheduler airflow-dag-processor
```

Если изменён Dockerfile, принудительно пересобери:

```bash
docker compose build --pull
./start.sh
```

## 5. Подключить адрес airflow.handyman.moscow к существующему Nginx

```bash
./install-nginx.sh
```

Скрипт копирует **только** `nginx/airflow.conf` в `../outline_wiki/nginx/conf.d/airflow.conf`, при необходимости создаёт backup старого файла, выполняет `nginx -t` и reload **работающего** `homelab-nginx`.

Не добавляется второй Nginx, не меняются server blocks Outline/Keycloak/RustFS. Необходимые Docker DNS aliases и общая сеть `outline-network` уже предусмотрены в существующем Outline-стеке.

Проверки:

```bash
docker exec homelab-nginx nginx -t
docker exec homelab-nginx getent hosts airflow-api-server
docker exec airflow-api-server getent hosts pg3
curl -k -I https://airflow.handyman.moscow/
```

`curl -k` — **только диагностическая** проверка, если сертификат выпущен локальным CA, которому система пока не доверяет. Для браузера нужно импортировать корневой CA или использовать публичный сертификат. Не отключай TLS-проверку в Airflow/Keycloak.

Если получен 502, сначала проверь статус и логи API Server и разрешение `airflow-api-server` из Nginx.

## 6. Настроить авторизацию (не пропускать)

**Вход в Keycloak и права на чтение DAG — разные вещи.** Apache Keycloak Auth Manager требует разрешений в client Authorization.

После запуска, когда клиент `airflow` создан, выполни (будет запрос пароля администратора Keycloak):

```bash
# Диагностика: сухой прогон настройки ресурсов / ролей
# Учётная запись admin может относиться к realm master.
docker compose run --rm airflow-cli keycloak-auth-manager create-all --username admin --dry-run

# Только после успешной проверки — создание разрешений
docker compose run --rm airflow-cli keycloak-auth-manager create-all --username admin
```

Если администратор Keycloak находится в другом realm, укажи `--user-realm`. Команда меняет **авторизационные ресурсы клиента airflow**; если этого не сделать, возможны успешный вход в Keycloak и пустой/недоступный интерфейс Airflow.

После создания разрешений **назначь своему пользователю** подходящую роль клиента (для личного стенда — Admin по доступной модели клиента). Наличие роли в Keycloak не следует путать с системной ролью realm-admin. Не включай разрешения для всех пользователей по умолчанию.

Документация: https://airflow.apache.org/docs/apache-airflow-providers-keycloak/stable/auth-manager/manage/permissions.html

## 7. Проверить end-to-end

В браузере открой **https://airflow.handyman.moscow**. После входа через существующий Keycloak найди DAG `homelab_smoke_test`. Он **без расписания** (`schedule=None`), запускается вручную, выводит строку `Airflow 3 LocalExecutor: task executed successfully`.

Этот smoke test проверит сразу пять вещей: загрузку DAG, `LocalExecutor`, связность API Server / Scheduler, запись результатов в pg3 и просмотр логов в веб-интерфейсе.

## 8. Остановка и безопасность

```bash
./stop.sh     # останавливает ТОЛЬКО три фоновых процесса Airflow
./start.sh    # повторный запуск, БД и логи сохраняются
```

- Для начального стенда **нет** внешней публикации порта `8080`; путь снаружи идёт через существующий Nginx.
- `pg3` уже слушает только `127.0.0.1:55434` на хосте; контейнеры взаимодействуют по `pg3:5432`.
- `.env` содержит секреты и игнорируется Git; настройки ролей и ключей не публиковать.
- `FORWARDED_ALLOW_IPS="*"` подходит только если API Server изолирован за доверенным Nginx и прочие участники Docker-сетей доверенные. Для более строгой изоляции зафиксировать IP прокси.
- LocalExecutor исполняет код DAG с правами процесса Scheduler. Не принимать неизвестные DAG и не предоставлять загрузку кода посторонним.
- Для восстановления сохраняй **pg3**, `.env`, DAG и `logs`; не меняй Fernet key без процедуры ротации.

> Apache предупреждает, что Keycloak Auth Manager ещё является экспериментальным. Эта схема подходит для личного стенда, но перед публичным production-использованием необходимы проверка версий, TLS, политики доступа и резервное копирование.

## 9. Позже: Sparkice

У Sparkice будет отдельный Compose и собственный Git-репозиторий. Airflow не требует развертывания Spark прямо сейчас. Когда Sparkice будет готов, используем общую сеть и S3 (RustFS), добавим DAG запуска Spark-задания через подходящий оператор/контракт без развёртывания новых S3 и Keycloak.
