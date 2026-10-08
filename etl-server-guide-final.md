# Гайд по развёртыванию ETL-сервера

Стек: Postgres + RabbitMQ + ElasticSearch + единый Airflow-кластер на образе
`openmetadata/ingestion` (CeleryExecutor) + OpenMetadata Server — всё на Podman
Quadlets, AlmaLinux 10.2. Airflow один: обслуживает и ETL-DAG'и компании (провайдеры
MSSQL/SFTP/Samba/SSH/JDBC/ODBC/dbt/HTTP), и ingestion-пайплайны OpenMetadata,
зарегистрированные как Pipeline Service «Airflow» — отдельного встроенного Airflow
внутри OpenMetadata не разворачивается. Внутренние сервисы (Postgres, RabbitMQ,
ElasticSearch, Airflow Flower, Airflow API Server, OpenMetadata Server) слушают только
`127.0.0.1`; наружу смотрит только один порт — 80, nginx разводит Airflow UI,
OpenMetadata и Flower по путям `/airflow/`, `/openmetadata/`, `/flower/` (последний —
за отдельным логином, поверх общей точки входа). Подсеть
`etl-network` подбирается автоматически под хост, секреты генерируются чистым bash без
внешних зависимостей, бэкапы Postgres идут по systemd-таймеру, лимиты контейнеров
рассчитаны под сервер 32 ГБ RAM / 4-8 vCPU.

**Среда:** AlmaLinux 10.2, root, Podman Quadlets, официальные образы.
**Хранилище:** `/var/storage/containers/` (конфиги, DAGs, скрипты) + `/var/storage/volumes/`
(данные БД, ES, RabbitMQ) + `/var/storage/backups/` (дампы Postgres).

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    ВНЕШНИЕ СИСТЕМЫ                          │
│  (MSSQL, PostgreSQL, SFTP, REST API, dbt, файлы)            │
└──────────────┬─────────────────────────┬────────────────────┘
               │                         │
               │ Провайдеры Airflow      │ Коннекторы OpenMetadata
               │ (движение данных, ETL)  │ (чтение метаданных)
               ▼                         ▼
        ┌──────────────────────────────────────────┐
        │     Airflow DAGs — единый кластер        │
        │  ETL-процессы  +  ingestion-пайплайны    │
        └────────────────────┬─────────────────────┘
                              │ Задачи через RabbitMQ
                              ▼
                  ┌─────────────────────────┐
                  │      Airflow Workers    │
                  │    (выполнение задач)   │
                  └─────┬───────────────┬───┘
       Результаты ETL   │               │  Метаданные из ingestion-DAG'ов
       (целевые         │               │  (HTTP API → OpenMetadata Server)
        системы/DWH)    ▼               ▼
                                  ┌──────────────────────────┐
                                  │   OpenMetadata Server    │
                                  │  (хранение метаданных)   │
                                  └────────────┬─────────────┘
                                               │ PostgreSQL + ElasticSearch
                                               ▼
┌─────────────────────────────────────────────────────────────┐
│                    ХРАНИЛИЩЕ ДАННЫХ                         │
│  PostgreSQL (airflow + openmetadata_db)                     │
│  ElasticSearch (индексы метаданных)                         │
└─────────────────────────────────────────────────────────────┘
```

Ключевое отличие от «наивной» схемы с двумя параллельными путями: у `OpenMetadata
Ingestion` нет собственного независимого маршрута исполнения — это обычные DAG'и,
которые выполняются **на тех же самых** Airflow Workers через тот же RabbitMQ, что
и ETL-DAG'и компании. Отдельного встроенного Airflow под ingestion нет (см. раздел
«OpenMetadata Server» и примечание под ним).

HTTP API между Airflow и OpenMetadata Server работает в обе стороны, и на схеме
показано только направление «передать результат»:
- **Worker → Server** — задача внутри ingestion-DAG'а отправляет собранные метаданные;
- **Server → Airflow** (не показано на схеме отдельной стрелкой, т.к. идёт не через
  Worker, а напрямую к api-server) — OpenMetadata **запускает** ingestion-DAG'и
  через Airflow REST API. Это и есть Pipeline Service «Airflow», настроенный через
  `PIPELINE_SERVICE_CLIENT_ENDPOINT`/`PIPELINE_SERVICE_CLIENT_ENABLED` в `etl.env` (Шаг 6).

### Версии компонентов

| Компонент | Версия | Образ | Примечание |
|---|---|---|---|
| PostgreSQL | 17 | `docker.io/postgres:17` | БД Airflow + БД OpenMetadata — совместимость см. пояснение ниже |
| RabbitMQ | 3.13 (management) | `docker.io/rabbitmq:3.13-management` | брокер Celery |
| ElasticSearch | 9.3.0 | `docker.elastic.co/elasticsearch/elasticsearch:9.3.0` | минимум 9.0.0, рекомендуется 9.3.0 — см. пояснение ниже |
| Nginx | 1.27 (alpine) | `docker.io/nginx:1.27-alpine` | реверс-прокси |
| Apache Airflow | 3.3.1 | `localhost/etl-airflow:1.13.6` (свой, `FROM openmetadata/ingestion:1.13.6`) | версия жёстко зашита в базовый тег `ingestion` — не выбирается отдельно от версии OpenMetadata; провайдеры зашиты сборкой, см. «Кастомный образ Airflow» в Шаге 3 |
| OpenMetadata Server | 1.13.6 | `docker.getcollate.io/openmetadata/server:1.13.6` | |
| OpenMetadata Ingestion | 1.13.6 | `docker.getcollate.io/openmetadata/ingestion:1.13.6` | тот же образ, что и Airflow-кластер — см. «Архитектура» выше |

**Про версию PostgreSQL.** Официальная документация именно под Airflow 3.3.1 (не
общий алиас «stable», который со временем указывает на другой релиз) прямо
перечисляет поддерживаемые версии: **PostgreSQL 13, 14, 15, 16, 17** — 17 в списке
есть явно. У OpenMetadata верхней границы нет вообще, только нижняя (`12.0 или
выше` на части страниц, `15 или выше` — на более новых, той же линии документации,
что уже подтвердила ES 9.x) — то есть 17 не просто поддерживается, а выше нового
рекомендуемого минимума. Оба клиента БД (`psycopg2` у Airflow, `org.postgresql.Driver`
у OpenMetadata) работают поверх стабильного wire-протокола Postgres, который не
меняется между мажорными версиями — рисков на уровне драйверов нет.

**Про версию ElasticSearch.** У самой OpenMetadata документация по этому вопросу
на момент написания гайда противоречит сама себе между страницами: часть страниц
(`/latest/deployment/bare-metal`, `-SNAPSHOT/production-ready-requirements`) всё ещё
показывает старое ограничение «до 8.11.4», а другие, более точечно версионированные
страницы того же семейства (`v1.13.x/deployment/kubernetes/aks`,
`v2.0.x/deployment/bare-metal`, `v2.0.x/deployment/kubernetes/on-prem`) прямо
называют **Elasticsearch 9.x (минимум 9.0.0, рекомендуется 9.3.0)** как
поддерживаемую версию для этой линейки. Так как более свежие/специфичные страницы
явно называют конкретную рекомендованную версию (9.3.0) — а не просто унаследовали
старый текст — используем её. Плюс это подтверждено практическим прогоном (открытый
отчёт о развёртывании OpenMetadata + Elasticsearch 9.3.0: миграция, индексация,
end-to-end ingestion и поиск отработали без проблем).

Тем не менее это мажорный скачок с 8.x, объективно менее обкатанный в связке с
OpenMetadata, чем 8.11.4 — после первого деплоя стоит явно прогнать через UI
OpenMetadata один тестовый ingestion до созданной таблицы и убедиться, что она
находится через поиск, а не полагаться только на здоровый `HealthCmd` контейнера.

**Почему версия Airflow не выбирается отдельно от OpenMetadata.** В этой архитектуре
нет отдельно устанавливаемого Airflow — весь Celery-кластер работает на образе
`openmetadata/ingestion`, а версия Airflow внутри него фиксирована конкретным тегом
OpenMetadata: 1.13.0 бандлит Airflow 3.2.1, начиная с 1.13.5 (и в 1.13.6) — Airflow
3.3.1 (подтверждено официальным changelog: «Airflow → 3.3.1», исправление CVE). То
есть выбор тега `1.13.6` уже даёт нужную версию Airflow — это не два независимых
решения, а одно.

---

## Оглавление

**Архитектура**
- Схема потоков данных и метаданных — раздел «Архитектура» выше
- Таблица версий всех компонентов и пояснение по выбору ElasticSearch 9.3.0 — раздел «Версии компонентов»

**PostgreSQL**
- Хранилище конфигураций Airflow и OpenMetadata — раздел «PostgreSQL (localhost)»

**Apache Airflow**
- Message broker: RabbitMQ — раздел «RabbitMQ (localhost)»
- WebServer: Nginx — раздел «Nginx (внешний доступ)» (конфиг — Шаг 4)
- Executor: Celery Executor — разделы «Airflow Worker», «Airflow Flower (за nginx, localhost)» (мониторинг очереди, свой логин через Basic Auth)
- Server: Airflow — разделы «Airflow Init (Oneshot)», «Airflow API Server», «Airflow Scheduler», «Airflow Dag Processor», «Airflow Triggerer» (два последних — новые обязательные компоненты в Airflow 3.x)
- Провайдеры данных (dbt-cloud, http, jdbc, odbc, mssql, postgres, samba, sftp, ssh, fab, celery) — зашиты в кастомный образ, раздел «Кастомный образ Airflow» в Шаге 3

**OpenMetadata**
- Search Engine: ElasticSearch — раздел «ElasticSearch (localhost)»
- OpenMetadata: execute-migrate-all — раздел «OpenMetadata Migrate (Oneshot)»
- OpenMetadata: Server — раздел «OpenMetadata Server (за nginx, localhost)»
- Ingestion Framework — объединена с Airflow-кластером, отдельный контейнер не разворачивается (примечание сразу после раздела «OpenMetadata Server»)
- Коннекторы (Airflow / MSSQL / PostgreSQL / OpenAPI-REST / dbt Integration / SFTP / Custom Drive) — настраиваются в UI после деплоя (то же примечание)

**Инфраструктура и эксплуатация** *(вне исходных требований, но нужно для деплоя)*
- Шаг 1 — структура каталогов и права
- Шаг 2 — сеть etl-network
- Шаг 3 — сборка кастомного образа Airflow (провайдеры, пиннинг версий через constraints-файл)
- Шаг 4 — конфигурация Nginx (разделение Airflow/OpenMetadata/Flower по URL)
- Шаг 5 — бэкапы (Postgres, ElasticSearch, RabbitMQ, конфигурация)
- Шаг 6 — скрипт развёртывания deploy.sh
- Шаг 7 — запуск и проверка
- Мониторинг в Zabbix — PostgreSQL (плагин агента 2) и контейнеры Podman (Docker-плагин через сокет Podman)
- Итоговая структура файлов
- Управление

---

## Шаг 1. Подготовка структуры каталогов и прав

```bash
dnf install -y podman slirp4netns fuse-overlayfs

# netavark/aardvark-dns — сетевой стек, на котором держится вся DNS-резолюция
# по NetworkAlias= в этом гайде (Шаг 3). На AlmaLinux 10 тянутся как зависимости
# пакета podman автоматически; если по какой-то причине не установились — гайд
# сломается на первом же NetworkAlias, поэтому лучше свериться сразу:
rpm -q netavark aardvark-dns || dnf install -y netavark aardvark-dns

# sysctl для ElasticSearch
cat > /etc/sysctl.d/99-elasticsearch.conf <<'EOF'
vm.max_map_count=262144
EOF
sysctl -p /etc/sysctl.d/99-elasticsearch.conf

# Каталоги
mkdir -p /var/storage/volumes/{postgres,rabbitmq,elasticsearch}
mkdir -p /var/storage/containers/{airflow/{dags,etl-dags,logs,plugins,dag_generated_configs},nginx}
mkdir -p /var/storage/backups
mkdir -p /etc/containers/systemd

# Права для баз данных (Postgres/RabbitMQ=999, ES=1000)
chown -R 999:999 /var/storage/volumes/postgres
chmod 700 /var/storage/volumes/postgres
chown -R 999:999 /var/storage/volumes/rabbitmq
chmod 700 /var/storage/volumes/rabbitmq
chown -R 1000:1000 /var/storage/volumes/elasticsearch
chmod 700 /var/storage/volumes/elasticsearch

# Права для Airflow: UID 50000 (проверено на образе openmetadata/ingestion:1.13.6)
chown -R 50000:0 /var/storage/containers/airflow
```

---

## Шаг 2. Сеть Quadlet — подсеть подбирается автоматически под хост

Вместо жёстко зашитой подсети — определяем свободный `/24` по факту занятости на хосте
(маршруты, адреса интерфейсов, уже существующие Podman-сети). Кандидаты берутся из
диапазона `172.28.0.0–172.31.255.0` — сознательно вне `10.0.0.0/8` и `172.16-27.x.x`,
чтобы не пересечься ни с текущей сетью хоста (`10.234.x.x`), ни с типичными
VPN/VLAN-диапазонами.

Файл `/etc/containers/systemd/etl.network` в этом рецепте больше не создаётся вручную
заранее — он появляется автоматически на Шаге 6 при первом запуске `deploy.sh` (логика
подбора — функция `pick_free_subnet`, см. там). При повторных запусках, если файл уже
существует, подсеть не пересчитывается — Podman не умеет ресайзить сеть под уже
поднятыми контейнерами без их пересоздания.

---

## Шаг 3. Quadlet-файлы контейнеров

> **Почему у шести контейнеров два имени.** Podman резолвит хосты в
> `etl-network` по фактическому `ContainerName=` (через aardvark-dns), а не по
> короткому имени из названия Quadlet-файла. Все `ContainerName=` здесь — с
> префиксом `etl-` (удобно отличать в `podman ps`/`podman exec`/`journalctl`
> от чужих контейнеров на хосте), а connection-строки в `etl.env` и upstream'ы
> в `nginx.conf` — без префикса (`postgres`, `rabbitmq`, `elasticsearch`,
> `airflow-api-server`, `airflow-flower`, `openmetadata-server`). Чтобы короткие
> имена реально резолвились, у этих шести контейнеров явно прописан
> `NetworkAlias=` — без него хосты вроде `postgres` внутри сети просто не
> существовали бы. Остальным контейнерам (scheduler/worker/nginx/init/migrate)
> алиас не нужен — к ним никто не обращается по имени изнутри сети.

### Кастомный образ Airflow — провайдеры зашиты в сборку, не ставятся при старте

`_PIP_ADDITIONAL_REQUIREMENTS` (механизм runtime-установки пакетов при каждом
старте контейнера) сам Airflow прямым текстом называет фичей для
разработки/тестирования и явно предупреждает не использовать её в проде — сообщение
выводится при каждом запуске: `NEVER use it in production! Instead, build a custom
image`. Два практических риска, помимо самого предупреждения: установка идёт заново
при **каждом** старте контейнера (если в момент рестарта недоступен PyPI —
Airflow не поднимется вообще, хотя раньше работал без сети), и версии провайдеров
не зафиксированы — при следующем перезапуске можно внезапно получить более новую
мажорную версию с breaking changes без единого шага в этом гайде, который бы это
заметил. Собираем свой образ вместо этого — ровно так, как рекомендует официальная
документация Airflow.

**Узнать версию Python внутри базового образа** — она нужна, чтобы взять правильный
constraints-файл (см. ниже):
```bash
podman run --rm --entrypoint python3 docker.getcollate.io/openmetadata/ingestion:1.13.6 --version
```
(`--entrypoint` обязателен: штатный entrypoint образа подставляет `airflow` перед
любой командой, и без него вызов превратится в `airflow python3`.)
Дальше в примерах используется `3.12` — если у вас вывелась другая версия, замените
`PYTHON_VERSION` в `Dockerfile` ниже на неё.

**Dockerfile.** Версии провайдеров фиксируются через официальный constraints-файл
Airflow — это файл, который сама команда Apache Airflow публикует под каждый релиз,
с гарантированно протестированным набором совместимых версий всех пакетов
экосистемы (а не «последние на момент сборки», которые могут внезапно конфликтовать
друг с другом):
```bash
mkdir -p /var/storage/containers/airflow-image
cat > /var/storage/containers/airflow-image/Dockerfile <<'EOF'
FROM docker.getcollate.io/openmetadata/ingestion:1.13.6

ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" \
    apache-airflow-providers-dbt-cloud \
    apache-airflow-providers-http \
    apache-airflow-providers-jdbc \
    apache-airflow-providers-odbc \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-postgres \
    apache-airflow-providers-samba \
    apache-airflow-providers-sftp \
    apache-airflow-providers-ssh \
    apache-airflow-providers-fab \
    apache-airflow-providers-celery
EOF
```

**Сборка** (один раз, на самом сервере — без внешнего registry, локальный тег
достаточно, раз образ используется только на этом хосте):
```bash
podman build -t localhost/etl-airflow:1.13.6 /var/storage/containers/airflow-image
```

Дальше во всех Quadlet-файлах Airflow-контейнеров (`Image=`) используется
`localhost/etl-airflow:1.13.6` вместо `docker.getcollate.io/openmetadata/ingestion:1.13.6`
напрямую — OpenMetadata Server и Migrate (Шаг 3 ниже) провайдеры не нужны, поэтому
их `Image=` не меняется.

> Если когда-нибудь понадобится больше одного сервера (несколько worker-хостов,
> HA) — локальный тег `localhost/...` перестанет работать: его видит только
> Podman на этой машине. Тогда нужен настоящий registry (свой Harbor/Nexus,
> либо push в `docker.getcollate.io`-совместимый приватный реестр) и `Image=`
> со полным адресом реестра вместо `localhost/...`. Для одного сервера, как
> здесь, локальный тег — самый простой рабочий вариант.

### PostgreSQL (localhost)
```bash
cat > /etc/containers/systemd/postgres.container <<'EOF'
[Unit]
Description=PostgreSQL
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.io/postgres:17
ContainerName=etl-postgres
NetworkAlias=postgres
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/volumes/postgres:/var/lib/postgresql/data:Z
Volume=/var/storage/containers/init-db.sql:/docker-entrypoint-initdb.d/init-db.sql:ro,z
Volume=/var/storage/containers/postgres/pg_hba.conf:/etc/postgresql-custom/pg_hba.conf:ro,z
PublishPort=127.0.0.1:5432:5432
Network=etl.network
HealthCmd=pg_isready -U airflow
HealthInterval=10s
HealthRetries=5
PodmanArgs=--memory=4g --memory-swap=4g --cpus=2
Exec=postgres -c shared_buffers=1GB -c effective_cache_size=3GB -c max_connections=200 -c work_mem=8MB -c maintenance_work_mem=256MB -c hba_file=/etc/postgresql-custom/pg_hba.conf

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Лимиты под сервер 32 ГБ / 4-8 vCPU: `shared_buffers` ~25% лимита (1GB),
> `effective_cache_size` — оценка того, сколько ОС+Postgres суммарно закешируют (3GB).
> `max_connections=200`: на живом кластере уже при нескольких ingestion-пайплайнах
> было занято 86 из 100 — из них 66 держит пул OpenMetadata, остальное Airflow
> (у каждого компонента свой пул, воркер с `concurrency=12`). Пул OpenMetadata по
> умолчанию растёт до 100 соединений — поэтому в `etl.env` он явно ограничен 50
> (`DB_CONNECTION_POOL_MAX_SIZE`, Шаг 6), иначе один OpenMetadata мог бы занять
> весь лимит. `work_mem=8MB` —
> снижен вдвое вместе с ростом соединений: худший случай (несколько sort/hash на
> запрос × все соединения) должен оставаться в пределах лимита контейнера 4 ГБ.
> Загрузку соединений отслеживайте так:
> `podman exec etl-postgres psql -U airflow -c "SELECT datname, state, count(*) FROM pg_stat_activity GROUP BY 1,2 ORDER BY 3 DESC;"`
> `--memory-swap` явно приравнен к `--memory`, чтобы Podman не разрешил своп сверх лимита
> по умолчанию (до 2×) — для БД своп страниц означает непредсказуемые задержки на чтении
> вместо чистого OOM, который хотя бы виден и предсказуем.
>
> **`hba_file=/etc/postgresql-custom/pg_hba.conf`** — кастомный файл аутентификации,
> создаётся в Шаге 6 (нужна подсеть `etl-network`, известная только там — тот же
> случай, что и с `resolver` у nginx). Идея: `trust` (без пароля) — но только для
> подключений **внутри `etl-network`**, а не `POSTGRES_HOST_AUTH_METHOD=trust`
> (единственный вариант через штатную переменную образа), который открыл бы `trust`
> для «любого контейнера на этом хосте» — официальная документация образа
> прямо предупреждает именно об этом. Файл монтируется отдельно от `$PGDATA`
> (`-c hba_file=...`, а не подмена файла внутри `/var/lib/postgresql/data`), чтобы
> не зависеть от порядка, в котором entrypoint образа генерирует/дополняет
> auto-сгенерированный `pg_hba.conf` при первой инициализации.
>
> Это **не убирает** необходимость знать пароль для подключений извне `etl-network`
> (SSH-туннель на 5432, Шаг 7) — правило `trust` скоуплено строго по CIDR подсети,
> для всего остального (`host all all all`) остаётся обычная `scram-sha-256`-аутентификация
> по паролю, которую Postgres добавляет сам при первой инициализации. И это не снимает
> риск: если когда-нибудь к `etl-network` подключат посторонний контейнер (например,
> для отладки), он получит доступ к Postgres без пароля — тот же самый шаг, что нужен
> и для легитимного использования сети, так что реального расширения периметра тут нет,
> но это стоит держать в голове.
>
> Принцип нарочно простой: снаружи — обычный пароль, внутри `etl-network` — `trust`,
> без дополнительных различений источника ради этого же принципа.
>
> **Один нюанс без практической проверки, но не требующий доработки.**
> `PublishPort=127.0.0.1:5432:5432` — это DNAT через iptables/netavark, и неочевидно
> заранее, каким Postgres **внутри контейнера** увидит источник SSH-туннелированного
> подключения — реальный внешний адрес или адрес шлюза `etl-network` (`172.28.0.1`),
> который сам подпадает под CIDR из `trust`-правила. Если второе — соединения через
> SSH-туннель тоже пройдут без пароля. Это осознанно оставлено как есть в обоих
> случаях — усложнять `pg_hba.conf` ради различения этого источника не стали. Если
> захочется проверить из любопытства: подключиться через SSH-туннель (Шаг 7) и
> посмотреть, спросит ли `psql` пароль.

### RabbitMQ (localhost)
```bash
cat > /etc/containers/systemd/rabbitmq.container <<'EOF'
[Unit]
Description=RabbitMQ
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.io/rabbitmq:3.13-management
ContainerName=etl-rabbitmq
NetworkAlias=rabbitmq
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/volumes/rabbitmq:/var/lib/rabbitmq:Z
Volume=/var/storage/containers/rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro,z
PublishPort=127.0.0.1:5672:5672
PublishPort=127.0.0.1:15672:15672
Network=etl.network
HealthCmd=rabbitmq-diagnostics -q ping
HealthInterval=10s
HealthRetries=5
PodmanArgs=--memory=1.5g --memory-swap=1.5g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

Erlang VM в RabbitMQ 3.13 определяет доступную память через `/proc/meminfo` хоста, а не
через cgroup-лимит контейнера — то есть увидит все 32 ГБ, и штатный
`vm_memory_high_watermark` (40% по умолчанию) посчитает от них, а не от лимита `1.5g`.
Явно фиксируем абсолютный порог, чтобы RabbitMQ душил publishers раньше, чем контейнер
упрётся в OOM:

```bash
mkdir -p /var/storage/containers/rabbitmq
cat > /var/storage/containers/rabbitmq/rabbitmq.conf <<'EOF'
vm_memory_high_watermark.absolute = 1.2GB
disk_free_limit.absolute = 2GB
EOF
```

### ElasticSearch (localhost)
```bash
cat > /etc/containers/systemd/elasticsearch.container <<'EOF'
[Unit]
Description=ElasticSearch
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.elastic.co/elasticsearch/elasticsearch:9.3.0
ContainerName=etl-elasticsearch
NetworkAlias=elasticsearch
Environment=discovery.type=single-node
Environment=xpack.security.enabled=false
Environment="ES_JAVA_OPTS=-Xms2g -Xmx2g"
Environment=path.repo=/usr/share/elasticsearch/snapshots
Volume=/var/storage/volumes/elasticsearch:/usr/share/elasticsearch/data:Z
Volume=/var/storage/backups/es-snapshots:/usr/share/elasticsearch/snapshots:Z
PublishPort=127.0.0.1:9200:9200
Network=etl.network
HealthCmd=curl -sf http://localhost:9200/_cluster/health
HealthInterval=15s
HealthRetries=10
PodmanArgs=--memory=4g --memory-swap=4g --cpus=2

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> `Environment="ES_JAVA_OPTS=..."` — **в кавычках обязательно**: Quadlet разбирает
> `Environment=` по правилам systemd, где пробел разделяет присваивания, и без кавычек
> `-Xmx4g` отвалится как отдельная невалидная запись.
>
> `Xms` всегда равен `Xmx` — иначе JVM ресайзит кучу под нагрузкой, что даёт
> stop-the-world паузы в моменты пиковой нагрузки. Heap — ровно половина лимита
> контейнера (4GB из 8GB): остальное JVM использует под off-heap (Lucene-сегменты,
> файловый кеш ОС).
>
> Heap 2 ГБ при лимите 4 ГБ — с запасом для метаданных OpenMetadata: на живом
> кластере все индексы вместе занимают ~350 МБ. Если каталог вырастет до миллионов
> объектов, увеличивайте оба значения вместе, сохраняя соотношение 1:2.

### Airflow Init (Oneshot)

> **Важно про образ и про версию Airflow.** Ниже везде используется
> `docker.getcollate.io/openmetadata/ingestion:1.13.6` вместо `apache/airflow` — по
> документу Airflow «входит в состав OpenMetadata и не требует отдельной установки»:
> официальный контейнер `ingestion` уже содержит Airflow с предустановленными пакетами
> OpenMetadata, и именно его нужно масштабировать до Celery-кластера, а не поднимать
> второй, независимый Airflow рядом. **Версия Airflow внутри этого образа не выбирается
> отдельно** — она жёстко зашита в конкретный тег `ingestion`: OpenMetadata 1.13.6
> бандлит Airflow 3.3.1 (это подтверждено в официальном changelog 1.13.5/2.0.1 —
> "Airflow → 3.3.1"). То есть выбор тега `1.13.6` уже даёт нужную версию Airflow, а не
> два независимых решения.
>
> **Airflow 3.x — это не Airflow 2.x с патчем.** Архитектура изменилась принципиально:
> - `webserver` заменён на `api-server` (FastAPI вместо Flask, REST API v2 вместо v1);
> - парсинг DAG'ов вынесен из scheduler в отдельный **обязательный** компонент
>   `dag-processor` — без него scheduler вообще не увидит DAG-файлы;
> - появился `triggerer` — обслуживает deferrable-операторы (часть провайдеров,
>   включая некоторые сенсоры, использует его по умолчанию);
> - workers общаются с api-server по HTTP (Execution API), а не напрямую с БД —
>   для этого нужен общий JWT-секрет между всеми компонентами;
> - логин по-прежнему через `airflow users create`, но только если явно подключить
>   `FabAuthManager` — новый дефолт в Airflow 3 его не использует.
>
> **Проверено на реальном образе** (все пункты прогнаны и подтверждены):
> - UID пользователя `airflow` — `50000` (совпадает с `chown -R 50000:0`, Шаг 1);
> - `pip`/сеть внутри образа рабочие — база для `RUN pip install` в `Dockerfile`
>   (Шаг 3, «Кастомный образ Airflow») отработала без проблем;
> - `curl` есть, `/usr/bin/curl` — на нём построены `HealthCmd` ниже;
> - `airflow version` → **3.3.1**, ровно как заявлено в changelog OpenMetadata;
> - `airflow api-server --help` и `airflow dag-processor --help` — обе подкоманды
>   существуют, флаг `--proxy-headers` у `api-server` на месте;
> - `airflow celery flower --help` (после установки `apache-airflow-providers-celery`,
>   см. `Dockerfile` в Шаге 3) — флаги `-A/--basic-auth` и `-u/--url-prefix`
>   подтверждены; в юните Flower они задаются через переменные
>   `AIRFLOW__CELERY__FLOWER_BASIC_AUTH`/`_URL_PREFIX` (см. примечание к Flower ниже).
>
> Дополнительной проверки перед деплоем не требуется.

```bash
cat > /etc/containers/systemd/airflow-init.container <<'EOF'
[Unit]
Description=Airflow Init
After=postgres.service rabbitmq.service
Requires=postgres.service rabbitmq.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-init
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
Volume=/var/storage/containers/init-airflow.sh:/init-airflow.sh:ro,z
Network=etl.network
Entrypoint=/bin/bash
Exec=/init-airflow.sh

[Service]
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
```

> `Entrypoint=/bin/bash` + `Exec=/init-airflow.sh` подменяют штатный entrypoint
> образа — но это уже не имеет значения для провайдеров: они зашиты в сам образ
> `localhost/etl-airflow:1.13.6` через `Dockerfile` (Шаг 3), а не ставятся entrypoint'ом
> при старте, так что `db migrate`/`users create` видят их независимо от того, какой
> entrypoint используется в конкретном контейнере.
> `Type=oneshot` + `RemainAfterExit=yes` — юнит считается «активным» после завершения
> команды, а не всё время работы; на этом основан `Requires=airflow-init.service` у
> всех остальных Airflow-компонентов — systemd не пустит их, пока миграция БД и
> создание админа не завершатся успешно. `airflow users create` работает и в Airflow 3,
> но только когда установлен и подключён `apache-airflow-providers-fab` (ставится в
> `Dockerfile`, Шаг 3; подключается через `AIRFLOW__CORE__AUTH_MANAGER` в `etl.env`,
> Шаг 6) — без этого команда либо не найдётся, либо созданный пользователь не сможет
> залогиниться через обычную форму логина.

### Airflow API Server
```bash
cat > /etc/containers/systemd/airflow-api-server.container <<'EOF'
[Unit]
Description=Airflow API Server
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-api-server
NetworkAlias=airflow-api-server
EnvironmentFile=/var/storage/containers/etl.env
Environment=FORWARDED_ALLOW_IPS=*
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
PublishPort=127.0.0.1:8080:8080
Network=etl.network
Exec=api-server --proxy-headers
HealthCmd=curl -sf http://localhost:8080/airflow/api/v2/monitor/health
HealthInterval=10s
HealthRetries=6
PodmanArgs=--memory=1g --memory-swap=1g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> **Переименовано из "Airflow Webserver".** В Airflow 3 `webserver` (Flask, REST API
> v1) заменён на `api-server` (FastAPI, REST API v2) — команда контейнера и имя
> ContainerName приведены в соответствие, чтобы `podman ps` не врал о том, что реально
> запущено.
>
> `--proxy-headers` — обязательный флаг для uvicorn, без него api-server не доверяет
> заголовкам `X-Forwarded-*` от nginx. `FORWARDED_ALLOW_IPS=*` — нужен, когда прокси не
> в том же контейнере/неймспейсе, что и сам процесс (у нас nginx — отдельный контейнер),
> иначе uvicorn проигнорирует заголовки от «недоверенного» источника даже с
> `--proxy-headers`. `*` — приемлемо для закрытого внутреннего сегмента; для более
> строгой настройки можно указать подсеть `etl-network` вместо `*`.
>
> **Все маршруты Airflow живут под `/airflow`** — это следствие
> `AIRFLOW__API__BASE_URL=http://<IP>/airflow` (Шаг 6), проверено на живом кластере:
> и REST API, и `/airflow/auth/token`, и Execution API для воркеров, и маршруты
> плагина OpenMetadata. Поэтому `HealthCmd` бьёт в `/airflow/api/v2/monitor/health`,
> и **все внутренние адреса Airflow в `etl.env`** (`EXECUTION_API_SERVER_URL`,
> `PIPELINE_SERVICE_CLIENT_ENDPOINT`) тоже с префиксом `/airflow`. Если его
> пропустить, health-проверки через `wget -L`/браузер могут пройти за счёт
> редиректа и замаскировать ошибку, а реальные `POST`/`PATCH`-вызовы получат 404.
>
> **Каталог `dag_generated_configs` общий для всех Airflow-контейнеров.** Когда
> аналитик нажимает Deploy в UI OpenMetadata, плагин в api-server пишет DAG-файл в
> `/opt/airflow/dags`, а его конфигурацию — в `/opt/airflow/dag_generated_configs`.
> Парсит DAG dag-processor, исполняет worker — и обоим нужен тот же конфиг. Без
> общего volume каталог существовал бы только внутри api-server.

### Airflow Scheduler
```bash
cat > /etc/containers/systemd/airflow-scheduler.container <<'EOF'
[Unit]
Description=Airflow Scheduler
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-scheduler
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
Network=etl.network
Exec=scheduler
HealthCmd=airflow jobs check --job-type SchedulerJob --local
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=1.5g --memory-swap=1.5g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> В Airflow 3 scheduler **больше не парсит DAG-файлы сам** — только триггерит запуски
> уже распарсенных и сериализованных в БД DAG'ов. За парсинг отвечает отдельный
> `airflow-dag-processor` ниже — без него scheduler будет висеть «здоровым», но ни один
> DAG не появится в интерфейсе. `HealthCmd` использует **CLI-проверку** (`airflow jobs
> check`) — она читает heartbeat заданий напрямую из таблицы `job` в БД, никакого HTTP
> для этого не требуется. Подтверждено по `--help` на реальном образе:
> `--job-type` принимает ровно `SchedulerJob`/`TriggererJob`/`DagProcessorJob`.
> `--local` (тоже из `--help`) проверяет только задания этого хоста — вместо
> `--hostname "$HOSTNAME"`: знак `$` в юните раскрывает уже systemd, и `$HOSTNAME`
> из окружения юнита оказался бы пустым.
>
> `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true` в `etl.env` (Шаг 6) — это **другой**,
> независимый механизм: опциональный HTTP-сервер на порту 8974 для liveness-проб
> в духе Kubernetes. Наш `HealthCmd` от него не зависит — переменная оставлена на
> будущее (например, если понадобится вынести пробы во внешний мониторинг), но
> можно спокойно убрать, если не планируете её использовать.

### Airflow Dag Processor
```bash
cat > /etc/containers/systemd/airflow-dag-processor.container <<'EOF'
[Unit]
Description=Airflow Dag Processor
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-dag-processor
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
Network=etl.network
Exec=dag-processor
HealthCmd=airflow jobs check --job-type DagProcessorJob --local
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=1g --memory-swap=1g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> **Новый в Airflow 3, обязательный.** В Airflow 2 парсингом DAG-файлов занимался
> сам scheduler в своём главном цикле; в Airflow 3 это вынесено в отдельный процесс —
> заявленная цель Apache — изоляция: код автора DAG теперь не выполняется в том же
> процессе, что планирует и раздаёт задачи. Практическое следствие для нас: если
> забыть про этот контейнер (что легко сделать, апгрейдя чек-лист с Airflow 2), DAG'и
> просто не появятся ни в интерфейсе, ни в `airflow dags list` — при этом ни один
> другой компонент не покажет явной ошибки.

### Airflow Triggerer
```bash
cat > /etc/containers/systemd/airflow-triggerer.container <<'EOF'
[Unit]
Description=Airflow Triggerer
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-triggerer
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
Network=etl.network
Exec=triggerer
HealthCmd=airflow jobs check --job-type TriggererJob --local
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=1g --memory-swap=1g --cpus=0.5

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Обслуживает deferrable-операторы (асинхронное ожидание без занятого worker-слота) —
> часть провайдеров (в т.ч. некоторые сенсоры из установленных нами пакетов) может
> использовать этот режим по умолчанию в новых версиях. CPU нужно немного — это
> событийный цикл (asyncio), а вот память — **не меньше 1 ГБ**: образ
> `openmetadata/ingestion` тяжёлый, и триггерер уже на пустом старте занимает около
> 500 МБ (замерено на живом кластере: пик 513 МБ). При лимите 512 МБ его убивает
> OOM раньше, чем он успевает записать heartbeat, и в UI Airflow индикатор Triggerer
> остаётся красным, хотя контейнер формально «healthy».

### Airflow Worker
```bash
cat > /etc/containers/systemd/airflow-worker.container <<'EOF'
[Unit]
Description=Airflow Celery Worker
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-worker
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
Network=etl.network
Exec=celery worker
HealthCmd=/bin/bash -c 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d celery@`hostname`'
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=8g --memory-swap=8g --cpus=3

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> **Память 8 ГБ и `AIRFLOW__CELERY__WORKER_CONCURRENCY=6`** (в `etl.env`, Шаг 6).
> Ingestion-задачи OpenMetadata тяжёлые: каждая — отдельный Python-процесс, который
> уже при загрузке библиотек (pandas, numexpr, коннекторы) занимает 0.5–1 ГБ и больше
> на крупных базах. На живом кластере при лимите 3 ГБ задачи, стартовавшие
> одновременно по одинаковому расписанию, убивал OOM («Process terminated by signal.
> Likely out of memory error»). 8 ГБ на 6 слотов — ~1.3 ГБ на задачу; лишние задачи
> не теряются, а ждут слота в очереди RabbitMQ. Если нужно больше параллельных
> задач — растите память пропорционально, а не только concurrency. И разнесите
> расписания пайплайнов в OpenMetadata (02:00, 02:30, 03:00…), чтобы не стартовали
> разом.
>
> В Airflow 3 worker обращается к api-server по HTTP за заданиями (Execution API), а
> не читает БД напрямую — это требует общего `AIRFLOW__API_AUTH__JWT_SECRET` со всеми
> остальными компонентами (Шаг 6); при рассинхроне секрета worker будет падать с
> ошибкой авторизации, а не тихо простаивать. В `HealthCmd` имя хоста подставляется
> через `` `hostname` ``, а не `$HOSTNAME` — чтобы в юните не было знака `$`, который
> раскрывает systemd. Путь к celery-приложению — из провайдера
> `apache-airflow-providers-celery` (в Airflow 3 executor живёт только там).

### Airflow Flower (за nginx, localhost)
```bash
cat > /etc/containers/systemd/airflow-flower.container <<'EOF'
[Unit]
Description=Airflow Flower
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=localhost/etl-airflow:1.13.6
ContainerName=etl-airflow-flower
NetworkAlias=airflow-flower
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/etl-dags:/opt/airflow/etl-dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/dag_generated_configs:/opt/airflow/dag_generated_configs:z
PublishPort=127.0.0.1:5555:5555
Network=etl.network
Exec=celery flower
HealthCmd=curl -s -o /dev/null http://localhost:5555/flower/
HealthInterval=15s
HealthRetries=6
PodmanArgs=--memory=512m --memory-swap=512m --cpus=0.5

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> **Логин и сабпуть Flower задаются переменными, а не флагами в `Exec=`.**
> `airflow celery flower` берёт значения по умолчанию для `--basic-auth` и
> `--url-prefix` из конфигурации Airflow — `AIRFLOW__CELERY__FLOWER_BASIC_AUTH` и
> `AIRFLOW__CELERY__FLOWER_URL_PREFIX` в `etl.env` (Шаг 6). Так в юните нет ни
> паролей, ни знаков `$`, которые systemd пытался бы раскрыть сам, и не нужно
> подменять `Entrypoint=`: штатный entrypoint образа сам дописывает `airflow` перед
> `Exec=celery flower`, как и у остальных компонентов.
>
> `HealthCmd` намеренно без `-f`: без логина Flower отвечает `401`, и для curl это
> успешный ответ (процесс жив и слушает порт). Если Flower упал, curl вернёт ошибку
> соединения, и проверка провалится. Так в health-проверке тоже не нужен пароль.

### Nginx (внешний доступ)
```bash
cat > /etc/containers/systemd/nginx.container <<'EOF'
[Unit]
Description=Nginx
After=airflow-api-server.service openmetadata-server.service airflow-flower.service
Wants=airflow-api-server.service openmetadata-server.service airflow-flower.service

[Container]
Image=docker.io/nginx:1.27-alpine
ContainerName=etl-nginx
Volume=/var/storage/containers/nginx/nginx.conf:/etc/nginx/nginx.conf:ro,z
PublishPort=80:80
Network=etl.network
PodmanArgs=--memory=256m --memory-swap=256m --cpus=0.25

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Единственный контейнер с портом на всех интерфейсах (`80:80` без `127.0.0.1:`) —
> это осознанно единственная внешняя точка входа: к Airflow (`/airflow/`),
> OpenMetadata (`/openmetadata/`) и Flower (`/flower/`), см. Шаг 4.
>
> `Wants=`, а не `Requires=`: при `Requires=` падение любого одного сервиса
> (например, Flower) останавливает и nginx — и вместе с ним пропадают все три
> интерфейса. С `Wants=` nginx продолжает работать, а недоступный путь просто
> отдаёт 502. Параметр `resolve` в `upstream` (Шаг 4) позволяет nginx стартовать,
> даже если какой-то бэкенд в этот момент ещё не резолвится. TLS/443 в этом
> рецепте не настраивается.

### OpenMetadata Migrate (Oneshot)
```bash
cat > /etc/containers/systemd/openmetadata-migrate.container <<'EOF'
[Unit]
Description=OpenMetadata Migration
After=postgres.service elasticsearch.service network-online.target
Requires=postgres.service elasticsearch.service
Wants=network-online.target

[Container]
Image=docker.getcollate.io/openmetadata/server:1.13.6
ContainerName=execute-migrate-all
EnvironmentFile=/var/storage/containers/etl.env
Exec=./bootstrap/openmetadata-ops.sh migrate
Network=etl.network

[Service]
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
```

> Проверено на реальном образе `docker.getcollate.io/openmetadata/server:1.13.6`:
> скрипт — `openmetadata-ops.sh`, подкоманда — `migrate`, переменная JVM heap —
> `OPENMETADATA_HEAP_OPTS`. Всё совпадает с тем, что уже прописано здесь и в
> контейнере ниже — дополнительной проверки перед деплоем не требуется.

### Сабпуть OpenMetadata — через `BASE_PATH`, без своего `openmetadata.yaml`

> OpenMetadata работает под `/openmetadata/` за счёт одной переменной
> `BASE_PATH=/openmetadata/` в `etl.env` (Шаг 6): штатный `openmetadata.yaml` образа
> сам подставляет её в пути UI, API и статики. Это подтверждено живым кластером —
> интерфейс и API отвечают на `/openmetadata/...`, а правки переменных окружения
> (`PIPELINE_SERVICE_CLIENT_ENDPOINT` и др.) подхватываются, значит используется
> штатный файл образа со всеми `${...}`-подстановками.
>
> Свой `openmetadata.yaml` поверх штатного **не монтируем**: заменённый файл теряет
> все остальные `${...}`-подстановки (pipeline-клиент, JWT, поиск), а точечный патч
> путей через `sed` рискует дать двойной префикс вида `/openmetadata/openmetadata/api`.

### OpenMetadata Server (за nginx, localhost)
```bash
cat > /etc/containers/systemd/openmetadata-server.container <<'EOF'
[Unit]
Description=OpenMetadata Server
After=openmetadata-migrate.service
Requires=openmetadata-migrate.service

[Container]
Image=docker.getcollate.io/openmetadata/server:1.13.6
ContainerName=etl-om-server
NetworkAlias=openmetadata-server
EnvironmentFile=/var/storage/containers/etl.env
Environment="OPENMETADATA_HEAP_OPTS=-Xms2g -Xmx2g"
PublishPort=127.0.0.1:8585:8585
Network=etl.network
HealthCmd=wget -qO- http://localhost:8585/openmetadata/api/v1/system/version > /dev/null
HealthInterval=10s
HealthRetries=10
PodmanArgs=--memory=3g --memory-swap=3g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Heap **2 ГБ** (`-Xms2g -Xmx2g`, в кавычках — см. примечание к ES) при лимите
> контейнера 3 ГБ: остаток нужен JVM вне heap (потоки, metaspace, буферы). На
> живом кластере с 1 ГБ сервер под нагрузкой ingestion упирался в потолок. Если
> кавычки потерять, `-Xmx` молча отвалится и JVM сама выберет максимум — 25% от
> лимита контейнера. Проверка: `podman inspect etl-om-server --format
> '{{range .Config.Env}}{{println .}}{{end}}' | grep HEAP`.

> Пайплайны ingestion (metadata-сканирование источников) теперь выполняются как обычные
> DAG'и на этом же Celery-кластере — `PIPELINE_SERVICE_CLIENT_ENABLED=true` +
> `PIPELINE_SERVICE_CLIENT_ENDPOINT` в `etl.env` (Шаг 6) регистрируют его как Pipeline Service «Airflow»
> в OpenMetadata. Отдельный контейнер `openmetadata-ingestion` с собственным встроенным
> Airflow не нужен — именно это и означает формулировку документа «Airflow входит
> в состав OpenMetadata и не требует отдельной установки».
>
> **Порт ушёл на localhost, доступ — через nginx на `/openmetadata/`** (Шаг 4);
> сабпуть задаёт `BASE_PATH=/openmetadata/` в `etl.env`. API — по адресу
> `/openmetadata/api/v1/...` (подтверждено на живом кластере), на нём построены
> `HealthCmd` выше и `SERVER_HOST_API_URL` в Шаге 6.
>
> **`PIPELINE_SERVICE_CLIENT_ENDPOINT` — с префиксом `/airflow`.** Именно через него
> OpenMetadata разворачивает и запускает ingestion-пайплайны, созданные аналитиком
> в UI (Test Connection, Deploy, Trigger AutoPilot). Плагин `openmetadata-managed-apis`
> в Airflow 3 регистрирует свои маршруты как Flask Blueprint — поэтому в
> `/airflow/openapi.json` их не видно, это нормально, — и они живут под тем же
> префиксом, что и всё приложение: `/airflow/pluginsv2/api/v2/openmetadata/...`.
> Проверка после деплоя (через `wget`: `curl` в образе `openmetadata/server` нет):
> ```bash
> podman exec etl-om-server wget -S -qO- http://airflow-api-server:8080/airflow/pluginsv2/api/v2/openmetadata/health
> ```
> Ожидается `200` и `{"status":"healthy","version":"1.13.6.1"}`.
>
> Отдельный риск — сама интеграция: OM 1.13.6 официально поддерживает триггер
> ingestion-пайплайнов через Airflow 3.x (в changelog 1.13.5 есть фикс именно для
> этого сценария — "Deploying an ingestion pipeline intermittently returned a 500 or
> timed out on Airflow 3.x"), но раз баг такого рода чинили совсем недавно —
> обязательно проверьте создание и ручной запуск одного тестового ingestion-пайплайна
> через UI OpenMetadata после деплоя, прежде чем полагаться на автоматическое
> расписание в проде.

---

## Шаг 4. Конфигурация Nginx — разделение по URL, не по портам

Вместо отдельного порта 8585 для OpenMetadata и 5555 для Flower — единственный порт 80
наружу с разными путями: `/airflow/`, `/openmetadata/`, `/flower/`. Правило `location = /`
— просто удобный редирект по умолчанию, необязателен. Flower защищён собственной
Basic Auth (Шаг 3) независимо от nginx — это по-прежнему актуально, даже когда доступ
идёт через один общий порт с остальными сервисами.

> **Файл `nginx.conf` фактически создаётся в Шаге 6, не здесь.** Причина —
> директива `resolver` (см. ниже) требует IP шлюза сети `etl-network`, а он
> становится известен только после того, как `deploy.sh` подберёт подсеть
> (Шаг 2). Здесь — только объяснение содержимого, реальный `cat > nginx.conf`
> ищите в `deploy.sh`, сразу после блока определения `$GATEWAY`. Там же, по той
> же причине (нужна подсеть), генерируется `pg_hba.conf` для Postgres — см.
> примечание в разделе «PostgreSQL» (Шаг 3).

```nginx
events { worker_connections 1024; }

http {
    resolver 172.28.0.1 valid=30s ipv6=off;   # ← реальный шлюз подставляется в Шаге 6

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    upstream airflow {
        zone airflow 64k;
        server airflow-api-server:8080 resolve;
    }
    upstream openmetadata {
        zone openmetadata 64k;
        server openmetadata-server:8585 resolve;
    }
    upstream flower {
        zone flower 64k;
        server airflow-flower:5555 resolve;
    }

    server {
        listen 80;
        server_name _;

        location = / {
            return 302 /airflow/;
        }
        location = /airflow      { return 301 /airflow/; }
        location = /openmetadata { return 301 /openmetadata/; }
        location = /flower       { return 301 /flower/; }

        location /airflow/ {
            proxy_pass http://airflow;
            proxy_set_header Host $http_host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_redirect off;
            proxy_read_timeout 3600s;
            proxy_request_buffering off;
            add_header Content-Security-Policy "frame-ancestors 'self';" always;
        }

        location /openmetadata/ {
            proxy_pass http://openmetadata;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_read_timeout 300s;
            proxy_send_timeout 300s;
        }

        location /flower/ {
            proxy_pass http://flower;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Prefix /flower;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
        }
    }
}
```

Три `location` не режут префикс (`proxy_pass http://<upstream>;` без пути после хоста
передаёт исходный URI как есть) — приложение само ожидает видеть путь целиком:
`AIRFLOW__API__BASE_URL` у Airflow, `BASE_PATH` у OpenMetadata, `--url-prefix` у
Flower (Шаг 3/6) регистрируют маршруты сразу под этим путём. Это подтверждено
официальным примером nginx-конфига в документации Airflow 3.3.1 для реверс-прокси.

Что появилось нового по сравнению с предыдущей версией конфига:
- **`resolver ... valid=30s` + `resolve` у каждого `server` внутри `upstream {}`.**
  Без этого nginx резолвит имя контейнера (`airflow-api-server`, и т.д.) **один раз**
  при старте/reload и кеширует IP навсегда — если контейнер перезапустится и получит
  новый IP (а Podman на custom-сети это не гарантирует сохранять), nginx продолжит
  стучаться в мёртвый адрес до собственного рестарта. Важный нюанс: одного
  `resolver` недостаточно — при статическом хосте в `proxy_pass` (без `resolve`)
  директива `resolver` попросту не используется, это самая частая ошибка в такой
  настройке. Параметр `resolve` у `server` внутри `upstream` — official open-source
  способ получить динамическое переразрешение с nginx 1.27.3+ (наш `nginx:1.27-alpine`
  этому требованию удовлетворяет), без него пришлось бы городить `proxy_pass` через
  `set $var` в каждом location. **`zone <имя> 64k;` в каждом `upstream` обязателен**:
  `resolve` работает только для групп в разделяемой памяти, без `zone` nginx не
  проходит проверку конфига («requires upstream to be in shared memory») и не стартует.
- **Редиректы `location = /airflow`/`/openmetadata`/`/flower` без слэша на конце.**
  Без них переход по ссылке без завершающего слэша не матчится ни на один из
  `location /airflow/ {...}` блоков (префиксное совпадение в nginx требует точного
  совпадения префикса) и падает в 404 вместо ожидаемого редиректа.
- **`proxy_read_timeout 300s`** у `/openmetadata/`. Дефолт nginx — 60 секунд, а часть
  страниц OpenMetadata (Settings → Services → Pipelines) строится долго: сервер
  опрашивает Airflow о статусе каждого ingestion-пайплайна. Пока Airflow занят,
  ответ может не уложиться в минуту, и nginx отдаёт `504 Gateway Time-out`
  (наблюдалось на живом кластере).
- **`proxy_request_buffering off`** у `/airflow/` — не буферизует тело запроса
  целиком во временный файл перед тем, как переслать дальше; заметно для
  REST API с крупными телами запросов.
- **`map $http_upgrade $connection_upgrade`** — вместо буквального `Connection "upgrade"`
  на каждый запрос. Буквальное значение ломает обычные (не-WebSocket) запросы в редких
  edge-case'ах прокси; `map`-конструкция — рекомендация из официальной документации
  Airflow 3, ставит `close` для запросов без апгрейда и `upgrade` для запросов с ним.
- **`Content-Security-Policy: frame-ancestors 'self'`** только у `/airflow/` — в
  Airflow 3 часть UI (в т.ч. страницы auth manager) рендерится через iframe; если
  CSP запрещает `frame-ancestors` (например, унаследован от другого location или
  добавлен глобально где-то ещё), эти элементы не отрисуются. У OpenMetadata и Flower
  такой проблемы нет, добавлять им этот заголовок не нужно.
- **`$http_host` вместо `$host`** в `/airflow/` — сохраняет порт в заголовке `Host`,
  если Airflow сравнивает его с `AIRFLOW__API__BASE_URL` при валидации запроса
  (в части версий Airflow строгая проверка `Host` включена по умолчанию за прокси).
- **`X-Forwarded-Host`/`X-Forwarded-Prefix`** у `/flower/` — часть Flower-сборок
  использует именно эти заголовки (а не `Host`/угадывание по `proxy_pass`) для
  построения абсолютных ссылок в интерфейсе под сабпутом.



---

## Шаг 5. Бэкапы (Postgres + ElasticSearch + RabbitMQ + конфигурация) — systemd-таймер вместо cron

Бэкапится всё, из чего реально нельзя тривиально пересобрать состояние:

- **Postgres** — `pg_dumpall` (метабаза Airflow + каталог OpenMetadata);
- **ElasticSearch** — снапшот через штатный Snapshot API (индекс метаданных технически
  пересобираем из Postgres переиндексацией, но снапшот на порядок быстрее полного
  восстановления и не требует ручных действий в UI);
- **RabbitMQ** — экспорт *определений* (пользователи, vhost'ы, очереди, exchange'и,
  политики) через Management API. Сами сообщения в очередях не бэкапятся осознанно —
  это транзитная очередь задач Celery, а не хранилище данных: после восстановления
  Airflow сам пересоздаст нужные задачи по расписанию DAG'ов;
- **Конфигурация** — все Quadlet-юниты, `etl.env`/`secrets.env`, `nginx.conf`,
  `deploy.sh` и сопутствующие скрипты одним архивом. Без этого при потере хоста
  придётся вручную восстанавливать весь этот гайд по памяти.

### Каталог снапшотов ElasticSearch

ES пишет снапшоты только в каталог, явно объявленный через `path.repo` — этот `Volume=`
и `Environment=path.repo=...` уже добавлены в `elasticsearch.container` (Шаг 3).
Здесь достаточно один раз создать сам каталог на хосте с нужными правами до первого
запуска ES (если контейнер уже был запущен без этих строк — после правки
потребуется `systemctl restart elasticsearch`):

```bash
mkdir -p /var/storage/backups/es-snapshots
chown -R 1000:1000 /var/storage/backups/es-snapshots
```

### Скрипт бэкапа

```bash
cat > /var/storage/containers/etl-backup.sh <<'EOF'
#!/bin/bash
set -eo pipefail
# pipefail: без него при падении pg_dumpall конвейер вернул бы код gzip (успех),
# и на диске тихо остался бы пустой дамп
umask 077   # дампы содержат данные и хэши паролей — только для root
BACKUP_DIR=/var/storage/backups
SECRETS=/var/storage/containers/secrets.env
TIMESTAMP=$(date +%F)
MIN_FREE_GB=10

source "$SECRETS"
mkdir -p "$BACKUP_DIR"

# ── 0. Проверка места ДО запуска — лучше явно упасть в лог, чем оставить
#      наполовину записанный дамп или забить диск под ноль ──────────────
free_gb=$(df --output=avail -BG "$BACKUP_DIR" | tail -1 | tr -dc '0-9')
if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
    echo "ОШИБКА: свободно всего ${free_gb}G на $BACKUP_DIR, порог — ${MIN_FREE_GB}G. Бэкап пропущен." >&2
    exit 1
fi

# ── 1. Postgres ──────────────────────────────────────────
podman exec etl-postgres pg_dumpall -U airflow | gzip > "$BACKUP_DIR/pg-${TIMESTAMP}.sql.gz"

# ── 2. ElasticSearch — снапшот через Snapshot API ──────────
REPO_NAME=etl_backup_repo
SNAPSHOT_NAME="snapshot-${TIMESTAMP}"

# Регистрация репозитория идемпотентна — если уже есть, PUT просто перезапишет тем же
curl -sf -X PUT "http://localhost:9200/_snapshot/${REPO_NAME}" \
    -H 'Content-Type: application/json' \
    -d '{"type":"fs","settings":{"location":"/usr/share/elasticsearch/snapshots"}}' \
    > /dev/null

curl -sf -X PUT "http://localhost:9200/_snapshot/${REPO_NAME}/${SNAPSHOT_NAME}?wait_for_completion=true" \
    > /dev/null

# Ротация снапшотов старше 14 дней (сам каталог снапшотов ES не чистит)
CUTOFF=$(date -d '-14 days' +%F 2>/dev/null || date -v-14d +%F)
for snap in $(curl -sf "http://localhost:9200/_snapshot/${REPO_NAME}/_all" | grep -oE '"snapshot-[0-9-]+"' | tr -d '"'); do
    snap_date=${snap#snapshot-}
    if [[ "$snap_date" < "$CUTOFF" ]]; then
        curl -sf -X DELETE "http://localhost:9200/_snapshot/${REPO_NAME}/${snap}" > /dev/null
    fi
done

# ── 3. RabbitMQ — определения (без содержимого очередей) ───
curl -sf -u "airflow:${RABBITMQ_PASS}" http://localhost:15672/api/definitions \
    | gzip > "$BACKUP_DIR/rabbitmq-defs-${TIMESTAMP}.json.gz"

# ── 4. Конфигурация ─────────────────────────────────────
tar czf "$BACKUP_DIR/config-${TIMESTAMP}.tar.gz" \
    --warning=no-file-changed \
    /var/storage/containers/etl.env \
    /var/storage/containers/secrets.env \
    /var/storage/containers/init-db.sql \
    /var/storage/containers/init-airflow.sh \
    /var/storage/containers/nginx/nginx.conf \
    /var/storage/containers/postgres/pg_hba.conf \
    /var/storage/containers/rabbitmq/rabbitmq.conf \
    /var/storage/containers/airflow-image/Dockerfile \
    /var/storage/containers/airflow/etl-dags \
    /var/storage/containers/deploy.sh \
    /var/storage/containers/etl-backup.sh \
    /etc/containers/systemd/*.container \
    /etc/containers/systemd/*.network \
    /etc/systemd/system/etl-backup.service \
    /etc/systemd/system/etl-backup.timer \
    2>/dev/null || true
chmod 600 "$BACKUP_DIR/config-${TIMESTAMP}.tar.gz"

# ── 5. Ротация файловых бэкапов (Postgres/RabbitMQ/конфиг) старше 14 дней ──
find "$BACKUP_DIR" -maxdepth 1 -name 'pg-*.sql.gz' -mtime +14 -delete
find "$BACKUP_DIR" -maxdepth 1 -name 'rabbitmq-defs-*.json.gz' -mtime +14 -delete
find "$BACKUP_DIR" -maxdepth 1 -name 'config-*.tar.gz' -mtime +14 -delete
EOF
chmod +x /var/storage/containers/etl-backup.sh
```

> `tar` в шаге 4 намеренно с `|| true` — если какой-то из файлов конфигурации
> отсутствует (например, ещё не создан на момент первого прогона), архивация
> остальных не должна валить весь бэкап через `set -e`.
>
> Порог `MIN_FREE_GB=10` — отправная точка, не расчётная величина. При диске
> 512 ГБ, поделённом между `/var/storage/volumes` (данные БД/ES/RabbitMQ),
> `/var/storage/containers` (DAG'и, логи Airflow) и `/var/storage/backups`,
> разумный стартовый бюджет под сами бэкапы — 30-50 ГБ (Postgres — основная
> статья, ES-снапшоты и RabbitMQ-определения обычно на порядок меньше, конфиг —
> считанные килобайты). Порог проверки стоит держать заметно ниже этого бюджета
> (не впритык), чтобы получить сигнал заранее, а не в момент, когда бэкапы уже
> перестали влезать.

Сервис (обычный systemd-юнит, не Quadlet — это не контейнер, а запуск скрипта на хосте):

```bash
cat > /etc/systemd/system/etl-backup.service <<'EOF'
[Unit]
Description=Backup Postgres/ElasticSearch/RabbitMQ/config for ETL stack
After=postgres.service elasticsearch.service rabbitmq.service
Requires=postgres.service elasticsearch.service rabbitmq.service

[Service]
Type=oneshot
ExecStart=/var/storage/containers/etl-backup.sh
EOF
```

Таймер (ежедневно в 03:00, со сдвигом до 5 минут, чтобы не бить точно в одну секунду
при нескольких хостах; `Persistent=true` — если хост был выключен в 03:00, бэкап
выполнится сразу после следующего старта):

```bash
cat > /etc/systemd/system/etl-backup.timer <<'EOF'
[Unit]
Description=Daily ETL stack backup timer

[Timer]
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=300
Persistent=true

[Install]
WantedBy=timers.target
EOF
```

Проверка после включения (Шаг 7):
```bash
systemctl list-timers etl-backup.timer
# Разовый прогон вручную, не дожидаясь 03:00:
systemctl start etl-backup.service
ls -la /var/storage/backups/
# Ожидается: pg-<дата>.sql.gz, rabbitmq-defs-<дата>.json.gz, config-<дата>.tar.gz
curl -s http://localhost:9200/_snapshot/etl_backup_repo/_all | grep -o '"snapshot":"[^"]*"'
```

---

## Шаг 6. Скрипт развертывания (`deploy.sh`)

> Провайдеры данных Airflow (dbt-cloud, http, jdbc, odbc, mssql, postgres, samba, sftp,
> ssh, fab, celery) в `etl.env` больше не упоминаются — они зашиты в сборку
> `localhost/etl-airflow:1.13.6` через `Dockerfile` (Шаг 3, «Кастомный образ
> Airflow»), с версиями, зафиксированными официальным constraints-файлом Airflow.
> Если этот образ ещё не собран — `deploy.sh` ниже упадёт на первом же
> `systemctl start airflow-init` с ошибкой «image not found», см. проверку в начале
> скрипта.

```bash
cat > /var/storage/containers/deploy.sh <<'DEPLOY'
#!/bin/bash
set -e

STORAGE="/var/storage"
CONTAINERS_DIR="$STORAGE/containers"
SECRETS="$CONTAINERS_DIR/secrets.env"
ENV="$CONTAINERS_DIR/etl.env"
NETWORK_FILE="/etc/containers/systemd/etl.network"
# grep -v исключает 172.28-31.x.x — это наш же диапазон etl-network (см. ниже).
# Без исключения при повторном запуске deploy.sh (когда сеть уже поднята и у её
# bridge-интерфейса уже есть IP вроде 172.28.0.1) hostname -I может отдать этот
# адрес первым, и AIRFLOW__API__BASE_URL/итоговые URL уедут на несуществующий хост.
# 10.88.x.x — дефолтная сеть podman (интерфейс podman0), по той же причине.
IP=$(hostname -I | tr ' ' '\n' | grep -vE '^172\.(2[89]|3[01])\.|^10\.88\.' | head -1)

# ── Проверка: кастомный образ Airflow должен быть уже собран (Шаг 3) ──
if ! podman image exists localhost/etl-airflow:1.13.6; then
    echo "ОШИБКА: localhost/etl-airflow:1.13.6 не найден." >&2
    echo "Соберите его сначала (Шаг 3, «Кастомный образ Airflow»):" >&2
    echo "  podman build -t localhost/etl-airflow:1.13.6 /var/storage/containers/airflow-image" >&2
    exit 1
fi

# ── 0. Подбор свободной подсети для etl-network ──────────
pick_free_subnet() {
    # Всё, что уже занято на хосте: маршруты, адреса интерфейсов,
    # плюс подсети существующих Podman-сетей
    local used
    used="$( { ip -4 route show; ip -4 addr show; } | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}(/[0-9]+)?' )"
    if command -v podman >/dev/null && [ -n "$(podman network ls -q 2>/dev/null)" ]; then
        used+=$'\n'"$(podman network inspect $(podman network ls -q) 2>/dev/null \
            | grep -oE '"subnet": *"[^"]+"' | cut -d'"' -f4)"
    fi

    # Кандидаты только из 172.28-31.0.0/16 — сознательно вне 10.0.0.0/8
    # (не пересечётся с сетью хоста 10.234.x.x) и вне 172.16-27.x.x
    # (типичный диапазон офисных VPN/VLAN)
    for third in $(seq 28 31); do
        for fourth in $(seq 0 16 240); do
            candidate="172.${third}.${fourth}.0/24"
            if ! grep -qE "^172\.${third}\.${fourth}\." <<<"$used"; then
                echo "$candidate"
                return 0
            fi
        done
    done
    return 1
}

if [ ! -f "$NETWORK_FILE" ]; then
    echo "[0/8] Определение свободной подсети для etl-network..."
    SUBNET=$(pick_free_subnet) || {
        echo "   ОШИБКА: не нашлось свободного /24 в 172.28.0.0-172.31.255.0" >&2
        echo "   Задайте Subnet/Gateway в $NETWORK_FILE вручную и перезапустите." >&2
        exit 1
    }
    if [[ "$SUBNET" == 10.* ]]; then
        echo "   ОШИБКА: подобранная подсеть $SUBNET пересекается с сетью хоста 10.0.0.0/8" >&2
        exit 1
    fi
    GATEWAY="${SUBNET%.0/24}.1"
    echo "   → подсеть $SUBNET, шлюз $GATEWAY (по факту занятости на хосте)"
    cat > "$NETWORK_FILE" <<EOF
[Network]
NetworkName=etl-network
Subnet=${SUBNET}
Gateway=${GATEWAY}
EOF
else
    GATEWAY=$(grep -oP '(?<=^Gateway=).*' "$NETWORK_FILE")
    SUBNET=$(grep -oP '(?<=^Subnet=).*' "$NETWORK_FILE")
    echo "[0/8] $NETWORK_FILE уже существует — подсеть не пересчитываю (сеть уже используется контейнерами), шлюз $GATEWAY"
fi

# ── nginx.conf — создаётся здесь, а не отдельным шагом, потому что resolver
#    требует уже известный $GATEWAY (см. пояснение в Шаге 4) ───────────────
cat > /var/storage/containers/nginx/nginx.conf <<'EOF'
events { worker_connections 1024; }

http {
    resolver __GATEWAY__ valid=30s ipv6=off;

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    upstream airflow {
        zone airflow 64k;
        server airflow-api-server:8080 resolve;
    }
    upstream openmetadata {
        zone openmetadata 64k;
        server openmetadata-server:8585 resolve;
    }
    upstream flower {
        zone flower 64k;
        server airflow-flower:5555 resolve;
    }

    server {
        listen 80;
        server_name _;

        location = / {
            return 302 /airflow/;
        }
        location = /airflow      { return 301 /airflow/; }
        location = /openmetadata { return 301 /openmetadata/; }
        location = /flower       { return 301 /flower/; }

        location /airflow/ {
            proxy_pass http://airflow;
            proxy_set_header Host $http_host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_redirect off;
            proxy_read_timeout 3600s;
            proxy_request_buffering off;
            add_header Content-Security-Policy "frame-ancestors 'self';" always;
        }

        location /openmetadata/ {
            proxy_pass http://openmetadata;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_read_timeout 300s;
            proxy_send_timeout 300s;
        }

        location /flower/ {
            proxy_pass http://flower;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Prefix /flower;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
        }
    }
}
EOF
sed -i "s/__GATEWAY__/${GATEWAY}/" /var/storage/containers/nginx/nginx.conf

# ── pg_hba.conf — trust только для подключений внутри etl-network,
#    пароль для всего остального (тот же принцип, что и у resolver выше:
#    нужен $SUBNET, известный только после определения сети) ────────────
mkdir -p /var/storage/containers/postgres
cat > /var/storage/containers/postgres/pg_hba.conf <<EOF
# Локальные подключения (Unix-сокет внутри контейнера) — всегда без пароля,
# это поведение самого образа postgres, независимо от строк ниже
local   all             all                                     trust

# etl-network — без пароля, но строго в границах этой подсети
host    all             all             ${SUBNET}               trust

# Всё остальное (SSH-туннель на 5432 снаружи, Шаг 7, и вообще любой
# другой источник) — обычная аутентификация по паролю
host    all             all             all                     scram-sha-256
EOF
# Владелец — uid 999 (postgres в контейнере): процесс Postgres должен прочитать файл
chown 999:999 /var/storage/containers/postgres/pg_hba.conf
chmod 600 /var/storage/containers/postgres/pg_hba.conf

# ── 1. Генераторы (чистый bash) ─────────────────────────
pw() {
    local length=${1:-24}
    local chars='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    local out=''
    for ((i = 0; i < length; i++)); do
        out+="${chars:$((RANDOM % ${#chars})):1}"
    done
    echo "$out"
}
hex() {
    local bytes=${1:-16}
    local out=''
    for ((i = 0; i < bytes; i++)); do
        printf -v byte '%02x' $((RANDOM % 256))
        out+="$byte"
    done
    echo "$out"
}
fernet_key() {
    local raw=''
    for ((i = 0; i < 32; i++)); do
        printf -v b '\\x%02x' $((RANDOM % 256))
        raw+="$b"
    done
    printf "$raw" | base64 -w0 | tr '+/' '-_'
}

# ── 2. Секреты ────────────────────────────────────
SECRETS_JUST_GENERATED=false
if [ ! -f "$SECRETS" ]; then
    echo "[1/8] Генерация секретов..."
    SECRETS_JUST_GENERATED=true
    cat > "$SECRETS" <<EOF
POSTGRES_PASSWORD=$(pw)
OPENMETADATA_DB_PASSWORD=$(pw)
RABBITMQ_PASS=$(pw)
FERNET_KEY=$(fernet_key)
AIRFLOW_JWT_SECRET=$(hex 32)
AIRFLOW_ADMIN_USER=etl_admin
AIRFLOW_ADMIN_PASS=$(pw)
FLOWER_ADMIN_USER=flower_admin
FLOWER_ADMIN_PASS=$(pw)
JWT_KEY_ID=$(hex 16)
EOF
    chmod 600 "$SECRETS"
    echo "   → $SECRETS создан"
else
    echo "[1/8] Секреты уже существуют, пропуск"
fi
source "$SECRETS"

# ── Детект рассинхрона: секреты только что сгенерированы заново, а volume'ы
#    БД/брокера уже были проинициализированы раньше (secrets.env потерян,
#    данные — нет). POSTGRES_PASSWORD/RABBITMQ_DEFAULT_PASS/init-db.sql
#    применяются образами ТОЛЬКО при первой инициализации пустого volume —
#    на существующих данных они молча игнорируются, и реальные пароли внутри
#    остаются от предыдущего набора секретов, а не от только что созданного.
POSTGRES_NEEDS_PASSWORD_SYNC=false
RABBITMQ_NEEDS_PASSWORD_SYNC=false
if [ "$SECRETS_JUST_GENERATED" = true ]; then
    if [ -f /var/storage/volumes/postgres/PG_VERSION ]; then
        POSTGRES_NEEDS_PASSWORD_SYNC=true
        echo "   ⚠ secrets.env создан заново, но /var/storage/volumes/postgres уже"
        echo "     содержит инициализированную БД — пароли Postgres актуализирую"
        echo "     автоматически после старта (см. ниже)."
    fi
    if [ -n "$(ls -A /var/storage/volumes/rabbitmq 2>/dev/null)" ]; then
        RABBITMQ_NEEDS_PASSWORD_SYNC=true
        echo "   ⚠ secrets.env создан заново, но /var/storage/volumes/rabbitmq уже"
        echo "     не пуст — пароль RabbitMQ актуализирую автоматически после старта."
    fi
fi

# ── 3. init-db.sql ────────────────────────────
echo "[2/8] Подготовка init-db.sql..."
cat > "$CONTAINERS_DIR/init-db.sql" <<EOF
CREATE USER openmetadata_user WITH PASSWORD '${OPENMETADATA_DB_PASSWORD}';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;
-- OpenMetadata (движок workflow Flowable) может «забыть» закрыть транзакцию;
-- такая сессия держит соединение и блокировки. Postgres закроет её сам.
ALTER ROLE openmetadata_user SET idle_in_transaction_session_timeout = '15min';
EOF
# init-скрипты entrypoint образа выполняет уже от пользователя postgres (uid 999)
chown 999:999 "$CONTAINERS_DIR/init-db.sql"
chmod 600 "$CONTAINERS_DIR/init-db.sql"

# ── 4. init-airflow.sh ────────────────────────────
echo "[3/8] Подготовка init-airflow.sh..."
cat > "$CONTAINERS_DIR/init-airflow.sh" <<EOF
#!/bin/bash
set -e
airflow db migrate
airflow users create \
    --username "\$AIRFLOW_ADMIN_USER" \
    --firstname ETL \
    --lastname Admin \
    --role Admin \
    --email etl-admin@local \
    --password "\$AIRFLOW_ADMIN_PASS" || true
EOF
chmod 755 "$CONTAINERS_DIR/init-airflow.sh"

# ── 5. etl.env ──────────────────────────────
echo "[4/8] Формирование etl.env..."
cat > "$ENV" <<EOF
POSTGRES_USER=airflow
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=airflow
RABBITMQ_DEFAULT_USER=airflow
RABBITMQ_DEFAULT_PASS=${RABBITMQ_PASS}
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres:5432/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:${POSTGRES_PASSWORD}@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=amqp://airflow:${RABBITMQ_PASS}@rabbitmq:5672/
AIRFLOW__CELERY__WORKER_CONCURRENCY=6
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=[{"name":"dags-folder","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{}},{"name":"etl","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/etl-dags"}}]
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__TEST_CONNECTION=Enabled
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/airflow/execution/
AIRFLOW__API_AUTH__JWT_SECRET=${AIRFLOW_JWT_SECRET}
AIRFLOW__API__BASE_URL=http://${IP}/airflow
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW_ADMIN_USER=${AIRFLOW_ADMIN_USER}
AIRFLOW_ADMIN_PASS=${AIRFLOW_ADMIN_PASS}
AIRFLOW__CELERY__FLOWER_BASIC_AUTH=${FLOWER_ADMIN_USER}:${FLOWER_ADMIN_PASS}
AIRFLOW__CELERY__FLOWER_URL_PREFIX=flower
OPENMETADATA_CLUSTER_NAME=openmetadata
FERNET_KEY=${FERNET_KEY}
BASE_PATH=/openmetadata/
DB_DRIVER_CLASS=org.postgresql.Driver
DB_SCHEME=postgresql
DB_HOST=postgres
DB_PORT=5432
DB_USER=openmetadata_user
DB_USER_PASSWORD=${OPENMETADATA_DB_PASSWORD}
DB_PARAMS=sslmode=disable
DB_CONNECTION_POOL_MAX_SIZE=50
DB_CONNECTION_POOL_MIN_SIZE=10
DB_CONNECTION_POOL_MIN_IDLE=10
DB_CONNECTION_POOL_INITIAL_SIZE=10
SEARCH_TYPE=elasticsearch
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_SCHEME=http
PIPELINE_SERVICE_CLIENT_ENABLED=true
PIPELINE_SERVICE_CLIENT_ENDPOINT=http://airflow-api-server:8080/airflow
SERVER_HOST_API_URL=http://openmetadata-server:8585/openmetadata/api
AIRFLOW_USERNAME=${AIRFLOW_ADMIN_USER}
AIRFLOW_PASSWORD=${AIRFLOW_ADMIN_PASS}
AUTHENTICATION_PROVIDER=basic
AUTHENTICATION_ENABLE_SELF_SIGNUP=false
JWT_ISSUER=open-metadata.org
JWT_KEY_ID=${JWT_KEY_ID}
SERVER_HOST=0.0.0.0
SERVER_PORT=8585
EOF
chmod 600 "$ENV"
# Примечания:
# 1. AIRFLOW__API__BASE_URL зафиксировал текущий $IP на момент первого запуска.
#    Если IP сервера сменится (DHCP, переезд) — поправьте эту строку в $ENV
#    вручную и перезапустите airflow-api-server, иначе ссылки в интерфейсе
#    Airflow будут вести на старый адрес. Если у сервера есть постоянное
#    DNS-имя, лучше сразу использовать его вместо переменной $IP выше.
# 2. AIRFLOW__API_AUTH__JWT_SECRET должен быть ОДИНАКОВЫМ на всех Airflow-
#    компонентах (api-server, scheduler, dag-processor, triggerer, worker) —
#    он уже такой, поскольку все они читают один и тот же $ENV через
#    EnvironmentFile=. Если когда-нибудь разнесёте компоненты на разные
#    etl.env — не забудьте синхронизировать именно эту переменную, иначе
#    получите "Invalid auth token" при обращении worker'ов к api-server.
# 2a. AIRFLOW__CORE__EXECUTION_API_SERVER_URL — тоже ОБЯЗАТЕЛЬНО с префиксом
#    /airflow: Execution API (через него воркер стартует и завершает задачи)
#    смонтирован под тем же BASE_URL, что и всё приложение. Без префикса воркер
#    получает 404 на task_instances.start, и задача падает, не выйдя из queued
#    («finished with state failed, but the task instance's state attribute is
#    queued»). Проверено на живом кластере: /execution/health → 404,
#    /airflow/execution/health → 200.
# 3. PIPELINE_SERVICE_CLIENT_ENDPOINT — ОБЯЗАТЕЛЬНО с префиксом /airflow.
#    AIRFLOW__API__BASE_URL монтирует всё приложение Airflow (включая
#    Flask-маршруты плагина openmetadata-managed-apis) под /airflow. Без
#    префикса OpenMetadata стучится в /pluginsv2/api/v2/... и получает 404,
#    в UI это выглядит как «Ingestion Scheduler is unable to respond» /
#    «Platform Service Client Unavailable». Проверено на живом кластере:
#    /airflow/pluginsv2/api/v2/openmetadata/health → 200.
#    Имя переменной — именно PIPELINE_SERVICE_CLIENT_ENDPOINT (ключ apiEndpoint
#    в openmetadata.yaml образа), не AIRFLOW_HOST.
# 3a. SERVER_HOST_API_URL — обратный адрес, по которому плагин в Airflow
#    сообщает результаты (Test Connection, ingestion) серверу OpenMetadata.
#    Ключ metadataApiEndpoint в openmetadata.yaml, дефолт — localhost:8585/api,
#    который из контейнера Airflow недоступен (Connection refused → в UI
#    «airflow API returned Internal Server Error»). Внутренний адрес, мимо
#    nginx, с сабпутом /openmetadata (его задаёт BASE_PATH ниже).
# 4. DB_PARAMS=sslmode=disable — параметр pgjdbc (наш Postgres без TLS внутри
#    закрытой сети). Раньше здесь стояли allowPublicKeyRetrieval/useSSL/
#    serverTimezone — это параметры MySQL Connector/J, для PostgreSQL
#    бессмысленные и потенциально просто игнорируемые драйвером. `sslmode`
#    (не `ssl`) — потому что «disable» валидное значение именно для sslmode;
#    у голого параметра `ssl` допустимы только true/false.
# 2c. AIRFLOW__CORE__TEST_CONNECTION=Enabled — кнопка Test в Admin → Connections
#     (api-server сам открывает соединение; править подключения — только админам).
# 2b. DAG_BUNDLE_CONFIG_LIST — два источника DAG'ов (DAG bundles, Airflow 3):
#    «dags-folder» (/opt/airflow/dags) — только DAG'и, которые генерирует
#    OpenMetadata; «etl» (/opt/airflow/etl-dags) — собственные DAG'и компании.
#    Имя «dags-folder» не менять: на него ссылаются задеплоенные пайплайны.
# 4a. DB_CONNECTION_POOL_* — пул соединений OpenMetadata к Postgres. Дефолт в
#    openmetadata.yaml образа — до 100 соединений, то есть один OpenMetadata
#    готов занять весь max_connections Postgres. Под нагрузкой ingestion пул
#    растёт, Postgres отказывает, Hikari ждёт соединение connectionTimeout=30s —
#    и все запросы UI висят по 30 секунд, а потом 503 (наблюдалось на живом
#    кластере). 50 для OpenMetadata + пулы Airflow помещаются в
#    max_connections=200 (Шаг 3) с запасом.
# 5a. FERNET_KEY — ключ, которым OpenMetadata шифрует пароли подключений к
#    СУБД, введённые аналитиками в UI. Без переменной используется дефолтный
#    ключ из openmetadata.yaml образа — он публично известен. ВНИМАНИЕ: на уже
#    работающем инстансе ключ не менять — сохранённые пароли подключений
#    перестанут расшифровываться. Только для нового деплоя.
# 5b. AIRFLOW__CELERY__FLOWER_BASIC_AUTH / _URL_PREFIX — логин и сабпуть Flower
#    (см. примечание к юниту Flower в Шаге 3).
# 5. BASE_PATH=/openmetadata/ — обязательно с завершающим слэшем. Без него
#    OpenMetadata на части сборок собирает свои внутренние пути (UI/статика)
#    некорректно — визуально это похоже на «съеденную» букву в пути или
#    склеенные без разделителя сегменты URL.

# ── 6. Helper для ожидания healthcheck ────────
wait_healthy() {
    local name="$1"
    local max="${2:-60}"
    echo "   Ожидание готовности $name..."
    for ((i = 0; i < max; i++)); do
        status=$(podman inspect --format '{{.State.Health.Status}}' "$name" 2>/dev/null || echo none)
        if [ "$status" = "healthy" ]; then
            return 0
        fi
        sleep 2
    done
    echo "   ОШИБКА: $name не стал healthy за $((max * 2)) сек — podman logs $name"
    exit 1
}

# ── 7. Предзагрузка образов ──────────────────────────
# systemd даёт сервису 90 секунд на старт; образы ES и OpenMetadata при первом
# pull тянутся дольше, и сервис упал бы по таймауту. Качаем заранее.
echo "[5/8] Загрузка образов..."
for img in docker.io/postgres:17 docker.io/rabbitmq:3.13-management \
           docker.elastic.co/elasticsearch/elasticsearch:9.3.0 \
           docker.io/nginx:1.27-alpine \
           docker.getcollate.io/openmetadata/server:1.13.6; do
    podman image exists "$img" || podman pull "$img"
done

# ── 8. Запуск ────────────────────────────────────────
echo "[6/8] Запуск сервисов..."
systemctl daemon-reload

systemctl start postgres rabbitmq elasticsearch
wait_healthy etl-postgres

if [ "$POSTGRES_NEEDS_PASSWORD_SYNC" = true ]; then
    echo "   Актуализация паролей Postgres под новые secrets.env..."
    podman exec -i etl-postgres psql -U airflow -d airflow -c \
        "ALTER USER airflow WITH PASSWORD '${POSTGRES_PASSWORD}';" \
        || echo "   ⚠ Не удалось обновить пароль airflow — проверьте вручную (Управление)"
    podman exec -i etl-postgres psql -U airflow -d airflow -c \
        "ALTER USER openmetadata_user WITH PASSWORD '${OPENMETADATA_DB_PASSWORD}';" \
        || echo "   ⚠ Не удалось обновить пароль openmetadata_user (роль могла не существовать) — проверьте вручную"
fi

wait_healthy etl-rabbitmq

if [ "$RABBITMQ_NEEDS_PASSWORD_SYNC" = true ]; then
    echo "   Актуализация пароля RabbitMQ под новые secrets.env..."
    podman exec etl-rabbitmq rabbitmqctl change_password airflow "${RABBITMQ_PASS}" \
        || echo "   ⚠ Не удалось обновить пароль RabbitMQ — проверьте вручную (Управление)"
fi

wait_healthy etl-elasticsearch 90

# Одноузловой ES: репликам негде жить, без этого все индексы висят в yellow.
# Шаблон с priority 0 применяется ко всем новым индексам (их создаёт OpenMetadata),
# PUT на _all — к уже существующим (на случай повторного деплоя).
curl -sf -X PUT localhost:9200/_index_template/single-node-no-replicas \
    -H 'Content-Type: application/json' \
    -d '{"index_patterns":["*"],"priority":0,"template":{"settings":{"number_of_replicas":0}}}' >/dev/null \
    || echo "   ⚠ не удалось задать шаблон ES без реплик — статус будет yellow, на работу не влияет"
curl -sf -X PUT localhost:9200/_all/_settings \
    -H 'Content-Type: application/json' -d '{"index":{"number_of_replicas":0}}' >/dev/null || true

if [ "$POSTGRES_NEEDS_PASSWORD_SYNC" = true ]; then
    echo ""
    echo "   ⚠ ВАЖНО: логины Airflow (${AIRFLOW_ADMIN_USER}) и OpenMetadata"
    echo "     (admin) создаются только при первой инициализации и на"
    echo "     существующей БД тихо пропускаются — пароли для них в новом"
    echo "     secrets.env НЕ актуальны, автоматически не синхронизируются."
    echo "     Реальный логин — тот, что был при первом деплое. Если он тоже"
    echo "     утерян — сбросить вручную после запуска (см. «Управление» в гайде)."
    echo ""
fi

systemctl start airflow-init

systemctl start airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer airflow-worker airflow-flower
wait_healthy etl-airflow-api-server 60
wait_healthy etl-airflow-scheduler 60
wait_healthy etl-airflow-dag-processor 60
wait_healthy etl-airflow-triggerer 60
wait_healthy etl-airflow-worker 60
wait_healthy etl-airflow-flower 60

systemctl start openmetadata-migrate
systemctl start openmetadata-server
wait_healthy etl-om-server 90

systemctl start nginx

# Автозапуск Quadlet-юнитов задаётся их секцией [Install] (WantedBy=multi-user.target):
# генератор Quadlet применяет её сам при daemon-reload. `systemctl enable` для
# сгенерированных юнитов не работает и под `set -e` оборвал бы скрипт.
# Обычный systemd-таймер бэкапов включается как обычно:
echo "[7/8] Включение таймера бэкапов..."
systemctl enable --now etl-backup.timer

echo ""
echo "============================================"
echo "         РАЗВЕРТЫВАНИЕ ЗАВЕРШЕНО"
echo "============================================"
echo ""
echo " Airflow UI    http://${IP}/airflow"
echo "   ${AIRFLOW_ADMIN_USER} / ${AIRFLOW_ADMIN_PASS}"
echo ""
echo " OpenMetadata  http://${IP}/openmetadata"
echo "   admin@open-metadata.org / admin — встроенный админ, СМЕНИТЕ пароль при первом входе"
echo ""
echo " Flower        http://${IP}/flower"
echo "   ${FLOWER_ADMIN_USER} / ${FLOWER_ADMIN_PASS}"
echo ""
echo " Внутренние сервисы (доступ через SSH-туннель, без своего логина):"
echo "   Postgres:      ssh -L 5432:127.0.0.1:5432 root@${IP}"
echo "   RabbitMQ:      ssh -L 15672:127.0.0.1:15672 root@${IP}"
echo "   ElasticSearch: ssh -L 9200:127.0.0.1:9200 root@${IP}"
echo "   OpenMetadata (прямой порт, минуя nginx): ssh -L 8585:127.0.0.1:8585 root@${IP}"
echo "   Flower (прямой порт, минуя nginx, логин тот же что и выше): ssh -L 5555:127.0.0.1:5555 root@${IP}"
echo ""
echo " Бэкапы: /var/storage/backups (ежедневно 03:00, таймер etl-backup.timer:"
echo "   Postgres + ElasticSearch-снапшоты + RabbitMQ-определения + конфигурация)"
echo " Секреты: $SECRETS"
echo "============================================"
DEPLOY

chmod +x /var/storage/containers/deploy.sh
```

---

## Шаг 7. Запуск и проверка

```bash
# Systemd-юниты бэкапа и Quadlet-файлы уже должны быть на месте (Шаги 3 и 5)
/var/storage/containers/deploy.sh
```

**Проверка:**
```bash
# Конфиг nginx валиден (resolve/zone — функции nginx 1.27.3+, проверяем в самом контейнере)
podman exec etl-nginx nginx -t

# Внешний порт — теперь только один
ss -tlnp | grep ':80 '
# Ожидается: 0.0.0.0:80

# Внутренние — только localhost, включая OpenMetadata (был снаружи, теперь за nginx)
ss -tlnp | grep -E '5432|5672|9200|5555|8080|8585'
# Ожидается: 127.0.0.1:5432, 127.0.0.1:5672, 127.0.0.1:9200, 127.0.0.1:5555,
#            127.0.0.1:8080, 127.0.0.1:8585

# Разделение по URL реально работает
source /var/storage/containers/secrets.env
curl -sI http://localhost/airflow/ | head -1
curl -sI http://localhost/openmetadata/ | head -1
curl -sI http://localhost/flower/ | head -1
# Ожидается 401 без логина/пароля — Basic Auth реально требуется:
curl -sI -u "$FLOWER_ADMIN_USER:$FLOWER_ADMIN_PASS" http://localhost/flower/ | head -1
# Ожидается 200 с правильными кредами

# Контейнеры
podman ps --format "table {{.Names}}\t{{.Status}}"

# Таймер бэкапов
systemctl list-timers etl-backup.timer

# OpenMetadata видит плагин managed-apis в Airflow (без этого Test Connection
# и Deploy пайплайнов из UI не работают)
podman exec etl-om-server wget -qO- http://airflow-api-server:8080/airflow/pluginsv2/api/v2/openmetadata/health
# Ожидается: {"status":"healthy","version":"1.13.6.1"}

# Воркер видит Execution API (без этого задачи падают, не выходя из queued).
# curl без -L — чтобы редирект не замаскировал неверный путь
podman exec etl-airflow-worker curl -s -o /dev/null -w "%{http_code}\n" http://airflow-api-server:8080/airflow/execution/health
# Ожидается: 200

# Обратная связь: плагин в Airflow достучится до OpenMetadata по SERVER_HOST_API_URL
podman exec etl-airflow-api-server curl -s http://openmetadata-server:8585/openmetadata/api/v1/system/version
# Ожидается JSON с версией 1.13.6

# Провайдеры реально установились (не молча пропущены)
podman exec etl-airflow-worker airflow providers list
```

---

## Мониторинг в Zabbix (agent 2)

Хост мониторится агентом 2 с двумя плагинами: **PostgreSQL** (отдельный пакет,
loadable-плагин) и **Docker** (встроенный в агент). Docker-плагин работает с
Podman через его Docker-совместимое API. Сам агент и шаблоны Linux здесь не
описаны — только то, что специфично для этого сервера.

### PostgreSQL — шаблон «PostgreSQL by Zabbix agent 2»

**1. Пользователь мониторинга в базе.** Имя и пароль задаются явно (подставьте свои).
Роли `pg_monitor` достаточно: суперпользователь не нужен, данные таблиц недоступны.

```bash
ZBX_USER='имя_пользователя'
ZBX_PASS='пароль'

podman exec -i etl-postgres psql -U airflow -d postgres -v ON_ERROR_STOP=1 \
  -v usr="$ZBX_USER" -v pass="$ZBX_PASS" <<'SQL'
CREATE ROLE :"usr" WITH LOGIN PASSWORD :'pass' INHERIT;
GRANT pg_monitor TO :"usr";
SQL

podman exec etl-postgres psql -U airflow -d postgres -c "\du $ZBX_USER"   # Member of: {pg_monitor}
```

Пользователь создаётся вручную в живой базе — при пересоздании тома Postgres
(чистый передеплой) эту команду нужно выполнить заново, иначе `pgsql.ping` молча
вернёт `0`.

`CONNECT` на базы по умолчанию есть у `PUBLIC`; проверка, если где-то отзывали:

```bash
podman exec etl-postgres psql -U airflow -d postgres -Atc \
  "SELECT datname, has_database_privilege('$ZBX_USER', datname, 'CONNECT') FROM pg_database WHERE NOT datistemplate;"
```

**2. Плагин агента.**

```bash
dnf install zabbix-agent2-plugin-postgresql     # версия должна совпадать с zabbix-agent2
systemctl restart zabbix-agent2
ps fax | grep -A1 [z]abbix_agent2               # дочерний процесс zabbix-agent2-plugin-postgresql
```

**3. Проверка.** Порядок параметров у агента 2: `URI, пользователь, пароль, база`
(не `хост, порт, …` как у шаблона для агента 1 — иначе Postgres увидит
пользователя с именем `5432`).

```bash
zabbix_agent2 -t 'pgsql.ping["tcp://127.0.0.1:5432","ПОЛЬЗОВАТЕЛЬ","ПАРОЛЬ","postgres"]'   # [s|1.000000]
```

**4. Хост в Zabbix.** Шаблон «PostgreSQL by Zabbix agent 2» (не «…by Zabbix agent»),
макросы на хосте:

| Макрос | Значение |
|---|---|
| `{$PG.CONNSTRING.AGENT2}` | `tcp://127.0.0.1:5432` — не `localhost`: порт опубликован только на IPv4, `localhost` может уйти в `::1` |
| `{$PG.USER}` | пользователь из п.1 |
| `{$PG.PASSWORD}` | пароль из п.1, тип **Secret text** |
| `{$PG.DATABASE}` | `postgres` — база для первого подключения; остальные найдёт discovery |

Подключения с хоста к опубликованному порту Postgres видит как внешние —
срабатывает правило pg_hba с паролем, а не trust, поэтому пароль должен совпадать
точно. Базы (`airflow`, `openmetadata_db`, `postgres`) обнаруживаются правилом
«Database discovery» — после привязки шаблона запустите его через «Execute now»,
иначе ждать до часа.

### Контейнеры Podman — шаблон «Docker by Zabbix agent 2»

**1. API-сокет Podman и доступ для пользователя zabbix.**

```bash
systemctl enable --now podman.socket

mkdir -p /etc/systemd/system/podman.socket.d
cat > /etc/systemd/system/podman.socket.d/zabbix.conf <<'EOF2'
[Socket]
SocketGroup=zabbix
SocketMode=0660
EOF2
systemctl daemon-reload
systemctl restart podman.socket

# /run/podman по умолчанию 0700 root — zabbix не дойдёт до сокета внутри.
# x без r: доступ по точному пути, без листинга. /run чистится при загрузке,
# поэтому права закрепляются через tmpfiles.d
echo 'd /run/podman 0711 root root -' > /etc/tmpfiles.d/podman-zabbix.conf
systemd-tmpfiles --create /etc/tmpfiles.d/podman-zabbix.conf

ls -ld /run/podman                      # drwx--x--x
ls -l /run/podman/podman.sock           # srw-rw---- root zabbix
sudo -u zabbix curl -s --unix-socket /run/podman/podman.sock http://d/_ping; echo   # OK
```

**2. Плагин Docker → сокет Podman.**

```bash
echo 'Plugins.Docker.Endpoint=unix:///run/podman/podman.sock' > /etc/zabbix/zabbix_agent2.d/plugins.d/docker.conf
systemctl restart zabbix-agent2
```

**3. Проверка с сервера Zabbix** (`zabbix_agent2 -t` на хосте идёт от root и
проблемы с правами не покажет):

```bash
zabbix_get -s 10.234.1.227 -k docker.ping                    # 1
zabbix_get -s 10.234.1.227 -k docker.containers.discovery   # JSON со списком etl-*
```

Если `permission denied`, а `sudo -u zabbix curl …/_ping` отвечает `OK` — это
SELinux (домен `zabbix_agent_t` → сокет Podman):

```bash
ausearch -m avc -ts recent | grep zabbix | audit2allow -M zabbix_podman
semodule -i zabbix_podman.pp
systemctl restart zabbix-agent2
```

**4. Хост в Zabbix.** Шаблон «Docker by Zabbix agent 2», затем «Execute now» у
правил «Containers discovery» и «Images discovery». Метрики «per second» и графики
дашбордов заполняются за 15–30 минут.

Ожидаемо остаются в «Not supported» ~3 элемента — полей Docker, которых нет в
совместимом API Podman (Swarm, плагины, blkio при cgroups v2). Их отключить
(для элементов по контейнерам — через override в правиле обнаружения).

Безопасность: доступ к API-сокету Podman — это полный контроль над контейнерами,
то есть фактически root. Плагин только читает, но группа `zabbix` получает весь API.

### Если данные не идут

| Симптом | Причина |
|---|---|
| `Unknown metric pgsql.ping` | Плагин не загружен: не перезапущен агент, нет `plugins.d/postgresql.conf` или `Include` на него, разные версии агента и плагина |
| `pgsql.ping` = `0` | Плагин не подключился — смотреть `podman logs etl-postgres --since 15m 2>&1 \| grep FATAL` |
| `password authentication failed for user "5432"` | Ключ вызывается в формате агента 1 (привязан шаблон «…by Zabbix agent» или ручная проверка в старом формате) |
| `dial unix /run/podman/podman.sock: permission denied` | Права на `/run/podman` / сокет или SELinux — см. выше |
| Дашборд «No data», а в Latest data значения есть | Мало истории: нужны два опроса для «per second»; проверить период дашборда |

---

## Итоговая структура файлов

```
/var/storage/
├── containers/
│   ├── deploy.sh
│   ├── etl-backup.sh
│   ├── secrets.env              ← chmod 600
│   ├── etl.env                  ← chmod 600
│   ├── init-db.sql              ← владелец 999, chmod 600
│   ├── init-airflow.sh
│   ├── airflow-image/
│   │   └── Dockerfile           ← FROM openmetadata/ingestion, провайдеры
│   ├── airflow/
│   │   ├── dags/                    ← DAG'и, которые генерирует OpenMetadata (bundle dags-folder)
│   │   ├── etl-dags/                ← собственные DAG'и (bundle etl)
│   │   ├── dag_generated_configs/   ← конфиги пайплайнов, которые деплоит OpenMetadata
│   │   ├── logs/
│   │   └── plugins/
│   ├── rabbitmq/
│   │   └── rabbitmq.conf
│   ├── postgres/
│   │   └── pg_hba.conf           ← владелец 999, chmod 600, trust только для etl-network
│   └── nginx/
│       └── nginx.conf
├── volumes/
│   ├── postgres/
│   ├── elasticsearch/
│   └── rabbitmq/
└── backups/
    ├── es-snapshots/              ← репозиторий снапшотов ElasticSearch
    ├── pg-<дата>.sql.gz
    ├── rabbitmq-defs-<дата>.json.gz
    └── config-<дата>.tar.gz       ← chmod 600 (содержит secrets.env)

/etc/containers/systemd/
├── etl.network
├── postgres.container
├── rabbitmq.container
├── elasticsearch.container
├── airflow-init.container
├── airflow-api-server.container
├── airflow-scheduler.container
├── airflow-dag-processor.container
├── airflow-triggerer.container
├── airflow-worker.container
├── airflow-flower.container
├── nginx.container
├── openmetadata-migrate.container      (ContainerName=execute-migrate-all)
└── openmetadata-server.container

/etc/systemd/system/
├── etl-backup.service
└── etl-backup.timer
```

Образ `localhost/etl-airflow:1.13.6` (Шаг 3) — не файл на этой файловой системе, а
собранный `podman build` образ, лежит в локальном хранилище Podman
(`podman images` покажет). `airflow-image/Dockerfile` — это исходник, из которого он
собирается, а не сам образ.

---

## Управление

```bash
# Статус
systemctl status postgres rabbitmq elasticsearch \
    airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer airflow-worker \
    openmetadata-server nginx

# Логи
journalctl -u airflow-scheduler -f
journalctl -u airflow-dag-processor -f
journalctl -u openmetadata-server -f
journalctl -u etl-backup.service

# Ручной прогон бэкапа (Postgres + ES-снапшот + RabbitMQ-определения + конфиг)
systemctl start etl-backup.service

# Перезапуск
systemctl restart airflow-worker

# Пересборка образа Airflow после правки Dockerfile (Шаг 3) — версии
# провайдеров или базовый тег изменились
podman build -t localhost/etl-airflow:1.13.6 /var/storage/containers/airflow-image
systemctl restart airflow-init airflow-api-server airflow-scheduler \
    airflow-dag-processor airflow-triggerer airflow-worker airflow-flower

# Остановка всего
systemctl stop openmetadata-server nginx \
    airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer \
    airflow-worker airflow-flower \
    elasticsearch rabbitmq postgres

# После reboot всё запустится автоматически (включая таймер бэкапов)
```

### Куда класть свои DAG'и

Свои DAG'и — в `/var/storage/containers/airflow/etl-dags` (в контейнерах —
`/opt/airflow/etl-dags`), **не** в `dags/`. Каталог `dags/` целиком отдан
OpenMetadata: туда он складывает сгенерированные ingestion-DAG'и с именами-UUID.
В Airflow это два независимых DAG bundle — `dags-folder` и `etl`
(`AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` в `etl.env`), так что сущности
не смешиваются ни на диске, ни в интерфейсе (колонка bundle).

```bash
groupadd etl-dags
usermod -aG etl-dags <логин>          # для каждого автора DAG'ов
chown 50000:etl-dags /var/storage/containers/airflow/etl-dags
chmod 2775 /var/storage/containers/airflow/etl-dags   # setgid: новые файлы наследуют группу
```

Файлы DAG'ов должны быть читаемы для uid 50000 (под ним работают контейнеры
Airflow): при обычном `umask` новые файлы получают 644/664 — этого достаточно.
Файл с `chmod 600` от root Airflow молча не увидит. SELinux-метку файлы наследуют от
каталога (volume смонтирован с `:z`). Новый DAG подхватывается за 30–60 секунд,
рестарт не нужен:

```bash
podman exec etl-airflow-dag-processor airflow dags list-import-errors
podman exec etl-airflow-dag-processor airflow dags list | grep <имя_dag>
```

Синтаксис — Airflow 3: `schedule=` (не `schedule_interval=`), `start_date=datetime(...)`
(функции `days_ago` больше нет), импорт операторов — из
`airflow.providers.standard.operators.*` или `airflow.sdk`.

### Пользователь для загрузки DAG'ов по SSH/SCP

```bash
useradd -m -s /bin/bash -G etl-dags dagdev
passwd dagdev                               # или ключ в ~/.ssh/authorized_keys
ln -s /var/storage/containers/airflow/etl-dags /home/dagdev/dags
chown -h dagdev:dagdev /home/dagdev/dags

# Маска 0002 для SFTP-сессий группы: загруженные файлы читаемы для Airflow (uid 50000)
# и редактируемы коллегами по группе. scp в OpenSSH 9+ работает через SFTP.
cat > /etc/ssh/sshd_config.d/50-etl-dags.conf <<'EOF'
Match Group etl-dags
    ForceCommand internal-sftp -u 0002
EOF
sshd -t && systemctl reload sshd
```

`ForceCommand internal-sftp` оставляет только передачу файлов (scp, sftp, WinSCP),
без интерактивной оболочки. Если консоль пользователю нужна — не создавайте этот
файл, а пропишите `umask 0002` в его `~/.bashrc`. При `AllowUsers`/`AllowGroups` в
`sshd_config` добавьте пользователя туда. Загрузка с Windows:
`scp .\my_dag.py dagdev@<IP>:dags/` (без `-p`: он перенёс бы локальные права файла,
и файл с `600` Airflow не прочитает).

### Целевое состояние для внешних DAG'ов и его проверка

| Объект | Должно быть |
|---|---|
| Группа `etl-dags` | существует, в ней все авторы DAG'ов |
| Пользователь `dagdev` | доп. группа `etl-dags`; доступ только SFTP/SCP |
| uid `50000` | на хосте не заводится — это `airflow` в контейнерах |
| `/var/storage`, `.../containers`, `.../airflow` | `755` (минимум `o+x`) — путь проходим для `dagdev` |
| `.../airflow/etl-dags` | `50000:etl-dags`, **`2775`**, SELinux `container_file_t` |
| файлы внутри `etl-dags` | `dagdev:etl-dags`, **`664`** (маска SFTP `0002`) |
| `.../airflow/dags` | `50000:0`, `755` — только OpenMetadata, `dagdev` не пишет |
| `.../airflow/dags/etl` | не существует |
| `/home/dagdev/dags` | симлинк → `/var/storage/containers/airflow/etl-dags` |
| `/home/dagdev/.ssh` / `authorized_keys` | `700` / `600`, владелец `dagdev`, SELinux `ssh_home_t` |
| `/etc/ssh/sshd_config.d/50-etl-dags.conf` | `Match Group etl-dags` → `ForceCommand internal-sftp -u 0002` |
| `etl.env` | `DAG_BUNDLE_CONFIG_LIST` с bundle `dags-folder` и `etl` |
| все `airflow-*.container` | `Volume=...etl-dags:/opt/airflow/etl-dags:z` |

Скрипт проверки (только читает, печатает `OK`/`FAIL` по каждому пункту):

```bash
cat > /usr/local/sbin/check-etl-dags.sh <<'EOF'
#!/bin/bash
D=/var/storage/containers/airflow/etl-dags
U=dagdev
ok(){ echo "OK    $*"; }; bad(){ echo "FAIL  $*"; }

getent group etl-dags >/dev/null            && ok "группа etl-dags"            || bad "группы etl-dags нет"
id "$U" >/dev/null 2>&1                     && ok "пользователь $U"            || bad "пользователя $U нет"
id -nG "$U" 2>/dev/null | grep -qw etl-dags && ok "$U в группе etl-dags"       || bad "$U не в группе etl-dags"

st=$(stat -c '%u:%G %a' "$D" 2>/dev/null)
[ "$st" = "50000:etl-dags 2775" ]           && ok "$D = $st"                   || bad "$D = '$st' (нужно 50000:etl-dags 2775)"
ls -Zd "$D" 2>/dev/null | grep -q container_file_t && ok "SELinux container_file_t" || bad "SELinux: $(ls -Zd "$D" 2>/dev/null | awk '{print $1}')"
runuser -u "$U" -- test -w "$D"             && ok "$U может писать в $D"       || bad "$U не может писать в $D (права на путь?)"
runuser -u "$U" -- test -w /var/storage/containers/airflow/dags \
                                            && bad "$U может писать в dags/ (не должен)" || ok "$U не пишет в dags/"
n=$(find "$D" -type f ! -perm -o=r 2>/dev/null | wc -l)
[ "$n" -eq 0 ]                              && ok "все файлы читаемы для Airflow" || bad "$n файл(ов) нечитаемы для uid 50000: $(find "$D" -type f ! -perm -o=r | head -3 | tr '\n' ' ')"
[ ! -e /var/storage/containers/airflow/dags/etl ] && ok "старого dags/etl нет" || bad "остался /var/storage/containers/airflow/dags/etl"

[ "$(readlink /home/$U/dags)" = "$D" ]      && ok "симлинк ~$U/dags"           || bad "симлинк ~$U/dags -> '$(readlink /home/$U/dags)'"
if [ -f /home/$U/.ssh/authorized_keys ]; then
  [ "$(stat -c '%U %a' /home/$U/.ssh)" = "$U 700" ] && ok ".ssh 700" || bad ".ssh: $(stat -c '%U %a' /home/$U/.ssh)"
  [ "$(stat -c '%U %a' /home/$U/.ssh/authorized_keys)" = "$U 600" ] && ok "authorized_keys 600" || bad "authorized_keys: $(stat -c '%U %a' /home/$U/.ssh/authorized_keys)"
  ls -Z /home/$U/.ssh/authorized_keys | grep -q ssh_home_t && ok "authorized_keys ssh_home_t" || bad "authorized_keys без ssh_home_t (restorecon -Rv /home/$U/.ssh)"
else
  echo "INFO  ключа нет — вход только по паролю"
fi

fc=$(sshd -T -C user=$U,host=localhost,addr=127.0.0.1 2>/dev/null | grep -i '^forcecommand')
echo "$fc" | grep -q 'internal-sftp -u 0002' && ok "sshd: $fc" || bad "sshd: ForceCommand для $U не задан ('$fc')"

grep -q '"name":"etl"' /var/storage/containers/etl.env && ok "bundle etl в etl.env" || bad "bundle etl не задан в etl.env"
units=(/etc/containers/systemd/airflow-*.container)
[ -e "${units[0]}" ] || bad "юниты airflow-*.container не найдены"
for f in "${units[@]}"; do
  [ -e "$f" ] && { grep -q 'etl-dags:/opt/airflow/etl-dags' "$f" && ok "volume etl-dags в $(basename "$f")" || bad "нет volume etl-dags в $(basename "$f")"; }
done
podman exec etl-airflow-dag-processor test -r /opt/airflow/etl-dags 2>/dev/null \
                                            && ok "каталог виден в контейнере dag-processor" || bad "dag-processor не видит /opt/airflow/etl-dags"
EOF
chmod 700 /usr/local/sbin/check-etl-dags.sh
/usr/local/sbin/check-etl-dags.sh
```

### Если пароли Postgres/RabbitMQ разошлись с `secrets.env`

`deploy.sh` синхронизирует их автоматически, только когда сам обнаруживает
рассинхрон (секреты только что созданы заново, а volume уже был проинициализирован
раньше). Если нужно сделать это вручную — например, синхронизация не сработала,
или volume RabbitMQ пуст не был, но пароль всё равно не совпадает:

```bash
source /var/storage/containers/secrets.env

# Postgres — через локальный сокет (trust, пароль не нужен, см. Шаг 3)
podman exec -it etl-postgres psql -U airflow -d airflow -c \
    "ALTER USER airflow WITH PASSWORD '${POSTGRES_PASSWORD}';"
podman exec -it etl-postgres psql -U airflow -d airflow -c \
    "ALTER USER openmetadata_user WITH PASSWORD '${OPENMETADATA_DB_PASSWORD}';"

# RabbitMQ — rabbitmqctl работает через cookie кластера, старый пароль не нужен
podman exec etl-rabbitmq rabbitmqctl change_password airflow "${RABBITMQ_PASS}"

# После смены — перезапустить всё, что держит соединение с этими паролями
systemctl restart airflow-init airflow-api-server airflow-scheduler \
    airflow-dag-processor airflow-triggerer airflow-worker airflow-flower \
    openmetadata-server
```

### Если логин Airflow/OpenMetadata (не пароль в БД, а сам вход) не совпадает

Это другой случай — `airflow users create`/создание админа OpenMetadata выполняются
только при самой первой инициализации и на существующих данных тихо пропускаются,
поэтому `deploy.sh` их не трогает вообще, даже при обнаруженном рассинхроне БД.
Если реальный логин неизвестен (старый `secrets.env` утерян вместе с паролем):

- **Airflow** — команда для сброса пароля существующего пользователя зависит от
  версии CLI FAB-провайдера; проверьте `podman exec etl-airflow-api-server airflow
  users --help` на предмет подходящей подкоманды (`reset-password` в части версий)
  перед тем как полагаться на конкретное имя — в этом гайде оно не проверялось.
- **OpenMetadata** — встроенный админ при basic-аутентификации:
  `admin@open-metadata.org` / `admin` (сменить при первом входе). Остальных
  пользователей — через UI (`Settings → Users`), если есть доступ под каким-то
  админским логином; если нет ни одного рабочего — потребуется прямое вмешательство
  в БД `openmetadata_db` (таблица пользователей), что уже не входит в этот гайд.

