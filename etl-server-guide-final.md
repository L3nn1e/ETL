# Гайд по развёртыванию ETL-сервера

Стек: Postgres + RabbitMQ + ElasticSearch + единый Airflow-кластер на образе
`openmetadata/ingestion` (CeleryExecutor) + OpenMetadata Server — всё на Podman
Quadlets, AlmaLinux 10.2. Airflow один: обслуживает и ETL-DAG'и компании (провайдеры
MSSQL/SFTP/Samba/SSH/JDBC/ODBC/dbt/HTTP), и ingestion-пайплайны OpenMetadata,
зарегистрированные как Pipeline Service «Airflow» — отдельного встроенного Airflow
внутри OpenMetadata не разворачивается. Внутренние сервисы (Postgres, RabbitMQ,
ElasticSearch, Airflow Flower, Airflow webserver, OpenMetadata Server) слушают только
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
│                    ВНЕШНИЕ СИСТЕМЫ                            │
│  (MSSQL, PostgreSQL, SFTP, REST API, dbt, файлы)              │
└──────────────┬─────────────────────────┬──────────────────────┘
               │                         │
               │ Провайдеры Airflow      │ Коннекторы OpenMetadata
               │ (движение данных, ETL)  │ (чтение метаданных)
               ▼                         ▼
        ┌────────────────────────────────────────┐
        │     Airflow DAGs — единый кластер        │
        │  ETL-процессы  +  ingestion-пайплайны    │
        └────────────────────┬─────────────────────┘
                              │ Задачи через RabbitMQ
                              ▼
                  ┌─────────────────────────┐
                  │      Airflow Workers      │
                  │    (выполнение задач)     │
                  └─────┬───────────────┬─────┘
       Результаты ETL   │               │  Метаданные из ingestion-DAG'ов
       (целевые         │               │  (HTTP API → OpenMetadata Server)
        системы/DWH)    ▼               ▼
                                  ┌──────────────────────────┐
                                  │   OpenMetadata Server      │
                                  │  (хранение метаданных)     │
                                  └────────────┬────────────────┘
                                               │ PostgreSQL + ElasticSearch
                                               ▼
┌─────────────────────────────────────────────────────────────┐
│                    ХРАНИЛИЩЕ ДАННЫХ                            │
│  PostgreSQL (airflow + openmetadata_db)                        │
│  ElasticSearch (индексы метаданных)                             │
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
  `AIRFLOW_HOST`/`PIPELINE_SERVICE_CLIENT_ENABLED` в `etl.env` (Шаг 6).

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
mkdir -p /var/storage/containers/{airflow/{dags,logs,plugins},nginx}
mkdir -p /var/storage/backups
mkdir -p /etc/containers/systemd

# Права для баз данных (Postgres/RabbitMQ=999, ES=1000)
chown -R 999:999 /var/storage/volumes/postgres
chmod 700 /var/storage/volumes/postgres
chown -R 999:999 /var/storage/volumes/rabbitmq
chmod 700 /var/storage/volumes/rabbitmq
chown -R 1000:1000 /var/storage/volumes/elasticsearch
chmod 700 /var/storage/volumes/elasticsearch

# Права для Airflow (UID 50000 в apache/airflow — сверьте, что тот же UID
# используется и в docker.getcollate.io/openmetadata/ingestion, см. примечание
# к образу в Шаге 3):
# podman run --rm --entrypoint id docker.getcollate.io/openmetadata/ingestion:1.13.6 airflow
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
podman run --rm docker.getcollate.io/openmetadata/ingestion:1.13.6 python3 --version
```
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
PublishPort=127.0.0.1:5432:5432
Network=etl.network
HealthCmd=pg_isready -U airflow
HealthInterval=10s
HealthRetries=5
PodmanArgs=--memory=4g --memory-swap=4g --cpus=2
Exec=postgres -c shared_buffers=1GB -c effective_cache_size=3GB -c max_connections=100 -c work_mem=16MB -c maintenance_work_mem=256MB

[Service]
Restart=always

[Install]
WantedBy=multi-user.target

```

> Лимиты под сервер 32 ГБ / 4-8 vCPU: `shared_buffers` ~25% лимита (1GB),
> `effective_cache_size` — оценка того, сколько ОС+Postgres суммарно закешируют (3GB),
> `max_connections=100` с запасом под Airflow-кластер + БД OpenMetadata одновременно,
> `work_mem=16MB` — при 100 соединениях в худшем случае (несколько sort/hash-узлов на
> запрос) это может дать несколько ГБ, отслеживайте по факту через `pg_stat_activity`.
> `--memory-swap` явно приравнен к `--memory`, чтобы Podman не разрешил своп сверх лимита
> по умолчанию (до 2×) — для БД своп страниц означает непредсказуемые задержки на чтении
> вместо чистого OOM, который хотя бы виден и предсказуем.

### RabbitMQ (localhost)
```bash
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
Environment=ES_JAVA_OPTS=-Xms4g -Xmx4g
Environment=path.repo=/usr/share/elasticsearch/snapshots
Volume=/var/storage/volumes/elasticsearch:/usr/share/elasticsearch/data:Z
Volume=/var/storage/backups/es-snapshots:/usr/share/elasticsearch/snapshots:Z
PublishPort=127.0.0.1:9200:9200
Network=etl.network
HealthCmd=curl -sf http://localhost:9200/_cluster/health
HealthInterval=15s
HealthRetries=10
PodmanArgs=--memory=8g --memory-swap=8g --cpus=2

[Service]
Restart=always

[Install]
WantedBy=multi-user.target

```

> `Xms` всегда равен `Xmx` — иначе JVM ресайзит кучу под нагрузкой, что даёт
> stop-the-world паузы в моменты пиковой нагрузки. Heap — ровно половина лимита
> контейнера (4GB из 8GB): остальное JVM использует под off-heap (Lucene-сегменты,
> файловый кеш ОС).

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
>   подтверждены, ровно то, что уже используется в `Exec=` Flower ниже.
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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
PublishPort=127.0.0.1:8080:8080
Network=etl.network
Exec=api-server --proxy-headers
HealthCmd=/bin/bash -c 'curl -sf http://localhost:8080/airflow/api/v2/monitor/health || curl -sf http://localhost:8080/api/v2/monitor/health'
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
> **`HealthCmd` пробует оба варианта пути** (`/airflow/...` и `/...` без префикса) —
> потому что неочевидно заранее, действительно ли `AIRFLOW__API__BASE_URL` (Шаг 6)
> сдвигает реальные маршруты приложения под префикс, или влияет только на генерацию
> ссылок в интерфейсе. Официальный пример реверс-прокси для Airflow 3 показывает nginx
> **без обрезки префикса** (`proxy_pass http://localhost:8080;` целиком, без rewrite) —
> это косвенно говорит, что маршруты действительно перемещаются, но однозначно
> подтвердить можно только на реальном контейнере. После первого деплоя проверьте,
> какой из двух путей реально отвечает 200, и упростите `HealthCmd` до одного варианта:
> ```bash
> curl -sI http://localhost:8080/airflow/api/v2/monitor/health
> curl -sI http://localhost:8080/api/v2/monitor/health
> ```

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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Network=etl.network
Exec=scheduler
HealthCmd=/bin/bash -c 'airflow jobs check --job-type SchedulerJob --hostname "$HOSTNAME"'
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
> DAG не появится в интерфейсе. `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true` в
> `etl.env` (Шаг 6) поднимает встроенный HTTP-сервер здоровья, на котором и основан
> `airflow jobs check` в `HealthCmd`.

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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Network=etl.network
Exec=dag-processor
HealthCmd=/bin/bash -c 'airflow jobs check --job-type DagProcessorJob --hostname "$HOSTNAME"'
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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Network=etl.network
Exec=triggerer
HealthCmd=/bin/bash -c 'airflow jobs check --job-type TriggererJob --hostname "$HOSTNAME"'
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=512m --memory-swap=512m --cpus=0.5

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Обслуживает deferrable-операторы (асинхронное ожидание без занятого worker-слота) —
> часть провайдеров (в т.ч. некоторые сенсоры из установленных нами пакетов) может
> использовать этот режим по умолчанию в новых версиях. Ресурсы скромные — это
> событийный цикл (asyncio), а не тяжёлые вычисления.

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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Network=etl.network
Exec=celery worker
HealthCmd=/bin/bash -c 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$HOSTNAME" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$HOSTNAME"'
HealthInterval=30s
HealthRetries=5
PodmanArgs=--memory=3g --memory-swap=3g --cpus=3

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> `AIRFLOW__CELERY__WORKER_CONCURRENCY=12` задаётся в `etl.env` (Шаг 6) — задачи в
> основном I/O-bound (MSSQL/SFTP/Samba/HTTP), поэтому конкурентность заметно выше
> числа ядер оправдана: воркер большую часть времени ждёт сеть/диск, а не считает.
> В Airflow 3 worker обращается к api-server по HTTP за заданиями (Execution API), а
> не читает БД напрямую — это требует общего `AIRFLOW__API_AUTH__JWT_SECRET` со всеми
> остальными компонентами (Шаг 6); при рассинхроне секрета worker будет падать с
> ошибкой авторизации, а не тихо простаивать. `HealthCmd` пробует оба пространства
> имён celery-приложения (`airflow.providers.celery...` и старый `airflow.executors...`)
> — так делает и официальный docker-compose Airflow 3, на случай расхождений между
> патч-версиями провайдера Celery.

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
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
PublishPort=127.0.0.1:5555:5555
Network=etl.network
Entrypoint=/bin/bash
Exec=-c "exec celery flower --url-prefix=flower --basic-auth=\$FLOWER_ADMIN_USER:\$FLOWER_ADMIN_PASS"
HealthCmd=/bin/bash -c "curl -sf -u \$FLOWER_ADMIN_USER:\$FLOWER_ADMIN_PASS http://localhost:5555/flower/"
HealthInterval=15s
HealthRetries=6
PodmanArgs=--memory=512m --memory-swap=512m --cpus=0.5

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> **`${VAR}` прямо в `Exec=`/`HealthCmd=` — ненадёжно, экранированный `\$VAR` внутри
> `bash -c` — надёжно.** Раньше здесь стояла подстановка вида
> `Exec=celery flower --basic-auth=${FLOWER_ADMIN_USER}:...` в расчёте на то, что
> systemd сам заменит `${VAR}` значениями из `EnvironmentFile=` при генерации
> `ExecStart=`. У этого есть неочевидная зависимость от того, на каком этапе разбора
> юнита переменные из `EnvironmentFile=` вообще становятся видны механизму
> `$VAR`-подстановки — гарантии тут меньше, чем кажется. Надёжнее не полагаться на
> это вообще: `Entrypoint=/bin/bash` + `Exec=-c "... \$VAR"` с экранированным `\$`
> (чтобы systemd НЕ трогал переменную на этапе генерации) откладывает подстановку до
> момента, когда команда реально выполняется **внутри контейнера** — там `$VAR` берёт
> обычная shell-интерполяция bash из процессного окружения, которое `EnvironmentFile=`
> гарантированно туда прокидывает (через `--env-file` у podman). То же самое и для
> `HealthCmd=` — оборачиваем в `/bin/bash -c "..."` с тем же экранированием, по той же
> причине.
>
> `--url-prefix=flower` и флаги через дефис (`--basic-auth`, не `--basic_auth`) —
> потому что `celery flower` здесь фактически вызывается как `airflow celery flower`
> (через `apache-airflow-providers-celery`, зашитый в `Dockerfile`, Шаг 3 — без него
> команды `celery worker`/`celery flower` не существуют вообще, в CLI нет даже группы
> `celery`), а не как отдельный пакет `flower` — у CLI-обёртки Airflow имена флагов
> другие, чем в документации самого Flower. Оба флага (`-A/--basic-auth`,
> `-u/--url-prefix`) подтверждены на реальном образе.

### Nginx (внешний доступ)
```bash
cat > /etc/containers/systemd/nginx.container <<'EOF'
[Unit]
Description=Nginx
After=airflow-api-server.service openmetadata-server.service airflow-flower.service
Requires=airflow-api-server.service openmetadata-server.service airflow-flower.service

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
> OpenMetadata (`/openmetadata/`) и Flower (`/flower/`), см. Шаг 4. `Requires=`
> теперь на все три сервиса — раньше OpenMetadata и Flower стартовали независимо от
> nginx (у обоих были свои прямые порты), теперь они обязательны, иначе
> соответствующий путь будет отдавать 502. TLS/443 в этом рецепте не настраивается.

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
Environment=OPENMETADATA_HEAP_OPTS=-Xms1g -Xmx1g
PublishPort=127.0.0.1:8585:8585
Network=etl.network
HealthCmd=wget -qO- http://localhost:8585/openmetadata/api/v1/system/version > /dev/null
HealthInterval=10s
HealthRetries=10
PodmanArgs=--memory=2g --memory-swap=2g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> Пайплайны ingestion (metadata-сканирование источников) теперь выполняются как обычные
> DAG'и на этом же Celery-кластере — `PIPELINE_SERVICE_CLIENT_ENABLED=true` +
> `AIRFLOW_HOST` в `etl.env` (Шаг 6) регистрируют его как Pipeline Service «Airflow»
> в OpenMetadata. Отдельный контейнер `openmetadata-ingestion` с собственным встроенным
> Airflow не нужен — именно это и означает формулировку документа «Airflow входит
> в состав OpenMetadata и не требует отдельной установки».
>
> **Порт ушёл на localhost, доступ — через nginx на `/openmetadata/`** (Шаг 4), для
> этого сам OpenMetadata сконфигурирован через `BASE_PATH=/openmetadata` в `etl.env`.
> Официально задокументированный механизм у OpenMetadata (переменная `BASE_PATH`
> переносит разом UI, `/api/v1/...` и статику под префикс) — но точный итоговый путь
> API стоит свериться на реальном контейнере перед тем, как полагаться на него в
> healthcheck выше и в `AIRFLOW_HOST` ниже:
> ```bash
> wget -qO- http://localhost:8585/openmetadata/api/v1/system/version
> ```
> Если путь соберётся иначе — поправьте `HealthCmd` здесь и адрес в `wget` внутри
> `wait_healthy` (Шаг 6).
>
> **`AIRFLOW_HOST` — не тот URL, что раньше.** До Airflow 3 OpenMetadata просто ходил
> на `http://airflow-webserver:8080` (REST API v1). Теперь: (1) сервис называется
> `airflow-api-server`, (2) API — v2, (3) неизвестно заранее, требует ли реальный
> маршрут префикс `/airflow` (см. примечание к `Airflow API Server` в Шаге 3 — тот же
> вопрос, что и с `HealthCmd`). Проверьте после деплоя, какой из вариантов отвечает,
> и пропишите рабочий в `AIRFLOW_HOST` — командой `wget`, а не `curl`: `curl` в образе
> `openmetadata/server` может отсутствовать (в отличие от `openmetadata/ingestion`, где
> он подтверждён), поэтому запускать эти проверки нужно через `wget` внутри контейнера:
> ```bash
> podman exec etl-om-server wget -qO- http://airflow-api-server:8080/airflow/api/v2/monitor/health
> podman exec etl-om-server wget -qO- http://airflow-api-server:8080/api/v2/monitor/health
> ```
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

```bash
cat > /var/storage/containers/nginx/nginx.conf <<'EOF'
events { worker_connections 1024; }

http {
    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    upstream airflow {
        server airflow-api-server:8080;
    }
    upstream openmetadata {
        server openmetadata-server:8585;
    }
    upstream flower {
        server airflow-flower:5555;
    }

    server {
        listen 80;
        server_name _;

        location = / {
            return 302 /airflow/;
        }

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
            add_header Content-Security-Policy "frame-ancestors 'self';" always;
        }

        location /openmetadata/ {
            proxy_pass http://openmetadata;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        location /flower/ {
            proxy_pass http://flower;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
        }
    }
}
EOF
```

Три `location` теперь устроены **одинаково** — все три не режут префикс
(`proxy_pass http://<upstream>;` без пути после хоста передаёт исходный URI как есть).
Так было не всегда: до Airflow 3 у `/airflow/` был другой механизм (обрезка +
заголовок-подсказка `X-Forwarded-Prefix`), но в Airflow 3 `AIRFLOW__API__BASE_URL`
(Шаг 6) работает так же, как `BASE_PATH` у OpenMetadata и `--url-prefix` у Flower —
приложение само ожидает видеть путь целиком, ничего обрезать не нужно. Это подтверждено
официальным примером nginx-конфига в документации Airflow 3.3.1 для реверс-прокси.

Что появилось нового по сравнению с предыдущей версией конфига:
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
set -e
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
    /var/storage/containers/nginx/nginx.conf \
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
#!/bin/bash
set -e

STORAGE="/var/storage"
CONTAINERS_DIR="$STORAGE/containers"
SECRETS="$CONTAINERS_DIR/secrets.env"
ENV="$CONTAINERS_DIR/etl.env"
NETWORK_FILE="/etc/containers/systemd/etl.network"
IP=$(hostname -I | awk '{print $1}')

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
    echo "[0/8] $NETWORK_FILE уже существует — подсеть не пересчитываю (сеть уже используется контейнерами)"
fi

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
if [ ! -f "$SECRETS" ]; then
    echo "[1/8] Генерация секретов..."
    cat > "$SECRETS" <<EOF
POSTGRES_PASSWORD=$(pw)
OPENMETADATA_DB_PASSWORD=$(pw)
RABBITMQ_PASS=$(pw)
FERNET_KEY=$(fernet_key)
WEBSERVER_SECRET=$(hex 32)
AIRFLOW_ADMIN_USER=etl_admin
AIRFLOW_ADMIN_PASS=$(pw)
OM_ADMIN_USER=om_admin
OM_ADMIN_PASS=$(pw)
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

# ── 3. init-db.sql ────────────────────────────
echo "[2/8] Подготовка init-db.sql..."
cat > "$CONTAINERS_DIR/init-db.sql" <<EOF
CREATE USER openmetadata_user WITH PASSWORD '${OPENMETADATA_DB_PASSWORD}';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;
EOF
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
AIRFLOW__CELERY__WORKER_CONCURRENCY=12
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
AIRFLOW__API_AUTH__JWT_SECRET=${AIRFLOW_JWT_SECRET}
AIRFLOW__API__BASE_URL=http://${IP}/airflow
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW_ADMIN_USER=${AIRFLOW_ADMIN_USER}
AIRFLOW_ADMIN_PASS=${AIRFLOW_ADMIN_PASS}
FLOWER_ADMIN_USER=${FLOWER_ADMIN_USER}
FLOWER_ADMIN_PASS=${FLOWER_ADMIN_PASS}
OPENMETADATA_CLUSTER_NAME=openmetadata
BASE_PATH=/openmetadata
DB_DRIVER_CLASS=org.postgresql.Driver
DB_SCHEME=postgresql
DB_HOST=postgres
DB_PORT=5432
DB_USER=openmetadata_user
DB_USER_PASSWORD=${OPENMETADATA_DB_PASSWORD}
DB_PARAMS=sslmode=disable
SEARCH_TYPE=elasticsearch
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_SCHEME=http
PIPELINE_SERVICE_CLIENT_ENABLED=true
AIRFLOW_HOST=http://airflow-api-server:8080
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
# 3. AIRFLOW_HOST для OpenMetadata указан БЕЗ префикса /airflow — согласно
#    официальным примерам конфигурации Airflow 3 Execution API (внутренний,
#    не проходит через nginx). Если после деплоя оба curl из примечания в
#    Шаге 3 («AIRFLOW_HOST — не тот URL, что раньше») покажут, что рабочий
#    вариант — с префиксом, поменяйте эту строку на
#    http://airflow-api-server:8080/airflow.

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

# ── 8. Запуск ────────────────────────────────────────
echo "[5/8] Запуск сервисов..."
systemctl daemon-reload

systemctl start postgres rabbitmq elasticsearch
wait_healthy etl-postgres
wait_healthy etl-rabbitmq
wait_healthy etl-elasticsearch 90

systemctl start airflow-init

systemctl start airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer airflow-worker airflow-flower
wait_healthy etl-airflow-api-server 60
wait_healthy etl-airflow-dag-processor 60
wait_healthy etl-airflow-triggerer 60
wait_healthy etl-airflow-flower 60

systemctl start openmetadata-migrate
systemctl start openmetadata-server
wait_healthy etl-om-server 90

systemctl start nginx

echo "[6/8] Включение автозапуска..."
systemctl enable postgres rabbitmq elasticsearch \
    airflow-init airflow-api-server airflow-scheduler \
    airflow-dag-processor airflow-triggerer airflow-worker airflow-flower \
    openmetadata-migrate openmetadata-server nginx

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
echo "   ${OM_ADMIN_USER} / ${OM_ADMIN_PASS}"
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

```

---

## Шаг 7. Запуск и проверка

```bash
# Systemd-юниты бэкапа и Quadlet-файлы уже должны быть на месте (Шаги 3 и 5)
/var/storage/containers/deploy.sh
```

**Проверка:**
```bash
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

# Провайдеры реально установились (не молча пропущены)
podman exec etl-airflow-worker airflow providers list
```

---

## Итоговая структура файлов

```
/var/storage/
├── containers/
│   ├── deploy.sh
│   ├── etl-backup.sh
│   ├── secrets.env              ← chmod 600
│   ├── etl.env                  ← chmod 600
│   ├── init-db.sql              ← chmod 600
│   ├── init-airflow.sh
│   ├── airflow-image/
│   │   └── Dockerfile           ← FROM openmetadata/ingestion, провайдеры
│   ├── airflow/
│   │   ├── dags/
│   │   ├── logs/
│   │   └── plugins/
│   ├── rabbitmq/
│   │   └── rabbitmq.conf
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
