# Гайд по развёртыванию ETL-сервера

Стек: Postgres + RabbitMQ + ElasticSearch + единый Airflow-кластер на образе
`openmetadata/ingestion` (CeleryExecutor) + OpenMetadata Server — всё на Podman
Quadlets, AlmaLinux 10.2. Airflow один: обслуживает и ETL-DAG'и компании (провайдеры
MSSQL/SFTP/Samba/SSH/JDBC/ODBC/dbt/HTTP), и ingestion-пайплайны OpenMetadata,
зарегистрированные как Pipeline Service «Airflow» — отдельного встроенного Airflow
внутри OpenMetadata не разворачивается. Внутренние сервисы (Postgres, RabbitMQ,
ElasticSearch, Airflow Flower, Airflow webserver, OpenMetadata Server) слушают только
`127.0.0.1`; наружу смотрит только один порт — 80, nginx разводит Airflow UI и
OpenMetadata по путям `/airflow/` и `/openmetadata/`. Подсеть
`etl-network` подбирается автоматически под хост, секреты генерируются чистым bash без
внешних зависимостей, бэкапы Postgres идут по systemd-таймеру, лимиты контейнеров
рассчитаны под сервер 32 ГБ RAM / 4-8 vCPU.

**Среда:** AlmaLinux 10.2, root, Podman Quadlets, официальные образы.
**Хранилище:** `/var/storage/containers/` (конфиги, DAGs, скрипты) + `/var/storage/volumes/`
(данные БД, ES, RabbitMQ) + `/var/storage/backups/` (дампы Postgres).

---

## Архитектура

```
┌───────────────────────────────────────────────────────────────┐
│                    ВНЕШНИЕ СИСТЕМЫ                            │
│  (MSSQL, PostgreSQL, SFTP, REST API, dbt, файлы)              │
└──────────────┬─────────────────────────┬──────────────────────┘
               │                         │
               │ Провайдеры Airflow      │ Коннекторы OpenMetadata
               │ (движение данных, ETL)  │ (чтение метаданных)
               ▼                         ▼
        ┌──────────────────────────────────────────┐
        │     Airflow DAGs — единый кластер        │
        │  ETL-процессы  +  ingestion-пайплайны    │
        └─────────────────────┬────────────────────┘
                              │ Задачи через RabbitMQ
                              ▼
                  ┌───────────────────────────┐
                  │      Airflow Workers      │
                  │    (выполнение задач)     │
                  └─────┬───────────────┬─────┘
       Результаты ETL   │               │  Метаданные из ingestion-DAG'ов
       (целевые         │               │  (HTTP API → OpenMetadata Server)
        системы/DWH)    ▼               ▼
                                  ┌─────────────────────────────┐
                                  │   OpenMetadata Server       │
                                  │  (хранение метаданных)      │
                                  └────────────┬────────────────┘
                                               │ PostgreSQL + ElasticSearch
                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ХРАНИЛИЩЕ ДАННЫХ                             │
│  PostgreSQL (airflow + openmetadata_db)                         │
│  ElasticSearch (индексы метаданных)                             │
└─────────────────────────────────────────────────────────────────┘
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
  Worker, а напрямую к webserver/API) — OpenMetadata **запускает** ingestion-DAG'и
  через Airflow REST API. Это и есть Pipeline Service «Airflow», настроенный через
  `AIRFLOW_HOST`/`PIPELINE_SERVICE_CLIENT_ENABLED` в `etl.env` (Шаг 6).

---

## Оглавление

**Архитектура**
- Схема потоков данных и метаданных — раздел «Архитектура» выше

**PostgreSQL**
- Хранилище конфигураций Airflow и OpenMetadata — раздел «PostgreSQL (localhost)»

**Apache Airflow**
- Message broker: RabbitMQ — раздел «RabbitMQ (localhost)»
- WebServer: Nginx — раздел «Nginx (внешний доступ)» (конфиг — Шаг 4)
- Executor: Celery Executor — разделы «Airflow Worker», «Airflow Flower (localhost)» (мониторинг очереди)
- Server: Airflow — разделы «Airflow Init (Oneshot)», «Airflow Webserver», «Airflow Scheduler»
- Провайдеры данных (dbt-cloud, http, jdbc, odbc, mssql, postgres, samba, sftp, ssh) — заданы в `etl.env` внутри Шага 6

**OpenMetadata**
- Search Engine: ElasticSearch — раздел «ElasticSearch (localhost)»
- OpenMetadata: execute-migrate-all — раздел «OpenMetadata Migrate (Oneshot)»
- OpenMetadata: Server — раздел «OpenMetadata Server (за nginx, localhost)»
- Ingestion Framework — объединена с Airflow-кластером, отдельный контейнер не разворачивается (примечание сразу после раздела «OpenMetadata Server»)
- Коннекторы (Airflow / MSSQL / PostgreSQL / OpenAPI-REST / dbt Integration / SFTP / Custom Drive) — настраиваются в UI после деплоя (то же примечание)

**Инфраструктура и эксплуатация** *(вне исходных требований, но нужно для деплоя)*
- Шаг 1 — структура каталогов и права
- Шаг 2 — сеть etl-network
- Шаг 4 — конфигурация Nginx (разделение Airflow/OpenMetadata по URL)
- Шаг 5 — бэкапы (Postgres, ElasticSearch, RabbitMQ, конфигурация)
- Шаг 6 — скрипт развёртывания deploy.sh
- Шаг 7 — запуск и проверка
- Итоговая структура файлов
- Управление

---

## Шаг 1. Подготовка структуры каталогов и прав

```bash
dnf install -y podman podman-plugins slirp4netns fuse-overlayfs

# sysctl для ElasticSearch
cat > /etc/sysctl.d/99-elasticsearch.conf <<'EOF'
vm.max_map_count=262144
EOF
sysctl -p /etc/sysctl.d/99-elasticsearch.conf

# Каталоги
mkdir -p /var/storage/volumes/{postgres,rabbitmq,elasticsearch}
mkdir -p /var/storage/containers/{airflow/{dags,logs,plugins,python-deps},nginx}
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
# podman run --rm --entrypoint id docker.getcollate.io/openmetadata/ingestion:1.5.2 airflow
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

> **Почему у пяти контейнеров два имени.** Podman резолвит хосты в
> `etl-network` по фактическому `ContainerName=` (через aardvark-dns), а не по
> короткому имени из названия Quadlet-файла. Все `ContainerName=` здесь — с
> префиксом `etl-` (удобно отличать в `podman ps`/`podman exec`/`journalctl`
> от чужих контейнеров на хосте), а connection-строки в `etl.env` и upstream'ы
> в `nginx.conf` — без префикса (`postgres`, `rabbitmq`, `elasticsearch`,
> `airflow-webserver`, `openmetadata-server`). Чтобы короткие имена реально
> резолвились, у этих пяти контейнеров явно прописан `NetworkAlias=` — без
> него хосты вроде `postgres` внутри сети просто не существовали бы.
> Остальным контейнерам (scheduler/worker/flower/nginx/init/migrate) алиас не
> нужен — к ним никто не обращается по имени изнутри сети.

### PostgreSQL (localhost)
```bash
cat > /etc/containers/systemd/postgres.container <<'EOF'
[Unit]
Description=PostgreSQL
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.io/postgres:16
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
Command=postgres -c shared_buffers=1GB -c effective_cache_size=3GB -c max_connections=100 -c work_mem=16MB -c maintenance_work_mem=256MB

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
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
Image=docker.elastic.co/elasticsearch/elasticsearch:8.10.2
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
EOF
```

> `Xms` всегда равен `Xmx` — иначе JVM ресайзит кучу под нагрузкой, что даёт
> stop-the-world паузы в моменты пиковой нагрузки. Heap — ровно половина лимита
> контейнера (4GB из 8GB): остальное JVM использует под off-heap (Lucene-сегменты,
> файловый кеш ОС).

### Airflow Init (Oneshot)

> **Важно про образ.** Ниже везде используется `docker.getcollate.io/openmetadata/ingestion:1.5.2`
> вместо `apache/airflow` — по документу Airflow «входит в состав OpenMetadata и не
> требует отдельной установки»: официальный контейнер `ingestion` уже содержит Airflow
> с предустановленными пакетами OpenMetadata, и именно его нужно масштабировать до
> Celery-кластера, а не поднимать второй, независимый Airflow рядом. Прежде чем катить
> в прод, сверьте на этом образе три вещи, которые в `apache/airflow` работают
> «из коробки», а в кастомном образе OpenMetadata могли измениться:
> ```bash
> # 1. Тот же ли UID/GID у пользователя airflow
> podman run --rm --entrypoint id docker.getcollate.io/openmetadata/ingestion:1.5.2 airflow
> # 2. Тот же ли entrypoint обрабатывает _PIP_ADDITIONAL_REQUIREMENTS
> podman run --rm --entrypoint cat docker.getcollate.io/openmetadata/ingestion:1.5.2 \
>     /entrypoint | grep -i PIP_ADDITIONAL
> # 3. Принимает ли образ те же CLI-команды (webserver/scheduler/celery worker/celery flower)
> podman run --rm docker.getcollate.io/openmetadata/ingestion:1.5.2 airflow version
> # 4. Есть ли curl внутри — на нём построен HealthCmd вебсервера ниже
> podman run --rm --entrypoint which docker.getcollate.io/openmetadata/ingestion:1.5.2 curl
> ```
> Если что-то из этого разойдётся — Quadlet-файлы ниже нужно будет поправить точечно
> (обычно достаточно скорректировать `Entrypoint=`/`Command=`), сама схема (один
> Celery-кластер на всех) не меняется.

```bash
cat > /etc/containers/systemd/airflow-init.container <<'EOF'
[Unit]
Description=Airflow Init
After=postgres.service rabbitmq.service
Requires=postgres.service rabbitmq.service

[Container]
Image=docker.getcollate.io/openmetadata/ingestion:1.5.2
ContainerName=etl-airflow-init
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/python-deps:/home/airflow/.local:z
Volume=/var/storage/containers/init-airflow.sh:/init-airflow.sh:ro,z
Network=etl.network
Entrypoint=/bin/bash
Command=/init-airflow.sh

[Service]
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
```

> `Entrypoint=/bin/bash` + `Command=/init-airflow.sh` подменяют штатный entrypoint
> образа — из-за этого `_PIP_ADDITIONAL_REQUIREMENTS` (задаётся в `etl.env`, Шаг 6) тут
> не отрабатывает, и это нормально: `db migrate`/`users create` провайдерам не нужны.
> `Type=oneshot` + `RemainAfterExit=yes` — юнит считается «активным» после завершения
> команды, а не всё время работы; на этом основан `Requires=airflow-init.service` у
> webserver/scheduler/worker/flower — systemd не пустит их, пока миграция БД и создание
> админа не завершатся успешно.

### Airflow Webserver
```bash
cat > /etc/containers/systemd/airflow-webserver.container <<'EOF'
[Unit]
Description=Airflow Webserver
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=docker.getcollate.io/openmetadata/ingestion:1.5.2
ContainerName=etl-airflow-webserver
NetworkAlias=airflow-webserver
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/python-deps:/home/airflow/.local:z
PublishPort=127.0.0.1:8080:8080
Network=etl.network
Command=webserver
HealthCmd=curl -sf http://localhost:8080/health
HealthInterval=10s
HealthRetries=6
PodmanArgs=--memory=1g --memory-swap=1g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

> `127.0.0.1:8080` — веб-интерфейс Airflow не выходит наружу напрямую, только через
> nginx на 80-м порту (Шаг 4); прямой 8080 нужен лишь для локальной диагностики через
> SSH-туннель.

### Airflow Scheduler
```bash
cat > /etc/containers/systemd/airflow-scheduler.container <<'EOF'
[Unit]
Description=Airflow Scheduler
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=docker.getcollate.io/openmetadata/ingestion:1.5.2
ContainerName=etl-airflow-scheduler
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/python-deps:/home/airflow/.local:z
Network=etl.network
Command=scheduler
PodmanArgs=--memory=1.5g --memory-swap=1.5g --cpus=1

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

### Airflow Worker
```bash
cat > /etc/containers/systemd/airflow-worker.container <<'EOF'
[Unit]
Description=Airflow Celery Worker
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=docker.getcollate.io/openmetadata/ingestion:1.5.2
ContainerName=etl-airflow-worker
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/python-deps:/home/airflow/.local:z
Network=etl.network
Command=celery worker
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

### Airflow Flower (localhost)
```bash
cat > /etc/containers/systemd/airflow-flower.container <<'EOF'
[Unit]
Description=Airflow Flower
After=postgres.service rabbitmq.service airflow-init.service
Requires=postgres.service rabbitmq.service airflow-init.service

[Container]
Image=docker.getcollate.io/openmetadata/ingestion:1.5.2
ContainerName=etl-airflow-flower
EnvironmentFile=/var/storage/containers/etl.env
Volume=/var/storage/containers/airflow/dags:/opt/airflow/dags:z
Volume=/var/storage/containers/airflow/logs:/opt/airflow/logs:z
Volume=/var/storage/containers/airflow/plugins:/opt/airflow/plugins:z
Volume=/var/storage/containers/airflow/python-deps:/home/airflow/.local:z
PublishPort=127.0.0.1:5555:5555
Network=etl.network
Command=celery flower
PodmanArgs=--memory=512m --memory-swap=512m --cpus=0.5

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF
```

### Nginx (внешний доступ)
```bash
cat > /etc/containers/systemd/nginx.container <<'EOF'
[Unit]
Description=Nginx
After=airflow-webserver.service openmetadata-server.service
Requires=airflow-webserver.service openmetadata-server.service

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
> это осознанно единственная внешняя точка входа: и к Airflow (`/airflow/`), и к
> OpenMetadata (`/openmetadata/`), см. Шаг 4. `Requires=` теперь на оба сервиса —
> раньше OpenMetadata стартовал независимо от nginx (у него был свой прямой порт
> 8585), теперь он обязателен, иначе `/openmetadata/` будет отдавать 502. TLS/443
> в этом рецепте не настраивается.

### OpenMetadata Migrate (Oneshot)
```bash
cat > /etc/containers/systemd/openmetadata-migrate.container <<'EOF'
[Unit]
Description=OpenMetadata Migration
After=postgres.service elasticsearch.service network-online.target
Requires=postgres.service elasticsearch.service
Wants=network-online.target

[Container]
Image=docker.getcollate.io/openmetadata/server:1.5.2
ContainerName=execute-migrate-all
EnvironmentFile=/var/storage/containers/etl.env
Command=./bootstrap/openmetadata-ops.sh migrate
Network=etl.network

[Service]
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
```

> Проверено на реальном образе `docker.getcollate.io/openmetadata/server:1.5.2`:
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
Image=docker.getcollate.io/openmetadata/server:1.5.2
ContainerName=etl-om-server
NetworkAlias=openmetadata-server
EnvironmentFile=/var/storage/containers/etl.env
Environment=OPENMETADATA_HEAP_OPTS=-Xms1g -Xmx1g
PublishPort=127.0.0.1:8585:8585
Network=etl.network
HealthCmd=curl -sf http://localhost:8585/openmetadata/api/v1/system/version
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
> curl -sf http://localhost:8585/openmetadata/api/v1/system/version
> ```
> Если путь соберётся иначе — поправьте `HealthCmd` здесь и адрес в `curl` внутри
> `wait_healthy` (Шаг 6).
>
> **`AIRFLOW_HOST` тоже может понадобиться со сабпутом.** `AIRFLOW__WEBSERVER__BASE_URL`
> у Airflow (Шаг 6) не только меняет ссылки в интерфейсе, но у части версий Airflow
> сдвигает и реальные маршруты приложения под этот префикс — тогда OpenMetadata,
> обращаясь к Airflow API напрямую по имени контейнера (мимо nginx), должен ходить
> уже на `http://airflow-webserver:8080/airflow`, а не на корень. Проверьте после
> деплоя, какой из двух вариантов реально отвечает, прежде чем полагаться на
> Pipeline Service в проде:
> ```bash
> podman exec etl-om-server curl -sf http://airflow-webserver:8080/health
> podman exec etl-om-server curl -sf http://airflow-webserver:8080/airflow/health
> ```
> и пропишите в `etl.env` рабочий вариант.

---

## Шаг 4. Конфигурация Nginx — разделение по URL, не по портам

Вместо отдельного порта 8585 для OpenMetadata — единственный порт 80 наружу с
разными путями: `/airflow/` и `/openmetadata/`. Правило `location = /` — просто
удобный редирект по умолчанию, необязателен.

```bash
cat > /var/storage/containers/nginx/nginx.conf <<'EOF'
events { worker_connections 1024; }

http {
    upstream airflow {
        server airflow-webserver:8080;
    }
    upstream openmetadata {
        server openmetadata-server:8585;
    }

    server {
        listen 80;
        server_name _;

        location = / {
            return 302 /airflow/;
        }

        location /airflow/ {
            proxy_pass http://airflow/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Prefix /airflow;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_read_timeout 3600s;
        }

        location /openmetadata/ {
            proxy_pass http://openmetadata;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
EOF
```

Два `location` устроены по-разному — это не опечатка:

- **`/airflow/`** — nginx **обрезает** префикс (`proxy_pass http://airflow/;` с
  завершающим `/`), а `X-Forwarded-Prefix` сообщает Airflow, что было обрезано, —
  чтобы он сам подставлял `/airflow` обратно при генерации ссылок и редиректов
  (это штатный механизм `ENABLE_PROXY_FIX`, см. Шаг 6).
- **`/openmetadata/`** — nginx **не обрезает** префикс (`proxy_pass http://openmetadata;`
  без пути), потому что OpenMetadata сам ожидает видеть `/openmetadata/...` целиком —
  это то, как работает его `BASE_PATH` (см. Шаг 6): приложение регистрирует свои
  маршруты сразу под этим префиксом, а не «не знает» о нём и ждёт подсказки через
  заголовок, как Airflow.

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
> ssh) задаются переменной `_PIP_ADDITIONAL_REQUIREMENTS` внутри генерации `etl.env`
> ниже (шаг «Формирование etl.env»).

```bash
cat > /var/storage/containers/deploy.sh <<'DEPLOY'
#!/bin/bash
set -e

STORAGE="/var/storage"
CONTAINERS_DIR="$STORAGE/containers"
SECRETS="$CONTAINERS_DIR/secrets.env"
ENV="$CONTAINERS_DIR/etl.env"
NETWORK_FILE="/etc/containers/systemd/etl.network"
IP=$(hostname -I | awk '{print $1}')

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
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-dbt-cloud apache-airflow-providers-http apache-airflow-providers-jdbc apache-airflow-providers-odbc apache-airflow-providers-microsoft-mssql apache-airflow-providers-postgres apache-airflow-providers-samba apache-airflow-providers-sftp apache-airflow-providers-ssh apache-airflow[celery]
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres:5432/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:${POSTGRES_PASSWORD}@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=amqp://airflow:${RABBITMQ_PASS}@rabbitmq:5672/
AIRFLOW__CELERY__WORKER_CONCURRENCY=12
AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__WEBSERVER__SECRET_KEY=${WEBSERVER_SECRET}
AIRFLOW__WEBSERVER__BASE_URL=http://${IP}/airflow
AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
AIRFLOW_ADMIN_USER=${AIRFLOW_ADMIN_USER}
AIRFLOW_ADMIN_PASS=${AIRFLOW_ADMIN_PASS}
OPENMETADATA_CLUSTER_NAME=openmetadata
BASE_PATH=/openmetadata
DB_DRIVER_CLASS=org.postgresql.Driver
DB_SCHEME=postgresql
DB_HOST=postgres
DB_PORT=5432
DB_USER=openmetadata_user
DB_USER_PASSWORD=${OPENMETADATA_DB_PASSWORD}
DB_PARAMS=allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC
SEARCH_TYPE=elasticsearch
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_SCHEME=http
PIPELINE_SERVICE_CLIENT_ENABLED=true
AIRFLOW_HOST=http://airflow-webserver:8080
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
# Примечание: AIRFLOW__WEBSERVER__BASE_URL зафиксировал текущий $IP на момент
# первого запуска. Если IP сервера сменится (DHCP, переезд) — поправьте эту
# строку в $ENV вручную и перезапустите airflow-webserver, иначе ссылки в
# интерфейсе Airflow будут вести на старый адрес. Если у сервера есть
# постоянное DNS-имя, лучше сразу использовать его вместо переменной $IP выше.

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

systemctl start airflow-webserver airflow-scheduler airflow-worker airflow-flower
wait_healthy etl-airflow-webserver 60

systemctl start openmetadata-migrate
systemctl start openmetadata-server
wait_healthy etl-om-server 90

systemctl start nginx

echo "[6/8] Включение автозапуска..."
systemctl enable postgres rabbitmq elasticsearch \
    airflow-init airflow-webserver airflow-scheduler \
    airflow-worker airflow-flower \
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
echo " Внутренние сервисы (доступ через SSH-туннель):"
echo "   Postgres:      ssh -L 5432:127.0.0.1:5432 root@${IP}"
echo "   RabbitMQ:      ssh -L 15672:127.0.0.1:15672 root@${IP}"
echo "   ElasticSearch: ssh -L 9200:127.0.0.1:9200 root@${IP}"
echo "   Flower:        ssh -L 5555:127.0.0.1:5555 root@${IP}"
echo "   OpenMetadata (прямой порт, минуя nginx): ssh -L 8585:127.0.0.1:8585 root@${IP}"
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
# Внешний порт — теперь только один
ss -tlnp | grep ':80 '
# Ожидается: 0.0.0.0:80

# Внутренние — только localhost, включая OpenMetadata (был снаружи, теперь за nginx)
ss -tlnp | grep -E '5432|5672|9200|5555|8080|8585'
# Ожидается: 127.0.0.1:5432, 127.0.0.1:5672, 127.0.0.1:9200, 127.0.0.1:5555,
#            127.0.0.1:8080, 127.0.0.1:8585

# Разделение по URL реально работает
curl -sI http://localhost/airflow/ | head -1
curl -sI http://localhost/openmetadata/ | head -1

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
│   ├── airflow/
│   │   ├── dags/
│   │   ├── logs/
│   │   ├── plugins/
│   │   └── python-deps/
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
├── airflow-webserver.container
├── airflow-scheduler.container
├── airflow-worker.container
├── airflow-flower.container
├── nginx.container
├── openmetadata-migrate.container      (ContainerName=execute-migrate-all)
└── openmetadata-server.container

/etc/systemd/system/
├── etl-backup.service
└── etl-backup.timer
```

---

## Управление

```bash
# Статус
systemctl status postgres rabbitmq elasticsearch \
    airflow-webserver airflow-scheduler airflow-worker \
    openmetadata-server nginx

# Логи
journalctl -u airflow-scheduler -f
journalctl -u openmetadata-server -f
journalctl -u etl-backup.service

# Ручной прогон бэкапа (Postgres + ES-снапшот + RabbitMQ-определения + конфиг)
systemctl start etl-backup.service

# Перезапуск
systemctl restart airflow-worker

# Остановка всего
systemctl stop openmetadata-server nginx \
    airflow-webserver airflow-scheduler airflow-worker airflow-flower \
    elasticsearch rabbitmq postgres

# После reboot всё запустится автоматически (включая таймер бэкапов)
```
