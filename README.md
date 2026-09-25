# AIS Graph Analytics

AIS Graph Analytics collects vessel positions through Kafka or HAIS historical
files, stores canonical AIS observations in ClickHouse, and builds port-visit and
Neo4j/GDS graph snapshots for analysis with dbt and optional Metabase.

It demonstrates an end-to-end data engineering workflow: live and historical
ingestion, canonical time-series storage, repeatable Airflow orchestration,
graph analytics, BI delivery, and automated CI checks.

## Project at a glance

```text
Live AIS -> Kafka -> Kafka Connect -> Neo4j latest vessel state
                 \-> ClickHouse canonical position history

HAIS GeoParquet -> Airflow -> ClickHouse raw layer -> dbt normalization
                                                   \-> canonical position history

Canonical positions -> port visits -> Neo4j port network -> GDS
    -> ClickHouse metric snapshots -> dbt enrichment -> Metabase
```

The analytical workflow infers qualified port stays, publishes consecutive
observed port movements, calculates weighted PageRank and Louvain communities in
Neo4j GDS, and exports versioned results back to ClickHouse for Metabase.

### Verified portfolio snapshot

The local acceptance run completed on September 24, 2026. For the September 23
UTC processing window it processed **2,941,803 positions**, detected **2,696 port
visits**, and produced **51 country-enriched port metric rows**. The broader
persistence check preserved **48,688,051 canonical historical rows**, Neo4j graph
counts and constraints, ClickHouse analytical checksums, and all **136 Airflow run
records** across a full Compose stop/start cycle.

The first GitHub Actions run passed all four jobs: Shell/Compose validation,
pipeline and Airflow unit tests, producer unit tests, and dbt parsing. The local
test run passed **183 unit tests**.

These values document a tested portfolio run; they are not throughput benchmarks
or production service-level claims.

### Presentation

- [English presentation (PDF)](AIS-Graph-Analytics-ENG.pdf)
- [Russian presentation (PDF)](AIS-Graph-Analytics-RUS.pdf)

### Quick links

- [Fresh-clone quickstart](#fresh-clone-quickstart)
- [Architecture and repository layout](#architecture)
- [Port visits and graph summaries](#port-visits-and-graph-summaries)
- [Known analytical limitation](#known-limitation-port-visits-spanning-processing-window-boundaries)
- [Continuous integration](#continuous-integration)
- [Current validation status](#status)

## Continuous integration

[The CI workflow](.github/workflows/ci.yml) runs on pushes, pull requests, and
manual dispatch. It validates tracked Bash scripts and all Compose profiles,
builds the project Airflow image, runs pipeline/Airflow and producer unit tests,
and parses dbt with the example profile and a placeholder password. No repository
secrets or running databases are required. Pipeline/Airflow tests run in a
container with networking disabled and a read-only checkout.

The pipeline test runner is [scripts/ci-unit-tests.sh](scripts/ci-unit-tests.sh).
Run it in the Airflow image as shown in the workflow; each suite uses a separate
Python process. CI excludes the live-database integration check and does not run
`dbt build`, ingest AIS data, or replace runtime acceptance and backup/restore
checks. A successful `dbt parse` validates project parsing, not database queries.

## Fresh-clone quickstart

Run commands from the repository root in **Bash**, keeping the same shell for the
dbt steps. Steps marked optional can be skipped. The historical example processes
September 1–16, 2026; substitute dates for files you actually have.

| Component | How it runs | When needed |
| --- | --- | --- |
| Kafka, Kafka Connect, ksqlDB, Kafbat UI, ClickHouse, Neo4j | Core Compose services (no profile) | Core runtime |
| Airflow | Compose `batch` profile | HAIS ingestion and daily analytics |
| AIS producer | Compose `live` profile | Optional live AIS ingestion |
| Metabase | Compose `analytics` profile | Optional visualization |
| dbt | Host Python virtual environment | HAIS normalization and analytical models |
| Airbyte | Separately deployed; not in Compose | Optional country enrichment only |

### 1. Prerequisites

- Git, Docker Engine/Desktop running, and Docker Compose v2 with `--wait` support.
- Host Bash, `curl`, `jq`, and Python 3.11 with `venv`/`pip` for dbt; `python3`
  must also be available for the daily-run helper.
- Network access for images, Python packages, Neo4j plugins/JDBC download, and the
  configured UN OCHA WPI port-reference API. The Kafka connector 5.5.3 JAR is
  already checked in under `plugins/`.
- Available local ports listed [below](#versions-and-ports), plus ClickHouse
  `8123`/`9000`, Neo4j Browser `7474`, optional Airflow `8080`, and Metabase `3000`.
- For live ingestion: BarentsWatch client credentials authorized for AIS access.
  For historical processing: manually obtained daily HAIS GeoParquet files.

### 2. Clone the repository

```sh
git clone https://github.com/KonstantinLofichenko/ais-graph-analytics.git
cd ais-graph-analytics
```

### 3. Create local environment configuration

```sh
cp .env.example .env
```

### 4. Create the shared HAIS directory

```sh
mkdir -p data/hais
```

The template sets `HAIS_HOST_DIR=./data/hais`. Both Airflow and ClickHouse mount
this directory read-only; their container users need read/traverse permissions.

### 5. Configure required local secrets

Edit `.env` and set `NEO4J_PASSWORD` and `CLICKHOUSE_PASSWORD`; retain or choose
`CLICKHOUSE_USER`. For optional live ingestion, also set `BW_AIS_CLIENT_ID` and
`BW_AIS_CLIENT_SECRET`. Leave `AIS_MESSAGE_LIMIT` empty for continuous ingestion.
Airbyte/country API credentials are not required for this quickstart.

```sh
cp neo4j/connectors/ais-sink.example.json neo4j/connectors/ais-sink.json
```

Edit the copied JSON: replace its password placeholder with the same Neo4j password.
Keep `neo4j.uri` as `bolt://neo4j:7687`. `.env` and this local connector config are
ignored by Git. Never commit secrets or share expanded `docker compose config`
output. Airflow standalone generates its own login password; the template's
`AIRFLOW_PASSWORD` does not configure that login.

### 6. Start core Docker services

```sh
docker compose up -d --wait --wait-timeout 300
docker compose ps
```

This starts Kafka, Kafka Connect, ksqlDB, Kafbat UI, ClickHouse, and Neo4j. The
`live`, `batch`, and `analytics` profiles remain optional. Fresh ClickHouse volumes
run `clickhouse/init/` automatically; existing volumes do not rerun initialization.

### 7. Bootstrap tables, constraints, and the connector

```sh
./scripts/bootstrap.sh
curl --fail --silent --show-error \
  http://localhost:8083/connectors/ais-neo4j-sink/status | jq '.'
```

Confirm the connector and its tasks report `RUNNING`. Bootstrap creates/verifies
`ais.positions`, applies additive migrations **003, 004, and 005**, applies all
Neo4j constraints, and registers/updates the sink using the local JSON. It is
rerunnable but does not reconcile existing table schemas. It does **not** run
`001_replacing_ais_positions.sql`, dbt, or analytical DAGs.

### 8. Optionally start Metabase

```sh
docker compose --profile analytics up -d metabase
```

Open [Metabase](http://localhost:3000), complete its local setup, and add ClickHouse
using Docker hostname `clickhouse`, port `8123`, and your local credentials.
Database connections and dashboards are not provisioned by this repository.
See [Metabase dashboard transfer](metabase/README.md) for JSON export/import scripts.
Country-enriched models are optional and require the external source in step 14.

### 9. Optionally start the continuous producer

```sh
AIS_MESSAGE_LIMIT= docker compose --profile live up -d --build --no-deps ais-producer
docker compose logs --tail 30 -f ais-producer
```

This explicitly disables a message limit, including one left in an older `.env`.
The service uses `restart: unless-stopped`. Ctrl-C exits the log viewer only;
`docker compose stop ais-producer` stops ingestion. Airflow does not orchestrate it.

### 10. Optionally run a bounded producer test

**Stop the continuous producer and any host-side producers first.** Use a one-off
container so its successful exit is not followed by a service restart:

```sh
docker compose stop ais-producer
docker compose --profile live build ais-producer
docker compose --profile live run --rm --no-deps \
  -e AIS_MESSAGE_LIMIT=20 ais-producer
```

The test container is removed on exit; the continuous service remains stopped
until explicitly started again. Twenty messages need not create twenty Vessel
nodes because the sink merges by MMSI. See [producer validation](#producer-behavior-and-validation).

### 11. Configure dbt from the profile template

```bash
python3.11 -m venv .venv-dbt
source .venv-dbt/bin/activate
python -m pip install -r dbt/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
export DBT_PROFILES_DIR="$PWD/dbt"
export CLICKHOUSE_USER=default  # match your local .env
read -r -s -p 'ClickHouse password: ' DBT_ENV_SECRET_CLICKHOUSE_PASSWORD
printf '\n'
export DBT_ENV_SECRET_CLICKHOUSE_PASSWORD
dbt parse --project-dir dbt
dbt debug --project-dir dbt
```

The password must match `.env`; dbt does not load that file automatically. The
ignored local profile defaults to `localhost:8123` and schema `analytics`. No
`dbt deps` is needed. Reactivate the environment and export credentials in each
new shell. See [dbt reference](dbt/README.md) for profile overrides and version pins.

### 12. Ingest HAIS and load canonical AIS

Place completed, immutable files named `hais_YYYY-MM-DD.snappy.parquet` in
`data/hais/`, one for **every date September 1–16** in this example. Files are not
downloaded by the DAG. Then start Airflow and trigger raw ingestion:

```sh
docker compose --profile batch up -d --build --wait --wait-timeout 300 airflow
docker compose exec -T airflow airflow dags unpause ais_hais_historical_ingestion
docker compose exec -T airflow airflow dags trigger ais_hais_historical_ingestion \
  --conf '{"start_date":"2026-09-01","end_date":"2026-09-16"}'
```

**Wait for success before continuing.** Use [Airflow](http://localhost:8080) and
review `raw.hais_ingestion_runs`; see [Airflow login](airflow/README.md#start-and-stop)
and [HAIS audit/recovery](pipelines/hais/README.md#audit-and-retry-behavior).
Previously successful file-name/size pairs are skipped; unresolved attempts need
manual review before a retry.

`raw.hais_positions` is source-faithful: source fields, sentinel values, and
source duplicates are retained, with geometry omitted. Build the normalization
view and load the shared **canonical normalized** table `raw.ais_positions`:

```sh
dbt run --project-dir dbt --select stg_hais_positions
dbt run-operation load_hais_to_ais_positions --project-dir dbt \
  --args '{"start_date":"2026-09-01","end_date":"2026-09-17"}'
```

| Operation | Start | End | Example includes |
| --- | --- | --- | --- |
| HAIS file ingestion | Inclusive | **Inclusive** | September 1–16 |
| dbt canonical load | Inclusive | **Exclusive**, UTC midnight | September 1–16 |
| Daily analytics | Inclusive | **Exclusive**, UTC midnight | September 1–16 |

Supply valid, trusted dates to the macro: it interpolates SQL and does not validate
the date range. It performs an INSERT, with no success ledger; reruns add physical
versions resolved by `ReplacingMergeTree`. Overlapping HAIS loads can replace
live/REST records, including names/ship types with HAIS NULLs. Review source
coverage before loading. See [load details](dbt/README.md#4-load-a-bounded-range-into-canonical-ais).

### 13. Run daily analytics

Unpause the master and its independently runnable children, then trigger only the
master for the same canonical date range:

```sh
for dag in ais_port_visits ais_gds_metrics ais_graph_metrics_export ais_analytics_pipeline; do
  docker compose exec -T airflow airflow dags unpause "$dag"
done
bash airflow/daily_run.sh 2026-09-01 2026-09-17 5000000
```

This runs 16 sequential daily chains:
`ais_port_visits -> ais_gds_metrics -> ais_graph_metrics_export -> next day`.
Wait for master success before continuing. Each next day waits for the previous
export; a failure stops advancement. `max_rows` is a per-day safety ceiling, not a
sampling limit; adjust it to the dataset. The existing run ID is UTC window start,
e.g. `2026-09-01T00:00:00Z`. Do not run competing graph-changing DAGs in parallel.
The [cross-window visit limitation](#known-limitation-port-visits-spanning-processing-window-boundaries)
still applies. See [orchestration details](airflow/README.md#sequential-daily-analytics).

### 14. Build dbt models and run tests

```sh
dbt build --project-dir dbt --select +vessels current_port_visits
```

This builds the selected models and runs their existing tests without country
data. It does not invoke the canonical-load macro, port detection, or GDS.

**Optional enrichment:** only after separately provisioning and populating
`raw.countries` through Airbyte, run:

```sh
dbt build --project-dir dbt --select +countries +port_graph_metrics_enriched
```

This creates `analytics.countries` and the enriched graph-metrics view for
Metabase. The country source and dashboard setup are not reproducible from this
repo alone. Avoid unqualified `dbt build` until that source exists. See
[Optional Airbyte enrichment](#optional-airbyte-enrichment).

### 15. Shut down without deleting volumes

After active ingestion/analytics have finished:

```sh
docker compose --profile live --profile batch --profile analytics down
```

Named volumes and host HAIS files are retained. **Do not add `-v`** when preserving
data. The sections below are background, validation, and troubleshooting references.

## Known limitation: port visits spanning processing-window boundaries

Port-visit detection currently processes each UTC day independently.

If a vessel remains continuously inside the same port across midnight, the
physical stay may be represented as multiple daily visit fragments because:

- the current daily run finalizes an active visit at the end of its input window;
- detector state is not persisted between daily runs;
- the next daily run starts with no knowledge of the previous observation.

This can cause:

- overcounting of physical port visits;
- fragmented stay durations;
- small differences in derived port-to-port connections and graph metrics.

The issue was identified during data-quality testing against raw AIS observations.
It is acceptable for the current portfolio implementation.

A production implementation would use either:

- persistent detector state across processing windows; or
- continuous-range physical visit detection followed by daily analytical
  projection.

## Architecture

```text
BarentsWatch AIS
  -> Kafka topic ais.positions
      -> Kafka Connect 5.5.3 -> Neo4j Vessel latest state
      -> ClickHouse Kafka engine/materialized view -> raw.ais_positions

HAIS GeoParquet
  -> Airflow audited ingestion -> raw.hais_positions
  -> dbt normalization/load -> raw.ais_positions

raw.ais_positions
  -> Airflow port-visit detection -> ClickHouse visits + Neo4j VISITED
  -> CONNECTED_TO publication -> Neo4j GDS PageRank/Louvain
  -> ClickHouse graph snapshots -> dbt enrichment -> Metabase
```

Docker Compose is the supported local runtime for Neo4j, ClickHouse, Kafka, Kafka
Connect, ksqlDB, Kafbat UI, Airflow, the optional producer, and Metabase on the
shared `ais-network` network. Docker clients use `bolt://neo4j:7687`; host clients
use `bolt://localhost:7687`.

The repository is organized by platform concern:

- `producers/ais/`: AIS ingestion producers
- `airflow/dags/`: orchestration workflows
- `airbyte/`: source and destination connector configuration
- `clickhouse/init/`: ClickHouse initialization SQL
- `neo4j/cypher/`: graph schema and Cypher queries
- `neo4j/gds/`: Graph Data Science workflows
- `neo4j/connectors/`: local Kafka Connect connector configurations
- `dbt/`: analytical transformations
- `metabase/`: dashboard and collection exports
- `docs/architecture/`: architecture documentation

## Versions and ports

| Component | Version | Address | Purpose |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:4.3.1` | `localhost:9092` / `ais-kafka:29092` | Host / Docker listeners |
| Kafka Connect | `confluentinc/cp-kafka-connect:8.3.1` | `http://localhost:8083` | Connector REST API |
| ksqlDB | `confluentinc/cp-ksqldb-server:8.3.1` | `http://localhost:8088` | Streaming SQL REST API |
| Kafbat UI | `ghcr.io/kafbat/kafka-ui:latest` | `http://localhost:8081` | Kafka UI |
| Neo4j Kafka Connector | `5.5.3` | Docker plugin path | Neo4j sink connector |
| Docker Neo4j | `2026.07.1` | `neo4j://localhost:7687` | Graph database |

Kafka advertises `ais-kafka:29092` to Docker services and `localhost:9092` to macOS. Kafka Connect mounts `./plugins` at `/usr/share/java/plugins`; the Neo4j connector 5.5.3 JAR is included in the repository.

Kafka stores broker data in the `kafka-data` named volume at `/var/lib/kafka/data`.
**Adding this volume does not migrate data from the existing broker container.**
Before recreating that container, back up or explicitly migrate its current Kafka
storage if those records must be retained. Existing Neo4j and ClickHouse named
volume mappings are unchanged. Airflow's `batch` service waits for healthy
ClickHouse and Neo4j before starting.

## ClickHouse raw layer

ClickHouse stores the complete historical AIS event stream in `raw.ais_positions`. Neo4j stores graph entities and the latest known vessel state; ClickHouse stores every AIS observation for historical and time-series analysis. dbt models use the `analytics` database; see [dbt setup](dbt/README.md).
`raw.hais_positions` preserves the HAIS source; the dbt staging view and load macro
populate the canonical normalized `raw.ais_positions` table.

The raw table uses monthly event-time partitions:

```sql
PARTITION BY toYYYYMM(msgtime)
```

This keeps large historical datasets manageable by event month rather than ingestion month. Its primary access pattern is vessel movement over a time range, so it is sorted by:

```sql
ORDER BY (mmsi, msgtime)
```

The initialization file is [clickhouse/init/01_raw.sql](clickhouse/init/01_raw.sql). Init scripts normally run only when ClickHouse initializes a new data volume, so an existing volume must be validated or updated explicitly.

## Kafka to ClickHouse streaming

AIS events flow through the ClickHouse pipeline as follows:

```text
Kafka ais.positions
  -> raw.ais_positions_kafka (Kafka Engine consumer interface)
  -> raw.ais_positions_mv (field and timestamp transformation)
  -> raw.ais_positions (durable ReplacingMergeTree history)
```

The Kafka Engine table is a consumer interface, not persistent analytical storage. The materialized view maps the incoming camelCase AIS fields and parses `msgtime`; the ReplacingMergeTree table stores every event durably while converging duplicate versions. Neo4j is a separate Kafka consumer for graph entities and latest vessel state, so ClickHouse event counts and Neo4j `Vessel` counts are intentionally different.

`raw.ais_positions` uses `ReplacingMergeTree(ingested_at)` so overlapping historical and live ingestion can converge to one logical event per `(mmsi, msgtime)`. Physical duplicate parts may exist temporarily before background merges; use `FINAL` when validating the logical view. The manual migration is [clickhouse/migrations/001_replacing_ais_positions.sql](clickhouse/migrations/001_replacing_ais_positions.sql). Pause AIS producers and ClickHouse Kafka ingestion before running it. It keeps the pre-migration table as `raw.ais_positions_merge_backup` and does not run automatically from Docker startup.

**Existing installations only:** migration 001 is not part of the fresh-clone quickstart.
Run it only when upgrading an older raw table, after pausing producers and ClickHouse ingestion:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/001_replacing_ais_positions.sql
```

After migration, validate logical uniqueness with:

```sql
SELECT
  count() AS physical_rows,
  uniqExact((mmsi, msgtime)) AS logical_events
FROM raw.ais_positions FINAL;

SELECT
  mmsi,
  msgtime,
  count()
FROM raw.ais_positions
GROUP BY mmsi, msgtime
HAVING count() > 1
ORDER BY count() DESC
LIMIT 20;
```

Useful validation queries:

```sql
SHOW TABLES FROM raw;

SHOW CREATE TABLE raw.ais_positions_kafka;

SHOW CREATE TABLE raw.ais_positions_mv;

SELECT count()
FROM raw.ais_positions;

SELECT
  count(DISTINCT mmsi) AS vessels,
  count() AS events
FROM raw.ais_positions;

SELECT
  mmsi,
  count() AS event_count
FROM raw.ais_positions
GROUP BY mmsi
HAVING event_count > 1
ORDER BY event_count DESC;
```

## Neo4j model

The initial sink writes one `Vessel` node per MMSI:

```cypher
MERGE (v:Vessel {mmsi: event.mmsi})
ON CREATE SET v.createdAt = datetime()
SET v.name = event.name,
   v.shipType = event.shipType,
   v.lastLatitude = event.latitude,
   v.lastLongitude = event.longitude,
   v.lastSpeedOverGround = event.speedOverGround,
   v.lastCourseOverGround = event.courseOverGround,
   v.lastRateOfTurn = event.rateOfTurn,
   v.lastTrueHeading = event.trueHeading,
   v.lastNavigationalStatus = event.navigationalStatus,
   v.lastStream = event.stream,
   v.lastSeen = datetime(event.msgtime),
   v.updatedAt = datetime()
```

It updates the vessel's latest position and navigation properties. `createdAt` is
set only for a new vessel; `updatedAt` changes for every processed event. It does
not create a `Position` node for every AIS event. The full event history remains
in ClickHouse.

## Producer behavior and validation

The Docker producer preserves the original AIS JSON fields and uses MMSI as the
Kafka key.
SIGINT/SIGTERM flush Kafka with a 35-second deadline; Compose allows 45 seconds.
No data-arrival healthcheck is used: quiet upstream periods are not a failure.

OAuth uses `client_credentials` with scope `ais`. A fresh token is requested before `expires_in`
elapses, including during an idle stream. A 401 triggers renewal; rejection of a fresh token
fails clearly. Network errors, EOF, 429 and server errors reconnect with capped exponential
backoff and jitter. Numeric Retry-After delays are honored up to 300 seconds.
Permanent HTTP/configuration errors exit without logging credentials or response bodies.

Records retain the original JSON fields and MMSI key. Only acknowledged deliveries count
toward the limit. Kafka idempotence protects retries within a producer session; this is not
end-to-end exactly-once delivery across restarts or upstream reconnects. Delivery failures
stop the producer instead of silently skipping an event. The live API has no replay checkpoint,
so outages/reconnects can leave gaps or repeat upstream events.

For a bounded validation, stop other producers first, record Kafka partition end offsets,
then use the [one-off test](#10-optionally-run-a-bounded-producer-test). The sum of end offsets should increase by exactly 20:

```sh
docker exec ais-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server ais-kafka:29092 --topic ais.positions --time -1
```

Compare `SELECT count() FROM raw.ais_positions` before and after ingestion; allow consumer lag.
In Neo4j compare `Vessel` properties and `lastSeen`, not just node count, because existing
vessels are updated. The Kafka Connect sink and its tasks should remain `RUNNING`.

Run deterministic lifecycle and failure tests without credentials:

```sh
python3 -m unittest discover -s producers/ais/tests -v
```

## Port visits and graph summaries

The port-visit batch infers visits from canonical ClickHouse AIS history and stores
`(Vessel)-[:VISITED]->(Port)` summaries in Neo4j. Individual visits remain in
ClickHouse; existing live Vessel properties are preserved. The Airflow DAG downloads
and validates its configured WPI reference before processing. It publishes managed
`CONNECTED_TO` relationships; GDS writes PageRank/Louvain metrics, and the separate
export DAG retains snapshots in `analytics.port_graph_metrics`.

See [port detection](pipelines/port_visits/README.md),
[connections](airflow/README.md#three-tasks),
[GDS/export](pipelines/graph_metrics/README.md), and
[manual GDS](neo4j/gds/README.md) for algorithms, retry behavior, and validation.

## Optional Airbyte enrichment

Airbyte is **not deployed by `docker-compose.yml`** and is **not required for the
core AIS analytics pipeline**. It is used separately to ingest country reference
data from the REST Countries API into ClickHouse table `raw.countries` (database/
schema `raw`), declared as `source('raw', 'countries')` in dbt. The staging model
expects JSON strings in `codes`, `names`, and `capitals`, plus a `region` field.
Airbyte deployment, source credentials, and the source-to-ClickHouse connection
must be configured separately.

The sanitized source, destination, and connection export is documented in
[`airbyte/`](airbyte/README.md). It preserves the selected stream,
schedule, sync mode, namespace, ClickHouse target, and schema-change behavior,
while replacing secrets and deployment-specific IDs with placeholders. The
custom Connector Builder manifest is included in
[`airbyte/exports/countries-to-clickhouse/`](airbyte/exports/countries-to-clickhouse/);
import it before using the source configuration.

The dependent dbt models are:

- `stg_countries`: extracts and normalizes country codes, names, region, and capital.
- `countries`: builds `analytics.countries` from `stg_countries`.
- `port_graph_metrics_enriched`: joins `countries` to port/graph metrics using
  `analytics.ports.country` and the country ISO alpha-2 code. Its LEFT JOIN still
  requires the country relation to exist.

**Without Airbyte:** skip the optional country-enrichment command in quickstart
step 14 and use only its `dbt build --project-dir dbt --select +vessels current_port_visits`
command. Do not run unqualified `dbt build`, which includes the country-dependent
models. HAIS ingestion, canonical AIS loading, port visits, GDS, and export to
`analytics.port_graph_metrics` remain available without country data.

Once `raw.countries` is populated, follow step 14's optional command or the
[dbt enrichment reference](dbt/README.md#optional-country-enrichment). Country and
enriched-view SQL migrations were superseded by these dbt models; do not apply
the removed migrations.

## Troubleshooting

1. **Container name already in use**

  Old containers created manually with `docker run` can conflict with Compose. Check with `docker ps -a` and remove only obsolete AIS containers if necessary.

2. **`ais-network` already exists but was not created by Compose**

  An old manually created network may conflict with the Compose-owned network. Inspect it with `docker network inspect ais-network` and remove only the obsolete network after stopping the old containers.

3. **ksqlDB is unhealthy although `/info` returns `RUNNING`**

  `cp-ksqldb-server:8.3.1` does not contain `curl`. The correct healthcheck is `bash -c '</dev/tcp/127.0.0.1/8088'`.

4. **Kafka Connect is unhealthy**

  `cp-kafka-connect:8.3.1` does not contain `curl`. The correct healthcheck is `bash -c '</dev/tcp/127.0.0.1/8083'`.

5. **Kafka Connect cannot reach Neo4j**

  Do not use `localhost` from inside Docker. Use `bolt://neo4j:7687` in the connector configuration.

6. **Neo4j plugin is missing from `/connector-plugins`**

  Check both plugin locations and restart only Kafka Connect:

  ```sh
  ls -lh plugins
  docker exec kafka-connect ls -lh /usr/share/java/plugins
  docker compose restart kafka-connect
  ```

## Status

Completed and verified:

- live Kafka ingestion into ClickHouse and the latest Vessel state in Neo4j;
- HAIS ingestion and canonical dbt load;
- daily port visits, port connections, GDS metrics, export, and Metabase dashboard;
- connector recovery and full Compose stop/start persistence;
- local unit/dbt validation and the first successful GitHub Actions run;
- English and Russian portfolio presentations.

Deferred for later validation:

- backup and restore recovery;
- isolated fresh-clone installation with empty volumes.

Run `./scripts/status.sh` to show Neo4j status, Compose service status, Kafka
topics, and registered Kafka Connect connectors.
