# AIS Graph Analytics

[![CI](https://github.com/KonstantinLofichenko/ais-graph-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/KonstantinLofichenko/ais-graph-analytics/actions/workflows/ci.yml)
[![Release](https://github.com/KonstantinLofichenko/ais-graph-analytics/actions/workflows/release.yml/badge.svg)](https://github.com/KonstantinLofichenko/ais-graph-analytics/actions/workflows/release.yml)

AIS Graph Analytics is a portfolio data-engineering platform for live and historical AIS vessel data. It combines streaming ingestion, batch orchestration, analytical modeling, graph analytics, AI enrichment, dashboarding, automated testing, and versioned container releases in one reproducible project.

The project is intentionally multi-engine: ClickHouse keeps durable event history and analytical tables, Neo4j models vessel/port relationships and graph metrics, Kafka carries the live AIS stream, dbt owns analytical transformations, and Airflow coordinates daily processing.

## Architecture

![AIS Graph Analytics architecture](docs/assets/architecture.png)

### Main data paths

```text
Live AIS
BarentsWatch -> Python producer -> Kafka -> ClickHouse raw history
                                      -> Kafka Connect -> Neo4j current vessel state

Historical AIS
HAIS GeoParquet -> Airflow -> ClickHouse raw.hais_positions
                           -> dbt normalization -> raw.ais_positions

Analytics
raw.ais_positions -> dbt marts -> anomaly candidates -> AI enrichment
                  -> port visits -> Neo4j/GDS -> graph snapshots -> Metabase
```

## What the project demonstrates

- **Streaming ingestion:** BarentsWatch AIS -> Kafka -> ClickHouse and Neo4j.
- **Historical ingestion:** HAIS GeoParquet files loaded through Airflow into a canonical ClickHouse event model.
- **Analytical engineering:** dbt staging, marts, tests, seeds, and ClickHouse-specific modeling.
- **Graph analytics:** vessel/port graph processing with Neo4j, APOC, PageRank, Louvain communities, and graph snapshot export.
- **AI enrichment:** bounded, cache-aware OpenAI enrichment of daily vessel anomalies with full response history.
- **BI:** Metabase dashboards for ports, vessels, graph communities, anomalies, and AI insights.
- **CI:** isolated validation of configuration, Airflow/pipeline code, producer code, dbt + ClickHouse integration, and Kafka + Neo4j smoke behavior.
- **Release automation:** semantic Git tags build and publish project-owned Docker images to GitHub Container Registry and create a GitHub Release.

## Technology stack

| Area | Technology |
| --- | --- |
| Streaming | Apache Kafka 4.3.1, Kafka Connect, ksqlDB, Kafbat UI |
| Storage / analytics | ClickHouse 26.3 |
| Transformation | dbt Core 1.12.5, dbt-clickhouse 1.10.3 |
| Orchestration | Apache Airflow |
| Graph | Neo4j 2026.07.1, APOC, Graph Data Science |
| BI | Metabase |
| AI enrichment | OpenAI API |
| Optional reference ingestion | Airbyte |
| CI/CD | GitHub Actions, GitHub Container Registry |
| Runtime | Docker Compose |

## Core analytical objects

| Object | Purpose |
| --- | --- |
| `raw.ais_positions` | Canonical AIS event history from live and historical sources |
| `raw.hais_positions` | Source-faithful HAIS historical landing table |
| `analytics.vessels` | Vessel identity and current analytical state |
| `analytics.vessel_daily_features` | Daily movement, speed, stationary, and navigation-status features |
| `analytics.vessel_daily_anomalies` | Deterministic daily anomaly candidates |
| `analytics.vessel_ai_enrichment` | Historical AI responses keyed by date/model/prompt/input hash |
| `analytics.vessel_daily_enriched` | Daily vessel analytics with current valid AI enrichment |
| `analytics.port_visits` | Port-visit history with logical tombstones for stale replacements |
| `analytics.port_graph_metrics` | Versioned graph metric snapshots |
| `analytics.port_graph_metrics_enriched` | Graph metrics enriched with reference dimensions |
| `analytics.port_graph_communities` | One row per graph community and activity date |

## Fresh-clone quick start

Run from the repository root in Bash.

### 1. Clone and configure

```bash
git clone https://github.com/KonstantinLofichenko/ais-graph-analytics.git
cd ais-graph-analytics
cp .env.example .env
mkdir -p data/hais
```

Set at least `NEO4J_PASSWORD` and `CLICKHOUSE_PASSWORD` in `.env`. For live AIS ingestion, also configure the BarentsWatch credentials. Keep secrets out of Git.

Create the local Kafka Connect configuration:

```bash
cp neo4j/connectors/ais-sink.example.json neo4j/connectors/ais-sink.json
```

Set the connector password to the same Neo4j password and keep the Docker URI as `bolt://neo4j:7687`.

### 2. Start the core platform

```bash
docker compose up -d --wait --wait-timeout 300
./scripts/bootstrap.sh
./scripts/status.sh
```

Core Compose services include Kafka, Kafka Connect, ksqlDB, Kafbat UI, ClickHouse, and the customized Neo4j image.

`bootstrap.sh` creates/verifies the Kafka topic, applies additive ClickHouse migrations `003` through `010`, applies Neo4j constraints, and creates/updates the Kafka Connect sink.

### 3. Optional profiles

```bash
# Live AIS producer
docker compose --profile live up -d --build ais-producer

# Airflow batch / daily analytics
docker compose --profile batch up -d --build --wait --wait-timeout 300 airflow

# Metabase
docker compose --profile analytics up -d metabase
```

### 4. Configure dbt

```bash
python3.11 -m venv .venv-dbt
source .venv-dbt/bin/activate
python -m pip install -r dbt/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
export DBT_PROFILES_DIR="$PWD/dbt"
export CLICKHOUSE_USER=default
read -r -s -p 'ClickHouse password: ' DBT_ENV_SECRET_CLICKHOUSE_PASSWORD
printf '\n'
export DBT_ENV_SECRET_CLICKHOUSE_PASSWORD

dbt deps --project-dir dbt --profiles-dir dbt
dbt debug --project-dir dbt --profiles-dir dbt
```

### 5. Historical HAIS ingestion

Place completed files such as:

```text
data/hais/hais_2026-09-01.snappy.parquet
```

Trigger historical ingestion for the required date range, then normalize/load it into the canonical `raw.ais_positions` table. Historical file dates are inclusive; canonical-load end dates are exclusive UTC boundaries.

See [Detailed project documentation](docs/README.md) for the complete sequence and validation notes.

## Daily orchestration

The master Airflow DAG is `daily_ais_pipeline` and is scheduled at **02:00 UTC**. It also supports an explicit `dag_run.conf.activity_date` for controlled reruns/backfills.

At a high level the daily flow coordinates:

```text
source/canonical readiness
        -> dbt analytics
        -> vessel anomaly selection
        -> AI enrichment
        -> port visits
        -> Neo4j graph refresh / GDS
        -> graph metrics export
        -> downstream dashboard-ready tables
```

AI calls are outside the database. The enrichment pipeline uses deterministic input hashing and exact cache lookup so unchanged inputs do not generate duplicate API calls. Historical AI responses remain available for auditability.

## Graph model

Neo4j keeps the current graph state while ClickHouse retains historical analytical snapshots.

Representative graph structures include:

```text
(:Vessel)-[:VISITED]->(:Port)
(:Port)-[:CONNECTED_TO]->(:Port)
(:Port)-[:MEMBER_OF]->(:Community)
```

GDS calculations include PageRank and Louvain community detection. Community labels are deterministic and derived from representative high-PageRank ports. Graph history is exported back to ClickHouse for time-based analysis and Metabase reporting.

## Metabase

The current dashboard is organized around three user-facing areas:

- **Ports** - port activity, connections, PageRank, and communities.
- **Vessels** - vessel identity, type/category, activity, and data-quality context.
- **Anomalies & AI Insights** - deterministic anomaly candidates and valid AI enrichment.

Technical snapshot identifiers remain in the storage model, but user-facing filtering is standardized around `activity_date` where applicable.

## CI

`.github/workflows/ci.yml` runs five independent jobs:

| Job | What it validates |
| --- | --- |
| Configuration | Shell syntax and Docker Compose configuration |
| Pipelines | Airflow image build plus isolated pipeline/DAG unit tests |
| Producer | Python unit tests plus AIS producer image build |
| dbt + ClickHouse | Fresh ClickHouse, init SQL, migrations, deterministic CI fixture, dbt seed/build/tests |
| Kafka + Neo4j | Kafka produce/consume smoke test and customized Neo4j startup/write/read/constraints |

CI does not call BarentsWatch, OpenAI, Airbyte, production Metabase, or a developer workstation.

## Releases and Docker images

A stable semantic tag triggers `.github/workflows/release.yml`:

```bash
git tag -a v1.0.3 -m "Release v1.0.3"
git push origin v1.0.3
```

The release workflow validates the tag/commit, runs CI, builds project-owned images, pushes them to GHCR, and creates a GitHub Release.

Published project images:

```text
ghcr.io/konstantinlofichenko/ais-graph-analytics-airflow
ghcr.io/konstantinlofichenko/ais-graph-analytics-producer
ghcr.io/konstantinlofichenko/ais-graph-analytics-neo4j
```

Stable releases receive version aliases such as:

```text
1.0.3
1.0
1
latest
```

Example:

```bash
docker pull ghcr.io/konstantinlofichenko/ais-graph-analytics-airflow:latest
docker pull ghcr.io/konstantinlofichenko/ais-graph-analytics-producer:latest
docker pull ghcr.io/konstantinlofichenko/ais-graph-analytics-neo4j:latest
```

Third-party infrastructure images such as Kafka, ClickHouse, Metabase, and ksqlDB are referenced from their upstream registries and are not republished as project packages.

## Repository layout

```text
airflow/                 Airflow image, DAGs, and orchestration helpers
airbyte/                 Optional source/destination connector configuration
clickhouse/init/         Fresh-volume initialization SQL
clickhouse/migrations/   Additive schema/data migrations
dbt/                     Sources, seeds, staging models, marts, macros, tests
metabase/                Dashboard/card export/import assets
neo4j/                   Custom image, Cypher, constraints, GDS, connector config
pipelines/               Python processing pipelines
producers/ais/           Live BarentsWatch AIS producer
scripts/                 Bootstrap, CI, status, and operational helpers
.github/workflows/       CI and release automation
docs/                    Architecture and project documentation
```

## Important data semantics

- `raw.ais_positions` uses event time and `ReplacingMergeTree(ingested_at)`; logical validation should use `FINAL` when replacement convergence matters.
- Daily vessel stationary percentage is observation-based, not elapsed-time based.
- Navigation status is treated as noisy supporting data, not an unquestioned truth source.
- `port_visits.activity_date` and graph snapshot semantics use the pipeline UTC window start.
- Port visits and graph snapshots use logical tombstones for stale rows instead of destructive physical deletes.
- Neo4j represents current graph state; ClickHouse retains historical graph snapshots.

## Known limitation: visits across daily boundaries

Port-visit detection currently works on daily UTC windows. A physical stay that spans midnight can be represented as multiple daily visit fragments because detector state is not persisted across processing windows. That can slightly overcount visits and alter derived port-to-port graph metrics.

For a production implementation, use persistent detector state or detect physical visits over a continuous range before projecting them into daily analytics.

## Documentation

Start with:

- [Detailed project documentation](docs/README.md)
- `airflow/README.md` - orchestration and Airflow usage
- `dbt/README.md` - dbt profiles, models, and loading procedures
- `pipelines/port_visits/README.md` - port-visit logic
- `pipelines/graph_metrics/README.md` - graph export and metrics
- `neo4j/gds/README.md` - manual graph analytics reference

## Project status

The current platform includes working live/historical ingestion, ClickHouse/dbt analytics, Neo4j graph processing, AI enrichment, Metabase dashboards, automated CI, semantic releases, and GHCR publishing.

Planned learning extensions such as Flink streaming and an Iceberg/MinIO/Trino lakehouse are intentionally **not** part of the current runtime yet.
