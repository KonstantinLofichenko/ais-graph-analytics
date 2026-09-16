# Port graph metrics export

Export the current Neo4j Port network's written `pageRank` and `communityId`
properties to `analytics.port_graph_metrics` in ClickHouse. Finish connection
publishing and the [manual GDS workflow](../../neo4j/gds/README.md), including
`05_write_back.cypher`, before running the exporter. GDS execution and Metabase
configuration are not automated by this pipeline.

The analytical flow is:

```text
AIS positions -> port visits -> CONNECTED_TO graph -> Neo4j GDS
  -> pageRank/communityId write-back -> Airflow ais_graph_metrics_export
  -> analytics.port_graph_metrics -> analytics.port_graph_metrics_enriched -> Metabase
```

## Setup and execution

Apply the [manual migration](../../clickhouse/migrations/003_port_graph_metrics.sql)
from the repository root. It does not run automatically at Docker startup:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/003_port_graph_metrics.sql
```

The exporter uses the existing port-visit Python dependencies and root `.env`
configuration: `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`,
`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE`. No new dependencies
are needed. Run locally from the repository root:

```sh
.venv/bin/python pipelines/graph_metrics/export.py
# Equivalent module invocation:
.venv/bin/python -m pipelines.graph_metrics.export
```

Airflow reuses the existing Compose database environment. Its separate manual DAG,
`ais_graph_metrics_export`, contains one task, `export_port_metrics`, with
`schedule=None`, no catchup, and one active run/task. Rebuild the image because it
copies the new module and DAG:

```sh
docker compose --profile batch up -d --build airflow
docker compose exec airflow airflow dags unpause ais_graph_metrics_export
docker compose exec airflow airflow dags trigger ais_graph_metrics_export
```

## Snapshot validation

Only Ports at either end of a Port-to-Port `CONNECTED_TO` relationship with
`managedBy = 'port-connections-v1'` are exported. Isolated Ports and unrelated
relationships are excluded. The selected relationships must identify exactly one
`runId` and one valid, timezone-aware `windowStart`/`windowEnd` interval.

All active Ports must have a finite, nonnegative `pageRank` and a `communityId`
representable as `UInt64`. Each canonical Neo4j `portId` must exist as `port_id` in
`analytics.ports FINAL`. Missing metrics, invalid run/window metadata, or unknown
port IDs fail before any metrics are inserted; the exporter does not invent values.

Keep connection publishing and GDS write-back stable throughout export. The current
node properties do not store the run that produced them. The exporter validates
relationship metadata and complete active-port coverage, but cannot prove that
scores came from the current run. Complete GDS write-back for the current snapshot
before exporting it.

## Storage and retries

The table uses `ReplacingMergeTree(exported_at)`, partitions by
`toYYYYMM(snapshot_date)`, and orders by `(run_id, port_id)`. Exported rows include
the connection run's window, `page_rank`, and `community_id`. `snapshot_date` is
the UTC date of `window_end`, rather than the export date, so a retry in a later
month uses the same partition and logical keys.

Retries can append physical versions. Read with `FINAL` to obtain one logical row
per run/port with the latest `exported_at`. A retry of a run assumes the same active
Port set and observation window; this exporter does not delete older rows for
Ports removed from a reused run. Different run IDs retain separate snapshots.

## Enriched analytical view

`analytics.port_graph_metrics_enriched` joins metric facts to the port and country
reference dimensions. Names, coordinates, and country descriptions stay in those
dimensions instead of being duplicated in `analytics.port_graph_metrics`. The view
uses left joins on `g.port_id = p.port_id` and `p.country = c.country_code`.
Both dimensions are read with `FINAL`, providing their latest descriptions rather
than historical attributes for each metric snapshot. Metric facts are also read
with `FINAL` through the subquery aliased as `g`. The enriched Metabase view exposes
one logically deduplicated row per `(run_id, port_id)`, using the latest
`exported_at`, without exposing physical retry versions before background merges.

After initializing `analytics.ports` and applying
[002_countries.sql](../../clickhouse/migrations/002_countries.sql) and
[003_port_graph_metrics.sql](../../clickhouse/migrations/003_port_graph_metrics.sql),
apply the [view migration](../../clickhouse/migrations/004_port_graph_metrics_enriched.sql)
manually from the repository root:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/004_port_graph_metrics_enriched.sql
```

The view is a convenient Metabase source for a geographic map using `latitude` and
`longitude`, rankings by `page_rank`, Louvain groups by `community_id`, and filters
on `snapshot_date` or `run_id`. This migration adds no Metabase configuration or
dbt models and requires no exporter changes.

### First real export

The supplied manual validation recorded:

| Field | Value |
| --- | --- |
| `run_id` | `1715a5292cb4aa871163a967acba9005d2555557bebb592e6f7767eb925f1106` |
| `snapshot_date` | `2026-09-15` |
| Active ports | 40 |
| Louvain communities | 6 |
| Minimum PageRank | `0.15000000000000002` |
| Maximum PageRank | `2.2141319640894372` |

## Validate

```sh
.venv/bin/python -m unittest discover -s pipelines/graph_metrics/tests -v
```

```sql
SELECT run_id, snapshot_date, port_id, page_rank, community_id,
       window_start, window_end, exported_at
FROM analytics.port_graph_metrics FINAL
ORDER BY snapshot_date DESC, run_id, page_rank DESC, port_id;
```

Inspect the enriched view:

```sql
SELECT
    port_name,
    country_name,
    latitude,
    longitude,
    page_rank,
    community_id
FROM analytics.port_graph_metrics_enriched
ORDER BY page_rank DESC
LIMIT 20;
```
