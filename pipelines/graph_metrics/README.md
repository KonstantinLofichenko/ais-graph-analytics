# Port graph metrics export

Export the current Neo4j Port network's written `pageRank` and `communityId`
properties to `analytics.port_graph_metrics` in ClickHouse. After connection
publishing, trigger the manual Airflow DAG `ais_gds_metrics` to run GDS, write
metrics, and validate existing visit lineage. Then run `ais_graph_metrics_export`
to export the validated Neo4j metrics to ClickHouse.
The [manual Cypher scripts](../../neo4j/gds/README.md) remain available for inspection.
Metabase configuration is separate.

The analytical flow is:

```text
AIS positions -> port visits -> CONNECTED_TO graph -> Airflow ais_gds_metrics
  -> Neo4j GDS -> pageRank/communityId write-back -> Airflow ais_graph_metrics_export
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
are needed. To export already validated metrics without rerunning GDS,
run locally from the repository root:

```sh
.venv/bin/python pipelines/graph_metrics/export.py
# Equivalent module invocation:
.venv/bin/python -m pipelines.graph_metrics.export
```

Airflow reuses the existing Compose database environment, Neo4j user, and database
for every GDS stage. `ais_gds_metrics` has `schedule=None`, no catchup, and one active
run. Rebuild Airflow because its existing `COPY` instructions include the changed
pipeline code and new DAG; no Dockerfile change is needed:

```sh
docker compose --profile batch up -d --build airflow
docker compose exec airflow airflow dags unpause ais_gds_metrics
docker compose exec airflow airflow dags trigger ais_gds_metrics
```

The existing `ais_graph_metrics_export` DAG still runs only `export_port_metrics`
and requires complete metrics and matching visit lineage for the current managed run:

```sh
docker compose exec airflow airflow dags unpause ais_graph_metrics_export
docker compose exec airflow airflow dags trigger ais_graph_metrics_export
```

## Automated GDS stages

The manually triggered DAG runs these stages sequentially:

```text
validate snapshot -> recreate directed/undirected projections -> stream PageRank
  -> stream Louvain and collect stats -> clear/write metrics
  -> validate written metrics -> always clean up projections -> complete_gds_metrics
```

Algorithm settings match the manual scripts: directed PageRank weighted by
`movementCount`, damping factor 0.85, at most 20 iterations; undirected Louvain
with the same weight; concurrency 1. The write calls execute the algorithms again,
as in the manual workflow. Stream, stats, and write runs can assign different
numeric community labels; those labels are not stable identifiers.

Before write-back, both the DAG and manual `05_write_back.cypher` clear only
`pageRank` and `communityId` from all Ports. They then write the current metrics
on active projected Ports. Existing `visitRunId`, `visitWindowStart`, and
`visitWindowEnd` remain unchanged; GDS does not write any lineage properties.
The lineage rule is `Port.visitRunId == CONNECTED_TO.runId == captured DAG run_id`.
Missing or mismatched visit lineage and incomplete metrics block export.

For manual execution, complete GDS write-back for the current graph before using
the standalone exporter. Visit metadata identifies the input snapshot; it does
not independently prove when the metric values were calculated.

The DAG checks the original snapshot's run/window metadata, counts, and an
ephemeral graph-state checksum between stages. The checksum detects relationship
or weight changes even when the run ID is reused; it is not another run ID.
The standalone export DAG reads and validates the current graph metadata and visit
lineage. It does not receive the GDS DAG's captured snapshot; keep the graph stable
between GDS completion and export.

Both workflows use the fixed GDS catalog names `ais-port-connections-directed` and
`ais-port-connections-undirected`. Keep manual GDS, connection publishing, and other
graph writers from overlapping this DAG. `max_active_runs=1` serializes only this
DAG; it is not a lock shared with other workflows.

Cleanup uses `ALL_DONE` without upstream result arguments, so it runs even after
validation or algorithm failures. A final success task preserves upstream failures;
successful cleanup cannot turn a failed workflow into a successful DAG run. If
Neo4j restarts or a projection disappears, rerun the whole DAG to recreate both
projections and their metrics.

## Snapshot validation

Only Ports at either end of a Port-to-Port `CONNECTED_TO` relationship with
`managedBy = 'port-connections-v1'` are exported. Isolated Ports and unrelated
relationships are excluded. The selected relationships must identify exactly one
`runId` and one valid, timezone-aware `windowStart`/`windowEnd` interval.

All active Ports must have a finite, nonnegative `pageRank`, a `communityId`
representable as `UInt64`, and `visitRunId` equal to the current managed run
ID. Each canonical Neo4j `portId` must exist as `port_id` in `analytics.ports FINAL`.
Missing or stale visit lineage, invalid metrics/run metadata, and unknown port IDs fail before any metrics are inserted.
The standalone CLI and `ais_graph_metrics_export` enforce the same visit-lineage
checks. Keep the graph stable through validation and export.

## Storage and retries

The table uses `ReplacingMergeTree(exported_at)`, partitions by
`toYYYYMM(snapshot_date)`, and orders by `(run_id, port_id)`. Exported rows include
the connection run's window, `page_rank`, and `community_id`. `snapshot_date` is
the UTC date of `window_end`, rather than the export date, so a retry in a later
month uses the same partition and logical keys.

The exporter preserves the upstream `runId` unchanged. New visit runs identify
their UTC `window_start` at whole-second precision with a trailing `Z`: for example,
`2026-09-15T11:00:00+03:00` becomes `2026-09-15T08:00:00Z`. The same ID passes through
visits, completed-run metadata, `VISITED`, `CONNECTED_TO`, and metric exports; the
exporter does not derive another identity. Windows may span 24, 36, 48 hours, or
other valid durations, with one immutable stored window per ID. Fractional seconds
are omitted only from the ID. Before publication writes, the visit pipeline checks
`analytics.port_visit_runs FINAL`: an existing ID requires both requested bounds
to match the stored UTC instants at full precision. Exact-window retries are
allowed; changing the end or the start's fractional seconds under that ID fails.

Retries can append physical versions. Read with `FINAL` to obtain one logical row
per run/port with the latest `exported_at`. A retry of a run assumes the same active
Port set and observation window; this exporter does not delete older rows for
Ports removed from a reused run. Different run IDs retain separate snapshots.

Existing hash IDs remain valid and readable. The two existing ClickHouse daily
graph snapshots are untouched by the identity refactor. Recomputing their windows
with timestamp IDs would create different logical keys; `FINAL` will not merge
them with the old hash IDs. Whether to keep or recompute those snapshots remains
a separate decision.

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

### First real export (historical hash ID)

The supplied manual validation predates timestamp run IDs and is retained unchanged:

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
