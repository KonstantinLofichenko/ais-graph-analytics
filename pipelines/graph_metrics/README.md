# Port graph metrics export

Export the current Neo4j Port network's written `pageRank` and `communityId`
properties to `analytics.port_graph_metrics` in ClickHouse. After connection
publishing, trigger the manual Airflow DAG `ais_gds_metrics` to run GDS, write
metrics, create Community nodes and memberships, and validate existing visit lineage. Then run `ais_graph_metrics_export`
to export the validated Neo4j metrics to ClickHouse.
The [manual Cypher scripts](../../neo4j/gds/README.md) remain available for inspection.
Metabase configuration is separate.

The analytical flow is:

```text
AIS positions -> port visits -> CONNECTED_TO graph -> Airflow ais_gds_metrics
  -> Neo4j GDS -> pageRank/communityId write-back -> Community/MEMBER_OF
  -> Airflow ais_graph_metrics_export
  -> analytics.port_graph_metrics -> analytics.port_graph_metrics_enriched -> Metabase
```

## Setup and execution

Complete the [fresh-clone quickstart](../../README.md#fresh-clone-quick-start).
Its bootstrap applies [003_port_graph_metrics.sql](../../clickhouse/migrations/003_port_graph_metrics.sql),
[006_graph_community_labels.sql](../../clickhouse/migrations/006_graph_community_labels.sql),
and [008_port_graph_metrics_tombstones.sql](../../clickhouse/migrations/008_port_graph_metrics_tombstones.sql).
The daily master sequences GDS and export; the standalone commands below are for
operating on an already published snapshot after setup.

The exporter uses the existing port-visit Python dependencies and root `.env`
configuration: `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`,
`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE`. No new dependencies
are needed. To export already validated metrics without rerunning GDS,
use the [optional host Python environment](../port_visits/README.md#run-from-the-repository-root)
and run from the repository root:

```sh
.venv/bin/python pipelines/graph_metrics/export.py
# Equivalent module invocation:
.venv/bin/python -m pipelines.graph_metrics.export
```

Airflow reuses the existing Compose database environment, Neo4j user, and database
for every GDS stage. `ais_gds_metrics` has `schedule=None`, no catchup, and one active
run. With Airflow already running, trigger GDS independently with:

```sh
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
  -> build_communities -> validate written metrics/memberships -> always clean up projections -> complete_gds_metrics
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

For manual execution, complete GDS write-back and community building for the current
graph before using the standalone exporter. Visit metadata identifies the input snapshot; it does
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
relationships are excluded. A durable `ConnectionSnapshot` owned by
`port-connections-v1` identifies the run/window, edge count, publication generation,
and publication time. Relationship metadata and count must match that publication.
GDS and export recheck it to detect graph changes. Missing or inconsistent metadata
fails; a valid published empty snapshot skips GDS calculation and exports zero rows.

All active Ports must have a finite, nonnegative `pageRank`, a `communityId`
representable as `UInt64`, and `visitRunId` equal to the current managed run
ID. Each canonical Neo4j `portId` must exist as `port_id` in `analytics.ports FINAL`.
Missing or stale visit lineage, invalid metrics/run metadata, and unknown port IDs fail before any metrics are inserted.
The standalone CLI and `ais_graph_metrics_export` enforce the same visit-lineage
checks. Keep the graph stable through validation and export.

## Current Community layer

After GDS write-back, `build_communities` atomically replaces all `MEMBER_OF`
relationships and `Community` nodes. It leaves Ports, `VISITED`, and `CONNECTED_TO`
untouched. Only active endpoints of the managed connection snapshot participate.
The empty-snapshot path removes the previous layer without creating communities.
The builder locks the existing `ConnectionSnapshot` publication while validating
and replacing the layer; its `generation` value is unchanged. Failed transactions
roll back the replacement. Other graph writers must still follow the sequencing
requirements above.

```text
(:Port {portId, pageRank, communityId, visitRunId})
  -[:MEMBER_OF]->
(:Community {community_id, run_id, community_name, community_label, port_count})
```

`community_id` alone is unique; `run_id` is provenance, not a key. Each eligible
Port must have exactly one current membership. Port `communityId` remains the
GDS property; the Community node and ClickHouse column use `community_id`.
Names rank members by descending PageRank, then ascending `portId` for ties.
`community_name` is the first port name; `community_label` joins up to three names
with ` / `. Names are trimmed; missing/blank names fall back to `portId`.
Validation checks membership, run ID, counts, names, and labels before export.
Optional vessel/visit aggregates are not added to Community nodes: distinct
vessels across ports require a separate aggregation. Existing per-port analytics
remain available in the enriched view.

For existing installations, apply the additive migration before exporting:

```sh
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/006_graph_community_labels.sql
```

The builder ensures the Community uniqueness constraint exists; it is also in
`neo4j/cypher/01_constraints.cypher` for bootstrap. After manual GDS write-back,
run the same builder used by Airflow, then export:

```sh
.venv/bin/python -m pipelines.graph_metrics.communities
.venv/bin/python -m pipelines.graph_metrics.export
```

Community labels are snapshot facts and are stored in `port_graph_metrics`,
then passed through `port_graph_metrics_enriched`. Old historical rows retain
NULL names/labels; no historical rewrite or backfill is performed. Re-exporting
the current run appends the usual replacement versions under `(run_id, port_id)`.
Numeric Louvain IDs can be reused by later runs; Neo4j stores no community history.
Rebuild/redeploy Airflow after code changes. Refresh the existing enriched view
with `dbt run --project-dir dbt --select port_graph_metrics_enriched` after applying
the migration (its existing country dimension is required).

```cypher
MATCH (c:Community)
RETURN c.community_id, c.community_name, c.community_label, c.port_count, c.run_id
ORDER BY c.port_count DESC;

MATCH (p:Port)-[:MEMBER_OF]->(c:Community)
RETURN c.community_name, count(p) AS ports ORDER BY ports DESC;

MATCH (p:Port)-[:MEMBER_OF]->(c:Community)
WITH p, count(c) AS memberships WHERE memberships > 1
RETURN p.portId, memberships;
```

## Storage and retries

The table uses `ReplacingMergeTree(exported_at)`, partitions by
`toYYYYMM(snapshot_date)`, and orders by `(run_id, port_id)`. Exported rows include
the connection run's window, `page_rank`, `community_id`, `community_name`, and
`community_label`. `snapshot_date` is
the UTC date of `window_start`, rather than the export date, so a retry in a later
month uses the same partition and logical keys.

Older exports used the window-end date. With all graph writers idle, inspect then
apply the controlled correction (no GDS/DAG rerun):

```sh
python -m pipelines.graph_metrics.backfill_snapshot_dates
python -m pipelines.graph_metrics.backfill_snapshot_dates --apply
```

This publishes corrected logical versions, retaining metrics, labels, deletion
flags, and keys. It changes `exported_at` as the version marker. Reruns are no-ops
once dates are correct. The utility refuses changes across monthly partitions;
such installations need a separately reviewed partition migration. The local
September 2026 history stays within the same month. The exporter validates the
current `FINAL` metadata so superseded window-end versions do not block retries.
Downstream `activity_date` remains an alias of the physical `snapshot_date`.


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

Retries append versions. Read with `FINAL WHERE is_deleted = 0` for the active
logical snapshot. Before each export, including a verified empty graph, the
exporter compares existing active port IDs for this run with the new set. Current
ports get `is_deleted = 0`; disappeared ports get copies of their previous rows
with `is_deleted = 1`. Both sets share one timestamp newer than every previous
version for that run and are sent in one INSERT request. The version watermark
intentionally reads physical history, including tombstones, without `FINAL`.

After writing, the exporter checks the exact active port-ID set (and count),
raising an error on any mismatch. An empty rerun tombstones all previously active
ports; repeating the empty export keeps zero active rows. A later nonempty rerun
can reactivate ports. Other run IDs and their historical labels are untouched.
Stored window/partition metadata must match before any write. Neo4j's publication
is rechecked after the ClickHouse reads, immediately before the INSERT.

Run one graph publisher/exporter at a time through the master DAG. There is no
cross-database transaction: a failed export may leave inserted versions, and its
Airflow task must succeed on retry before downstream work proceeds.

For existing installations, apply migration 008 before deploying the new exporter
and rebuilding the enriched dbt view. It adds `is_deleted UInt8 DEFAULT 0` without
rewriting existing metrics. The table engine, partition key, and sorting key stay
unchanged:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/008_port_graph_metrics_tombstones.sql
dbt run --project-dir dbt --select port_graph_metrics_enriched port_graph_communities
```

The opt-in integration fixture uses a disposable ClickHouse database and renders
the actual enriched/community dbt view SQL; it never changes a real graph snapshot:

```sh
python -m pipelines.graph_metrics.tests.integration_tombstones
```

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
Ports are read with `FINAL`; country descriptions come from the current dbt-built
`countries` table. These are current descriptions, not historical snapshot attributes. Metric facts are also read
with `FINAL` through the subquery aliased as `g`. The enriched Metabase view exposes
one logically deduplicated row per `(run_id, port_id)`, using the latest
`exported_at`, filtering `is_deleted = 0` so neither physical retry versions nor
tombstoned ports reach the enriched or communities views.

The view is now built by the
[dbt model](../../dbt/models/marts/port_graph_metrics_enriched.sql), not a manual SQL
migration. Follow [dbt setup](../../dbt/README.md), including the optional
`raw.countries` prerequisite, then run from the repository root:

```sh
dbt build --project-dir dbt --select +countries +port_graph_metrics_enriched
```

Skip this country-dependent view on a fresh installation without the external
Airbyte country source. The core HAIS/canonical AIS workflow does not require it.
The view supports geographic maps using `latitude`/`longitude`, rankings by
`page_rank`, Louvain groups by `community_id`, and snapshot/run filters. No Metabase
configuration is added by dbt.

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
SELECT run_id, snapshot_date, port_id, page_rank, community_id, community_name, community_label,
       window_start, window_end, exported_at
FROM analytics.port_graph_metrics FINAL
WHERE is_deleted = 0
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
