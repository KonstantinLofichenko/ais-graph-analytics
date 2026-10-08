# Port visits: Bergen pilot

This batch reads existing ClickHouse AIS history, infers separate visits, stores
versioned visit snapshots in ClickHouse, and refreshes vessel–port counts in Neo4j.
The [Airflow DAG](../../airflow/README.md) then publishes port-to-port movements in
a separate `publish_port_connections` task. The visit batch can also be run directly
with `run.py`; that command publishes `VISITED` only. It does not change the producer,
Kafka consumers, or current Vessel properties. GDS similarity is a subsequent
milestone.

## Run from the repository root

The [fresh-clone quickstart](../../README.md#fresh-clone-quick-start) uses Docker
Airflow for normal daily processing. The commands here are an optional host CLI
for previews, diagnostics, and non-daily timestamp windows. Set up its separate
Python environment first:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r pipelines/port_visits/requirements.txt

# Preview only: defaults to the preceding 30 days, writes no database objects.
.venv/bin/python pipelines/port_visits/run.py

# First application: also create additive ClickHouse tables/views.
.venv/bin/python pipelines/port_visits/run.py --apply --init

# Subsequent refreshes:
.venv/bin/python pipelines/port_visits/run.py --apply
```

Root `.env` supplies the existing ClickHouse and Neo4j passwords; no secrets are
passed on command lines. Optional connection overrides are in `.env.example`.
`CLICKHOUSE_URL` defaults to `http://localhost:8123`; `NEO4J_URI` defaults to
`bolt://localhost:7687`. These host CLI commands run on macOS/Linux; the normal
Airflow workflow runs the same batch inside Docker.

Use fixed UTC boundaries for reproducible previews/retries (start inclusive, end exclusive):

```sh
.venv/bin/python pipelines/port_visits/run.py \
  --start 2026-08-13T12:00:00Z --end 2026-09-12T12:00:00Z \
  --report /tmp/port-visits-preview.json
```

Add `--apply` after inspecting the report. Without fixed boundaries, every run uses
an updated window. A 30-day window does not mean we have 30 days of collected data.
Before loading any rows, the job counts them with `count() ... FINAL` for the exact
window and logs `actual_rows` (the automatically measured input size). `--max-rows`
(default 500,000) is a hard safety ceiling, not the requested batch size: the job
fails immediately, before fetching the full dataset, if `actual_rows` exceeds it.
This small pilot sorts/loads a bounded input into memory and is not yet a full-volume
production AIS processor. Narrow the window when needed; scaling requires moving
candidate selection/sessionization into ClickHouse or a streaming batch reader.

### Run identity

For new runs, `run_id = canonical UTC window_start`. `canonical_run_id(window_start)`
normalizes the start to UTC and formats it to whole seconds with a trailing `Z`. For example,
`2026-09-15T11:00:00+03:00` becomes `2026-09-15T08:00:00Z`. Fractional seconds are
omitted only from the ID; the stored window bounds and detection retain their
timestamp precision. The ID does not depend on `window_end`, pipeline version,
dataset, configuration, source rows, wall-clock time, or Airflow run/task IDs.

Windows may span 24, 36, 48 hours, or any other valid duration. Each run ID has one
immutable stored window. Before publication writes, the batch reads
`analytics.port_visit_runs FINAL` for that ID. If it already exists, both requested
bounds must equal the stored instants after UTC normalization, including fractional
seconds. Retrying the same exact window is allowed; changing the end or the start's
fractional seconds while retaining the same ID fails before publication writes.

The same string is passed through `analytics.port_visits`,
`analytics.port_visit_runs`, Neo4j `VISITED`/`CONNECTED_TO`, and
`analytics.port_graph_metrics`. Existing hash IDs remain readable and are not rewritten.

## Reference and detection rules

The seven port points and provenance are in `data/ports/`. Default rules:

- Assign each position to the nearest port within its configured 1,500 m radius.
  Ties use port ID, so overlaps never double-count an observation.
- Require known speed of at most 3 knots and at least two qualifying observations
  spanning 20 minutes. Unknown/unavailable speed or faster movement breaks a stay.
- No adjacent observations within a stay may be more than 15 minutes apart.
- Leaving the circle, moving to another qualifying port, a data gap, or the window
  ending closes a candidate. Short candidates are discarded.
- Exact duplicate observations do not increase the count. Conflicting observations
  at the same vessel/timestamp break the stay. Invalid coordinates are ignored.
- Observation timestamps are normalized to UTC microseconds for Python 3.9 support;
  the original nanosecond history in `raw.ais_positions` remains unchanged.

Override thresholds with `--min-stay-minutes`, `--max-gap-minutes`, and
`--max-speed-knots`; replace the JSON reference with `--ports`.

`arrival_at` is the first observed qualifying position, not a reported arrival.
`arrival_censored=1` means we lack a recent preceding position outside that port.
`last_observed_at` is the last qualifying position. `departure_at` is only set
when a subsequent timely observation is geographically outside the circle; it is
an estimate/upper bound, not an exact departure. For gaps, movement within the
circle, or window ends, departure remains NULL and `end_reason` records why.
`observed_stay_seconds` measures first-to-last qualifying observations, excluding
the departure interval. A visit crossing a window boundary can therefore be missed
if too little of its stay is observed inside the window.

Repeated visits are separate ClickHouse rows. Data gaps or moving within a port
can also split a true stay into multiple inferred visits; `visitCount` counts our
inferred segments, not certified port calls. Port radii and results require domain
validation before operational use. No inference of visits during unobserved gaps.

## Stored model

ClickHouse (`analytics` database):

- `ports`: latest reference record per port (`FINAL` when reading).
- `port_visits`: versioned visits per run, including boundary/evidence fields.
  Read active visits with `FINAL WHERE is_deleted = 0`; tombstones retain obsolete keys.
- `port_visit_runs`: successfully published run metadata, thresholds, dataset hash,
  observation window and counts.
- `current_port_visits`: only active visits from the most recently completed run,
  with `FINAL` and `is_deleted = 0` applied.

An older run's visits remain available for inspection. Do not sum visits across
runs: overlapping snapshots can represent the same underlying stay. New run IDs
identify the UTC window start at second precision; ownership/version labels and
dataset hashes remain separate metadata. ReplacingMergeTree plus `FINAL` gives
one logical row per key even before background merges. It does not merge an old
hash ID with a new timestamp ID for the same window. Empty runs are recorded and
clear the current managed summary.

Neo4j:

```text
(Vessel {mmsi})-[:VISITED {
  visitCount, managedBy, runId, windowStart, windowEnd
}]->(Port {portId, name, country, latitude, longitude, radiusM})

(Port {portId})-[:CONNECTED_TO {
  movementCount, vesselCount, firstSeen, lastSeen,
  managedBy, runId, windowStart, windowEnd
}]->(Port {portId})
```

`VISITED` aggregates the number of detected visits from a Vessel to a Port.
Only `VISITED` edges owned by `port-visits-v1` are replaced, in one transaction.
The batch never sets existing Vessel position/name properties. Historical vessels
missing from the live graph may be created with MMSI only. Ports retain reference
metadata and the last applied visit run/window, including for empty snapshots.
Old Port nodes/reference rows remain if a later reference omits them, but their
managed visit edges are removed. Port fields use `portId`; vessel IDs remain integer
`mmsi`, matching the Kafka sink. There are no `PortVisit` nodes.

### Consecutive port connections

`pipelines/port_connections/run.py` reads `analytics.port_visits FINAL WHERE is_deleted = 0` for the exact
completed visit `run_id` passed by the upstream task and validates its completed-run
metadata. It does not combine historical runs or select whichever run is latest.
If the completed run's `visit_count` differs from its active `FINAL` visit rows, publishing
fails before touching Neo4j.
Each vessel's visits are ordered by `arrival_at`, then `visit_id` and `port_id` to
resolve ties deterministically. Only adjacent visits form movements: A → B → C
produces A → B and B → C. Adjacent visits to the same port are ignored.

For each directed route, `movementCount` counts movements, `vesselCount` counts
distinct MMSIs, `firstSeen` is the earliest source visit's `arrival_at`, and
`lastSeen` is the latest destination visit's `arrival_at`. `CONNECTED_TO` means
“the next detected port visit was this port.” It does not prove that no unobserved
port was visited in between.

Publishing atomically replaces the current snapshot of Port-to-Port `CONNECTED_TO`
relationships owned by `port-connections-v1`, including those from earlier runs.
The publisher matches existing Ports by `portId` and fails before deleting anything
if a referenced Port is missing. It creates no Port nodes. Relationship `MERGE`
includes `managedBy`, preserving manually created relationships on the same route.
An empty valid run clears the owned connections. `VISITED` and relationships with
other ownership are unaffected. The connections reuse the visit run's `runId`,
`windowStart`, and `windowEnd`; no separate batch identity is generated.

For future port similarity, GDS must project Port-to-Vessel neighborhoods (reverse
the stored VISITED direction); that algorithm is not run in this milestone.

## Failure and retry behavior

Run one writer at a time from this checkout. A local file lock prevents overlapping
local visit runs; it is not a distributed lock. Airflow enforces one active DAG run
and one running task. Do not run host publishers concurrently with the DAG.

Writes occur in this order: ClickHouse reference → active visit versions and
obsolete-visit tombstones in one INSERT request → active `FINAL` count validation
→ atomic Neo4j refresh → ClickHouse completed-run record. Failed insertion,
validation, or graph publication raises an error and writes no new completion
record or successful Airflow handoff. The connection publisher still independently
checks completed `visit_count` against active visits before writing connections.

Recalculating the same start reuses the run ID. The publisher reads existing active
visits with `FINAL WHERE run_id = ... AND is_deleted = 0`, compares visit IDs, and
writes calculated visits with `is_deleted = 0`. Missing IDs get copies of their
previous logical rows with `is_deleted = 1`. All versions in that publish share
one timestamp, at least one microsecond newer than the run's previous maximum.
The version-watermark query intentionally reads physical versions (including
tombstones); an aggregate maximum does not need `FINAL`. Reappearing visits are
reactivated with a newer version. No UPDATE/DELETE mutations are used.

There is no cross-database transaction or distributed writer lock. A failure can
leave new ClickHouse versions or a graph refresh without a new completion record;
retry the same window to reconcile. For an already completed run ID, its older
completion record remains, so the current view is not an isolation boundary during
a failed rerun. Do not bypass the failed Airflow task or run concurrent publishers.

Existing installations must apply the additive
[007_port_visits_tombstones.sql](../../clickhouse/migrations/007_port_visits_tombstones.sql)
before deploying the updated publisher/readers. Existing rows default to active;
the migration also updates the bootstrap current-visits view. Bootstrap and
`--apply --init` support the new schema. Rebuild the dbt `current_port_visits` and
`port_graph_metrics_enriched` views so neither exposes tombstones:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/007_port_visits_tombstones.sql
# Use the configured dbt environment; the enriched view needs the countries model.
dbt run --project-dir dbt --select current_port_visits port_graph_metrics_enriched
```

Historical consistency check (including completed zero-visit runs):

```sql
SELECT r.run_id, r.visit_count, coalesce(v.active_visits, 0) AS active_visits
FROM analytics.port_visit_runs AS r FINAL
LEFT JOIN (
    SELECT run_id, count() AS active_visits
    FROM analytics.port_visits FINAL
    WHERE is_deleted = 0
    GROUP BY run_id
) AS v ON r.run_id = v.run_id
WHERE r.visit_count != coalesce(v.active_visits, 0)
ORDER BY r.run_id;
```

Airflow starts connection publishing only after the visit task succeeds, passing
only `run_id`, `window_start`, and `window_end` through XCom. The visit subprocess
writes this small completed result to a temporary `--result-json` file for its task
to read. The two tasks' graph writes are not atomic together: if connection
publishing fails, `VISITED` may already show the new run while `CONNECTED_TO` still
shows the previous snapshot. Retry `publish_port_connections` with the same metadata
to reconcile; its transaction preserves the previous connections if publishing
fails before commit.

A manually applied older window intentionally becomes the current graph snapshot;
there is no implicit chronological scheduling yet.

## Validate

Airflow DAG tests require its separate runtime; see [Airflow checks](../../airflow/README.md#checks).

```sh
.venv/bin/python -m unittest discover -s pipelines/port_visits/tests -v
.venv/bin/python -m unittest discover -s pipelines/port_connections/tests -v
# Opt-in DB integration: creates/drops its own temporary database;
# all synthetic Neo4j changes are rolled back.
.venv/bin/python pipelines/port_visits/tests/integration_check.py
```

```sql
SELECT count() FROM analytics.ports FINAL;
SELECT run_id, source_rows, visit_count, window_start, window_end
FROM analytics.port_visit_runs FINAL ORDER BY completed_at DESC;
SELECT * FROM analytics.current_port_visits ORDER BY mmsi, arrival_at;
```

```cypher
MATCH (p:Port) RETURN p.portId, p.name, p.visitRunId;
MATCH (v:Vessel)-[r:VISITED {managedBy:'port-visits-v1'}]->(p:Port)
RETURN v.mmsi, p.name, r.visitCount, r.windowStart, r.windowEnd;
MATCH (a:Port)-[r:CONNECTED_TO {managedBy:'port-connections-v1'}]->(b:Port)
RETURN a.portId, b.portId, r.movementCount, r.vesselCount,
       r.firstSeen, r.lastSeen, r.runId, r.windowStart, r.windowEnd;
```

Initial validation on 2026-09-12: 3,124 raw positions; 43 positions inside pilot
circles; 37 short candidates; zero qualifying visits. Seven ports were loaded into
both stores. This is a working empty result, not evidence of no real port calls.
Keep collecting AIS data or arrange a historical backfill before evaluating the
quality of detected real visits or running useful similarity analysis.

## Port timestamps

Neo4j ports use `createdAt` and `updatedAt`. ClickHouse uses `created_at` and
`updated_at`, following the existing snake_case convention. Creation is preserved
on reload; update time advances on every port load, even if attributes are unchanged.
Run with `--apply --init` once to add the ClickHouse creation column to older installs.
Airflow runs with `--apply` against initialized tables; run this migration before
using an older database with the DAG. For legacy rows, creation is the
earliest retained ClickHouse update timestamp. For existing Neo4j nodes without a
creation timestamp, creation is initialized at migration/load time. These backfills
cannot recover original creation times that were never recorded.

## Activity date upgrade

`activity_date` is the UTC processing window-start date, supplied explicitly by
the writer from the pipeline's resolved `start`. Airflow's `activity_date` becomes
that day's UTC start boundary; standalone/rolling runs use their resolved UTC
window start. Neither visit arrival time nor a parsed run ID supplies this date.
`current_port_visits` copies it from active visits without recalculating it.
Tombstones copy the obsolete row's original date.

With all port/graph writers idle, apply the additive column migration and inspect
the read-only backfill plan before publishing it:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/010_port_visits_activity_date.sql
python -m pipelines.port_visits.backfill_activity_dates
python -m pipelines.port_visits.backfill_activity_dates --apply
```

The physical column is `Nullable(Date)` so legacy unknown dates remain NULL until
backfilled, rather than receiving an invented default. The backfill joins logical
visit keys to `port_visit_runs FINAL` by `run_id` and uses the UTC `window_start`
date. Missing run metadata or conflicting non-null dates fail before publication.
Only missing logical rows (including tombstones) get newer `updated_at` versions;
visit payloads, deletion flags, completion metadata and uniqueness keys stay intact.
This avoids a full historical mutation. Obsolete physical versions can retain NULL
until normal merges; all logical `FINAL` rows must be non-null after the backfill.
The script is retry-safe and uses the local writer lock; keep external writers idle
too, because the lock is not distributed across containers.

For existing graph exports, also follow the
[snapshot-date correction](../graph_metrics/README.md#storage-and-retries), then:

```sh
dbt parse --project-dir dbt
dbt build --project-dir dbt --select current_port_visits port_graph_metrics_enriched port_graph_communities port_visit_activity_dates
dbt test --project-dir dbt --select current_port_visits port_graph_metrics_enriched port_graph_communities port_visit_activity_dates
```

The current-visits view still represents only the latest completed run; date
filtering it does not turn it into an all-history view. For historical visits,
query `analytics.port_visits FINAL WHERE is_deleted = 0` with `activity_date`.
