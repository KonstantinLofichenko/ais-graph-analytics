# Port visits: Bergen pilot

This batch reads existing ClickHouse AIS history, infers separate visits, stores
versioned visit snapshots in ClickHouse, and refreshes vessel–port counts in Neo4j.
It does not change the producer, Kafka consumers, or current Vessel properties.
GDS similarity is a subsequent milestone. Airflow orchestration is the next
milestone; the batch can currently be run directly with `run.py`.

## Run from the repository root

```sh
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
`bolt://localhost:7687`. The batch currently runs locally, on macOS/Linux.

Use fixed UTC boundaries for reproducible previews/retries (start inclusive, end exclusive):

```sh
.venv/bin/python pipelines/port_visits/run.py \
  --start 2026-08-13T12:00:00Z --end 2026-09-12T12:00:00Z \
  --report /tmp/port-visits-preview.json
```

Add `--apply` after inspecting the report. Without fixed boundaries, every run uses
an updated window. A 30-day window does not mean we have 30 days of collected data.
The job refuses to silently truncate beyond `--max-rows` (default 500,000). This
small pilot sorts/loads a bounded input into memory and is not yet a full-volume
production AIS processor. Narrow the window when needed; scaling requires moving
candidate selection/sessionization into ClickHouse or a streaming batch reader.

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
- `port_visits`: individual visits per run, including boundary/evidence fields.
- `port_visit_runs`: successfully published run metadata, thresholds, dataset hash,
  observation window and counts.
- `current_port_visits`: only the most recently completed run, with `FINAL` applied.

An older run's visits remain available for inspection. Do not sum visits across
runs: overlapping snapshots can represent the same underlying stay. The run ID
hashes the window, source rows, reference, and detector version/settings. Repeating
the same inputs uses the same IDs. ReplacingMergeTree plus `FINAL` gives one logical
row per key even before background merges. Increment `OWNER`'s version when changing
detection semantics. Empty runs are recorded and clear the current managed summary.

Neo4j:

```text
(Vessel {mmsi})-[:VISITED {
  visitCount, managedBy, runId, windowStart, windowEnd
}]->(Port {portId, name, country, latitude, longitude, radiusM})
```

Only `VISITED` edges owned by `port-visits-v1` are replaced, in one transaction.
The batch never sets existing Vessel position/name properties. Historical vessels
missing from the live graph may be created with MMSI only. Ports retain reference
metadata and the last applied visit run/window, including for empty snapshots.
Old Port nodes/reference rows remain if a later reference omits them, but their
managed visit edges are removed. Port fields use `portId`; vessel IDs remain integer
`mmsi`, matching the Kafka sink. There are no `PortVisit` nodes.

For future port similarity, GDS must project Port-to-Vessel neighborhoods (reverse
the stored VISITED direction); that algorithm is not run in this milestone.

## Failure and retry behavior

Run one writer at a time from this checkout. A local file lock prevents overlapping
local runs; it is not a distributed lock. When Airflow orchestration is enabled, it
must enforce one active run and preserve the same single-writer restriction.

Writes occur in this order: ClickHouse reference/visits → atomic Neo4j refresh →
ClickHouse completed-run record. There is no cross-database transaction. If a failure
occurs after the graph commit, its run ID may temporarily differ from ClickHouse's
latest completed run. Rerun with the same explicit window and unchanged inputs to
reconcile. Partial ClickHouse snapshots without a completed run are not exposed by
the current view. Check run IDs before downstream similarity/export work.

A manually applied older window intentionally becomes the current graph snapshot;
there is no implicit chronological scheduling yet.

## Validate

```sh
.venv/bin/python -m unittest discover -s pipelines/port_visits/tests -v
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
When Airflow orchestration is introduced, its initialization step should preserve
this migration behavior. For legacy rows, creation is the
earliest retained ClickHouse update timestamp. For existing Neo4j nodes without a
creation timestamp, creation is initialized at migration/load time. These backfills
cannot recover original creation times that were never recorded.
