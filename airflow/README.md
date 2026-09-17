# AIS Airflow (local)

Dedicated `ais-airflow` container on the existing `ais-network`. It uses Airflow
3.3.1 standalone mode, with its own persistent `airflow-data` volume for metadata,
logs, and generated login credentials. The old `airflow-intro` installation is not
used. Standalone is for local development; separate services and PostgreSQL should
replace it for a production deployment.

## Start and stop

From the AIS repository, with ClickHouse and Neo4j already running:

```sh
docker compose --profile batch up -d --build airflow
docker compose stop airflow
```

Open http://localhost:8080. The port is bound to loopback only. Login username:
`admin`. Display the generated password locally (do not commit/share the output):

```sh
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Rebuild after changing the DAG or pipeline code, including the
`pipelines/port_connections` and `pipelines/graph_metrics` modules: the Docker image
uses `COPY`, so restarting alone does not load these changes. Use the
`up -d --build airflow` command above.
The image copies only explicitly allowed code and SQL files, never root `.env`.
Compose injects only the database
credentials required by this batch. Inside Docker the targets are
`http://ais-clickhouse:8123` and `bolt://ais-neo4j:7687`.

## Three tasks

DAG: `ais_port_visits` (manual trigger, `schedule=None`, no catchup).

```text
download_ports -> run_port_visit_pipeline -> publish_port_connections
```

1. Download the seven-port Bergen pilot from the dated UN OCHA WPI API, normalize
   and validate it. Empty, duplicate-ID, error or truncated responses fail the task.
2. Use the downloaded records to run the existing batch with `--apply`. Ports and
   visits are written to ClickHouse; Port nodes and managed VISITED summaries are
   refreshed in Neo4j. By default each run uses the trailing `PORT_VISITS_WINDOW_HOURS`
   window, ending at the Airflow run start time, fixed across task retries.
3. Validate the completed visit run metadata, read only that run's individual visits
   from `analytics.port_visits FINAL`, and publish aggregated Port-to-Port
   `CONNECTED_TO` relationships. The publisher matches existing Ports by `portId`;
   a missing referenced Port fails the task without replacing the previous snapshot.

`VISITED` represents Vessel-to-Port aggregated visit counts. `CONNECTED_TO` represents
consecutive detected visits by the same vessel, ordered by `arrival_at` with
deterministic `visit_id`, then `port_id` tie breaks. A → B → C produces A → B and
B → C; adjacent visits to the same port produce no connection. A connection means
“the next detected port visit was this port,” and does not prove that no unobserved
port was visited in between.

Each directed route stores `movementCount`, distinct `vesselCount`, `firstSeen`
(earliest source arrival), `lastSeen` (latest destination arrival), and the visit
run's `runId`, `windowStart`, and `windowEnd`. One Neo4j transaction replaces the
current Port-to-Port `CONNECTED_TO` snapshot owned by `port-connections-v1`, including
previous runs. An empty completed run clears those owned connections. Relationship
`MERGE` includes ownership, preserving manual relationships even on the same route.

### Manual window and row limit

Trigger the existing explicit window, using the configured `max_rows` safety ceiling:

```sh
docker compose exec airflow airflow dags trigger ais_port_visits \
  --conf '{"start": "2026-09-14T08:00:00+00:00", "end": "2026-09-15T08:00:00+00:00"}'
```

Optional override of the safety ceiling for an unusually wide window:

```sh
docker compose exec airflow airflow dags trigger ais_port_visits \
  --conf '{"start": "2026-09-14T08:00:00+00:00", "end": "2026-09-15T08:00:00+00:00", "max_rows": 6000000}'
```

- `start`/`end` must both be supplied (or neither) as ISO-8601 timestamps with
  timezone information, and `start` must be earlier than `end`.
- `max_rows` is optional; it overrides `PORT_VISITS_MAX_ROWS` as a safety guard.
- If `start`/`end` are omitted, the DAG falls back to the rolling
  `PORT_VISITS_WINDOW_HOURS` window ending at the run's start time.
- The resolved `start`, `end`, `max_rows`, and their source (`dag_run.conf` or
  `rolling-default`) are logged before the batch runs.
- Before loading any AIS rows, `run.py` counts them and logs `actual_rows`:
  the automatically measured input size for the window. `max_rows` is only a
  safety ceiling, not the requested batch size; the run fails immediately if
  `actual_rows` exceeds it, before fetching the full dataset.
- New `run_id` values are the UTC `start` formatted to whole seconds with a trailing
  `Z`: `2026-09-15T11:00:00+03:00` becomes `2026-09-15T08:00:00Z`. The ID is independent
  of `end`, pipeline version, dataset/configuration, source rows, wall-clock time,
  and Airflow run/task IDs. Fractional seconds are omitted only from the ID;
  the resolved window bounds retain their precision.
- Windows may span 24, 36, 48 hours, or other valid durations. Each ID has one
  immutable stored window. Before publication writes, the visit batch checks
  `analytics.port_visit_runs FINAL` for that ID and compares both bounds as UTC
  instants at full precision. The same exact window may be retried; a different
  end or start fraction under an existing ID fails before publication writes.

The reference is passed as a small XCom payload, not a path that might disappear
between tasks. The second task creates temporary JSON files for the reference and
the subprocess's `--result-json` completed result, then removes them afterward.
It returns only `run_id`, `window_start`, and `window_end` to the connection task
through XCom. Visit rows stay in ClickHouse; no visit dataset is placed in XCom.
This same ID is preserved in ClickHouse visits/completed-run metadata, Neo4j
`VISITED` and `CONNECTED_TO`, and graph metric exports. Existing hash IDs remain
readable unchanged; they are not converted during downstream tasks.
The checked-in `data/ports/bergen.json` is not overwritten. This still queries the
`202511` source snapshot; it is not a claim of current global port coverage.

Enable/unpause the DAG in the UI and click Trigger. Or:

```sh
docker compose exec airflow airflow dags unpause ais_port_visits
docker compose exec airflow airflow dags trigger ais_port_visits
```

There are at most one active run and one running task for this DAG. Failed tasks
retry twice after a one-minute delay. A failed download prevents
`run_port_visit_pipeline`.
Do not simultaneously run the batch from the host: the existing local file lock
is not shared with the container. Source AIS data may change between retries;
fixed time bounds do not freeze late-arriving source records.

The two publishing tasks are not one atomic graph update. A connection task failure
can leave the new `VISITED` snapshot alongside the previous `CONNECTED_TO` snapshot.
Retry `publish_port_connections` with its upstream run metadata to reconcile them.
An individual connection transaction either replaces the whole owned snapshot or
keeps the previous one. A failed visit task prevents connection publishing.
If the completed run's `visit_count` differs from its `FINAL` visit rows, the
connection task fails before touching Neo4j.

The live AIS producer remains a separate continuous service. This DAG does not
start or stop it, and does not yet calculate GDS similarity. An empty valid visit
result is expected while history is sparse; the managed graph summary is then empty.

The healthcheck verifies scheduler heartbeat. UI and task success are separate
checks; a healthy container is not proof that a DAG run succeeded.

## Run GDS and export graph metrics

After connection publishing, manually trigger `ais_gds_metrics` (`schedule=None`,
no catchup, one active run). Its stages run sequentially:

```text
validate snapshot -> recreate two projections -> stream PageRank
  -> stream Louvain and collect stats -> clear/write metrics
  -> validate metrics -> existing exporter -> always clean up projections
```

It uses the [manual GDS workflow's](../neo4j/gds/README.md) algorithm settings and
the same configured Neo4j connection, user, and database throughout. Algorithm
writes recompute results, as the manual scripts do; numeric Louvain community
labels need not match between stream, stats, and write executions. Results go to
`analytics.port_graph_metrics`. Metabase configuration is separate.

Apply [003_port_graph_metrics.sql](../clickhouse/migrations/003_port_graph_metrics.sql)
manually once, then rebuild Airflow. The existing Dockerfile already copies the
pipeline and DAG directories; rebuilding includes the new workflow:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/003_port_graph_metrics.sql
docker compose --profile batch up -d --build airflow
docker compose exec airflow airflow dags unpause ais_gds_metrics
docker compose exec airflow airflow dags trigger ais_gds_metrics
```

The DAG and manual `05_write_back.cypher` clear only `pageRank` and `communityId`
from all Ports before writing metrics on active projected Ports. GDS leaves
`visitRunId`, `visitWindowStart`, and `visitWindowEnd` unchanged. Validation requires
`Port.visitRunId == CONNECTED_TO.runId == captured DAG run_id`, complete metrics,
and an unchanged managed graph snapshot before export.

The existing `ais_graph_metrics_export` DAG remains available as a single
`export_port_metrics` task with no automatic schedule or catchup. It exports only
complete metrics with visit lineage matching the current managed run. For manual
GDS, finish write-back for that graph first; visit metadata alone does not prove
metric freshness:

```sh
docker compose exec airflow airflow dags unpause ais_graph_metrics_export
docker compose exec airflow airflow dags trigger ais_graph_metrics_export
```

The exporter requires exactly one valid run/window among current Port-to-Port
`CONNECTED_TO` relationships owned by `port-connections-v1`. Every active endpoint
must have valid metrics and a matching `analytics.ports FINAL` ID; validation
finishes before any inserts. `snapshot_date` is the UTC date of `window_end`, so
retrying across a month boundary keeps the same partition and logical keys. Query
with `FINAL` to deduplicate physical retry versions.

The two existing ClickHouse daily graph snapshots are unchanged. Recomputing a
window under its new timestamp ID creates different keys from the old hash ID;
`FINAL` does not combine them. Keeping or recomputing those historical snapshots
is a separate decision.

The GDS workflow rechecks the original run/window metadata, graph counts, and an
ephemeral graph-state checksum to detect relationship or weight changes under the
same run ID. This checksum is not a new run identity. Export checks that original
snapshot again during its final metric read.

Manual GDS, connection publishing, and other graph writers must not overlap this
workflow. It uses the same fixed catalog names as the manual scripts:
`ais-port-connections-directed` and `ais-port-connections-undirected`.
`max_active_runs=1` serializes only this DAG, not other writers. Cleanup uses
`ALL_DONE` without upstream result arguments; a final success task keeps prior
failures from being masked by successful cleanup. If Neo4j restarts or projections
are lost, rerun the whole DAG. See [exporter details](../pipelines/graph_metrics/README.md)
for local execution and validation.

## Checks

```sh
.venv/bin/python -m unittest discover -s airflow/tests -v
.venv/bin/python -m unittest discover -s pipelines/port_visits/tests -v
.venv/bin/python -m unittest discover -s pipelines/port_connections/tests -v
.venv/bin/python -m unittest discover -s pipelines/graph_metrics/tests -v
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list ais_port_visits
docker compose exec airflow airflow tasks list ais_graph_metrics_export
docker compose exec airflow airflow tasks list ais_gds_metrics
```

To download ports manually without Airflow:

```sh
.venv/bin/python pipelines/port_visits/download_ports.py --output /tmp/bergen.json
```
