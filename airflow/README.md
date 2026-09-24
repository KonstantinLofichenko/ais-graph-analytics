# AIS Airflow (local)

Dedicated `ais-airflow` container on the existing `ais-network`. It uses Airflow
3.3.1 standalone mode, with its own persistent `airflow-data` volume for metadata,
logs, and generated login credentials. The old `airflow-intro` installation is not
used. Standalone is for local development; separate services and PostgreSQL should
replace it for a production deployment.

## Start and stop

For first installation and startup, follow the
[fresh-clone quickstart](../README.md#fresh-clone-quickstart), including its `batch`
profile step. Stop only Airflow from the repository root with:

```sh
docker compose stop airflow
```

Open http://localhost:8080. The port is bound to loopback only. Login username:
`admin`. Display the generated password locally (do not commit/share the output):

```sh
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Rebuild after changing the DAG or pipeline code, including the
`pipelines/port_connections` and `pipelines/graph_metrics` modules: the Docker image
uses `COPY`, so restarting alone does not load these changes. After code changes,
run `docker compose --profile batch up -d --build airflow` from the repository root.
The image copies only explicitly allowed code and SQL files, never root `.env`.
Compose injects only the database
credentials required by this batch. Inside Docker the targets are
`http://ais-clickhouse:8123` and `bolt://neo4j:7687`.

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
  --conf '{"start": "2026-09-14", "end": "2026-09-15"}'
```

Optional override of the safety ceiling for a busy day:

```sh
docker compose exec airflow airflow dags trigger ais_port_visits \
  --conf '{"start": "2026-09-14", "end": "2026-09-15", "max_rows": 6000000}'
```

- Explicit `start`/`end` must be `YYYY-MM-DD` dates exactly one day apart.
  They become UTC-midnight timestamps internally. Timestamp strings are rejected.
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
- The host pipeline CLI supports 24, 36, 48 hours, or other valid durations;
  explicit dates in this DAG must still span exactly one day. Each ID has one
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
  -> validate metrics -> always clean up projections -> complete_gds_metrics
```

It uses the [manual GDS workflow's](../neo4j/gds/README.md) algorithm settings and
the same configured Neo4j connection, user, and database throughout. Algorithm
writes recompute results, as the manual scripts do; numeric Louvain community
labels need not match between stream, stats, and write executions. After GDS
succeeds, `ais_graph_metrics_export` exports results to `analytics.port_graph_metrics`. Metabase configuration is separate.

The quickstart's bootstrap already applies
[003_port_graph_metrics.sql](../clickhouse/migrations/003_port_graph_metrics.sql).
After completing setup, these standalone commands are available for inspecting
one current snapshot; the daily master normally sequences them:

```sh
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

The exporter requires one valid durable `ConnectionSnapshot` owned by
`port-connections-v1`, with matching relationship metadata and edge count.
A published empty snapshot succeeds with zero exported rows; missing publication
metadata fails. Every active endpoint
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
same run ID. It also checks durable publication generation and timestamp. This
checksum is not a new run identity. The separate exporter captures and rechecks
its own snapshot; it does not receive the GDS DAG's captured snapshot.

Manual GDS, connection publishing, and other graph writers must not overlap this
workflow. It uses the same fixed catalog names as the manual scripts:
`ais-port-connections-directed` and `ais-port-connections-undirected`.
`max_active_runs=1` serializes only this DAG, not other writers. Cleanup uses
`ALL_DONE` without upstream result arguments; a final success task keeps prior
failures from being masked by successful cleanup. If Neo4j restarts or projections
are lost, rerun the whole DAG. See [exporter details](../pipelines/graph_metrics/README.md)
for local execution and validation.

## Checks

The Airflow image provides the DAG test dependencies; copy the current DAGs and
tests into a temporary directory in the running container (its image does not
include `airflow/tests`). These tests mock processing and do not trigger DAGs:

```sh
tar -cf - airflow/dags airflow/tests | docker compose exec -T airflow bash -c '
  set -e
  work=$(mktemp -d /tmp/ais-dag-tests.XXXXXX)
  tar -xf - -C "$work"
  export PYTHONPATH="$work/airflow/dags:/opt/ais"
  export AIRFLOW__CORE__DAGS_FOLDER="$work/airflow/dags"
  python -m unittest discover -s "$work/airflow/tests" -v
'
```

For pipeline tests and the manual downloader, first prepare the
[optional host environment](../pipelines/port_visits/README.md#run-from-the-repository-root).

```sh
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


## Sequential daily analytics

Manually trigger `ais_analytics_pipeline` with inclusive `start`, exclusive `end`,
and optional `max_rows` (otherwise the port-visits environment default applies):

```sh
docker compose exec -T airflow airflow dags trigger ais_analytics_pipeline \
  --conf '{"start":"2026-09-01","end":"2026-09-17","max_rows":5000000}'
```

Or use the helper from the repository root (requires host `python3`):

```sh
bash airflow/daily_run.sh 2026-09-01 2026-09-17 5000000
```

The helper accepts `START_DATE END_DATE [MAX_ROWS]`, validates strict `YYYY-MM-DD`
dates and a positive optional row limit, and triggers only the master DAG. Start
is inclusive and end is exclusive; omitting `MAX_ROWS` uses the port-visits
environment default. Airflow must already be running.

This runs 16 daily chains, each `ais_port_visits -> ais_gds_metrics ->
ais_graph_metrics_export`, waiting for export success before the next day starts.
A small Airflow 3 `TriggerDagRunOperator` subclass defers between child runs;
it reconstructs progress from the child completion event on each resume. No child
processing logic is copied. The master displays one coordinating task; child DAG
runs show individual stages. A running triggerer and unpaused children are required.

Explicit dates are required for the master; no rolling master default remains.
Standalone port visits retains its rolling default when both keys are omitted.
Existing callers passing explicit timestamps must switch to dates; use the existing
pipeline CLI directly if a non-daily explicit timestamp window is needed.
The analytical run ID still comes from the existing UTC start timestamp function,
e.g. `2026-09-01T00:00:00Z`. Airflow child execution IDs are only orchestration IDs.

Failure or an unexpected completion event stops advancement. Retries remain zero;
clearing the coordinating task does not reset existing child runs automatically.
For recovery, inspect the failed child and start a new master run for the desired
remaining date range after resolving the failure. Daily ClickHouse retry semantics
remain unchanged. Historical rows are not rewritten.

Do not independently trigger graph-changing port-visits runs during the master:
`max_active_runs=1` serializes masters but is not a global lock across child DAGs.
GDS/export still consume current graph metadata. GDS calculates, writes, and
validates Neo4j metrics; only the standalone export DAG writes them to ClickHouse.
A valid published empty snapshot skips GDS calculation and exports zero rows, so
the range can continue. Missing or inconsistent durable snapshot metadata fails.
