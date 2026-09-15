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

Rebuild after changing the DAG or pipeline code, including the new
`pipelines/port_connections` module: the Docker image uses `COPY`, so restarting
alone does not load these changes. Use the `up -d --build airflow` command above.
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
- Repeating the exact same `start`/`end` resolves to the same deterministic
  batch identity (`version + normalized UTC start/end`, never wall-clock time
  or fetched rows), so retrying an explicit window updates the same ClickHouse
  snapshot and Neo4j `VISITED` and `CONNECTED_TO` summaries rather than duplicating
  them.

The reference is passed as a small XCom payload, not a path that might disappear
between tasks. The second task creates temporary JSON files for the reference and
the subprocess's `--result-json` completed result, then removes them afterward.
It returns only `run_id`, `window_start`, and `window_end` to the connection task
through XCom. Visit rows stay in ClickHouse; no visit dataset is placed in XCom.
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

## Checks

```sh
.venv/bin/python -m unittest discover -s airflow/tests -v
.venv/bin/python -m unittest discover -s pipelines/port_visits/tests -v
.venv/bin/python -m unittest discover -s pipelines/port_connections/tests -v
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list ais_port_visits
```

To download ports manually without Airflow:

```sh
.venv/bin/python pipelines/port_visits/download_ports.py --output /tmp/bergen.json
```
