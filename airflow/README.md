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

Rebuild after changing the DAG or pipeline code. The image copies only explicitly
allowed code and SQL files, never root `.env`. Compose injects only the database
credentials required by this batch. Inside Docker the targets are
`http://ais-clickhouse:8123` and `bolt://ais-neo4j:7687`.

## Two tasks

DAG: `ais_port_visits` (manual trigger, no automatic schedule, no catchup).

```text
download_ports -> load_ports_and_visits
```

1. Download the seven-port Bergen pilot from the dated UN OCHA WPI API, normalize
   and validate it. Empty, duplicate-ID, error or truncated responses fail the task.
2. Use the downloaded records to run the existing batch with `--apply --init`.
   Ports and visits are written to ClickHouse; Port nodes and managed VISITED
   summaries are refreshed in Neo4j. Each run uses the preceding 30 days, ending
   at the Airflow run start time, fixed across task retries.

The reference is passed as a small XCom payload, not a path that might disappear
between tasks. The second task creates a temporary JSON file and removes it afterward.
The checked-in `data/ports/bergen.json` is not overwritten. This still queries the
`202511` source snapshot; it is not a claim of current global port coverage.

Enable/unpause the DAG in the UI and click Trigger. Or:

```sh
docker compose exec airflow airflow dags unpause ais_port_visits
docker compose exec airflow airflow dags trigger ais_port_visits
```

There are at most one active run and one running task for this DAG. Failed tasks
retry twice after a one-minute delay. A failed download prevents the load task.
Do not simultaneously run the batch from the host: the existing local file lock
is not shared with the container. Source AIS data may change between retries;
fixed time bounds do not freeze late-arriving source records.

The live AIS producer remains a separate continuous service. This DAG does not
start or stop it, and does not yet calculate GDS similarity. An empty valid visit
result is expected while history is sparse; the managed graph summary is then empty.

The healthcheck verifies scheduler heartbeat. UI and task success are separate
checks; a healthy container is not proof that a DAG run succeeded.

## Checks

```sh
.venv/bin/python -m unittest discover -s airflow/tests -v
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list ais_port_visits
```

To download ports manually without Airflow:

```sh
.venv/bin/python pipelines/port_visits/download_ports.py --output /tmp/bergen.json
```
