# Local Iceberg foundation

An optional `lakehouse` Compose profile provides MinIO object storage, an Apache
Iceberg REST catalog and a single-node Trino coordinator/worker. It also provides a manual, one-shot PySpark raw HAIS ingestion path. Existing
AIS pipelines are unchanged. The manual `ais_hais_lakehouse_load` Airflow DAG
now orchestrates the same raw and curated Spark jobs and their validations. All existing services and ports are unchanged.

## Start

From the repository root, keep the existing `.env` (or use `.env.example` for a
fresh checkout). Optional `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` variables
configure all three components consistently. Defaults are explicitly local
development credentials: `ais-local` / `ais-local-development`.

```sh
docker compose --profile lakehouse config --quiet
docker compose --profile lakehouse up -d --build --wait --wait-timeout 300 minio minio-init iceberg-rest trino
```

Explicit service names start only the lakehouse, avoiding the project's default
services. `minio-init` creates the `warehouse` bucket idempotently after MinIO is
healthy. The REST catalog waits for bucket creation; Trino waits for the catalog.

| Service | Local endpoint |
| --- | --- |
| MinIO S3 API | http://127.0.0.1:9002 |
| MinIO console | http://127.0.0.1:9003 |
| Iceberg REST catalog | http://127.0.0.1:8181/v1/config |
| Trino UI/API | http://127.0.0.1:8090 |

Containers use `http://minio:9000` and `http://iceberg-rest:8181`. Trino's
`iceberg` catalog uses native S3 access with path-style addressing and credentials
from environment variables. See the official [Trino REST catalog configuration](https://trino.io/docs/current/object-storage/metastores.html#rest-catalog)
and [S3 configuration](https://trino.io/docs/current/object-storage/file-system-s3.html).

MinIO's community images/downloads are no longer publicly available. The small
[Dockerfile](minio/Dockerfile) builds official pinned upstream sources (server
`RELEASE.2025-10-15T17-29-55Z`, client `RELEASE.2025-08-13T08-35-41Z`); the first build
requires internet access and takes longer. Trino is pinned to 483 and the Apache
REST fixture to 1.10.1.

## Validate

```sh
cat lakehouse/validate.sql | docker compose --profile lakehouse exec -T trino trino --file /dev/stdin
```

The fixture replaces only `iceberg.validation.foundation_smoke`, inserts two
rows, returns both rows and `row_count = 2`, and lists Parquet file paths under
`s3://warehouse/validation/`. Confirm actual data and metadata objects in MinIO:

```sh
docker compose --profile lakehouse run --rm --no-deps --entrypoint /bin/sh minio-init -ec 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mc ls --recursive local/warehouse/validation/'
```

Persistence check (only new services restart):

```sh
docker compose --profile lakehouse restart minio iceberg-rest trino
docker compose --profile lakehouse up -d --wait --wait-timeout 300 minio iceberg-rest trino
docker compose --profile lakehouse exec -T trino trino --execute 'SELECT count(*) FROM iceberg.validation.foundation_smoke'
```

Expected count remains `2`. Optional fixture cleanup:

```sh
docker compose --profile lakehouse exec -T trino trino --execute 'DROP TABLE iceberg.validation.foundation_smoke; DROP SCHEMA iceberg.validation'
docker compose --profile lakehouse stop trino iceberg-rest minio
```

MinIO objects persist in `minio-data`; the catalog's SQLite database persists in
`iceberg-catalog-data` mounted at `/tmp`. Both volumes are needed to retain table
access. Avoid `docker compose down -v`, which also deletes existing project data.
This is a local foundation: the Apache REST fixture uses SQLite, not a production
catalog backend; there is no catalog authentication, TLS or distributed HA. New
ports bind only to loopback. Changing credentials requires recreating the new
services with consistent environment values.

## Raw HAIS ingestion

The `hais-upload` service uses `HAIS_UPLOAD_HOST_DIR`, mounted read-only at
`/input`. Set this in your local `.env` to the extracted latest HAIS download
directory. It is separate from `HAIS_HOST_DIR`, which continues to serve the
existing ClickHouse/Airflow historical path. A nonexistent upload directory fails
instead of being created automatically. Only `*.parquet` files are copied,
retaining relative paths, to `s3://ais-data/hais/source/`; the existing Iceberg
warehouse remains `s3://warehouse/`.

Inspect the resolved source and verify its file inventory before running any
upload. Do not point this at all of Downloads, which may contain older datasets:

```sh
docker compose --profile lakehouse config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["hais-upload"]["volumes"])'
```

After verifying the source directory and its file inventory:

```sh
docker compose --profile lakehouse build spark
docker compose --profile lakehouse run --rm hais-upload
docker compose --profile lakehouse run --rm spark > /tmp/hais-raw-spark.log 2>&1
cat /tmp/hais-raw-spark.log
python3 lakehouse/validate_raw.py /tmp/hais-raw-spark.log
```

`hais-upload` is a one-shot client using the existing MinIO image. It uploads only
Parquet files and overwrites matching object keys on reruns; it does not delete
old source objects. Use a clean source prefix/dataset before switching datasets,
so unrelated older files are not included. `minio-init` creates both `warehouse`
and `ais-data` buckets idempotently. The host mount belongs only to the upload
service; Spark reads exclusively from MinIO.

`spark` is a one-shot local `[2]` PySpark driver, with a 2 GiB driver heap and no
new host ports or persistent volumes. Its jobs directory is mounted read-only.
The image pins Spark 3.5.7 (Python 3/Java 17), Iceberg runtime/AWS bundle 1.10.1,
Hadoop S3A 3.3.4 and AWS SDK v1 bundle 1.12.262. The bundled Parquet 1.13.1
reader jars are replaced as a consistent set by 1.16.0, which can parse the
source GEOMETRY logical annotation. Arrow 21.0.0 and pandas 2.2.3
inspect source footers and convert the explicit schema. Dependencies are baked
into the image, so submissions do not fetch Maven packages.

Environment variables:

| Variable | Purpose / default |
| --- | --- |
| `HAIS_UPLOAD_HOST_DIR` | Host directory mounted read-only by `hais-upload`; default `./data/hais`. |
| `HAIS_HOST_DIR` | Existing historical ClickHouse/Airflow input; unchanged. |
| `HAIS_SOURCE_URI` | Spark input prefix; `s3a://ais-data/hais/source/`. |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | Shared local credentials for upload, Arrow, S3A and Iceberg. |

### Raw schema and reruns

The job checks all source schemas before writing, prints the schema/sample and
counts rows from MinIO. The raw table is `iceberg.hais_raw.ais_positions` in Trino
and Spark (REST namespace `hais_raw`, table `ais_positions`). Its data/metadata
stay under `s3://warehouse/hais_raw/`, separate from the source bucket.

All source columns are retained, including geometry as binary bytes. Arrow
supplies an explicit schema because the source embeds a newer Spark
`geometry(OGC:CRS84)` annotation unsupported by this Spark runtime. Byte/short
fields widen to Iceberg integers without changing values; decimals retain their
precision/scale, and timestamps use Spark/Iceberg microsecond precision with a
UTC Spark session. GeoParquet/Spark-specific file annotations are not propagated
to Iceberg; this is raw geometry storage, not spatial processing. There are no
business filters, deduplication, enrichment or curated transformations.

Each successful run atomically replaces the complete raw table from the current
source prefix via Iceberg `createOrReplace`; it does not append duplicate rows.
Previous snapshots may retain old data files, so repeated full loads can consume
more disk. This is a manual full raw reload, not incremental ingestion. Do not
modify the source prefix while ingestion is running.

The job emits `RAW_INGEST_RESULT` only after Spark source and Iceberg counts match.
`validate_raw.py` compares Trino's count with this result and checks warehouse
file locations, schema and sample rows. It requires the local Trino endpoint
`http://127.0.0.1:8090` and only Python's standard library.

Persistence check:

```sh
docker compose --profile lakehouse restart minio iceberg-rest trino
docker compose --profile lakehouse up -d --wait --wait-timeout 300 minio iceberg-rest trino
python3 lakehouse/validate_raw.py /tmp/hais-raw-spark.log
```

Spark has no long-running state to restart: rerun `docker compose --profile
lakehouse run --rm spark` and repeat the validator to prove a new driver can
access the persistent catalog/table without duplicating rows. Keep both existing
MinIO and catalog volumes; do not remove project volumes.


### Validated dataset (2026-10-13)

The verified latest download contained 30 daily Parquet files named
`hais_2026-09-01.snappy.parquet` through `hais_2026-09-30.snappy.parquet`, totaling
585,032,591 bytes and 23,295,797 rows. MinIO object names and sizes matched the
host inventory. Spark printed all 14 source columns and returned the same count
as its Iceberg write and Trino. The count remained unchanged after restarting
MinIO/REST/Trino and after rerunning with a fresh Spark driver. Source timestamps
span September 1 through September 30; September 30 contains only 17,906 rows.

The existing historical ingestion tests also passed (11 tests). The raw source
files and local validation logs are runtime artifacts and are not committed.

## Local Trino memory

Trino mounts `trino/jvm.config` and `trino/config.properties` read-only, preserving
its single-node coordinator/worker mode and existing ports/catalog. JVM options
from the pinned image are retained except percentage-based heap sizing, replaced
by explicit sizes so Docker's available memory does not inflate the heap.

| Setting | Value | Reason |
| --- | --- | --- |
| `-Xmx` | `8G` | Fixed 8 GiB maximum heap for the local workload. |
| `-Xms` | `2G` | Smaller initial heap, which can grow to 8 GiB. |
| `query.max-memory` | `4GB` | Bounds one query's user memory across the single-node cluster. |
| `query.max-memory-per-node` | `4GB` | Same user-memory limit on the only worker. |
| `query.max-total-memory` | `6GB` | Allows user plus revocable query memory within the tracked pool. |
| `memory.heap-headroom-per-node` | `2GB` | Reserves 25% of heap for untracked allocations and JVM overhead within heap. |

The required relationships hold: `4GB + 2GB < 8GB` and `6GB > 4GB`. The memory
pool is `8GB - 2GB = 6GB`. These are query/heap limits, not a container RSS cap;
native allocations and other processes require memory outside the JVM heap.
See [Trino resource management](https://trino.io/docs/current/admin/properties-resource-management.html).

Apply only to Trino and validate:

```sh
docker compose --env-file .env.example --profile '*' config --quiet
docker compose --profile lakehouse up -d --no-deps --wait --wait-timeout 300 trino
docker compose --profile lakehouse exec -T trino trino --execute 'SELECT count(*) FROM iceberg.hais_raw.ais_positions'
docker compose --profile lakehouse exec -T trino trino --execute 'SELECT mmsi, count(*) AS positions, avg(speed_over_ground) AS avg_speed, min(date_time_utc), max(date_time_utc) FROM iceberg.hais_raw.ais_positions GROUP BY mmsi ORDER BY positions DESC LIMIT 10'
docker compose --profile lakehouse logs trino
```

Validated on 2026-10-13: UI worker telemetry reported heap capacity
8,589,934,592 bytes (8 GiB) and pool capacity 6,442,450,944 bytes (6 GiB).
The raw count remained 23,295,797 and the full-table vessel aggregation succeeded.
No memory-exhaustion/configuration errors occurred. Two transient memory-info
HTTP 503 warnings occurred while the server was starting; after readiness, memory
telemetry and queries succeeded.

## Curated HAIS layer

`iceberg.hais_raw.ais_positions` → PySpark →
`iceberg.hais_curated.ais_positions` reuses the same Spark service, REST catalog,
MinIO warehouse and Trino Iceberg catalog. It reads only the raw Iceberg table;
it does not upload, alter raw data or change the historical ClickHouse/Airflow
path. The same jobs can run manually or through the manual-trigger Airflow DAG below.

Run after the raw table exists:

```sh
docker compose --profile lakehouse run --rm spark /opt/ais/jobs/test_curate_hais.py
docker compose --profile lakehouse run --rm spark /opt/ais/jobs/curate_hais.py > /tmp/hais-curated.log 2>&1
cat /tmp/hais-curated.log
cat lakehouse/validate_curated.sql | docker compose --profile lakehouse exec -T trino trino --file /dev/stdin
python3 lakehouse/validate_curated.py
```

The SQL is read-only; the Python validator asserts milestone counts, NULL counts,
required Iceberg fields and the actual partition spec through REST metadata. It
also executes the SQL's daily aggregation with distinct vessel count/average
speed. Both raw and curated jobs use `spark_session.py` for the same configuration.
No new Docker services, volumes, ports or environment variables are needed.

### Schema and rules

| Raw column | Curated column | Curated type | Required | Rule |
| --- | --- | --- | --- | --- |
| `date_time_utc` | `date_time` | TIMESTAMP | Yes | UTC Spark session; preserve timestamp. |
| `mmsi` | `mmsi` | INT | Yes | Preserve. |
| `longitude` | `longitude` | DECIMAL(9,6) | Yes | Required six-decimal cast. |
| `latitude` | `latitude` | DECIMAL(8,6) | Yes | Required six-decimal cast. |
| `status` | `navigational_status` | INT | Yes | Rename only. |
| `course_over_ground` | `course_over_ground` | DECIMAL(5,2) | No | Keep `0 <= value < 360`; otherwise NULL. |
| `true_heading` | `true_heading` | INT | No | `511` → NULL; other values preserved. |
| `speed_over_ground` | `speed_over_ground` | DECIMAL(5,2) | No | `102.30` → NULL; other values preserved. |
| `rate_of_turn` | `rate_of_turn` | INT | No | `-128` → NULL; other values preserved. |
| `maneuvre` | `maneuver` | INT | Yes | Rename only. |
| `data_source` | `data_source` | STRING | Yes | Preserve. |
| `ais_class` | `ais_class` | STRING | Yes | Preserve. |
| `msg_type` | `message_type` | INT | Yes | Rename only. |
| `geometry` | `geometry` | BINARY | Yes | Preserve binary bytes. |

Spark TIMESTAMP is stored as Iceberg `timestamptz` and appears in Trino as
`timestamp(6) with time zone`. STRING/BINARY appear as `varchar`/`varbinary`.
Coordinates round to the requested decimal scale. ANSI mode makes decimal
overflow fail rather than silently introducing extra NULLs. No rows are filtered,
deduplicated or enriched, and no extra heading/speed/turn range rules are added.

### Partitioning and reruns

The only partition transform is `day(date_time)` (Spark spelling
`days(date_time)`), with data under `s3://warehouse/hais_curated/ais_positions`.
There is no MMSI bucket or hourly partition. Ten fields are declared NOT NULL in
Iceberg. The job checks their NULL counts before writing. It disables the Spark
projection's nullable-schema check for the write after that data check, preserving
the actual required Iceberg fields; no invalid rows are silently dropped.

The first run creates the table; each load atomically overwrites **all** rows via
Iceberg `overwrite(lit(True))`, retaining the table schema/partition spec. Reruns
replace the current full-month result without appending duplicates. Prior
snapshots can retain old files and consume disk. Do not run overlapping loads or
replace the raw table while a curated load is running. Existing incompatible
curated schemas require explicit review; this job does not migrate schemas.

Expected current-month validation:

| Check | Expected |
| --- | ---: |
| Raw rows | 23,295,797 |
| Curated rows | 23,295,797 |
| Rows with any required NULL | 0 |
| Course NULLs | 2,517,680 |
| Heading NULLs | 4,511,883 |
| Speed NULLs | 50,962 |
| Turn NULLs | 4,221,794 |
| Remaining sentinels / out-of-range course | 0 |

The course total is 2,517,662 occurrences of 360.00, one each of 448.00 and
471.00, and 16 of 511.00. Expected counts in the assertion script intentionally
apply to this verified dataset and must be reviewed for a different month.

Persistence check (only lakehouse services):

```sh
docker compose --profile lakehouse restart minio iceberg-rest trino
docker compose --profile lakehouse up -d --wait --wait-timeout 300 minio iceberg-rest trino
python3 lakehouse/validate_curated.py
```

Repeat the curated load and validator to check idempotency with a new Spark
driver. Keep the persistent MinIO and REST catalog volumes.

## Manual historical Airflow DAG

DAG ID: **`ais_hais_lakehouse_load`**. It has `schedule=None`, `catchup=False`,
`max_active_runs=1` and serial task execution. No Kafka, Flink, ClickHouse or upload
step is included. The old `ais_hais_historical_ingestion` DAG remains intact.

```text
check_source_files → ingest_raw_iceberg → validate_raw
    → curate_hais → validate_curated → final_summary
```

### Prerequisites and startup

- The verified Parquet files must already exist in `HAIS_SOURCE_URI` (default
  `s3a://ais-data/hais/source/`). Upload/verify them separately before triggering.
- MinIO, REST catalog and Trino must be healthy on the existing `ais-network`.
- Build the Spark image after job changes: DockerOperator uses scripts baked into
  the image, unlike manual Compose Spark runs, which bind-mount the jobs directory.
- Airflow mounts `/var/run/docker.sock` to launch disposable local Spark containers.
  The existing Airflow image includes Docker provider 4.5.9. Socket access was
  verified with the container's existing user/group; Spark is not installed in Airflow.

```sh
docker compose --profile batch --profile lakehouse build airflow spark
docker compose --profile batch --profile lakehouse up -d --wait --wait-timeout 300 airflow minio minio-init iceberg-rest trino
docker compose exec -T airflow airflow dags list-import-errors -o json
docker compose exec -T airflow airflow dags unpause ais_hais_lakehouse_load
docker compose exec -T airflow airflow dags trigger --run-id hais_september_manual_1 ais_hais_lakehouse_load
```

Alternatively open the Airflow UI at http://localhost:8080, find the DAG, unpause
it and choose **Trigger**. Inspect the task chain in Graph view and task logs in
the run detail. `schedule=None` means unpausing does not schedule automatic loads.

### Operators and execution

| Task | Operator / existing implementation |
| --- | --- |
| `check_source_files` | DockerOperator → `spark-submit ... check_source_files.py`; uses raw `inspect_source()` to verify Parquet footers, schemas and file count. Fails on missing files or storage errors. |
| `ingest_raw_iceberg` | DockerOperator → `spark-submit --master local[2] --driver-memory 2g /opt/ais/jobs/ingest_hais_raw.py`. |
| `validate_raw` | PythonOperator → `lakehouse.validate_raw.validate()`; checks positive count, equality to Spark, all 14 raw columns and warehouse file locations. |
| `curate_hais` | DockerOperator → the same Spark command with `/opt/ais/jobs/curate_hais.py`. |
| `validate_curated` | PythonOperator → `lakehouse.validate_curated.validate()`; all existing row/NULL/sentinel/schema/partition checks. |
| `final_summary` | PythonOperator → concise file count, validated raw/curated counts and success status from small XCom results. |

Spark containers use the existing image and Docker network, without host-input
mounts, temporary host-path mounts or new ports. Containers are removed after
completion. Commands capture verbose Spark output internally, logging only the
success result or the last 40 lines on failure. Direct manual Spark runs remain
available for full debugging output. Credentials use DockerOperator's private
environment rather than visible template fields.

Airflow reuses `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `HAIS_SOURCE_URI`, and
`LAKEHOUSE_SPARK_IMAGE` (default `ais-spark:3.5.7-iceberg-1.10.1`). Compose sets
`LAKEHOUSE_DOCKER_NETWORK=ais-network`, `TRINO_URL=http://trino:8080` and
`ICEBERG_REST_URL=http://iceberg-rest:8181` for Airflow. The standalone validators
retain their localhost defaults and accept these endpoint environment overrides.
No new external services, ports or data volumes are added.

### Failure behavior and logs

Source checking retries twice; Spark loads retry once, with a two-minute delay.
Both full-table loads retain their existing idempotent overwrite behavior.
Validators and final summary have zero retries; failed data-quality checks fail
visibly. All downstream dependencies require upstream success. Only one DAG run
is active at a time; do not overlap this DAG with independent manual table loads.

```sh
docker compose exec -T airflow airflow tasks states-for-dag-run ais_hais_lakehouse_load hais_september_manual_1 -o json
docker compose exec -T airflow airflow dags state ais_hais_lakehouse_load hais_september_manual_1
```

Full Airflow task logs persist in the existing `airflow-data` volume under
`/opt/airflow/logs`. Final validation can also run directly:

```sh
python3 lakehouse/validate_curated.py
docker compose --profile lakehouse exec -T trino trino --execute 'SELECT count(*) FROM iceberg.hais_raw.ais_positions'
docker compose --profile lakehouse exec -T trino trino --execute 'SELECT count(*) FROM iceberg.hais_curated.ais_positions'
```

Two scheduler-driven manual runs were validated on 2026-10-14:
`lakehouse_20261014_validation_1` and `lakehouse_20261014_validation_2`. All six
tasks succeeded in both. Each reported 30 source files and 23,295,797 raw/curated
rows; rerunning did not duplicate rows. UI API registration and task listing were
verified. The final isolated CI-style suite passed 272 tests, including eight new
DAG/validator tests. Historical ClickHouse/Airflow code and existing live services
were unchanged.

This is a local Docker execution model: the Airflow socket grants control of the
local Docker daemon. It is not a remote Spark cluster/security boundary. The
curated validator is still specific to the verified September dataset; review
expected counts before using a different dataset. Spark image updates must be
rebuilt explicitly. The DAG does not upload source files or provide a transaction
spanning both tables; a failed run can leave the successfully loaded raw table
available before curated validation completes.
