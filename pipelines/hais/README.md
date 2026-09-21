# HAIS historical file ingestion

Manual DAG: `ais_hais_historical_ingestion`. HAIS ordering and downloading remain
manual. Python only discovers/stats files, issues SQL, and handles audit events;
ClickHouse reads the GeoParquet with an explicit schema that excludes geometry.
Source values, sentinel values, and duplicate rows remain unchanged.

## Shared files and deployment

Compose mounts `${HAIS_HOST_DIR:-./data/hais}` read-only at:

- Airflow: `/opt/ais/data/hais`
- ClickHouse: `/var/lib/clickhouse/user_files/hais`

Place completed downloads in that host directory once, named
`hais_YYYY-MM-DD.snappy.parquet`. Alternatively set `HAIS_HOST_DIR` to an existing
absolute host directory in your local environment. Both container users need read
access and directory traversal permissions. Files must remain immutable during
execution; size and modification time are checked before and after loading.
Do not publish partial downloads under their final names.
The nested ClickHouse mount does not hide files already in the user_files root.
Move/copy any existing sample into the shared host directory before using this DAG.
Its original successful audit record is still recognized regardless of location.

Apply migrations `004_hais_positions.sql` and `005_hais_ingestion_runs.sql` first.
The audit table must have `row_count Nullable(UInt64)`. The DAG creates no tables.
Then deploy the changed image and mounts (these commands restart services):

```sh
mkdir -p data/hais
docker compose --profile batch up -d --build airflow clickhouse
```

Unpause the new DAG in Airflow before triggering:

```sh
docker compose exec -T airflow airflow dags trigger ais_hais_historical_ingestion \
  --conf '{"start_date":"2026-09-01","end_date":"2026-09-01"}'
```

Both required parameters are calendar dates in `YYYY-MM-DD` format, inclusive.
Timestamps, invalid dates, missing parameters, and reversed ranges fail validation.
`discover_files` checks the entire range before any inserts: a missing file fails
the preflight, rather than silently omitting a day. The resulting files are mapped
to individually visible `ingest_file` tasks labeled by source date. Only one task
and one DAG run execute at a time. Dates need not execute in chronological order.
Use bounded ranges within Airflow's configured `max_map_length` (normally 1024).

## Audit and retry behavior

The practical identity is **file_name + actual file_size**. A matching `success`
event skips the mapped task before any raw insert or new audit event. A changed
size is a different version and may be loaded, preserving any overlapping rows.

Each attempted load appends a `running` event, then a `success` or `failed` event
with the same `started_at`. Earlier events/attempts remain visible. `running` has
NULL row count/completion; `success` has the source-file count after a successful insert (including actual
zero) and completion time; failure has NULL count because insertion may be partial.
No additional attempt identifier or schema change is introduced.

ClickHouse counts the explicitly typed source file, then performs the synchronous
INSERT. After it succeeds and the file is verified unchanged, the source count is
recorded as the successful row count. No whole-table count or exclusive-writer
requirement is used for validation. Files must remain immutable throughout loading.
DAG concurrency limits still serialize this loader; do not concurrently load the
same file outside it, since the audit check and insert are not atomic.

Raw insertion and audit insertion are not transactional together. Automatic task
retries are disabled. A prior audit history without success blocks another insert,
including after partial inserts, lost responses, task termination, or a failed
success-audit write. Ordinary exceptions append `failed`; abrupt termination or
an unavailable database may leave only `running`. Both require operator review.
Reconcile database/query logs and raw data before deciding how to recover; never
mark success unless the entire load is confirmed, and never blindly clear audit
history and reload. This is a conservative duplicate-prevention guard, not a claim
of atomic exactly-once ingestion. No automatic rollback/delete is implemented.

The DAG uses `fail_fast=True`: a file failure stops the run and skips remaining
work. Already successful files remain loaded and will be skipped on the next run.
The manually registered September 1 sample (33,836,469 bytes, 1,227,749 rows) is
recognized by the same success check; it is not hard-coded into the loader.

## Validation

```sql
SELECT count() AS rows, uniqExact(mmsi) AS vessels,
       min(date_time_utc) AS first_position, max(date_time_utc) AS last_position
FROM raw.hais_positions
WHERE date_time_utc >= toDateTime64('2026-09-01 00:00:00', 6, 'UTC')
  AND date_time_utc < toDateTime64('2026-09-02 00:00:00', 6, 'UTC');
-- For the sample alone: 1227749 rows, 381 vessels,
-- 2026-09-01 00:00:12 through 2026-09-01 23:59:59.

SELECT file_name, file_size, source_date, status, row_count, started_at, completed_at
FROM raw.hais_ingestion_runs
ORDER BY started_at DESC, completed_at DESC;

SELECT count() > 0 AS should_skip
FROM raw.hais_ingestion_runs
WHERE file_name = 'hais_2026-09-01.snappy.parquet'
  AND file_size = 33836469 AND status = 'success';
```

Tests run inside the rebuilt Airflow image, without database access:

```sh
docker compose exec -T airflow bash -c \
  'cd /opt/ais && python -m unittest discover -s pipelines/hais/tests -v'
```

The separate DAG parse/parameter tests are in
`airflow/tests/test_ais_hais_historical_ingestion.py` and require Airflow 3.3.1.
