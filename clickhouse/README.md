# ClickHouse streaming ingestion

`init/01_raw.sql` consumes raw AIS positions. `init/04_flink_derived.sql` extends
that same Kafka Engine -> materialized view -> persistent analytics table pattern for
Flink's outputs. Metabase should query the persistent tables only.

| Kafka topic | Persistent table | Kafka table | Materialized view | Consumer group |
| --- | --- | --- | --- | --- |
| `ais.vessel.features` | `analytics.ais_vessel_features` | `raw.ais_vessel_features_kafka` | `raw.ais_vessel_features_mv` | `clickhouse_ais_vessel_features` |
| `ais.vessel.gaps` | `analytics.ais_vessel_gap_events` | `raw.ais_vessel_gap_events_kafka` | `raw.ais_vessel_gap_events_mv` | `clickhouse_ais_vessel_gaps` |

Both Kafka tables use the existing `ais-kafka:29092` broker, `JSONEachRow`, and one
consumer. These literal addresses/topics follow `01_raw.sql`; changing Flink's
output topics or moving brokers also requires updating this SQL configuration.
They do not dynamically read `.env`. Flink and ClickHouse consumer groups are
independent, and existing position-consumer offsets are untouched.

Both analytics tables start with `activity_date`: features use `toDate(window_end)`;
gap detections use `toDate(gap_detected_at)` and gap endings use
`toDate(gap_ended_at)`. These UTC dates are provided by DEFAULT expressions,
including for existing history without rewriting data parts. The gap date is
nullable to preserve NULL if an ended event has no end timestamp.

## Startup and schema

```sh
docker compose --profile streaming up -d
# Query via the existing container credentials, without printing the password:
docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM analytics.ais_vessel_features"'
```

The ClickHouse image runs the executable `init/04_flink_derived.sh` before the
matching SQL file. Compose enables `CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS=1` so
existing volumes are upgraded too. `scripts/bootstrap.sh` invokes the same script.
For an existing running ClickHouse installation, apply it without a server restart:

```sh
docker compose exec -T clickhouse sh /docker-entrypoint-initdb.d/04_flink_derived.sh
```

The migration first checks for conflicting old/new destinations. It removes only
views requiring retargeting, then uses
[RENAME TABLE](https://clickhouse.com/docs/sql-reference/statements/rename) to move
`raw.ais_vessel_features` and `raw.ais_vessel_gap_events` to `analytics`. Both
installed databases are Atomic: table UUIDs, data parts, historical rows, and
`ingested_at` are preserved. No INSERT/SELECT backfill is needed, so rerunning the
migration cannot duplicate copied history. Each table moves independently; a
rerun completes an interrupted move. If both an old and new table already exist,
the script refuses to merge or drop either history and requires manual resolution.
Do not run concurrent migration invocations.

Legacy persistent raw names are retired by the rename; no BI compatibility views
are left there. Kafka tables and materialized views remain in `raw`. The feature
Kafka table is recreated only when its old schema lacks `name`, using the same
consumer group and committed offsets. The gap Kafka table's schema/group are
unchanged. Views briefly pause ingestion during retargeting; Kafka buffers new
records. Existing at-least-once delivery limitations still apply around consumer
restarts, but no history is copied or discarded by this migration.

A nullable `name` column is added to the moved feature table. Historical feature
rows and old retained Kafka payloads without `name` remain NULL. Names are not
invented or reconstructed from raw positions. On a fresh installation, the SQL
creates analytics destinations directly. Normal reruns leave correctly configured
views/transports intact and use `CREATE/ADD COLUMN IF NOT EXISTS` for schema setup.

The complete schemas and explicit field mappings are in
[`init/04_flink_derived.sql`](init/04_flink_derived.sql). Features use numeric
`UInt32` MMSI, `UInt16` window minutes, `UInt64` position counts, nullable `Float64`
speed statistics, nullable `name`, and nullable reference attributes.
The Flink name is the latest non-null vessel name by AIS event time within the
window, independent of the latest reference-code observation. A later null name
does not erase an earlier non-null one; all-null/missing names produce NULL.
Equal assigned event timestamps use the last processed non-null name (right
accumulator on a merge tie). ClickHouse only maps this value; it does not select
or recalculate names. Metrics are not recalculated or converted to Float32.
The Kafka tables read floating-point JSON tokens as strings and the views cast
them with `precise_float_parsing=1`. This preserves the original binary Float64
values: in installed ClickHouse 26.3, direct JSON-to-Float64 parsing can still
round by one floating-point step even with that format setting enabled. Nullable
casts return NULL for absent tokens and fail visibly for invalid numeric strings.

Gap events retain the superset of DETECTED and ENDED payloads in one table,
including last coordinates. Fields absent from an event remain NULL: for example,
DETECTED has no recovery timestamp/duration and ENDED has no timeout or
`last_event_msgtime`. Gap durations remain Flink's `Float64` values representing
**total pipeline-observed silence**, including the timeout before detection.
They are not recalculated from detection and recovery timestamps.

Window and gap timestamps are UTC `DateTime64(3)`. Source AIS message timestamps
are nullable UTC `DateTime64(9)`, consistent with `raw.ais_positions`, preserving
fractional precision while normalizing timezone offsets. The materialized views
explicitly parse timestamps and add `ingested_at = now64(3)`. Nullable timestamps
use a safe parser input only for absent fields, then return NULL; invalid non-null
timestamps still raise errors. Original textual timezone spellings are normalized
to the same UTC instant.

Both destinations are plain `MergeTree` event history. Monthly partitions follow
the existing raw-position convention: features partition on `window_start`, gaps
on `gap_detected_at`. Ordering is `(mmsi, window_minutes, window_start)` for features
and `(mmsi, gap_detected_at, event_type)` for gap events. There is no TTL,
aggregation, replacement or deduplication. Kafka delivery retries can therefore
produce duplicate stored rows; exactly-once delivery is not promised.

## Failure visibility

[Kafka Engine's default error mode](https://clickhouse.com/docs/engines/table-engines/integrations/kafka)
raises parsing failures. Both derived tables explicitly use
`kafka_skip_broken_messages=0`, `kafka_handle_error_mode='default'`, strict unknown
fields and strict null handling. Invalid JSON, incompatible types, invalid
non-null timestamps, or destination constraints stop the affected batch and are
visible in ClickHouse logs and `system.kafka_consumers.exceptions`. Missing
nullable fields are allowed and become NULL. No invalid records are silently
skipped, and timestamp parse failures are not silently converted to NULL.

Required MMSI and basic required window/event fields have destination constraints
so missing/defaulted zero identifiers, invalid windows/counts and unknown event
types fail visibly too. This is schema validation, not feature calculation.

Use the monitoring query in
[`scripts/derived_stream_queries.sql`](scripts/derived_stream_queries.sql).
Exceptions are a retained history; compare their timestamps, committed offsets,
and increasing row counts to distinguish a recovered consumer from a current
stall. Fix the schema/producer mismatch or handle the offending record through an
explicit operational decision; these scripts never skip offsets automatically.
Do not `SELECT` Kafka Engine tables to inspect production messages: use an
independent Kafka consumer and query the persistent tables.

## Near-real-time queries and validation

[`scripts/derived_stream_queries.sql`](scripts/derived_stream_queries.sql) includes
recent five-minute windows, recent gap events, recovered gaps of at least 1800
seconds, counts by window/event type, a matched lifecycle, and consumer monitoring.
These are suitable starting points for later Metabase cards; no dashboard or dbt
model is added here.

```sh
# Full Kafka -> MV -> MergeTree test with disposable topics/database:
python3 -u clickhouse/tests/integration_derived.py
# Relevant existing Kafka/Flink configuration and producer tests:
python3 -m unittest discover -s flink/tests -p 'test_*.py' -v
python3 -m unittest discover -s producers/ais/tests -p 'test_*.py' -v
docker compose config --quiet
bash -n scripts/bootstrap.sh
sh -n clickhouse/init/04_flink_derived.sh
git diff --check
```

The opt-in integration test uses Docker CLI and Python stdlib. It tests all six
objects in the correct databases, lossless/idempotent legacy migration, view
targets, historical NULL names, 5/15/30/60-minute records with nullable vessel
names, exact numeric values,
DETECTED/ENDED schemas, omitted and explicit NULL values, UTC conversion,
nanosecond source timestamps, persistent delivery, and visible malformed-JSON /
invalid-timestamp failures. It cleans up its disposable database and topics and
never publishes fixtures to live AIS topics. CI runs it against isolated Kafka
and ClickHouse services. Override `CLICKHOUSE_TEST_CONTAINER`,
`KAFKA_TEST_CONTAINER`, and `KAFKA_TEST_BROKER` for another isolated test stack.

Production verification should also compare retained Kafka payloads with the
persistent rows, find a matched gap lifecycle, and capture counts twice while
streaming. New feature records normally follow five-minute event-time window
boundaries; gaps only arrive when natural silence/recovery occurs. Growing feature
counts or stored historical gaps alone do not prove gap events are still arriving.
