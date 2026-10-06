# PyFlink Kafka smoke job

The job reads required environment variables, with no broker/topic defaults in
Python. Configure these in the repository's local `.env` using `.env.example`:

```ini
KAFKA_BOOTSTRAP_SERVERS=ais-kafka:29092
KAFKA_TOPIC=ais.positions
FLINK_SINK_TOPIC=ais.positions.pyflink.tmp
```

`KAFKA_TOPIC` is shared with the AIS producer and topic-creation helper; a separate
`FLINK_SOURCE_TOPIC` is unnecessary. Compose explicitly passes all three settings
to both Flink containers. Missing or empty values fail before job submission.

```sh
docker compose --profile streaming up -d --build flink-jobmanager flink-taskmanager
docker compose exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d -py /opt/flink/jobs/ais_kafka_smoke.py
docker compose exec -T flink-jobmanager /opt/flink/bin/flink list -r
# After verifying output, cancel the smoke job using the ID returned above:
docker compose exec -T flink-jobmanager /opt/flink/bin/flink cancel <job-id>
```

The smoke job starts at latest source offsets and is unbounded. It needs new
source messages after submission; use the normal live producer rather than
injecting synthetic records into the production source topic. It writes only to
the configured temporary sink. The transformation, consumer group, parallelism,
and offset behavior are unchanged.

The examples in `sql/` intentionally retain their literal demonstration values.
They do not read these Python environment settings.

## Other Kafka clients

The Compose producer, Kafka Connect, ksqlDB, and Kafbat UI use the same
`KAFKA_BOOTSTRAP_SERVERS` setting. Topic/status scripts resolve the shared settings
through Compose, including `.env` and shell overrides, and require Docker and jq.
They execute Kafka CLI commands inside the broker container, so use a
container-reachable address. Only the two Kafka settings are extracted from the
resolved configuration; credentials are not printed.

For a producer run directly on the host, override `KAFKA_BOOTSTRAP_SERVERS` in that
process to the host listener (for this Compose stack, `localhost:9092`). Avoid
exporting that override globally when launching Compose containers.

Broker listener/controller addresses and its local healthcheck intentionally stay
fixed to the Compose topology. The existing ClickHouse Kafka-engine table and the
Neo4j connector example are also tied to the deployed AIS source. Changing the
shared topic/broker for a different deployment requires configuring those consumers
separately; this refactor does not migrate production tables or connector mappings.

## Local validation

```sh
python3 -m unittest discover -s flink/tests -p 'test_*.py' -v
sh -n scripts/kafka-env.sh scripts/create-kafka-topic.sh
bash -n scripts/status.sh scripts/bootstrap.sh
docker compose config --quiet
./scripts/create-kafka-topic.sh
./scripts/status.sh
```

## Shared reference data in derived streams

Reference enrichment lives inside the gap and vessel-feature jobs. Both consume
raw AIS directly and share `jobs/common/reference_data.py`; no intermediate
reference-enriched stream is required.

```text
dbt/seeds -> shared reference loader -> gap detector / feature windows
ais.positions -> gap detector -> ais.vessel.gaps
ais.positions -> feature windows -> ais.vessel.features
```

The loader reads the existing `ais_ship_types.csv` (`ship_type`, `ship_type_name`,
`ship_category`) and `ais_navigational_status.csv` (`navigational_status`,
`navigational_status_name`). These dbt seeds remain the only mapping source. Each
stateful/output operator loads both CSVs once in `open()`. Both derived jobs require
`FLINK_REFERENCE_DIR=/opt/flink/reference` and the existing read-only mount. No
per-event disk access or external lookups are performed. Required columns,
numeric/non-empty keys, duplicate keys and malformed rows are validated. Unknown
or null codes have null labels, while defined seed values such as “Not available”
and “Not defined” are preserved. Raw codes remain available even when unknown.

Submit either derived job with `--pyFiles /opt/flink/jobs`. This distributes the
`common` package to Python workers as well as making it available to the client;
no extra shared-module mount or container restart is needed. Seed changes take
effect on the next operator initialization.

Gap event reference semantics:

- `AIS_GAP_DETECTED`: `ship_type`, `ship_type_name`, `ship_category`,
  `navigation_status`, and `navigation_status_name` describe the last observation
  before silence.
- `AIS_GAP_ENDED`: ship fields describe the resumed observation. Navigation fields
  are explicitly `previous_navigation_status` / `previous_navigation_status_name`
  and `resumed_navigation_status` / `resumed_navigation_status_name`. A null resumed
  code stays null; it is not replaced with the pre-gap code. Existing source
  timestamps and processing-time duration semantics are unchanged.

Feature windows emit `ship_type`, `ship_type_name`, `ship_category`,
`last_navigation_status`, and `last_navigation_status_name` from the observation
with the greatest assigned event timestamp inside that window. Ship type is also
the last observed value, not assumed immutable. Later null values replace earlier
known values. For equal millisecond timestamps, the last processed observation
wins (the right accumulator wins a tie when merging). Original position counts
and speed statistics still aggregate every accepted observation as before.

The existing derived topics can contain older records without these added fields;
those historical records are retained. The smoke job remains an independent
learning example. The reusable `scripts/delete-kafka-topic.sh <topic>` helper
requires an explicit topic and refuses to delete the configured raw AIS source.

## Vessel AIS gap detection

`jobs/ais_gap_detector.py` is a separate, unbounded processing-time milestone. It
reads raw AIS directly and enriches its derived events from the shared seeds.
Configure these settings in local `.env` (the example keeps a ten-minute timeout):

```ini
FLINK_GAP_SOURCE_TOPIC=ais.positions
FLINK_GAP_TOPIC=ais.vessel.gaps
FLINK_GAP_GROUP_ID=ais-pyflink-gap-detector
FLINK_GAP_TIMEOUT_SECONDS=600
```

The existing `KAFKA_BOOTSTRAP_SERVERS` is reused. These settings and
`FLINK_REFERENCE_DIR` are required;
the timeout must be a positive integer number of seconds and the source and sink
must differ. Compose passes the gap settings to both Flink containers. Keep the
local and example timeout at `600` (ten minutes).

### State and timers

After parsing JSON, `keyBy(MMSI)` routes observations for the same vessel to the
same keyed operator. `GapDetector`, a `KeyedProcessFunction`, obtains a
Flink-managed `ValueState` handle in `open()`. Flink scopes this state and its
timers to the current MMSI: updating vessel A cannot overwrite vessel B's state.
There is no global Python dictionary of vessels.

Every valid event cancels that MMSI's previous processing-time timer, stores its
latest name, source `msgtime`, coordinates, observation processing time and timer
deadline, then registers `current_processing_time + timeout`. The source timestamp
is preserved exactly, including any original timezone representation. Optional
fields may be absent or null; the latest event's values are used.

At the deadline, the operator verifies that the timer is still current and emits
one `AIS_GAP_DETECTED` JSON event. It retains the last observation and marks the
keyed state with the detection processing time. The timer timestamp is cleared;
no recurring timer is registered, so continued silence does not repeat detection.

The first subsequent message for that MMSI emits exactly one `AIS_GAP_ENDED`
event, with `previous_event_msgtime`, `resumed_event_msgtime`, `gap_detected_at`,
`gap_ended_at`, and `gap_duration_seconds`. Duration is the full observed silence:
`(recovery_processing_time - last_observed_processing_time) / 1000`, including the
ten-minute timeout before detection. It is not derived from AIS timestamps or
just from the time since the detection alert. AIS timestamps remain unchanged,
even if a resumed event's source timestamp is older. The recovery name uses the
resumed name when non-null, otherwise the pre-gap name; both may be null.

Recovery immediately replaces the active-gap state with the resumed observation
and registers a fresh ten-minute timer. Messages for a vessel that never entered
a gap do not emit recovery events. Detection and recovery logs record the MMSI
and processing times in epoch milliseconds for timeline inspection. Active-gap
state is retained until recovery; this milestone adds no TTL, so vessels that
never return continue to occupy a small keyed-state entry.

`AIS_GAP_DETECTED` means **this pipeline has not observed an AIS message for N
seconds**. It does not establish that a vessel disappeared: receiver coverage,
source delays, or pipeline backpressure can also produce silence. Processing time
is the Flink operator's clock; `gap_detected_at` is the callback processing time
in UTC, which may be later than the timer deadline. Event-time/watermark processing
is intentionally deferred to the next milestone.

Malformed JSON, non-object JSON, and missing/invalid MMSIs are dropped from this
job with a warning that does not log the message payload. Accepted MMSIs are
integers or digit strings in `1..999999999`; booleans, floats, zero, and other
values are rejected. Numeric strings are normalized to integer keys. This avoids
creating a shared `None` key without filtering by vessel class. This job's explicit
drop policy does not change the smoke job.

### Submit and inspect

After applying the updated Compose environment to the Flink containers, list
running jobs first and do not submit another `AIS vessel gap detector` while one
is already running. Refreshing containers interrupts existing jobs; if keeping
another job running, pass the new settings with `docker compose exec -e` instead.
Only the submission process needs to read them; the timeout is serialized into
the operator sent to the TaskManager.

```sh
docker compose exec -T flink-jobmanager /opt/flink/bin/flink list -r
gap_topic=$(docker compose --profile streaming config --format json \
  | jq -r '.services["flink-jobmanager"].environment.FLINK_GAP_TOPIC')
./scripts/create-kafka-topic.sh "$gap_topic"
docker compose exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d --pyFiles /opt/flink/jobs -py /opt/flink/jobs/ais_gap_detector.py
```

The job starts at latest Kafka offsets and only monitors vessels observed after
submission. It can run alongside the feature job using the available TaskManager slots. Inspect the job at <http://localhost:8082> and configured source/gap topics
in Kafbat at <http://localhost:8081>. Check that source offsets keep advancing and
wait at least one timeout for naturally silent vessels; do not inject artificial
records into the shared AIS source.

The one-detection/one-recovery rule applies within an uninterrupted job execution.
This milestone does not enable checkpoints or transactional Kafka delivery: state is not durable
across a fresh submission, and exactly-once delivery across failures is not
promised. No ClickHouse writes or event-time state are added.

```sh
python3 -m unittest discover -s flink/tests -p 'test_*.py' -v
# Stop explicitly when finished:
docker compose exec -T flink-jobmanager /opt/flink/bin/flink cancel <job-id>
```

## Vessel features: event time, watermarks and sliding windows

`jobs/ais_vessel_features.py` is an independent Kafka-to-Kafka job. It parses AIS
JSON, assigns timestamps from `msgtime`, keys by MMSI, and incrementally aggregates
5/15/30/60-minute event-time windows, all sliding every five minutes. Parsing and
watermark assignment happen once; the keyed stream feeds four window branches
whose outputs are unioned into one Kafka sink. It shares only the reference loader
with the gap job; their time semantics remain independent.

Configure these non-secret settings in `.env`, using `.env.example`:

```ini
FLINK_FEATURE_SOURCE_TOPIC=ais.positions
FLINK_FEATURE_TOPIC=ais.vessel.features
FLINK_FEATURE_GROUP_ID=ais-pyflink-vessel-features-multi-window
FLINK_FEATURE_WINDOWS_MINUTES=5,15,30,60
FLINK_FEATURE_SLIDE_MINUTES=5
FLINK_FEATURE_WATERMARK_SECONDS=30
FLINK_FEATURE_IDLE_SECONDS=60
```

`KAFKA_BOOTSTRAP_SERVERS` is shared with the other jobs. All settings are required;
window sizes and slide must be positive integer minutes. The comma-separated
window list must be non-empty, contain no duplicates, and each size must be at
least the slide and divisible by it. Idle duration is positive integer seconds;
watermark delay may be zero or a positive integer number of seconds.
The source and sink must differ. Compose passes
these settings to the Flink containers. The additional `flink-feature-taskmanager`
provides capacity for the derived-stream jobs and optional smoke job, using the same TaskManager
configuration via a YAML anchor. It adds capacity without restarting the original
TaskManager or either running job; Flink chooses placement of tasks on available
slots.

### Time and aggregation semantics

- **Processing time** is the operator's clock when a record is handled. The gap
  detector still uses processing-time timers because it must detect silence even
  when no new AIS event timestamps arrive.
- **Event time** comes from the AIS `msgtime` field, independently of arrival time.
  ISO-8601 timestamps must have an explicit timezone; equivalent offsets normalize
  to the same epoch milliseconds. Output boundaries use UTC and a trailing `Z`.
- A **watermark** expresses event-time progress. Bounded out-of-orderness allows
  records to arrive out of timestamp order. With the configured delay, Flink emits
  watermarks based on the greatest observed timestamp minus 30 seconds (and its
  millisecond boundary adjustment), rather than using the machine's current time.
- A **sliding window** overlaps adjacent windows when its size exceeds its slide.
  All four sizes end on UTC five-minute boundaries. At 18:10, the intervals are
  `[18:05,18:10)`, `[17:55,18:10)`, `[17:40,18:10)`, and `[17:10,18:10)`.
  An event at exactly 18:10 belongs to windows containing that instant, never
  windows ending at 18:10. The 5m/5m branch is equivalent to a tumbling window.
  Windows close when the watermark passes their final millisecond; output
  normally follows the end plus watermark delay and scheduling/buffering.
  Only non-empty vessel windows produce records; this is an event-time cadence,
  not a wall-clock heartbeat.

Watermarks are assigned **after** JSON validation. PyFlink 2.2.1's `with_idleness`
marks an inactive timestamp-assignment subtask idle after the configured timeout,
so it cannot indefinitely hold back the downstream minimum watermark while other
subtasks advance. This is subtask-level idleness, not a per-Kafka-partition
watermark generator. At the current parallelism of one, all Kafka partitions feed
one assigner: an idle partition cannot pin its watermark while other partitions
supply newer timestamps. If the whole input becomes idle, idleness does not
manufacture advancing event time; unfinished windows wait for newer event data.

The job uses `AggregateFunction` to keep only position count, non-null speed count,
sum, minimum and maximum plus the latest event-time observation in each keyed
window. `ProcessWindowFunction` adds `window_minutes`, boundaries and reference labels,
loading the shared seeds once per operator initialization. Output fields are:

```text
mmsi, window_minutes, window_start, window_end, position_count, avg_speed, min_speed, max_speed
ship_type, ship_type_name, ship_category, last_navigation_status, last_navigation_status_name
```

Consumers should select `window_minutes` explicitly. Longer windows overlap,
so summing their position counts across consecutive outputs counts observations
more than once. The same vessel and end boundary can have four different sizes.

Every valid AIS message counts, including messages with missing or null
`speedOverGround`. Speed statistics exclude those values. All-null speed windows
emit null for min/avg/max; zero is a valid speed. Non-numeric, boolean, or non-finite
speeds are treated as missing with a warning, without dropping the position.
Finite source values are retained without sentinel normalization, and statistics
use the source speed units. No deduplication or geographic filtering is applied.

Malformed JSON, non-object JSON, missing/invalid MMSI, or missing/invalid `msgtime`
is dropped with a warning that does not include the payload. MMSI follows the gap
job's policy: integer or digit string in `1..999999999`, excluding booleans and
floats. Timezone-less or date-only timestamps are rejected.

There is no extra allowed lateness in this milestone: Flink's default drops
records whose window has already been finalized by the watermark. A delayed
record is not necessarily dropped simply because it is over 30 seconds old; its
window must already be closed. There is no late-data side output or correction
stream yet. In overlapping windows, a late event can be excluded from a closed
short window while still contributing to open longer windows. Incremental output
therefore reflects accepted records, and may differ from a later batch aggregation over all raw Kafka records when late data
arrives. Checkpoints and exactly-once delivery are not introduced here.

### Submit and validate

Check available slots before submission. Add the optional TaskManager only if
capacity is insufficient, without restarting existing jobs:

```sh
curl -fsS http://localhost:8082/overview
# Only if additional capacity is needed:
# docker compose --profile streaming up -d --no-deps flink-feature-taskmanager
docker compose exec -T flink-jobmanager /opt/flink/bin/flink list -r
feature_topic=$(docker compose --profile streaming config --format json \
  | jq -r '.services["flink-jobmanager"].environment.FLINK_FEATURE_TOPIC')
./scripts/create-kafka-topic.sh "$feature_topic"
# When the JobManager already has the new environment:
docker compose exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d --pyFiles /opt/flink/jobs -py /opt/flink/jobs/ais_vessel_features.py
```

If JobManager predates these environment additions, pass the configured variables
using `docker compose exec -e` at submission instead of recreating it. The job
serializes its configuration into the tasks; it does not need an existing worker
to reload `.env`. Do not submit a duplicate feature job.

Inspect <http://localhost:8082> for `AIS vessel multi-window event-time features` and the
configured output topic in Kafbat at <http://localhost:8081>. The source starts at
latest offsets: startup windows can be partial for **each size**. A complete
60-minute validation requires a window whose start is after the job began
consuming, then waiting until its end plus watermark delay. Do not mistake a
60-minute-labelled startup result for a full hour of captured data. For an
independent check, select a complete subsequent UTC window, group raw AIS records by
MMSI, count all valid messages and calculate speed statistics over non-null
speeds. Compare boundaries, count, minimum, maximum and average, accounting for
late records excluded by the watermark. Wait for window closure rather than
shortening the configured five-minute interval for validation.

```sh
.venv/bin/python -m unittest discover -s flink/tests -p 'test_*.py' -v
# Exercise the installed 2.2.1 assigner, event-time trigger and zero-lateness check:
docker exec -i -e PYTHONPATH=/opt/flink/jobs ais-flink-jobmanager python - \
  < flink/tests/test_vessel_windows.py
docker compose exec -T flink-jobmanager /opt/flink/bin/flink cancel <feature-job-id>
```
