# PyFlink streaming jobs

For the raw, streaming, graph and planned historical paths, see the shared
[architecture overview](../docs/README.md#2-architecture) and
[canonical diagram](../docs/assets/architecture.svg). This guide owns job
configuration, timing semantics and recovery operations. Flink processes continuous
Kafka streams; the planned PySpark/Iceberg/Trino historical path is separate.

## Automatic startup and recovery

```sh
docker compose --profile streaming up -d
docker compose logs -f flink-job-submitter
curl -fsS http://localhost:8082/jobs/overview
```

The one-shot `flink-job-submitter` starts the existing gap detector and multi-window
vessel-feature jobs, then exits with code 0 only when both REST statuses are
`RUNNING`. It uses the existing Flink image, Python jobs, environment settings,
reference seeds, and `--pyFiles /opt/flink/jobs`. The smoke job remains manual.
The preserved settings are gap timeout `600` seconds, feature windows `5,15,30,60`
minutes, slide `5` minutes, watermark `30` seconds, and idleness `60` seconds.

JobManager health, two registered TaskManagers, a Kafka protocol handshake against
the configured brokers, and at least one free task slot per missing job gate
submission. At the jobs' current parallelism of one, their default slot-sharing
group requires one slot each. Polling retries readiness every two seconds for up
to 300 seconds; this is a readiness loop, not a fixed startup delay. A failure
exits nonzero and is visible in the submitter logs; rerun after fixing readiness.
Kafka output topics retain the existing broker auto-creation behavior; the topic
helper below can also create them explicitly.

A shared named volume records Job IDs and serializes submitter executions with a
file lock. IDs are persisted **before** submission and passed as
`$internal.pipeline.job-id` (the installed Flink 2.2.1 internal fixed-ID option).
Thus a retry after an interrupted submission reuses the
same ID. Existing jobs, including transitional and restarting jobs, are identified
by their recorded ID. For jobs submitted manually before the registry existed,
the submitter checks the display name **and** source/operator plan before adopting
their IDs. That compatibility check is not a cryptographic application identity;
do not run a different job with the same name and operator plan. Multiple matching
active jobs cause a failure instead of another submission. Terminal jobs receive
fresh IDs; a completely new cluster can reuse IDs from the registry.

Repeated `up -d` restarts the exited submitter and checks the jobs without creating
copies. To reconcile explicitly (also useful after a manual cancellation):

```sh
docker compose --profile streaming run --rm --no-deps flink-job-submitter
```

Flink uses a [fixed-delay restart strategy](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/task_failure_recovery/):
up to ten attempts, ten seconds apart.
While the cluster remains alive, this retries failed task execution without the
submitter creating another job. After the retries are exhausted, fix the cause
and rerun the submitter. The submitter is a startup client, not a continuous
supervisor. An abrupt Docker daemon/container failure does not automatically
invoke it; use Compose startup/reconciliation again when the cluster is ready.

To restart only Flink, leaving Kafka, ClickHouse, Neo4j, Airflow and Metabase alone:

```sh
docker compose --profile streaming restart \
  flink-jobmanager flink-taskmanager flink-feature-taskmanager
# depends_on restart:true triggers the submitter for explicit Compose restarts.
# Alternatively, recreate the Flink services (e.g. after configuration changes):
docker compose --profile streaming up -d --no-deps --force-recreate \
  flink-jobmanager flink-taskmanager flink-feature-taskmanager flink-job-submitter
```

### Durable checkpoints and restore

Both derived jobs enable `EXACTLY_ONCE` checkpoint mode, filesystem storage,
retention on cancellation, and a single concurrent checkpoint. Settings default to:

```ini
FLINK_CHECKPOINT_INTERVAL_SECONDS=60
FLINK_CHECKPOINT_TIMEOUT_SECONDS=120
FLINK_CHECKPOINT_MIN_PAUSE_SECONDS=10
FLINK_CHECKPOINT_DIR=file:///opt/flink/checkpoints
FLINK_RESTART_ATTEMPTS=10
FLINK_RESTART_DELAY_SECONDS=10
```

Compose mounts **`./flink/checkpoints`** into the JobManager, both TaskManagers and
submitter at `/opt/flink/checkpoints`. The one-shot `flink-checkpoint-init` ensures
Flink can write the root directory. Checkpoint contents are gitignored. If changing
the URI, update every mount to expose the same host directory at that absolute
container path. Local filesystem storage works only while all workers share this
host mount; it is not multi-host HA, an off-host backup, or protection against
host/disk loss. Never delete checkpoint files while jobs use them. Retained files
from retired executions require operator-managed cleanup after confirming they
are no longer recovery dependencies. Keep the named submitter registry volume.

Within a live cluster, Flink's bounded fixed-delay strategy restores the latest
completed checkpoint on task failure. Following complete JobManager loss, the
submitter finds the newest finalized `_metadata` under the logical job's
`gap/` or `features/` checkpoint directory and submits with `-s` and
`-claimMode NO_CLAIM`. The original snapshot is retained; the restored execution's
first checkpoint is independent of it. Only missing jobs are submitted. A chosen
snapshot that fails restoration never causes a silent fallback to empty state.
If no completed snapshot exists (including the initial deployment), the submitter
logs a warning and starts fresh. Job IDs prevent duplicates; checkpoint metadata
and state files preserve state. Keep job-specific directories isolated; do not
copy unrelated snapshots into them or modify file timestamps to change ordering.

KafkaSource participates in checkpoints. Restored offsets take precedence over
`KafkaOffsetsInitializer.latest()`, which applies only to a fresh start. The
existing consumer group IDs are unchanged; committed Kafka group offsets are not
a substitute for the checkpoint. Kafka must retain the checkpoint's required
records throughout the outage. Source retention and topic deletion can invalidate
recovery.

Restored state includes MMSI `ValueState`, pending processing-time timers and
already-detected gaps, plus each 5/15/30/60-minute feature window accumulator and
its event-time timers. Overdue processing-time timers fire when execution resumes,
so the actual detection timestamp can be later than the saved deadline. Event
watermarks resume as new source records arrive; an idle source does not manufacture
event-time progress. Stable source, keyed process, window and sink UIDs identify
operators. Do not change state types, UIDs, parallelism/window configuration or
operator topology without a separate state-compatibility migration.

Both Kafka sinks use **AT_LEAST_ONCE** to flush output at checkpoints. Checkpoint
state has exactly-once semantics; output and ClickHouse ingestion do not have an
end-to-end exactly-once guarantee. Records emitted after the restored snapshot
may be replayed, including gap events. A checkpoint containing an already-detected
gap does not create a new detection merely because the cluster restarted.
Transactional Kafka sinks and ClickHouse deduplication are outside this change.

In the Flink UI (<http://localhost:8082>), open each job's **Checkpoints** page to
inspect completed/failed checkpoints, external paths and the restored checkpoint.
REST provides `/jobs/<job-id>/checkpoints` and `/checkpoints/config` under that job.
Before restarting, confirm both jobs have a completed checkpoint. Manual restore
uses the same code and configuration, and must not race the submitter:

```sh
# Check no copy of this job is active before manual submission.
docker compose exec -T flink-jobmanager /opt/flink/bin/flink run -d \
  -s file:///opt/flink/checkpoints/gap/<job-id>/chk-<id>/_metadata \
  -claimMode NO_CLAIM --pyFiles /opt/flink/jobs -py /opt/flink/jobs/ais_gap_detector.py
# Features use their features/ checkpoint and ais_vessel_features.py.
```

Reproducible opt-in validation (run from repository root):

```sh
python3 -m unittest discover -s flink/tests -p 'test_*.py' -v
# Private topics/groups and a separate temporary Flink cluster; production untouched.
# Uses a 30s gap and 5s checkpoint only in the test to shorten validation.
python3 flink/tests/integration_recovery.py
# Explicitly RECREATES ONLY production Flink services, then checks REST restores,
# duplicate safety and growing analytics counts; allow up to 15 minutes for gaps.
python3 flink/scripts/verify_live_recovery.py
```

The private test snapshots an already-detected vessel and a vessel with a pending
timer, stops its whole cluster, publishes a record during the outage, and restores
both jobs. It checks one detection/recovery for the first vessel, timer detection
for the second, all four feature windows retaining old plus new observations,
checkpoint restore counters and concurrent submitter idempotency. Reports are
written to `/tmp/ais-flink-isolated-recovery.json` and
`/tmp/ais-flink-production-recovery.json`. The production script records checkpoint
paths, Job IDs, restored status, analytics counts and unchanged start times for
Kafka, ClickHouse, Neo4j, Airflow and Metabase.

Validation on 2026-10-08 with Flink 2.2.1 passed both the private state/timer/window
exercise and the production full-cluster restart. Both production jobs reported
restoration and completed new checkpoints without failures; both analytics counts
increased and unrelated services retained their start times.

Manual submission commands below are fresh-start examples for debugging; without
`-s`, they do not restore a retained checkpoint. Prefer the submitter for normal
startup/recovery, or use the explicit manual restore example above. List REST jobs
first; do not manually submit a second copy of an active or transitioning job.
Prefer the submitter to start only missing jobs using the resolved Compose
configuration. Manual submissions outside this registry's lock must not race the
submitter.

## PyFlink Kafka smoke job

The job reads required environment variables, with no broker/topic defaults in
Python. Configure these in the repository's local `.env` using `.env.example`:

```ini
KAFKA_BOOTSTRAP_SERVERS=ais-kafka:29092
KAFKA_TOPIC=ais.positions
FLINK_SINK_TOPIC=ais.positions.pyflink.tmp
```

`KAFKA_TOPIC` is shared with the AIS producer and topic-creation helper; a separate
`FLINK_SOURCE_TOPIC` is unnecessary. Compose explicitly passes all three settings
to the Flink JobManager and TaskManagers. Missing or empty values fail before job submission.

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

Feature windows also emit `name`: the latest non-null vessel name by AIS event
time inside the window. Name selection has its own timestamp and is independent
of the reference-code observation; later null/missing names do not erase an
earlier name. All-null/missing names yield null. Empty strings are non-null and
are retained. Equal assigned millisecond timestamps use the last processed
non-null name; the right accumulator wins a merge tie.

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
must differ. Compose passes the gap settings to the Flink JobManager and TaskManagers. Keep the
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
in UTC, which may be later than the timer deadline. The gap job retains processing-time semantics; the separate feature job uses
event-time watermarks and windows.

Malformed JSON, non-object JSON, and missing/invalid MMSIs are dropped from this
job with a warning that does not log the message payload. Accepted MMSIs are
integers or digit strings in `1..999999999`; booleans, floats, zero, and other
values are rejected. Numeric strings are normalized to integer keys. This avoids
creating a shared `None` key without filtering by vessel class. This job's explicit
drop policy does not change the smoke job.

### Submit and inspect

Automatic startup normally handles submission. For manual debugging, after
applying the updated Compose environment to the Flink containers, list
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

A fresh job starts at latest Kafka offsets and monitors vessels observed after
submission. A restored job resumes checkpointed offsets and vessel state. It can run alongside the feature job using the available TaskManager slots. Inspect the job at <http://localhost:8082> and configured source/gap topics
in Kafbat at <http://localhost:8081>. Check that source offsets keep advancing and
wait at least one timeout for naturally silent vessels; do not inject artificial
records into the shared AIS source.

The one-detection/one-recovery rule is retained in checkpointed keyed state.
Output since the last restored checkpoint can replay with at-least-once delivery;
see the recovery guarantees above. ClickHouse ingestion and gap payloads are unchanged.

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
mmsi, name, window_minutes, window_start, window_end, position_count, avg_speed, min_speed, max_speed
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
arrives. Checkpoints preserve the partial accumulators; Kafka output remains
at-least-once as documented above.

### Submit and validate

Automatic startup includes both TaskManagers. For manual debugging, check
available slots before submission. Add the second TaskManager only if
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
latest offsets for a fresh start: startup windows can be partial for **each size**.
Restoration instead preserves partial windows and resumes checkpointed offsets. A complete
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

The derived Kafka outputs are persisted for BI in
`analytics.ais_vessel_features` and `analytics.ais_vessel_gap_events`; Kafka Engine
tables and ingestion views stay in `raw`. See [ClickHouse ingestion](../clickhouse/README.md).
Historical ClickHouse feature rows from before the name-field deployment retain
NULL names. The initial checkpointing deployment starts fresh because the older
executions had no checkpoints. Subsequent compatible submissions through the
submitter restore retained state rather than empty feature windows.

## Metabase near-real-time tabs

The [Norway Port Graph Analytics dashboard](../metabase/README.md) has two tabs
backed by these jobs: **Near Real-Time Vessel Analytics** reads the latest
completed windows from `analytics.ais_vessel_features`, and **Near Real-Time AIS
Gap Monitoring** reads lifecycle events from `analytics.ais_vessel_gap_events`.
Both tables expose `activity_date` first: `toDate(window_end)` for features,
`toDate(gap_detected_at)` for detections and `toDate(gap_ended_at)` for recoveries.
Dates use UTC; the gap date is nullable if an end timestamp is absent.

The dashboard's Window minute selector is mapped to the five feature charts/KPIs;
the Latest Vessels Features table currently retains its own five-minute default.
Gap cards use rolling one-hour/24-hour UTC ranges. Neither new tab is connected to
the dashboard's historical Date/Last Seen filters. Feature output follows
watermark closure, and gap detection means pipeline-observed silence, not proof
of vessel disappearance. Recovery can replay output with at-least-once delivery,
so dashboard event counts are not deduplicated lifecycle counts.
