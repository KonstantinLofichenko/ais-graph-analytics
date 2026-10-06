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

## Reference-data enrichment

`jobs/ais_reference_enrichment.py` is the second milestone. It preserves the raw
JSON fields and appends `ship_type_name`, `ship_category`, and
`navigation_status_name`. It looks up the raw `shipType` and `navigationalStatus`
codes in the existing dbt seeds, without a database or API request:

- `dbt/seeds/ais_ship_types.csv`: `ship_type`, `ship_type_name`, `ship_category`.
- `dbt/seeds/ais_navigational_status.csv`: `navigational_status`,
  `navigational_status_name` (exposed as `navigation_status_name` in Kafka).

Both Flink containers mount `dbt/seeds` read-only. Each map operator reads the two
CSVs once in `open()`, including after a restart, rather than on every event.
Restart the job after reference data changes. Unknown, missing, or null codes
produce JSON `null` labels; the original codes remain unchanged. Defined seed
labels such as “Not available” and “Not defined” are retained. Malformed JSON
raises an error, as in the smoke job; this milestone does not silently drop it or
provide a dead-letter topic.

Add these non-secret settings to local `.env` (examples in `.env.example`):

```ini
FLINK_ENRICHED_TOPIC=ais.positions.enriched
FLINK_REFERENCE_DIR=/opt/flink/reference
FLINK_ENRICHMENT_GROUP_ID=ais-pyflink-reference-enrichment
```

The job also requires the shared `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`.
Compose passes these settings into both containers. The sink must differ from
the source to prevent an enrichment feedback loop.

With no other Flink jobs running, refresh the containers to apply the mount and
settings, create the configured sink using the normal topic helper, then submit:

```sh
docker compose --profile streaming up -d --no-deps flink-jobmanager flink-taskmanager
enriched_topic=$(docker compose exec -T flink-jobmanager printenv FLINK_ENRICHED_TOPIC)
./scripts/create-kafka-topic.sh "$enriched_topic"
docker compose exec -T flink-jobmanager \
  /opt/flink/bin/flink run -d -py /opt/flink/jobs/ais_reference_enrichment.py
docker compose exec -T flink-jobmanager /opt/flink/bin/flink list -r
```

The topic helper's optional argument selects another topic; without it, the
helper still creates `KAFKA_TOPIC`. Creation is idempotent, using the existing
three partitions and replication factor one.

This unbounded job starts at latest offsets, uses a separate consumer group, and
needs live source events after submission. It has no event-time/watermark or keyed
state logic. It is a learning milestone, with no checkpoint/recovery or exactly-once
guarantee added here. Inspect the job at <http://localhost:8082> and the configured
raw/enriched topics in Kafbat at <http://localhost:8081>. Do not submit a second
copy while the first is running. Cancel explicitly when finished:

```sh
docker compose exec -T flink-jobmanager /opt/flink/bin/flink cancel <job-id>
```
