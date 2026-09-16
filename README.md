# AIS Graph Analytics

AIS Graph Analytics collects BarentsWatch Automatic Identification System (AIS) vessel positions, streams them through Kafka and Kafka Connect, writes the current vessel state to Neo4j, and stores complete raw position history in ClickHouse.

## Architecture

```text
BarentsWatch AIS -> Kafka topic ais.positions -> Kafka Connect 5.5.3 -> Homebrew Neo4j
                          -> ksqlDB
                          -> ClickHouse / dbt
                          -> Metabase
```

Neo4j runs directly on macOS through Homebrew. Kafka, Kafka Connect, ksqlDB, and Kafbat UI run in Docker Compose on the shared `ais-network` network. Because Kafka Connect runs inside Docker, its Neo4j URI must be `bolt://host.docker.internal:7687`, never `localhost:7687`.

The repository is organized by platform concern:

- `producers/ais/`: AIS ingestion producers
- `airflow/dags/`: orchestration workflows
- `airbyte/`: source and destination connector configuration
- `clickhouse/init/`: ClickHouse initialization SQL
- `neo4j/cypher/`: graph schema and Cypher queries
- `neo4j/gds/`: Graph Data Science workflows
- `neo4j/connectors/`: local Kafka Connect connector configurations
- `dbt/`: analytical transformations
- `metabase/`: dashboard and collection exports
- `docs/architecture/`: architecture documentation

## Versions and ports

| Component | Version | Address | Purpose |
| --- | --- | --- | --- |
| Kafka | `apache/kafka:4.3.1` | `localhost:9092` / `ais-kafka:29092` | Host / Docker listeners |
| Kafka Connect | `confluentinc/cp-kafka-connect:8.3.1` | `http://localhost:8083` | Connector REST API |
| ksqlDB | `confluentinc/cp-ksqldb-server:8.3.1` | `http://localhost:8088` | Streaming SQL REST API |
| Kafbat UI | `ghcr.io/kafbat/kafka-ui:latest` | `http://localhost:8081` | Kafka UI |
| Neo4j Kafka Connector | `5.5.3` | Docker plugin path | Neo4j sink connector |
| Homebrew Neo4j | `2026.07.1` | `neo4j://localhost:7687` | Graph database |

Kafka advertises `ais-kafka:29092` to Docker services and `localhost:9092` to macOS. Kafka Connect mounts `./plugins` at `/usr/share/java/plugins`; the Neo4j connector JAR is manually stored there.

## ClickHouse raw layer

ClickHouse stores the complete historical AIS event stream in `raw.ais_positions`. Neo4j stores graph entities and the latest known vessel state; ClickHouse stores every AIS observation for historical and time-series analysis. The existing `ais` database remains unchanged, and staging and marts databases are not created yet.

The raw table uses monthly event-time partitions:

```sql
PARTITION BY toYYYYMM(msgtime)
```

This keeps large historical datasets manageable by event month rather than ingestion month. Its primary access pattern is vessel movement over a time range, so it is sorted by:

```sql
ORDER BY (mmsi, msgtime)
```

The initialization file is [clickhouse/init/01_raw.sql](clickhouse/init/01_raw.sql). Init scripts normally run only when ClickHouse initializes a new data volume, so an existing volume must be validated or updated explicitly.

## Kafka to ClickHouse streaming

AIS events flow through the ClickHouse pipeline as follows:

```text
Kafka ais.positions
  -> raw.ais_positions_kafka (Kafka Engine consumer interface)
  -> raw.ais_positions_mv (field and timestamp transformation)
  -> raw.ais_positions (durable ReplacingMergeTree history)
```

The Kafka Engine table is a consumer interface, not persistent analytical storage. The materialized view maps the incoming camelCase AIS fields and parses `msgtime`; the ReplacingMergeTree table stores every event durably while converging duplicate versions. Neo4j is a separate Kafka consumer for graph entities and latest vessel state, so ClickHouse event counts and Neo4j `Vessel` counts are intentionally different.

`raw.ais_positions` uses `ReplacingMergeTree(ingested_at)` so overlapping historical and live ingestion can converge to one logical event per `(mmsi, msgtime)`. Physical duplicate parts may exist temporarily before background merges; use `FINAL` when validating the logical view. The manual migration is [clickhouse/migrations/001_replacing_ais_positions.sql](clickhouse/migrations/001_replacing_ais_positions.sql). Pause AIS producers and ClickHouse Kafka ingestion before running it. It keeps the pre-migration table as `raw.ais_positions_merge_backup` and does not run automatically from Docker startup.

Run the migration manually from the repository root after pausing producers and ClickHouse ingestion:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/001_replacing_ais_positions.sql
```

After migration, validate logical uniqueness with:

```sql
SELECT
  count() AS physical_rows,
  uniqExact((mmsi, msgtime)) AS logical_events
FROM raw.ais_positions FINAL;

SELECT
  mmsi,
  msgtime,
  count()
FROM raw.ais_positions
GROUP BY mmsi, msgtime
HAVING count() > 1
ORDER BY count() DESC
LIMIT 20;
```

Useful validation queries:

```sql
SHOW TABLES FROM raw;

SHOW CREATE TABLE raw.ais_positions_kafka;

SHOW CREATE TABLE raw.ais_positions_mv;

SELECT count()
FROM raw.ais_positions;

SELECT
  count(DISTINCT mmsi) AS vessels,
  count() AS events
FROM raw.ais_positions;

SELECT
  mmsi,
  count() AS event_count
FROM raw.ais_positions
GROUP BY mmsi
HAVING event_count > 1
ORDER BY event_count DESC;
```

## Configuration

Copy `.env.example` to `.env` and provide credentials only in your local environment:

```sh
cp .env.example .env
```

Never commit `.env`, passwords, tokens, or local connector configurations containing credentials. Use [neo4j/connectors/ais-sink.example.json](neo4j/connectors/ais-sink.example.json) as the safe template and copy it to `ais-sink.json` locally.

## Local Development Startup

The exact startup order after a reboot is:

1. Start Homebrew Neo4j:

  ```sh
  brew services start neo4j
  neo4j status
  ```

2. Start Docker infrastructure:

  ```sh
  docker compose up -d
  ```

  Or run `./scripts/start-local.sh` to start Neo4j, wait for the Compose healthchecks, create the topic, and print URLs.

3. Verify services:

  ```sh
  docker compose ps
  ```

  Expected: `ais-kafka` healthy, `kafka-connect` healthy, `ksqldb-server` healthy, and `kafbat-ui` Up.

4. Create `ais.positions`:

  ```sh
  ./scripts/create-kafka-topic.sh
  ```

  This idempotently creates three partitions with replication factor `1` using `--if-not-exists`.

5. Verify the topic:

  ```sh
  docker exec ais-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --list
  ```

  The output should include `ais.positions`.

6. Verify the Kafka Connect plugin:

  ```sh
  curl -s http://localhost:8083/connector-plugins | jq '.'
  ```

  Expected classes include `org.neo4j.connectors.kafka.sink.Neo4jConnector` and `org.neo4j.connectors.kafka.source.Neo4jConnector`, version `5.5.3`.

7. Verify Neo4j is reachable from Kafka Connect:

  ```sh
  docker exec kafka-connect bash -c \
    'echo > /dev/tcp/host.docker.internal/7687 && echo "Neo4j reachable"'
  ```

8. Create the Neo4j constraint:

  ```cypher
  CREATE CONSTRAINT vessel_mmsi_unique IF NOT EXISTS
  FOR (v:Vessel)
  REQUIRE v.mmsi IS UNIQUE;
  ```

  Run it in Neo4j Browser or with `cypher-shell`.

9. Register the Neo4j sink connector using a local config file:

  ```sh
  cp neo4j/connectors/ais-sink.example.json neo4j/connectors/ais-sink.json
  # Edit the local file and replace its password placeholder.
  curl -X POST http://localhost:8083/connectors \
    -H 'Content-Type: application/json' \
    -d @neo4j/connectors/ais-sink.json
  ```

10. Check connector status:

   ```sh
   curl -s \
    http://localhost:8083/connectors/ais-neo4j-sink/status \
    | jq '.'
   ```

   Expected: connector state `RUNNING` and task state `RUNNING`.

11. Open the local interfaces:

   - Kafbat: [http://localhost:8081](http://localhost:8081)
   - Neo4j Browser: [http://localhost:7474/browser/](http://localhost:7474/browser/)
   - Kafka Connect REST: [http://localhost:8083](http://localhost:8083)
   - ksqlDB: [http://localhost:8088](http://localhost:8088)

## Neo4j model

The initial sink writes one `Vessel` node per MMSI:

```cypher
MERGE (v:Vessel {mmsi: event.mmsi})
SET v.name = event.name,
   v.shipType = event.shipType,
   v.latitude = event.latitude,
   v.longitude = event.longitude,
   v.speedOverGround = event.speedOverGround,
   v.courseOverGround = event.courseOverGround,
   v.trueHeading = event.trueHeading,
   v.navigationalStatus = event.navigationalStatus,
   v.stream = event.stream,
   v.lastSeen = datetime(event.msgtime)
```

It updates `name`, `shipType`, `latitude`, `longitude`, `speedOverGround`, `courseOverGround`, `trueHeading`, `navigationalStatus`, `stream`, and `lastSeen`. It does not create a `Position` node for every AIS event.

## Live AIS producer test

The producer runs continuously by default on macOS/Linux. Set `AIS_MESSAGE_LIMIT` to a positive integer for a bounded run; unset or empty means continuous. Existing `.env` files may still contain `AIS_MESSAGE_LIMIT=20`: remove it or use the explicit overrides below.

1. Make sure the local infrastructure is running:

  ```sh
  ./scripts/start-local.sh
  ```

2. Make sure the Neo4j uniqueness constraint exists:

  ```cypher
  CREATE CONSTRAINT vessel_mmsi_unique IF NOT EXISTS
  FOR (v:Vessel)
  REQUIRE v.mmsi IS UNIQUE;
  ```

3. Put the BarentsWatch credentials in the local `.env` file. The file is ignored by Git and must never be committed:

  ```dotenv
  BW_AIS_CLIENT_ID=...
  BW_AIS_CLIENT_SECRET=...
  ```

4. Install the producer dependencies:

  ```sh
  python3 -m pip install -r producers/ais/requirements.txt
  ```

5. Run the 20-message producer from the repository root:

  ```sh
  AIS_MESSAGE_LIMIT=20 python3 producers/ais/producer.py
  ```

  It requests an OAuth client-credentials token with scope `ais`, reads the response as streaming NDJSON, preserves the original AIS JSON fields, and uses the event MMSI as the Kafka key.

6. Verify Kafka messages with a Kafka 4.3-compatible consumer:

  ```sh
  docker exec -it ais-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic ais.positions \
    --from-beginning \
    --formatter-property print.key=true \
    --formatter-property key.separator=:
  ```

7. Verify the vessels written by the Neo4j sink:

  ```cypher
  MATCH (v:Vessel)
  RETURN
     v.mmsi,
     v.name,
     v.latitude,
     v.longitude,
     v.speedOverGround,
     v.lastSeen
  ORDER BY v.lastSeen DESC;
  ```

  ```cypher
  MATCH (v:Vessel)
  RETURN count(v) AS vesselCount;
  ```

Twenty Kafka messages do not necessarily produce twenty `Vessel` nodes because the Neo4j sink merges by MMSI.

## Continuous producer in Docker

With the existing infrastructure and topic running:

```sh
AIS_MESSAGE_LIMIT= docker compose --profile live up -d --build --no-deps ais-producer
docker compose logs --tail 30 -f ais-producer
```

The `live` profile keeps ordinary infrastructure startup from unexpectedly starting ingestion.
The service reuses root `.env` at runtime and overrides Kafka to `ais-kafka:29092`.
Its build context includes only the producer and requirements; credentials are never copied into the image.
Do not share expanded `docker compose config` output, which can contain secrets.
Airflow does not orchestrate this live service.

For continuous local mode use `AIS_MESSAGE_LIMIT= python3 producers/ais/producer.py`.
Ctrl-C stops the local producer. Stop Docker ingestion with:

```sh
docker compose stop ais-producer
```

Ctrl-C while following Docker logs only stops the log viewer.
SIGINT/SIGTERM close HTTP work and flush Kafka with a 35-second deadline; Compose allows 45 seconds.
The producer runs as a non-root user and restarts on failure, while a successful bounded run stays stopped.
No data-arrival healthcheck is used: quiet upstream periods are not evidence of failure.

OAuth uses `client_credentials` with scope `ais`. A fresh token is requested before `expires_in`
elapses, including during an idle stream. A 401 triggers renewal; rejection of a fresh token
fails clearly. Network errors, EOF, 429 and server errors reconnect with capped exponential
backoff and jitter. Numeric Retry-After delays are honored up to 300 seconds.
Permanent HTTP/configuration errors exit without logging credentials or response bodies.

Records retain the original JSON fields and MMSI key. Only acknowledged deliveries count
toward the limit. Kafka idempotence protects retries within a producer session; this is not
end-to-end exactly-once delivery across restarts or upstream reconnects. Delivery failures
stop the producer instead of silently skipping an event. The live API has no replay checkpoint,
so outages/reconnects can leave gaps or repeat upstream events.

For a bounded validation, stop other producers first, record Kafka partition end offsets,
then run with `AIS_MESSAGE_LIMIT=20`. The sum of end offsets should increase by exactly 20:

```sh
docker exec ais-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server ais-kafka:29092 --topic ais.positions --time -1
```

Compare `SELECT count() FROM raw.ais_positions` before and after ingestion; allow consumer lag.
In Neo4j compare `Vessel` properties and `lastSeen`, not just node count, because existing
vessels are updated. The Kafka Connect sink and its tasks should remain `RUNNING`.

Run deterministic lifecycle and failure tests without credentials:

```sh
python3 -m unittest discover -s producers/ais/tests -v
```

## Port visits and graph summaries

The Bergen pilot batch adds seven reference ports, infers visits from ClickHouse
AIS history, and stores `(Vessel)-[:VISITED {visitCount}]->(Port)` summaries in Neo4j.
Individual visits remain in ClickHouse; existing live Vessel properties are preserved.

```sh
.venv/bin/python -m pip install -r pipelines/port_visits/requirements.txt
.venv/bin/python pipelines/port_visits/run.py                 # read-only preview
.venv/bin/python pipelines/port_visits/run.py --apply --init  # first publication
```

See [the port-visit workflow](pipelines/port_visits/README.md) for thresholds,
reproducible windows, validation, retry behavior, and data limitations.
This milestone prepares the graph for GDS; similarity is not yet added.
A dedicated two-task Airflow DAG now downloads ports and runs this batch. See
[Airflow setup and usage](airflow/README.md).

## Country reference dimension

`analytics.countries` provides country names, ISO codes, region, and capital city
for enriching ports: join `analytics.ports.country` to `countries.country_code`
using the ISO alpha-2 code. It uses `ReplacingMergeTree(updated_at)` keyed by
`country_code`; read with `FINAL` for the latest logical row per country. The manual
[countries migration](clickhouse/migrations/002_countries.sql) creates the table and
adds a temporary Norway seed only if `NO` is absent. It does not run automatically
at Docker startup. Apply it from the repository root:

```sh
docker exec -i ais-clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
  < clickhouse/migrations/002_countries.sql
```

The planned flow is public country API → Airbyte raw ingestion → dbt transformation
→ `analytics.countries`. This migration supplies the reference table and seed;
Airbyte ingestion and dbt transformations for countries are not implemented yet.

## Shutdown

```sh
docker compose down
brew services stop neo4j
```

Or run `./scripts/stop-local.sh`. `docker compose down` does not stop Homebrew Neo4j because Neo4j runs directly on macOS.

## Troubleshooting

1. **Container name already in use**

  Old containers created manually with `docker run` can conflict with Compose. Check with `docker ps -a` and remove only obsolete AIS containers if necessary.

2. **`ais-network` already exists but was not created by Compose**

  An old manually created network may conflict with the Compose-owned network. Inspect it with `docker network inspect ais-network` and remove only the obsolete network after stopping the old containers.

3. **ksqlDB is unhealthy although `/info` returns `RUNNING`**

  `cp-ksqldb-server:8.3.1` does not contain `curl`. The correct healthcheck is `bash -c '</dev/tcp/127.0.0.1/8088'`.

4. **Kafka Connect is unhealthy**

  `cp-kafka-connect:8.3.1` does not contain `curl`. The correct healthcheck is `bash -c '</dev/tcp/127.0.0.1/8083'`.

5. **Kafka Connect cannot reach Neo4j**

  Do not use `localhost` from inside Docker. Use `bolt://host.docker.internal:7687` in the connector configuration.

6. **Neo4j plugin is missing from `/connector-plugins`**

  Check both plugin locations and restart only Kafka Connect:

  ```sh
  ls -lh plugins
  docker exec kafka-connect ls -lh /usr/share/java/plugins
  docker compose restart kafka-connect
  ```

## Status

Run `./scripts/status.sh` to show Neo4j status, Compose service status, Kafka topics, and registered Kafka Connect connectors.
