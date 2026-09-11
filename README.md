# AIS Graph Analytics

AIS Graph Analytics collects BarentsWatch Automatic Identification System (AIS) vessel positions, streams them through Kafka and Kafka Connect, and writes the current vessel state to Neo4j. Raw position history will eventually belong in ClickHouse.

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

The first live producer milestone reads the BarentsWatch Live AIS stream and publishes exactly 20 valid vessel messages to `ais.positions`, then exits. It is a one-shot test, not a continuous service.

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
  python3 producers/ais/producer.py
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
