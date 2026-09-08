# AIS Graph Analytics

AIS Graph Analytics is a local development platform for collecting Automatic Identification System (AIS) vessel positions, streaming them through Kafka and ksqlDB, and preparing the data for graph analytics with Neo4j, analytical storage in ClickHouse, and downstream observability in Metabase.

## Architecture

```text
AIS producer -> Kafka (ais.positions) -> ksqlDB
                                      -> ClickHouse / dbt
                                      -> Neo4j / GDS
                                      -> Metabase
```

The repository is organized by platform concern:

- `producers/ais/`: AIS ingestion producers
- `airflow/dags/`: orchestration workflows
- `airbyte/`: source and destination connector configuration
- `clickhouse/init/`: ClickHouse initialization SQL
- `neo4j/cypher/`: graph schema and Cypher queries
- `neo4j/gds/`: Graph Data Science workflows
- `dbt/`: analytical transformations
- `metabase/`: dashboard and collection exports
- `docs/architecture/`: architecture documentation

## Local services

| Service | Container | Port | Purpose |
| --- | --- | --- | --- |
| Kafka | `ais-kafka` | `9092` | Host access to the Kafka broker |
| Kafka | `ais-kafka` | `29092` | Docker-network broker access |
| ksqlDB | `ksqldb-server` | `8088` | Streaming SQL REST API |
| Kafbat UI | `kafbat-ui` | `8081` | Kafka and ksqlDB web interface |

All services use the shared Docker network `ais-network`.

## Configuration

Copy `.env.example` to `.env` and provide credentials only in your local environment:

```sh
cp .env.example .env
```

Never commit `.env` or real credentials.

## Start the stack

Start the infrastructure in the background:

```sh
docker compose up -d
```

Stop the stack without removing volumes:

```sh
docker compose down
```

## Create `ais.positions`

After Kafka is running, create the topic with three partitions:

```sh
./scripts/create-kafka-topic.sh
```

The script creates `ais.positions` with replication factor `1` and is safe to run repeatedly because it uses `--if-not-exists`.

## Verify Kafka

Check the broker container:

```sh
docker compose ps ais-kafka
docker compose logs ais-kafka
```

List topics from the host using Kafka's external listener:

```sh
docker exec ais-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server ais-kafka:29092
```

The topic should include `ais.positions` after running the topic creation script.

## Verify ksqlDB

Check the service:

```sh
curl http://localhost:8088/info
```

A healthy service returns JSON containing the ksqlDB version and service identifier.

## Open Kafbat UI

Open [http://localhost:8081](http://localhost:8081) in a browser. The `local` Kafka cluster is configured to use `ais-kafka:29092`, and its ksqlDB endpoint is `http://ksqldb-server:8088`.
