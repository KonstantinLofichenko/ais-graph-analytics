#!/usr/bin/env bash
# Core Compose services must already be running and healthy.
set +x
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() {
  printf 'Bootstrap failed: %s\n' "$1" >&2
  exit 1
}

for tool in docker jq curl; do
  command -v "$tool" >/dev/null 2>&1 || fail "Required command not found: $tool"
done

connector_config=neo4j/connectors/ais-sink.json
[[ -f "$connector_config" ]] || fail \
  "Missing $connector_config. Copy ais-sink.example.json and set local credentials first."
# Validate without printing the config or errors that may contain credentials.
if ! jq -e '
  (.name | type == "string" and length > 0) and
  (.config | type == "object") and
  (.config["neo4j.uri"] == "bolt://neo4j:7687") and
  (.config["neo4j.authentication.basic.password"] |
    type == "string" and length > 0 and . != "REPLACE_WITH_LOCAL_NEO4J_PASSWORD")
' "$connector_config" >/dev/null 2>&1; then
  fail 'Invalid connector JSON, missing credentials, or neo4j.uri is not bolt://neo4j:7687.'
fi
connector_name=$(jq -er '.name | @uri' "$connector_config" 2>/dev/null) || fail 'Cannot read connector name.'

# Explicit allowlist: 001 is an existing-installation migration, never bootstrap it.
migrations=(
  clickhouse/migrations/003_port_graph_metrics.sql
  clickhouse/migrations/004_hais_positions.sql
  clickhouse/migrations/005_hais_ingestion_runs.sql
)
for sql_file in "${migrations[@]}" neo4j/cypher/01_constraints.cypher; do
  [[ -r "$sql_file" ]] || fail "Cannot read $sql_file"
done

printf 'Creating/verifying Kafka topic ais.positions...\n'
if ! sh scripts/create-kafka-topic.sh >/dev/null 2>&1; then
  fail 'Kafka topic setup failed; check the running broker.'
fi

for migration in "${migrations[@]}"; do
  printf 'Applying %s...\n' "$migration"
  if ! docker compose exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
    < "$migration" >/dev/null 2>&1; then
    fail "ClickHouse migration failed: $migration. Check database configuration and logs locally."
  fi
done

printf 'Applying Neo4j constraints...\n'
if ! docker compose exec -T neo4j sh -c \
  'cypher-shell -a bolt://neo4j:7687 -d neo4j -u neo4j -p "$HEALTHCHECK_PASSWORD" --non-interactive --fail-fast' \
  < neo4j/cypher/01_constraints.cypher >/dev/null 2>&1; then
  fail 'Neo4j constraints failed; check authentication, existing data, and database logs locally.'
fi

printf 'Creating/updating Neo4j Kafka sink connector...\n'
# PUT accepts the inner config object and creates or updates the named connector.
# Stream credentials on stdin; suppress response bodies, which echo configuration.
if ! jq -c '.config' "$connector_config" 2>/dev/null | \
  curl --silent --fail --max-time 120 --output /dev/null \
    --request PUT --header 'Content-Type: application/json' --data-binary @- \
    "http://localhost:8083/connectors/$connector_name/config" 2>/dev/null; then
  fail 'Connector registration failed; check Kafka Connect availability, plugin installation, and local config.'
fi

printf 'Bootstrap complete. Check connector/task status in Kafka Connect; no dbt or Airflow analytics were run.\n'
