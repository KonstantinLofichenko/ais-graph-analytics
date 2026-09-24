#!/usr/bin/env bash
set -euo pipefail

docker compose up -d

services=(ais-kafka kafka-connect ksqldb-server kafbat-ui ais-clickhouse ais-neo4j)
for attempt in {1..90}; do
  all_healthy=true
  for service in "${services[@]}"; do
    container_status=$(docker inspect --format '{{.State.Status}}' "$service" 2>/dev/null || true)
    health_status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$service" 2>/dev/null || true)
    if [[ "$container_status" != "running" || ( "$service" != "kafbat-ui" && "$health_status" != "healthy" ) ]]; then
      all_healthy=false
    fi
  done
  if [[ "$all_healthy" == true ]]; then
    break
  fi
  if [[ "$attempt" == 90 ]]; then
    docker compose ps
    exit 1
  fi
  sleep 2
done

./scripts/create-kafka-topic.sh

cat <<'EOF'
Local AIS infrastructure is ready:
  Kafbat UI:       http://localhost:8081
  Kafka Connect:   http://localhost:8083
  ksqlDB:          http://localhost:8088
  Neo4j Browser:   http://localhost:7474/browser/
EOF