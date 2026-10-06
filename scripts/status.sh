#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
. ./scripts/kafka-env.sh
load_kafka_config

docker compose ps

if docker inspect ais-kafka >/dev/null 2>&1; then
  docker exec ais-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --list
fi

if docker inspect kafka-connect >/dev/null 2>&1; then
  curl -s http://localhost:8083/connectors
  printf '\n'
fi
