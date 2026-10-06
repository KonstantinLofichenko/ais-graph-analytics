#!/usr/bin/env sh
set -eu

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ -z "$1" ]; }; then
  printf '%s\n' "Usage: $0 [topic] (defaults to configured KAFKA_TOPIC)" >&2
  exit 1
fi

cd "$(dirname "$0")/.."
. ./scripts/kafka-env.sh
load_kafka_config

docker exec ais-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic "${1:-$KAFKA_TOPIC}" \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS"
