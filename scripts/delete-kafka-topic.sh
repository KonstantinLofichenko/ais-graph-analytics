#!/usr/bin/env sh
# Explicit topic required: never default a destructive operation to the AIS source.
set -eu
if [ "$#" -ne 1 ] || [ -z "$1" ]; then
  printf '%s\n' "Usage: $0 topic" >&2
  exit 1
fi
cd "$(dirname "$0")/.."
. ./scripts/kafka-env.sh
load_kafka_config
if [ "$1" = "$KAFKA_TOPIC" ]; then
  printf '%s\n' 'Refusing to delete the configured AIS source topic' >&2
  exit 1
fi
docker exec ais-kafka /opt/kafka/bin/kafka-topics.sh \
  --delete --if-exists --topic "$1" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS"
