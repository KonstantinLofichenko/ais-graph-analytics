#!/usr/bin/env sh
set -eu

docker exec ais-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic ais.positions \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server ais-kafka:29092
