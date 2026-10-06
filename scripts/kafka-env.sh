#!/usr/bin/env sh
# Source from the repository root. Compose resolves .env and shell overrides;
# extract only the shared Kafka settings, never print the full resolved config.
load_kafka_config() {
  command -v jq >/dev/null 2>&1 || {
    printf '%s\n' 'Kafka configuration requires jq' >&2
    return 1
  }
  kafka_config=$(docker compose --profile live config --format json |
    jq -ce '.services["ais-producer"].environment |
      {KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC}') || return 1
  KAFKA_BOOTSTRAP_SERVERS=$(printf '%s' "$kafka_config" | jq -r '.KAFKA_BOOTSTRAP_SERVERS // empty')
  KAFKA_TOPIC=$(printf '%s' "$kafka_config" | jq -r '.KAFKA_TOPIC // empty')
  : "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required in .env or the environment}"
  : "${KAFKA_TOPIC:?KAFKA_TOPIC is required in .env or the environment}"
}
