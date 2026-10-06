"""Small, isolated PyFlink Kafka-to-Kafka AIS transformation."""

import json
import os

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required and must not be empty")
    return value


def transform(message: str) -> str:
    event = json.loads(message)
    return json.dumps(
        {
            "mmsi": event["mmsi"],
            "msgtime": event.get("msgtime"),
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
            "speedOverGround": event.get("speedOverGround"),
            "stream_upper": str(event.get("stream") or "").upper(),
        }
    )


def main() -> None:
    bootstrap_servers = required_env("KAFKA_BOOTSTRAP_SERVERS")
    source_topic = required_env("KAFKA_TOPIC")
    sink_topic = required_env("FLINK_SINK_TOPIC")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(source_topic)
        .set_group_id("ais-pyflink-smoke")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(sink_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), source_topic)
        .map(transform, output_type=Types.STRING())
        .sink_to(sink)
    )
    env.execute("AIS PyFlink Kafka smoke")


if __name__ == "__main__":
    main()
