"""Enrich live AIS JSON with the project's existing dbt reference seeds."""

import csv
import json
import os
from pathlib import Path

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
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


def load_reference(path, key, attributes):
    """Load a small seed once; reject ambiguous or incompatible reference data."""
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if not {key, *attributes}.issubset(reader.fieldnames or []):
            raise ValueError(f"Missing required reference columns in {path}")
        reference = {}
        for row in reader:
            code = row[key].strip()
            if not code or code in reference:
                raise ValueError(f"Empty or duplicate reference code in {path}: {code!r}")
            reference[code] = {name: row[name].strip() or None for name in attributes}
        return reference


def enrich_event(event, ship_types, navigation_statuses):
    """Preserve the input fields and use null for codes absent from the seeds."""
    if not isinstance(event, dict):
        raise ValueError("AIS JSON event must be an object")
    ship = ship_types.get(str(event.get("shipType")).strip(), {})
    navigation = navigation_statuses.get(str(event.get("navigationalStatus")).strip(), {})
    return {
        **event,
        "ship_type_name": ship.get("ship_type_name"),
        "ship_category": ship.get("ship_category"),
        "navigation_status_name": navigation.get("navigational_status_name"),
    }


class ReferenceEnrichment(MapFunction):
    def __init__(self, reference_dir):
        self.reference_dir = reference_dir

    def open(self, runtime_context):
        # Both files are read once per operator initialization, including restarts.
        directory = Path(self.reference_dir)
        self.ship_types = load_reference(
            directory / "ais_ship_types.csv", "ship_type", ("ship_type_name", "ship_category")
        )
        self.navigation_statuses = load_reference(
            directory / "ais_navigational_status.csv", "navigational_status",
            ("navigational_status_name",),
        )

    def map(self, message):
        # Match the smoke job: malformed JSON raises rather than silently dropping it.
        return json.dumps(enrich_event(json.loads(message), self.ship_types, self.navigation_statuses))


def main():
    bootstrap_servers = required_env("KAFKA_BOOTSTRAP_SERVERS")
    source_topic = required_env("KAFKA_TOPIC")
    sink_topic = required_env("FLINK_ENRICHED_TOPIC")
    reference_dir = required_env("FLINK_REFERENCE_DIR")
    group_id = required_env("FLINK_ENRICHMENT_GROUP_ID")
    if source_topic == sink_topic:
        raise SystemExit("FLINK_ENRICHED_TOPIC must differ from KAFKA_TOPIC")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(source_topic)
        .set_group_id(group_id)
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
        .map(ReferenceEnrichment(reference_dir), output_type=Types.STRING())
        .sink_to(sink)
    )
    env.execute("AIS reference-data enrichment")


if __name__ == "__main__":
    main()
