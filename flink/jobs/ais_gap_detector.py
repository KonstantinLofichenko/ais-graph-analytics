"""Detect pipeline-observed AIS silence using Flink keyed processing-time timers."""

from datetime import datetime, timezone
import json
import logging
import os
import re

from common.reference_data import ReferenceData

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

LOGGER = logging.getLogger(__name__)
JOB_NAME = "AIS vessel gap detector"


def required_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required and must not be empty")
    return value


def parse_timeout(value):
    if not re.fullmatch(r"[0-9]+", value) or not 0 < int(value) <= (2**63 - 1) // 1000:
        raise ValueError("FLINK_GAP_TIMEOUT_SECONDS must be a positive integer number of seconds")
    return int(value)


def parse_event(message):
    """Drop malformed/non-object JSON and invalid keys; never create a None key."""
    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        LOGGER.warning("Dropping AIS gap input: malformed JSON")
        return None
    if not isinstance(event, dict):
        LOGGER.warning("Dropping AIS gap input: JSON must be an object")
        return None
    mmsi = event.get("mmsi")
    # Accept integer or numeric-string MMSIs, without assigning labels to entities.
    if isinstance(mmsi, bool) or not isinstance(mmsi, (int, str)):
        LOGGER.warning("Dropping AIS gap input: missing or invalid MMSI")
        return None
    value = str(mmsi).strip()
    if not re.fullmatch(r"[0-9]{1,9}", value) or int(value) == 0:
        LOGGER.warning("Dropping AIS gap input: MMSI must be in 1..999999999")
        return None
    return {**event, "mmsi": int(value)}


def utc_timestamp(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def vessel_state(event, processing_time, timeout_seconds):
    return {
        "mmsi": event["mmsi"],
        "name": event.get("name"),
        "last_event_msgtime": event.get("msgtime"),
        "last_latitude": event.get("latitude"),
        "last_longitude": event.get("longitude"),
        "ship_type": event.get("shipType"),
        "navigation_status": event.get("navigationalStatus"),
        "last_observed_processing_time": processing_time,
        "timer_timestamp": processing_time + timeout_seconds * 1000,
        "gap_detected_processing_time": None,
    }


def gap_payload(state, fired_at, timeout_seconds, references):
    return {
        "event_type": "AIS_GAP_DETECTED",
        **{key: state[key] for key in (
            "mmsi", "name", "last_event_msgtime", "last_latitude", "last_longitude"
        )},
        "gap_detected_at": utc_timestamp(fired_at),
        "gap_timeout_seconds": timeout_seconds,
        **references.ship(state["ship_type"]),
        "navigation_status": state["navigation_status"],
        "navigation_status_name": references.navigation_name(state["navigation_status"]),
    }


def gap_ended_payload(state, event, resumed_at, references):
    return {
        "event_type": "AIS_GAP_ENDED",
        "mmsi": state["mmsi"],
        "name": event.get("name") if event.get("name") is not None else state["name"],
        "previous_event_msgtime": state["last_event_msgtime"],
        "resumed_event_msgtime": event.get("msgtime"),
        "gap_detected_at": utc_timestamp(state["gap_detected_processing_time"]),
        "gap_ended_at": utc_timestamp(resumed_at),
        # Full observed silence, including the timeout before detection.
        "gap_duration_seconds": (resumed_at - state["last_observed_processing_time"]) / 1000,
        **references.ship(event.get("shipType")),
        "previous_navigation_status": state["navigation_status"],
        "previous_navigation_status_name": references.navigation_name(state["navigation_status"]),
        "resumed_navigation_status": event.get("navigationalStatus"),
        "resumed_navigation_status_name": references.navigation_name(event.get("navigationalStatus")),
    }


def event_key(event):
    return event["mmsi"]


class GapDetector(KeyedProcessFunction):
    def __init__(self, timeout_seconds, reference_dir):
        self.timeout_seconds = timeout_seconds
        self.reference_dir = reference_dir

    def open(self, runtime_context):
        self.references = ReferenceData(self.reference_dir)
        self.latest = runtime_context.get_state(ValueStateDescriptor("latest-vessel", Types.STRING()))

    def process_element(self, event, ctx):
        timers = ctx.timer_service()
        processing_time = timers.current_processing_time()
        previous = self.latest.value()
        output = []
        if previous is not None:
            previous = json.loads(previous)
            if previous["timer_timestamp"] is not None:
                timers.delete_processing_time_timer(previous["timer_timestamp"])
            if previous.get("gap_detected_processing_time") is not None:
                output.append(json.dumps(gap_ended_payload(previous, event, processing_time, self.references)))
                LOGGER.info(
                    "AIS gap ended mmsi=%s last_observed_processing_time=%s "
                    "gap_detected_processing_time=%s resumed_at=%s",
                    previous["mmsi"], previous["last_observed_processing_time"],
                    previous["gap_detected_processing_time"], processing_time,
                )
        state = vessel_state(event, processing_time, self.timeout_seconds)
        self.latest.update(json.dumps(state))
        timers.register_processing_time_timer(state["timer_timestamp"])
        return output

    def on_timer(self, timestamp, ctx):
        stored = self.latest.value()
        if stored is None:
            return
        state = json.loads(stored)
        # Ignore a superseded timer if an old callback arrives after an update.
        if timestamp != state["timer_timestamp"]:
            return
        fired_at = ctx.timer_service().current_processing_time()
        # Retain the pre-gap observation until recovery; do not register another timer.
        state["timer_timestamp"] = None
        state["gap_detected_processing_time"] = fired_at
        self.latest.update(json.dumps(state))
        LOGGER.info(
            "AIS gap mmsi=%s last_observed_processing_time=%s timer_timestamp=%s fired_at=%s",
            state["mmsi"], state["last_observed_processing_time"], timestamp, fired_at,
        )
        yield json.dumps(gap_payload(state, fired_at, self.timeout_seconds, self.references))


def main():
    broker = required_env("KAFKA_BOOTSTRAP_SERVERS")
    source_topic = required_env("FLINK_GAP_SOURCE_TOPIC")
    sink_topic = required_env("FLINK_GAP_TOPIC")
    group_id = required_env("FLINK_GAP_GROUP_ID")
    timeout = parse_timeout(required_env("FLINK_GAP_TIMEOUT_SECONDS"))
    reference_dir = required_env("FLINK_REFERENCE_DIR")
    if source_topic == sink_topic:
        raise SystemExit("FLINK_GAP_TOPIC must differ from FLINK_GAP_SOURCE_TOPIC")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    source = (
        KafkaSource.builder().set_bootstrap_servers(broker).set_topics(source_topic)
        .set_group_id(group_id).set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema()).build()
    )
    sink = (
        KafkaSink.builder().set_bootstrap_servers(broker)
        .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(sink_topic)
                               .set_value_serialization_schema(SimpleStringSchema()).build())
        .build()
    )
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), source_topic)
        .map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda event: event is not None)
        .key_by(event_key, key_type=Types.LONG())
        .process(GapDetector(timeout, reference_dir), output_type=Types.STRING())
        .sink_to(sink)
    )
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
