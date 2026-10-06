"""Incremental vessel features in configurable sliding AIS event-time windows."""

from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
import re

from common.reference_data import ReferenceData

from pyflink.common import Duration, SimpleStringSchema, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

LOGGER = logging.getLogger(__name__)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
JOB_NAME = "AIS vessel multi-window event-time features"


def required_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required and must not be empty")
    return value


def seconds_setting(name, allow_zero=False):
    value = required_env(name)
    minimum = 0 if allow_zero else 1
    if not re.fullmatch(r"[0-9]+", value) or not minimum <= int(value) <= (2**63 - 1) // 1000:
        raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'} integer seconds")
    return int(value)


def parse_msgtime(value):
    """Require an explicit timezone and retain epoch-millisecond precision."""
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value
    ):
        raise ValueError("msgtime must be an ISO-8601 timestamp with an explicit timezone")
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    delta = instant - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def window_settings(windows, slide):
    """Validate minute settings before constructing the Flink graph."""
    limit = (2**63 - 1) // 60000
    if not re.fullmatch(r"[0-9]+", slide.strip()) or not 0 < int(slide) <= limit:
        raise ValueError("FLINK_FEATURE_SLIDE_MINUTES must be positive integer minutes")
    slide = int(slide)
    values = windows.split(",")
    if any(not re.fullmatch(r"[0-9]+", value.strip()) for value in values):
        raise ValueError("FLINK_FEATURE_WINDOWS_MINUTES must be a non-empty comma-separated integer list")
    values = [int(value) for value in values]
    if (len(set(values)) != len(values)
            or any(value < slide or value > limit or value % slide for value in values)):
        raise ValueError("FLINK_FEATURE_WINDOWS_MINUTES must contain unique positive multiples of the slide")
    return values, slide


def parse_event(message):
    try:
        event = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        LOGGER.warning("Dropping AIS feature input: malformed JSON")
        return None
    if not isinstance(event, dict):
        LOGGER.warning("Dropping AIS feature input: JSON must be an object")
        return None
    mmsi = event.get("mmsi")
    if (isinstance(mmsi, bool) or not isinstance(mmsi, (int, str))
            or not re.fullmatch(r"[0-9]{1,9}", str(mmsi).strip()) or int(mmsi) == 0):
        LOGGER.warning("Dropping AIS feature input: MMSI must be in 1..999999999")
        return None
    try:
        timestamp = parse_msgtime(event.get("msgtime"))
    except (ValueError, OverflowError):
        LOGGER.warning("Dropping AIS feature input: missing or invalid msgtime")
        return None
    speed = event.get("speedOverGround")
    if speed is not None:
        try:
            if isinstance(speed, bool) or not isinstance(speed, (int, float)):
                raise ValueError("Speed is not a JSON number")
            speed = float(speed)
            if not math.isfinite(speed):
                raise ValueError("Speed is not finite")
        except (ValueError, OverflowError):
            LOGGER.warning("Ignoring invalid AIS feature speed; position is still counted")
            speed = None
    return {"mmsi": int(mmsi), "event_timestamp_ms": timestamp, "speed": speed,
            "ship_type": event.get("shipType"), "navigation_status": event.get("navigationalStatus")}


class AisTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value["event_timestamp_ms"]


def event_key(event):
    return event["mmsi"]


class VesselAggregate(AggregateFunction):
    def create_accumulator(self):
        # Existing metrics plus the latest event-time observation for reference attributes.
        return (0, 0, 0.0, None, None, None)

    def add(self, value, accumulator):
        count, speed_count, total, minimum, maximum, latest = accumulator
        if latest is None or value["event_timestamp_ms"] >= latest["event_timestamp_ms"]:
            latest = {key: value[key] for key in ("event_timestamp_ms", "ship_type", "navigation_status")}
        speed = value["speed"]
        if speed is None:
            return count + 1, speed_count, total, minimum, maximum, latest
        return (count + 1, speed_count + 1, total + speed,
                speed if minimum is None else min(minimum, speed),
                speed if maximum is None else max(maximum, speed), latest)

    def get_result(self, accumulator):
        count, speed_count, total, minimum, maximum, latest = accumulator
        return {"position_count": count, "avg_speed": total / speed_count if speed_count else None,
                "min_speed": minimum, "max_speed": maximum, "last_observation": latest}

    def merge(self, left, right):
        minima = [value for value in (left[3], right[3]) if value is not None]
        maxima = [value for value in (left[4], right[4]) if value is not None]
        latest = left[5]
        if right[5] is not None and (latest is None or right[5]["event_timestamp_ms"] >= latest["event_timestamp_ms"]):
            latest = right[5]
        return (left[0] + right[0], left[1] + right[1], left[2] + right[2],
                min(minima) if minima else None, max(maxima) if maxima else None, latest)


def format_window(mmsi, window_minutes, start_ms, end_ms, aggregate):
    def utc(milliseconds):
        return (EPOCH + timedelta(milliseconds=milliseconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
    return json.dumps({"mmsi": mmsi, "window_minutes": window_minutes,
                       "window_start": utc(start_ms), "window_end": utc(end_ms),
                       **aggregate}, allow_nan=False)


class FeatureWindow(ProcessWindowFunction):
    def __init__(self, reference_dir, window_minutes):
        self.reference_dir = reference_dir
        self.window_minutes = window_minutes

    def open(self, runtime_context):
        self.references = ReferenceData(self.reference_dir)

    def process(self, key, context, elements):
        window = context.window()
        # Incremental aggregation supplies a single summary, not every position.
        aggregate = next(iter(elements))
        latest = aggregate["last_observation"]
        output = {name: value for name, value in aggregate.items() if name != "last_observation"}
        output.update(self.references.ship(latest["ship_type"]))
        output["last_navigation_status"] = latest["navigation_status"]
        output["last_navigation_status_name"] = self.references.navigation_name(latest["navigation_status"])
        yield format_window(key, self.window_minutes, window.start, window.end, output)


def main():
    broker = required_env("KAFKA_BOOTSTRAP_SERVERS")
    source_topic = required_env("FLINK_FEATURE_SOURCE_TOPIC")
    sink_topic = required_env("FLINK_FEATURE_TOPIC")
    group = required_env("FLINK_FEATURE_GROUP_ID")
    windows, slide = window_settings(required_env("FLINK_FEATURE_WINDOWS_MINUTES"),
                                    required_env("FLINK_FEATURE_SLIDE_MINUTES"))
    watermark_seconds = seconds_setting("FLINK_FEATURE_WATERMARK_SECONDS", allow_zero=True)
    idle_seconds = seconds_setting("FLINK_FEATURE_IDLE_SECONDS")
    reference_dir = required_env("FLINK_REFERENCE_DIR")
    if source_topic == sink_topic:
        raise SystemExit("FLINK_FEATURE_TOPIC must differ from FLINK_FEATURE_SOURCE_TOPIC")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    source = (
        KafkaSource.builder().set_bootstrap_servers(broker).set_topics(source_topic)
        .set_group_id(group).set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema()).build()
    )
    sink = (
        KafkaSink.builder().set_bootstrap_servers(broker)
        .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(sink_topic)
                               .set_value_serialization_schema(SimpleStringSchema()).build())
        .build()
    )
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(watermark_seconds))
        .with_idleness(Duration.of_seconds(idle_seconds))
        .with_timestamp_assigner(AisTimestampAssigner())
    )
    keyed = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), source_topic)
        .map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda event: event is not None)
        .assign_timestamps_and_watermarks(watermarks)
        .key_by(event_key, key_type=Types.LONG())
    )
    branches = [
        keyed.window(SlidingEventTimeWindows.of(Time.minutes(minutes), Time.minutes(slide)))
        .aggregate(VesselAggregate(), FeatureWindow(reference_dir, minutes),
                   accumulator_type=Types.PICKLED_BYTE_ARRAY(), output_type=Types.STRING())
        for minutes in windows
    ]
    output = branches[0].union(*branches[1:]) if len(branches) > 1 else branches[0]
    output.sink_to(sink)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
