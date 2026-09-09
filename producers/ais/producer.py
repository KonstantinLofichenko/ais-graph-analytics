#!/usr/bin/env python3
"""Publish 20 live BarentsWatch AIS messages to Kafka."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import requests
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

TOKEN_URL = os.getenv("BW_TOKEN_URL")
AIS_URL = os.getenv("BW_AIS_URL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MESSAGE_LIMIT = int(os.getenv("AIS_MESSAGE_LIMIT", "20"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def load_credentials() -> tuple[str, str]:
    """Load credentials and fail clearly when either is absent."""
    client_id = os.getenv("BW_AIS_CLIENT_ID")
    client_secret = os.getenv("BW_AIS_CLIENT_SECRET")
    missing = [
        name
        for name, value in (
            ("BW_AIS_CLIENT_ID", client_id),
            ("BW_AIS_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing required environment variable(s): " + ", ".join(missing)
        )
    return client_id, client_secret


def validate_configuration() -> None:
    """Fail clearly when a non-secret runtime setting is missing."""
    missing = [
        name
        for name, value in (
            ("BW_TOKEN_URL", TOKEN_URL),
            ("BW_AIS_URL", AIS_URL),
            ("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS),
            ("KAFKA_TOPIC", KAFKA_TOPIC),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing required environment variable(s): " + ", ".join(missing)
        )


def request_access_token(client_id: str, client_secret: str) -> str:
    """Request a BarentsWatch OAuth client-credentials token."""
    token_url = os.getenv("BW_TOKEN_URL")
    try:
        response = requests.post(
            token_url,
            data={
                "client_id": os.getenv("BW_AIS_CLIENT_ID"),
                "client_secret": os.getenv("BW_AIS_CLIENT_SECRET"),
                "scope": "ais",
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        raise RuntimeError("BarentsWatch OAuth authentication failed") from exc

    if not response.ok:
        print(f"HTTP status: {response.status_code}")
        print(f"BarentsWatch error response: {response.text}")
        raise RuntimeError("BarentsWatch OAuth authentication failed")

    try:
        token = response.json().get("access_token")
    except ValueError as exc:
        print(f"HTTP status: {response.status_code}")
        print(f"BarentsWatch error response: {response.text}")
        raise RuntimeError("BarentsWatch OAuth authentication failed") from exc

    if not token:
        print(f"HTTP status: {response.status_code}")
        print(f"BarentsWatch error response: {response.text}")
        raise RuntimeError("BarentsWatch OAuth authentication failed")
    return token


def publish_event(producer: Producer, event: dict[str, Any]) -> None:
    """Publish one event and wait for its delivery result."""
    delivery_error: list[KafkaException] = []

    def on_delivery(error: Optional[KafkaException], _message: Any) -> None:
        if error is not None:
            delivery_error.append(error)

    mmsi = str(event["mmsi"])
    try:
        producer.produce(
            KAFKA_TOPIC,
            key=mmsi,
            value=json.dumps(event, separators=(",", ":")),
            callback=on_delivery,
        )
        producer.flush()
    except (BufferError, KafkaException) as exc:
        raise RuntimeError("Kafka publish failed") from exc

    if delivery_error:
        raise RuntimeError("Kafka publish failed") from delivery_error[0]


def run() -> None:
    validate_configuration()
    client_id, client_secret = load_credentials()
    access_token = request_access_token(client_id, client_secret)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    published = 0

    try:
        with requests.get(
            AIS_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            stream=True,
            timeout=(30, 90),
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipped malformed AIS JSON line")
                    continue
                if not isinstance(event, dict):
                    logger.warning("Skipped AIS message that was not a JSON object")
                    continue

                mmsi = event.get("mmsi")
                if mmsi is None or not str(mmsi).strip():
                    continue

                try:
                    publish_event(producer, event)
                except RuntimeError as exc:
                    logger.error("%s", exc)
                    continue

                published += 1
                logger.info(
                    "Published %d/%d: MMSI=%s name=%s",
                    published,
                    MESSAGE_LIMIT,
                    mmsi,
                    event.get("name"),
                )
                if published == MESSAGE_LIMIT:
                    break
    except requests.RequestException as exc:
        raise RuntimeError("BarentsWatch AIS stream connection failed") from exc
    finally:
        producer.flush()

    if published != MESSAGE_LIMIT:
        raise RuntimeError(
            f"AIS stream ended after {published} successfully published messages"
        )
    logger.info("Published %d AIS messages to %s", MESSAGE_LIMIT, KAFKA_TOPIC)


if __name__ == "__main__":
    try:
        run()
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc
