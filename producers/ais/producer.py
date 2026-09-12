#!/usr/bin/env python3
"""Stream BarentsWatch AIS to Kafka (macOS/Linux)."""

import json
import logging
import math
import os
from pathlib import Path
import random
import signal
import time

import requests
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]
logger = logging.getLogger(__name__)


class Shutdown(BaseException):
    """Interrupt blocking HTTP work while still running cleanup."""


class RefreshToken(Exception):
    """Interrupt even an idle stream before the token expires."""


class Retryable(Exception):
    def __init__(self, message, delay=0):
        super().__init__(message)
        self.delay = delay


def message_limit(value):
    if not value or not value.strip():
        return None
    try:
        limit = int(value)
    except ValueError:
        raise RuntimeError("AIS_MESSAGE_LIMIT must be a positive integer or empty") from None
    if limit <= 0:
        raise RuntimeError("AIS_MESSAGE_LIMIT must be a positive integer or empty")
    return limit


def check_response(response, label):
    status = response.status_code
    if status == 429 or status >= 500:
        try:
            delay = min(300, max(0, float(response.headers.get("Retry-After", 0))))
        except ValueError:
            delay = 0
        raise Retryable(f"{label}: HTTP {status}", delay)
    if not 200 <= status < 300:
        # Never log response bodies, request headers, or credentials.
        raise RuntimeError(f"{label}: HTTP {status}; check configuration and access")


def request_access_token(session, config):
    started = time.monotonic()
    with session.post(
        config["BW_TOKEN_URL"],
        data={"client_id": config["BW_AIS_CLIENT_ID"],
              "client_secret": config["BW_AIS_CLIENT_SECRET"],
              "scope": "ais", "grant_type": "client_credentials"},
        timeout=(10, 15), allow_redirects=False,
    ) as response:
        check_response(response, "OAuth authentication")
        try:
            body = response.json()
            token = body["access_token"]
            lifetime = float(body["expires_in"])
            if not isinstance(token, str) or not token or not math.isfinite(lifetime) or lifetime <= 0:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            raise RuntimeError("Invalid OAuth token response") from None
    deadline = started + lifetime - min(60, lifetime * 0.1)
    return token, deadline


def publish_event(producer, topic, event):
    """Count only acknowledged records; fail on uncertain delivery, never skip."""
    delivered = []
    producer.produce(topic, key=str(event["mmsi"]),
                     value=json.dumps(event, separators=(",", ":")),
                     on_delivery=lambda error, message: delivered.append(error))
    # Poll in short intervals so Python signal handlers run promptly.
    deadline = time.monotonic() + 35
    while not delivered and time.monotonic() < deadline:
        producer.poll(0.2)
    if not delivered or delivered[0] is not None:
        raise RuntimeError("Kafka delivery failed or timed out; stopping to avoid silent loss")


def run():
    load_dotenv(ROOT_DIR / ".env")  # Explicit process settings take precedence.
    names = ("BW_TOKEN_URL", "BW_AIS_URL", "BW_AIS_CLIENT_ID", "BW_AIS_CLIENT_SECRET",
             "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC")
    config = {name: os.getenv(name) for name in names}
    missing = [name for name, value in config.items() if not value]
    if missing:
        raise RuntimeError("Missing required environment variable(s): " + ", ".join(missing))
    limit = message_limit(os.getenv("AIS_MESSAGE_LIMIT"))
    producer = Producer({"bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"],
                         "enable.idempotence": True, "delivery.timeout.ms": 30000})
    published = 0
    token, deadline = None, 0
    failures = 0
    rejected_token = False

    def stop(signum, frame):
        logger.info("Shutdown requested (%s)", signal.Signals(signum).name)
        raise Shutdown()

    def refresh(signum, frame):
        raise RefreshToken()

    previous = {sig: signal.signal(sig, handler) for sig, handler in
                ((signal.SIGINT, stop), (signal.SIGTERM, stop), (signal.SIGALRM, refresh))}
    logger.info("Starting AIS producer: %s", f"limit={limit}" if limit else "continuous")
    try:
        with requests.Session() as session:
            while limit is None or published < limit:
                try:
                    if token is None or time.monotonic() >= deadline:
                        token, deadline = request_access_token(session, config)
                        logger.info("OAuth token acquired")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        token = None
                        raise Retryable("Token expired before stream connection")
                    try:
                        signal.setitimer(signal.ITIMER_REAL, remaining)
                        with session.get(config["BW_AIS_URL"],
                                         headers={"Authorization": f"Bearer {token}"},
                                         stream=True, timeout=(10, 15), allow_redirects=False) as response:
                            if response.status_code == 401:
                                token = None
                                if rejected_token:
                                    raise RuntimeError("AIS rejected a fresh OAuth token; check client access")
                                rejected_token = True
                                raise Retryable("AIS token rejected; acquiring a new token")
                            check_response(response, "AIS stream")
                            rejected_token = False
                            logger.info("AIS stream connected")
                            for line in response.iter_lines():
                                if not line:
                                    continue
                                try:
                                    event = json.loads(line)
                                except (ValueError, UnicodeError):
                                    logger.warning("Skipped malformed AIS JSON")
                                    continue
                                if not isinstance(event, dict) or not str(event.get("mmsi") or "").strip():
                                    continue
                                # Do not interrupt a Kafka delivery with the token timer.
                                signal.setitimer(signal.ITIMER_REAL, 0)
                                publish_event(producer, config["KAFKA_TOPIC"], event)
                                published += 1
                                failures = 0
                                if published <= 20 or published % 1000 == 0:
                                    logger.info("Published %d AIS messages", published)
                                if limit is not None and published >= limit:
                                    return published
                                remaining = deadline - time.monotonic()
                                if remaining <= 0:
                                    raise RefreshToken()
                                signal.setitimer(signal.ITIMER_REAL, remaining)
                            raise Retryable("AIS stream ended; reconnecting")
                    finally:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                except RefreshToken:
                    token = None
                    logger.info("Refreshing OAuth token and reconnecting")
                    continue
                except (requests.RequestException, Retryable) as exc:
                    failures += 1
                    delay = max(getattr(exc, "delay", 0),
                                random.uniform(0.5, 1) * min(60, 2 ** min(failures, 6)))
                    logger.warning("%s; retry in %.1fs", str(exc) if isinstance(exc, Retryable)
                                   else "BarentsWatch network error", delay)
                    time.sleep(delay)
    except Shutdown:
        return published
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        # A second signal must not interrupt draining accepted Kafka records.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            pending = producer.flush(35)
            logger.info("Producer stopped: %d acknowledged messages; Kafka pending=%d", published, pending)
            if pending:
                raise RuntimeError("Kafka flush timed out with undelivered messages")
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run()
    except (RuntimeError, KafkaException, BufferError) as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from None
