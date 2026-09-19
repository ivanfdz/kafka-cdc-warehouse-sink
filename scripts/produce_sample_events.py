#!/usr/bin/env python3
"""Emit synthetic Avro CDC events so the pipeline can be exercised locally.

The events follow schemas/order_cdc_event.avsc and reference the orders seeded
by sql/warehouse_schema.sql, so the sink has something to resolve and the
warehouse tables actually fill up.

The generated mix is deliberately awkward:

* A share of the events carry ``link_origin = 'A'``, which the sink discards.
  That exercises the filter and the "skip without stalling the batch" path.
* A share are deletes, which exercise step 6 of the upsert.
* Updates are emitted as a delete immediately followed by an insert, which is
  what a real CDC connector does and what makes the ordering in step 6 matter.

Usage:
    python scripts/produce_sample_events.py
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level="INFO", format=LOG_FORMAT)
logger = logging.getLogger("sample-producer")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order_cdc_event.avsc"

#: Matches the orders seeded into orders_reference by the warehouse schema.
ORDER_IDS: list[str] = [f"ORD-{index:04d}" for index in range(1, 21)]

#: Customer references. Some are reused so the batch contains duplicates, which
#: is what the grouping in step 1 of the upsert has to cope with.
CUSTOMER_REFS: list[str] = [f"CUST-{index:05d}" for index in range(1, 9)]

CHANNELS: list[str] = ["web", "phone", "partner-api"]
STATUSES: list[str] = ["CONFIRMED", "PICKING", "SHIPPED", "CANCELLED"]


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def load_schema() -> str:
    """Read the Avro schema shipped with the repository."""
    return SCHEMA_PATH.read_text(encoding="utf-8")


def build_producer(schema_str: str) -> SerializingProducer:
    """Build a plaintext Avro producer for the local compose stack."""
    registry_config: dict[str, Any] = {
        "url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    }
    basic_auth = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH")
    if basic_auth:
        registry_config["basic.auth.user.info"] = basic_auth

    schema_registry = SchemaRegistryClient(registry_config)
    return SerializingProducer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": AvroSerializer(schema_registry, schema_str),
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def _event(
    order_id: str,
    customer_ref: str,
    op_type: str,
    link_origin: str,
    moment: datetime,
) -> dict[str, Any]:
    """Build a single change event payload."""
    epoch_millis = int(moment.timestamp() * 1000)
    return {
        "order_id": order_id,
        "customer_ref": customer_ref,
        "channel_id": random.choice(CHANNELS),
        "status": random.choice(STATUSES),
        "event_date": (date(2026, 1, 5) + timedelta(days=random.randint(0, 5))).isoformat(),
        "link_origin": link_origin,
        "op_type": op_type,
        "op_ts": epoch_millis,
        "current_ts": epoch_millis + random.randint(5, 250),
    }


def generate_events(
    count: int,
    delete_ratio: float,
    automatic_ratio: float,
    seed: int,
) -> Iterator[dict[str, Any]]:
    """Yield ``count`` events, including delete/insert pairs for updates."""
    random.seed(seed)
    moment = datetime.now(tz=timezone.utc) - timedelta(minutes=count)
    emitted = 0

    while emitted < count:
        order_id = random.choice(ORDER_IDS)
        customer_ref = random.choice(CUSTOMER_REFS)
        link_origin = "A" if random.random() < automatic_ratio else "M"
        moment = moment + timedelta(seconds=random.randint(1, 30))

        if random.random() < delete_ratio:
            yield _event(order_id, customer_ref, "D", link_origin, moment)
            emitted += 1
            continue

        if emitted + 2 <= count and random.random() < 0.3:
            # An update, the way a CDC connector emits it: delete then insert,
            # one millisecond apart, so ordering inside the batch is load bearing.
            yield _event(order_id, customer_ref, "D", link_origin, moment)
            yield _event(
                order_id,
                random.choice(CUSTOMER_REFS),
                "I",
                link_origin,
                moment + timedelta(milliseconds=1),
            )
            emitted += 2
            continue

        yield _event(order_id, customer_ref, "I", link_origin, moment)
        emitted += 1


def main() -> int:
    """Produce the configured number of events and flush."""
    topic = os.getenv("CDC_TOPIC") or f"cdc.{os.getenv('ENVIRONMENT', 'dev')}.orders"
    count = _env_int("EVENT_COUNT", 40)
    delete_ratio = _env_float("DELETE_RATIO", 0.2)
    automatic_ratio = _env_float("AUTOMATIC_RATIO", 0.25)
    seed = _env_int("RANDOM_SEED", 20260105)

    schema_str = load_schema()
    logger.info("Using schema %s", json.loads(schema_str)["name"])

    producer = build_producer(schema_str)
    logger.info("Producing %s events to %s", count, topic)

    produced = 0
    for event in generate_events(count, delete_ratio, automatic_ratio, seed):
        producer.produce(topic=topic, key=event["order_id"], value=event)
        produced += 1
        if produced % 100 == 0:
            producer.poll(0)

    remaining = producer.flush(30.0)
    if remaining:
        logger.error("%s events were not acknowledged", remaining)
        return 1

    logger.info("Produced %s events to %s", produced, topic)
    # Give a reader a moment to notice the records before the container exits.
    time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
