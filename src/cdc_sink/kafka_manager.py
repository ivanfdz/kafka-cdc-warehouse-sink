"""Thin wrappers around the confluent-kafka Avro clients.

Both wrappers build their client configuration from a :class:`KafkaSettings`
instance, so the same code runs against a plaintext local broker and against a
SASL_SSL managed cluster with nothing but environment changes.

The consumer is deliberately configured with ``enable.auto.commit=false``. The
sink owns offset progress and commits it only after the warehouse transaction
has been committed. See ``sink.py``.
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from .config import KafkaSettings

logger = logging.getLogger(__name__)


def build_schema_registry_client(settings: KafkaSettings) -> SchemaRegistryClient:
    """Create a Schema Registry client, adding basic auth only when configured."""
    config: dict[str, Any] = {"url": settings.schema_registry_url}
    if settings.schema_registry_basic_auth:
        config["basic.auth.user.info"] = settings.schema_registry_basic_auth
    return SchemaRegistryClient(config)


def _security_config(settings: KafkaSettings) -> dict[str, Any]:
    """Return the security block shared by the consumer and the producer."""
    config: dict[str, Any] = {"security.protocol": settings.security_protocol}
    if settings.uses_sasl:
        config["sasl.mechanism"] = settings.sasl_mechanism
        config["sasl.username"] = settings.sasl_username
        config["sasl.password"] = settings.sasl_password
    return config


class KafkaConsumerManager:
    """Own an Avro deserializing consumer and its Schema Registry client.

    Attributes:
        topic: Topic the sink reads change events from.
        group: Consumer group that carries the committed offsets.
        consumer: Configured ``DeserializingConsumer`` with auto-commit off.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        schema_registry: SchemaRegistryClient | None = None,
    ) -> None:
        self.settings = settings
        self.topic = settings.consumer_topic
        self.group = settings.consumer_group
        self.schema_registry = schema_registry or build_schema_registry_client(settings)
        self.value_deserializer = AvroDeserializer(self.schema_registry)
        self.key_deserializer = StringDeserializer("utf_8")
        self.consumer = DeserializingConsumer(self.build_config())

    @staticmethod
    def log_stats(payload: Any) -> None:
        """Client statistics callback."""
        logger.debug("Kafka client statistics: %s", payload)

    @staticmethod
    def log_error(payload: Any) -> None:
        """Client error callback. Errors here are informational, not fatal."""
        logger.warning("Kafka client error: %s", payload)

    @staticmethod
    def log_throttle(payload: Any) -> None:
        """Broker throttling callback."""
        logger.warning("Kafka client throttled: %s", payload)

    def build_config(self) -> dict[str, Any]:
        """Assemble the consumer configuration.

        ``enable.auto.commit`` is false and the poll interval is generous, so a
        slow warehouse transaction cannot trigger a rebalance mid-batch.
        """
        config: dict[str, Any] = {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "group.id": self.group,
            "key.deserializer": self.key_deserializer,
            "value.deserializer": self.value_deserializer,
            "logger": logger,
            "stats_cb": self.log_stats,
            "error_cb": self.log_error,
            "throttle_cb": self.log_throttle,
            "auto.offset.reset": self.settings.auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": self.settings.max_poll_interval_ms,
            "session.timeout.ms": self.settings.session_timeout_ms,
            "heartbeat.interval.ms": self.settings.heartbeat_interval_ms,
        }
        config.update(_security_config(self.settings))
        return config

    def latest_value_schema(self) -> str:
        """Return the registered value schema of the consumed topic.

        Useful as a startup check: if the subject is missing, the pipeline is
        pointed at the wrong registry or the connector has never produced.
        """
        subject = f"{self.topic}-value"
        return self.schema_registry.get_latest_version(subject).schema.schema_str


class KafkaProducerManager:
    """Own an Avro serializing producer for the optional republish topic.

    The sink can echo the records it applied onto a downstream topic so other
    consumers can react without querying the warehouse. This is optional and is
    only constructed when ``SINK_OUTPUT_TOPIC`` is set.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        value_schema: str,
        schema_registry: SchemaRegistryClient | None = None,
    ) -> None:
        if not settings.output_topic:
            raise ValueError("SINK_OUTPUT_TOPIC must be set to build a producer")
        self.settings = settings
        self.topic = settings.output_topic
        self.schema_registry = schema_registry or build_schema_registry_client(settings)
        self.value_schema = value_schema
        self.value_serializer = AvroSerializer(self.schema_registry, value_schema)
        self.key_serializer = StringSerializer("utf_8")
        self.producer = SerializingProducer(self.build_config())

    @staticmethod
    def delivery_report(error: Any, message: Any) -> None:
        """Log the outcome of an asynchronous delivery attempt."""
        if error is not None:
            logger.error("Delivery failed for key %s: %s", message.key(), error)
            return
        logger.debug(
            "Delivered key %s to %s[%s] at offset %s",
            message.key(),
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def build_config(self) -> dict[str, Any]:
        """Assemble the producer configuration with idempotent delivery on."""
        config: dict[str, Any] = {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "key.serializer": self.key_serializer,
            "value.serializer": self.value_serializer,
            "logger": logger,
            "enable.idempotence": True,
            "acks": "all",
        }
        config.update(_security_config(self.settings))
        return config

    def publish(self, key: str, value: dict[str, Any]) -> None:
        """Queue a record for delivery on the configured output topic."""
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            on_delivery=self.delivery_report,
        )

    def flush(self, timeout: float = 30.0) -> int:
        """Block until every queued record is acknowledged.

        Returns the number of records still in the queue, which is zero on a
        clean flush.
        """
        return self.producer.flush(timeout)
