"""Tests for the Kafka client wrappers.

The confluent-kafka classes are patched out, so nothing here resolves a broker
address or contacts a Schema Registry. What is under test is the configuration
the wrappers build, because that configuration is where the delivery guarantees
are actually decided.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest import mock

import pytest

from cdc_sink import kafka_manager
from cdc_sink.config import KafkaSettings
from cdc_sink.kafka_manager import (
    KafkaConsumerManager,
    KafkaProducerManager,
    build_schema_registry_client,
)

VALUE_SCHEMA = '{"type": "record", "name": "Stub", "fields": []}'


@pytest.fixture
def patched_clients(monkeypatch: pytest.MonkeyPatch) -> dict[str, mock.MagicMock]:
    """Replace every confluent-kafka entry point used by the wrappers."""
    doubles = {
        "SchemaRegistryClient": mock.MagicMock(name="SchemaRegistryClient"),
        "AvroDeserializer": mock.MagicMock(name="AvroDeserializer"),
        "AvroSerializer": mock.MagicMock(name="AvroSerializer"),
        "DeserializingConsumer": mock.MagicMock(name="DeserializingConsumer"),
        "SerializingProducer": mock.MagicMock(name="SerializingProducer"),
    }
    for name, double in doubles.items():
        monkeypatch.setattr(kafka_manager, name, double)
    return doubles


# ---------------------------------------------------------------------------
# Schema Registry client
# ---------------------------------------------------------------------------


def test_schema_registry_omits_basic_auth_when_not_configured(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    build_schema_registry_client(kafka_settings)

    config = patched_clients["SchemaRegistryClient"].call_args.args[0]
    assert config == {"url": "http://registry.test:8081"}
    assert "basic.auth.user.info" not in config


def test_schema_registry_adds_basic_auth_when_configured(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    settings = replace(kafka_settings, schema_registry_basic_auth="user:secret")

    build_schema_registry_client(settings)

    config = patched_clients["SchemaRegistryClient"].call_args.args[0]
    assert config["basic.auth.user.info"] == "user:secret"


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


def test_consumer_disables_auto_commit(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    """The core guarantee starts here: the client must never commit on its own."""
    manager = KafkaConsumerManager(kafka_settings)

    config = manager.build_config()
    assert config["enable.auto.commit"] is False


def test_consumer_config_carries_group_and_broker(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    manager = KafkaConsumerManager(kafka_settings)

    config = manager.build_config()
    assert config["bootstrap.servers"] == "broker.test:9092"
    assert config["group.id"] == "cdc-warehouse-sink-test"
    assert config["auto.offset.reset"] == "earliest"
    assert config["max.poll.interval.ms"] == 900_000
    assert config["session.timeout.ms"] == 600_000
    assert config["heartbeat.interval.ms"] == 3_000


def test_consumer_omits_sasl_for_plaintext(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    manager = KafkaConsumerManager(kafka_settings)

    config = manager.build_config()
    assert config["security.protocol"] == "PLAINTEXT"
    assert not any(key.startswith("sasl.") for key in config)


def test_consumer_includes_sasl_for_sasl_ssl(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    settings = replace(
        kafka_settings,
        security_protocol="SASL_SSL",
        sasl_username="service-account",
        sasl_password="placeholder-secret",
    )

    config = KafkaConsumerManager(settings).build_config()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "service-account"
    assert config["sasl.password"] == "placeholder-secret"


def test_consumer_is_built_with_the_generated_config(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    manager = KafkaConsumerManager(kafka_settings)

    patched_clients["DeserializingConsumer"].assert_called_once()
    passed = patched_clients["DeserializingConsumer"].call_args.args[0]
    assert passed["group.id"] == manager.group
    assert passed["enable.auto.commit"] is False


def test_consumer_reuses_injected_schema_registry(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    injected = mock.MagicMock(name="injected-registry")

    manager = KafkaConsumerManager(kafka_settings, schema_registry=injected)

    assert manager.schema_registry is injected
    patched_clients["SchemaRegistryClient"].assert_not_called()


def test_latest_value_schema_uses_the_value_subject(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    registry = mock.MagicMock()
    registry.get_latest_version.return_value.schema.schema_str = VALUE_SCHEMA

    manager = KafkaConsumerManager(kafka_settings, schema_registry=registry)

    assert manager.latest_value_schema() == VALUE_SCHEMA
    registry.get_latest_version.assert_called_once_with("cdc.test.orders-value")


@pytest.mark.parametrize(
    "callback, level",
    [("log_stats", "debug"), ("log_error", "warning"), ("log_throttle", "warning")],
)
def test_consumer_callbacks_only_log(
    callback: str, level: str, patched_clients: dict[str, mock.MagicMock]
) -> None:
    """The callbacks must never raise, whatever the client hands them."""
    with mock.patch.object(kafka_manager.logger, level) as logging_call:
        getattr(KafkaConsumerManager, callback)({"anything": True})

    logging_call.assert_called_once()


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


def test_producer_requires_an_output_topic(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    with pytest.raises(ValueError, match="SINK_OUTPUT_TOPIC"):
        KafkaProducerManager(kafka_settings, VALUE_SCHEMA)


def test_producer_enables_idempotence(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    settings = replace(kafka_settings, output_topic="cdc.test.orders.applied")

    config = KafkaProducerManager(settings, VALUE_SCHEMA).build_config()

    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"


def test_producer_publishes_with_a_delivery_callback(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    settings = replace(kafka_settings, output_topic="cdc.test.orders.applied")
    manager = KafkaProducerManager(settings, VALUE_SCHEMA)

    manager.publish("ORD-0001", {"order_id": "ORD-0001"})

    manager.producer.produce.assert_called_once()
    kwargs = manager.producer.produce.call_args.kwargs
    assert kwargs["topic"] == "cdc.test.orders.applied"
    assert kwargs["key"] == "ORD-0001"
    assert kwargs["on_delivery"] is KafkaProducerManager.delivery_report


def test_producer_flush_returns_outstanding_count(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    settings = replace(kafka_settings, output_topic="cdc.test.orders.applied")
    manager = KafkaProducerManager(settings, VALUE_SCHEMA)
    manager.producer.flush.return_value = 0

    assert manager.flush(1.0) == 0
    manager.producer.flush.assert_called_once_with(1.0)


def test_delivery_report_logs_failures(
    patched_clients: dict[str, mock.MagicMock],
) -> None:
    message = mock.MagicMock()
    message.key.return_value = "ORD-0001"

    with mock.patch.object(kafka_manager.logger, "error") as log_error:
        KafkaProducerManager.delivery_report("boom", message)

    log_error.assert_called_once()


def test_security_config_helper_is_shared_by_both_clients(
    kafka_settings: KafkaSettings, patched_clients: dict[str, mock.MagicMock]
) -> None:
    """Consumer and producer must not drift apart on authentication."""
    settings = replace(
        kafka_settings,
        security_protocol="SASL_SSL",
        sasl_username="service-account",
        sasl_password="placeholder-secret",
        output_topic="cdc.test.orders.applied",
    )

    consumer_config: dict[str, Any] = KafkaConsumerManager(settings).build_config()
    producer_config: dict[str, Any] = KafkaProducerManager(settings, VALUE_SCHEMA).build_config()

    security_keys = ["security.protocol", "sasl.mechanism", "sasl.username", "sasl.password"]
    assert {key: consumer_config[key] for key in security_keys} == {
        key: producer_config[key] for key in security_keys
    }
