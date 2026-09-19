"""Tests for the environment-driven configuration."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from cdc_sink.config import (
    ConfigurationError,
    KafkaSettings,
    Settings,
    SinkSettings,
    WarehouseSettings,
)

REQUIRED_WAREHOUSE_ENV = {
    "WAREHOUSE_USER": "tester",
    "WAREHOUSE_PASSWORD": "placeholder",
}


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove every variable the settings read so defaults are observable."""
    for name in (
        "ENVIRONMENT",
        "LOG_LEVEL",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_BASIC_AUTH",
        "CDC_TOPIC",
        "CONSUMER_GROUP",
        "SINK_OUTPUT_TOPIC",
        "BATCH_SIZE",
        "POLL_TIMEOUT_SECONDS",
        "SLEEP_INTERVAL_SECONDS",
        "CATALOG_SOURCE_ID",
        "WAREHOUSE_HOST",
        "WAREHOUSE_PORT",
        "WAREHOUSE_DATABASE",
        "WAREHOUSE_SCHEMA",
        "WAREHOUSE_USER",
        "WAREHOUSE_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_topic_defaults_to_the_environment_name(clean_env: pytest.MonkeyPatch) -> None:
    settings = KafkaSettings.from_env("staging")

    assert settings.consumer_topic == "cdc.staging.orders"


def test_topic_can_be_overridden(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("CDC_TOPIC", "cdc.staging.orders.v2")

    assert KafkaSettings.from_env("staging").consumer_topic == "cdc.staging.orders.v2"


def test_an_empty_variable_is_treated_as_unset(clean_env: pytest.MonkeyPatch) -> None:
    """An empty value in a ConfigMap must not shadow the default."""
    clean_env.setenv("KAFKA_BOOTSTRAP_SERVERS", "   ")

    assert KafkaSettings.from_env("dev").bootstrap_servers == "localhost:9092"


def test_uses_sasl_follows_the_security_protocol(clean_env: pytest.MonkeyPatch) -> None:
    assert KafkaSettings.from_env("dev").uses_sasl is False

    clean_env.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    assert KafkaSettings.from_env("dev").uses_sasl is True

    clean_env.setenv("KAFKA_SECURITY_PROTOCOL", "sasl_plaintext")
    assert KafkaSettings.from_env("dev").uses_sasl is True


def test_warehouse_credentials_are_mandatory(clean_env: pytest.MonkeyPatch) -> None:
    """A missing credential must fail at startup, not at the first batch."""
    with pytest.raises(ConfigurationError, match="WAREHOUSE_USER"):
        WarehouseSettings.from_env()


def test_warehouse_defaults(clean_env: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED_WAREHOUSE_ENV.items():
        clean_env.setenv(name, value)

    settings = WarehouseSettings.from_env()

    assert settings.host == "localhost"
    assert settings.port == 5432
    assert settings.database == "warehouse"
    assert settings.schema == "public"


def test_numeric_values_are_validated(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("BATCH_SIZE", "many")

    with pytest.raises(ConfigurationError, match="BATCH_SIZE must be an integer"):
        SinkSettings.from_env()


def test_sink_defaults(clean_env: pytest.MonkeyPatch) -> None:
    settings = SinkSettings.from_env()

    assert settings.batch_size == 500
    assert settings.poll_timeout_seconds == 10.0
    assert settings.catalog_source_id == 12


def test_full_settings_compose(clean_env: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED_WAREHOUSE_ENV.items():
        clean_env.setenv(name, value)
    clean_env.setenv("ENVIRONMENT", "prod")
    clean_env.setenv("LOG_LEVEL", "warning")

    settings = Settings.from_env()

    assert settings.environment == "prod"
    assert settings.log_level == "WARNING"
    assert settings.kafka.consumer_topic == "cdc.prod.orders"
    assert settings.warehouse.user == "tester"


def test_settings_are_immutable(clean_env: pytest.MonkeyPatch) -> None:
    """Configuration is read once at startup and cannot drift afterwards."""
    settings = SinkSettings.from_env()

    with pytest.raises(FrozenInstanceError):
        settings.batch_size = 1  # type: ignore[misc]
