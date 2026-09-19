"""Environment-driven configuration for the CDC warehouse sink.

Every value is read from the process environment. Nothing is hardcoded and no
credential has a default, so a misconfigured deployment fails fast instead of
silently pointing at the wrong cluster.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(RuntimeError):
    """Raised when a required environment variable is missing."""


def _get(name: str, default: str | None = None) -> str | None:
    """Read an environment variable, treating the empty string as unset."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _require(name: str) -> str:
    """Read a mandatory environment variable."""
    value = _get(name)
    if value is None:
        raise ConfigurationError(f"Required environment variable is not set: {name}")
    return value


def _get_int(name: str, default: int) -> int:
    value = _get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, got {value!r}") from exc


def _get_float(name: str, default: float) -> float:
    value = _get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number, got {value!r}") from exc


@dataclass(frozen=True)
class KafkaSettings:
    """Connection and consumer-group settings for Kafka and Schema Registry."""

    bootstrap_servers: str
    schema_registry_url: str
    consumer_topic: str
    consumer_group: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str | None = None
    sasl_password: str | None = None
    schema_registry_basic_auth: str | None = None
    auto_offset_reset: str = "earliest"
    max_poll_interval_ms: int = 900_000
    session_timeout_ms: int = 600_000
    heartbeat_interval_ms: int = 3_000
    output_topic: str | None = None

    @classmethod
    def from_env(cls, environment: str) -> KafkaSettings:
        default_topic = f"cdc.{environment}.orders"
        return cls(
            bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            schema_registry_url=_get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            consumer_topic=_get("CDC_TOPIC", default_topic),
            consumer_group=_get("CONSUMER_GROUP", "cdc-warehouse-sink"),
            security_protocol=_get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=_get("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=_get("KAFKA_SASL_USERNAME"),
            sasl_password=_get("KAFKA_SASL_PASSWORD"),
            schema_registry_basic_auth=_get("SCHEMA_REGISTRY_BASIC_AUTH"),
            auto_offset_reset=_get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            max_poll_interval_ms=_get_int("KAFKA_MAX_POLL_INTERVAL_MS", 900_000),
            session_timeout_ms=_get_int("KAFKA_SESSION_TIMEOUT_MS", 600_000),
            heartbeat_interval_ms=_get_int("KAFKA_HEARTBEAT_INTERVAL_MS", 3_000),
            output_topic=_get("SINK_OUTPUT_TOPIC"),
        )

    @property
    def uses_sasl(self) -> bool:
        """True when the security protocol asks for SASL authentication."""
        return "SASL" in self.security_protocol.upper()


@dataclass(frozen=True)
class WarehouseSettings:
    """Connection settings for the analytical warehouse."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "public"
    connect_timeout: int = 10
    application_name: str = "cdc-warehouse-sink"

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        return cls(
            host=_get("WAREHOUSE_HOST", "localhost"),
            port=_get_int("WAREHOUSE_PORT", 5432),
            database=_get("WAREHOUSE_DATABASE", "warehouse"),
            user=_require("WAREHOUSE_USER"),
            password=_require("WAREHOUSE_PASSWORD"),
            schema=_get("WAREHOUSE_SCHEMA", "public"),
            connect_timeout=_get_int("WAREHOUSE_CONNECT_TIMEOUT", 10),
            application_name=_get("WAREHOUSE_APPLICATION_NAME", "cdc-warehouse-sink"),
        )


@dataclass(frozen=True)
class SinkSettings:
    """Batching behaviour of the sink loop."""

    batch_size: int = 500
    poll_timeout_seconds: float = 10.0
    sleep_interval_seconds: float = 5.0
    catalog_source_id: int = 12
    default_link_score: int = 2

    @classmethod
    def from_env(cls) -> SinkSettings:
        return cls(
            batch_size=_get_int("BATCH_SIZE", 500),
            poll_timeout_seconds=_get_float("POLL_TIMEOUT_SECONDS", 10.0),
            sleep_interval_seconds=_get_float("SLEEP_INTERVAL_SECONDS", 5.0),
            catalog_source_id=_get_int("CATALOG_SOURCE_ID", 12),
            default_link_score=_get_int("DEFAULT_LINK_SCORE", 2),
        )


@dataclass(frozen=True)
class Settings:
    """Full application configuration."""

    environment: str
    log_level: str
    kafka: KafkaSettings
    warehouse: WarehouseSettings
    sink: SinkSettings

    @classmethod
    def from_env(cls) -> Settings:
        environment = _get("ENVIRONMENT", "dev")
        return cls(
            environment=environment,
            log_level=_get("LOG_LEVEL", "INFO").upper(),
            kafka=KafkaSettings.from_env(environment),
            warehouse=WarehouseSettings.from_env(),
            sink=SinkSettings.from_env(),
        )
