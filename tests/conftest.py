"""Shared test doubles.

No test in this suite opens a socket. Kafka, Schema Registry and the warehouse
are all replaced by in-process fakes, so the suite runs offline and is safe to
run in CI without any broker or database.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from cdc_sink.config import KafkaSettings, SinkSettings, WarehouseSettings


class Recorder:
    """Ordered log of the side effects a test cares about.

    Used instead of inspecting ``mock_calls`` when the assertion is about the
    relative order of operations across two different collaborators.
    """

    def __init__(self) -> None:
        self.events: list[str] = []

    def record(self, event: str) -> None:
        self.events.append(event)

    def index(self, event: str) -> int:
        """Index of the first occurrence of ``event``."""
        return self.events.index(event)

    def count(self, event: str) -> int:
        return self.events.count(event)

    def __contains__(self, event: str) -> bool:
        return event in self.events

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Recorder({self.events!r})"


class FakeCursor:
    """Minimal DB-API cursor that records statements instead of running them."""

    def __init__(
        self,
        recorder: Recorder | None = None,
        fetch_rows: Sequence[tuple[Any, ...]] | None = None,
    ) -> None:
        self.recorder = recorder
        self.statements: list[tuple[str, Sequence[Any] | None]] = []
        self.fetch_rows: list[tuple[Any, ...]] = list(fetch_rows or [])
        self.rowcount = 0
        self.closed = False

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self.statements.append((sql, list(params) if params is not None else None))
        self.rowcount = len(self.fetch_rows)
        if self.recorder is not None:
            self.recorder.record("sql.execute")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.fetch_rows)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # --- assertion helpers --------------------------------------------------

    @property
    def sql_texts(self) -> list[str]:
        return [sql for sql, _ in self.statements]

    def statements_matching(self, fragment: str) -> list[tuple[str, list[Any] | None]]:
        needle = fragment.lower()
        return [(sql, params) for sql, params in self.statements if needle in sql.lower()]

    def all_params(self) -> list[Any]:
        flat: list[Any] = []
        for _, params in self.statements:
            if params:
                flat.extend(params)
        return flat


class FakeWarehouse:
    """Stand-in for :class:`cdc_sink.warehouse.WarehouseManager`."""

    def __init__(
        self,
        recorder: Recorder | None = None,
        fetch_rows: Sequence[tuple[Any, ...]] | None = None,
        fail_on_commit: bool = False,
        fail_on_statement: str | None = None,
    ) -> None:
        self.recorder = recorder or Recorder()
        self.cursors: list[FakeCursor] = []
        self.fetch_rows = list(fetch_rows or [])
        self.fail_on_commit = fail_on_commit
        self.fail_on_statement = fail_on_statement
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        cursor = FakeCursor(recorder=self.recorder, fetch_rows=self.fetch_rows)
        if self.fail_on_statement is not None:
            original = cursor.execute
            trigger = self.fail_on_statement.lower()

            def failing_execute(sql: str, params: Any = None) -> None:
                original(sql, params)
                if trigger in sql.lower():
                    raise RuntimeError(f"warehouse rejected statement: {trigger}")

            cursor.execute = failing_execute  # type: ignore[method-assign]
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        self.recorder.record("warehouse.commit")
        if self.fail_on_commit:
            raise RuntimeError("warehouse commit failed")
        self.commits += 1

    def rollback(self) -> None:
        self.recorder.record("warehouse.rollback")
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    @property
    def last_cursor(self) -> FakeCursor:
        return self.cursors[-1]


class FakeMessage:
    """Stand-in for a polled confluent-kafka message."""

    def __init__(
        self,
        value: dict[str, Any] | None = None,
        error: Any = None,
        key: str = "key",
        offset: int = 0,
    ) -> None:
        self._value = value
        self._error = error
        self._key = key
        self._offset = offset

    def value(self) -> dict[str, Any] | None:
        return self._value

    def error(self) -> Any:
        return self._error

    def key(self) -> str:
        return self._key

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """Stand-in for a confluent-kafka consumer.

    ``poll`` walks a scripted sequence. An entry that is an exception class or
    instance is raised, which is how the recovery paths are driven.
    """

    def __init__(
        self,
        poll_results: Sequence[Any] | None = None,
        recorder: Recorder | None = None,
    ) -> None:
        self.recorder = recorder or Recorder()
        self.poll_results = list(poll_results or [])
        self.subscriptions: list[list[str]] = []
        self.commits: list[dict[str, Any]] = []
        self.polls = 0
        self.closed = False

    def subscribe(self, topics: Sequence[str]) -> None:
        self.subscriptions.append(list(topics))
        self.recorder.record("kafka.subscribe")

    def poll(self, timeout: float) -> Any:
        self.polls += 1
        if not self.poll_results:
            raise KeyboardInterrupt
        result = self.poll_results.pop(0)
        if isinstance(result, BaseException) or (
            isinstance(result, type) and issubclass(result, BaseException)
        ):
            raise result
        return result

    def commit(self, **kwargs: Any) -> None:
        self.commits.append(kwargs)
        self.recorder.record("kafka.commit")

    def close(self) -> None:
        self.closed = True
        self.recorder.record("kafka.close")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture
def sink_settings() -> SinkSettings:
    return SinkSettings(
        batch_size=3,
        poll_timeout_seconds=0.01,
        sleep_interval_seconds=0.0,
        catalog_source_id=12,
        default_link_score=2,
    )


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="broker.test:9092",
        schema_registry_url="http://registry.test:8081",
        consumer_topic="cdc.test.orders",
        consumer_group="cdc-warehouse-sink-test",
    )


@pytest.fixture
def warehouse_settings() -> WarehouseSettings:
    return WarehouseSettings(
        host="warehouse.test",
        port=5432,
        database="warehouse",
        user="tester",
        password="not-a-real-password",
    )


@pytest.fixture
def change_event() -> dict[str, Any]:
    """A relevant manual insert event."""
    return {
        "order_id": "ORD-0001",
        "customer_ref": "CUST-00001",
        "channel_id": "web",
        "status": "CONFIRMED",
        "event_date": "2026-01-05",
        "link_origin": "M",
        "op_type": "I",
        "op_ts": 1767600000000,
        "current_ts": 1767600000120,
    }


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the idle-poll pause instantaneous."""
    monkeypatch.setattr("cdc_sink.sink.time.sleep", lambda _seconds: None)
