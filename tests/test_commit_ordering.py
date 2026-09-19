"""The core claim: Kafka offsets are committed only after the warehouse commits.

These tests assert the ordering explicitly rather than inferring it. Everything
else in the repository is replaceable; this ordering is the reason the component
exists.
"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from cdc_sink.config import SinkSettings
from cdc_sink.sink import CdcWarehouseSink
from tests.conftest import FakeConsumer, FakeWarehouse, Recorder


def build_sink(
    recorder: Recorder,
    settings: SinkSettings,
    **warehouse_kwargs: Any,
) -> tuple[CdcWarehouseSink, FakeConsumer, FakeWarehouse]:
    """Wire a sink whose consumer and warehouse share one ordered recorder."""
    consumer = FakeConsumer(recorder=recorder)
    warehouse = FakeWarehouse(recorder=recorder, **warehouse_kwargs)
    sink = CdcWarehouseSink(
        consumer=consumer,
        warehouse=warehouse,  # type: ignore[arg-type]
        settings=settings,
        topic="cdc.test.orders",
    )
    return sink, consumer, warehouse


def test_warehouse_commits_before_kafka(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink, _consumer, _warehouse = build_sink(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    assert "warehouse.commit" in recorder
    assert "kafka.commit" in recorder
    assert recorder.index("warehouse.commit") < recorder.index("kafka.commit")


def test_every_statement_runs_before_the_warehouse_commit(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """No step may leak past the commit boundary into the next transaction."""
    sink, _consumer, _warehouse = build_sink(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    commit_position = recorder.index("warehouse.commit")
    statement_positions = [
        index for index, event in enumerate(recorder.events) if event == "sql.execute"
    ]
    assert statement_positions, "the flush executed no statements"
    assert max(statement_positions) < commit_position


def test_kafka_is_not_committed_when_the_warehouse_commit_fails(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """A failed warehouse commit must leave offsets untouched, so Kafka replays."""
    sink, consumer, warehouse = build_sink(recorder, sink_settings, fail_on_commit=True)
    sink.buffer(change_event)

    with pytest.raises(RuntimeError, match="warehouse commit failed"):
        sink.flush()

    assert consumer.commits == []
    assert "kafka.commit" not in recorder
    assert warehouse.rollbacks == 1


def test_kafka_is_not_committed_when_a_step_fails(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink, consumer, warehouse = build_sink(
        recorder, sink_settings, fail_on_statement="order_customer_links_history"
    )
    sink.buffer(change_event)

    with pytest.raises(RuntimeError, match="warehouse rejected statement"):
        sink.flush()

    assert consumer.commits == []
    assert warehouse.commits == 0
    assert warehouse.rollbacks == 1


def test_a_failed_batch_keeps_its_records_buffered(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """The batch must survive the failure so the retry reapplies the same rows."""
    sink, _consumer, _warehouse = build_sink(recorder, sink_settings, fail_on_commit=True)
    sink.buffer(change_event)

    with pytest.raises(RuntimeError):
        sink.flush()

    assert sink.buffered == 1


def test_offsets_are_committed_synchronously(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """An asynchronous commit would defeat the ordering the flush establishes."""
    sink, consumer, _warehouse = build_sink(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    assert consumer.commits == [{"asynchronous": False}]


def test_successful_flush_clears_the_buffer_and_counts(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink, _consumer, _warehouse = build_sink(recorder, sink_settings)
    sink.buffer(change_event)
    sink.buffer(change_event)

    applied = sink.flush()

    assert applied == 2
    assert sink.buffered == 0
    assert sink.flushes == 1


def test_empty_flush_touches_nothing(recorder: Recorder, sink_settings: SinkSettings) -> None:
    sink, consumer, warehouse = build_sink(recorder, sink_settings)

    assert sink.flush() == 0
    assert recorder.events == []
    assert consumer.commits == []
    assert warehouse.commits == 0


def test_ordering_holds_across_consecutive_batches(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink, _consumer, _warehouse = build_sink(recorder, sink_settings)

    for _ in range(3):
        sink.buffer(change_event)
        sink.flush()

    warehouse_commits = [
        index for index, event in enumerate(recorder.events) if event == "warehouse.commit"
    ]
    kafka_commits = [
        index for index, event in enumerate(recorder.events) if event == "kafka.commit"
    ]
    assert len(warehouse_commits) == len(kafka_commits) == 3
    # Pairwise: every warehouse commit precedes the Kafka commit of the same batch,
    # and that Kafka commit precedes the next warehouse commit.
    pairs = zip(warehouse_commits, kafka_commits, strict=True)
    for position, (warehouse_at, kafka_at) in enumerate(pairs):
        assert warehouse_at < kafka_at
        if position + 1 < len(warehouse_commits):
            assert kafka_at < warehouse_commits[position + 1]


def test_ordering_is_also_visible_through_mock_call_order(
    sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """Second, independent check using attached mocks rather than the recorder."""
    parent = mock.MagicMock()
    consumer = parent.consumer
    warehouse = parent.warehouse
    warehouse.cursor.return_value.__enter__.return_value.fetchall.return_value = []

    sink = CdcWarehouseSink(
        consumer=consumer,
        warehouse=warehouse,
        settings=sink_settings,
        topic="cdc.test.orders",
    )
    sink.buffer(change_event)
    sink.flush()

    names = [call[0] for call in parent.mock_calls]
    assert "warehouse.commit" in names
    assert "consumer.commit" in names
    assert names.index("warehouse.commit") < names.index("consumer.commit")
