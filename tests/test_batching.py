"""Tests for the batching, filtering and recovery behaviour of the sink loop."""

from __future__ import annotations

from typing import Any

import pytest

from cdc_sink.config import SinkSettings
from cdc_sink.sink import CdcWarehouseSink
from tests.conftest import FakeConsumer, FakeMessage, FakeWarehouse, Recorder


def make_sink(
    recorder: Recorder,
    settings: SinkSettings,
    poll_results: list | None = None,
) -> CdcWarehouseSink:
    consumer = FakeConsumer(poll_results=poll_results, recorder=recorder)
    warehouse = FakeWarehouse(recorder=recorder)
    return CdcWarehouseSink(
        consumer=consumer,
        warehouse=warehouse,  # type: ignore[arg-type]
        settings=settings,
        topic="cdc.test.orders",
    )


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def test_buffer_normalises_the_event(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink = make_sink(recorder, sink_settings)

    record = sink.buffer(change_event)

    assert record["order_id"] == "ORD-0001"
    assert record["customer_ref"] == "CUST-00001"
    assert record["op_type"] == "I"
    assert sink.buffered == 1


def test_buffer_converts_the_operation_timestamp_to_utc(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """op_ts arrives as epoch milliseconds and must land as a UTC timestamp."""
    sink = make_sink(recorder, sink_settings)

    record = sink.buffer({**change_event, "op_ts": 1767600000000})

    assert record["op_timestamp"] == "2026-01-05 08:00:00"


def test_buffer_tolerates_absent_optional_fields(
    recorder: Recorder, sink_settings: SinkSettings
) -> None:
    sink = make_sink(recorder, sink_settings)

    record = sink.buffer(
        {"order_id": "ORD-0002", "link_origin": "M", "op_type": "D", "op_ts": 1767600000000}
    )

    assert record["customer_ref"] is None
    assert record["status"] is None


# ---------------------------------------------------------------------------
# Relevance filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides, expected, reason",
    [
        ({}, True, "manual insert is applied"),
        ({"op_type": "D"}, True, "delete is applied"),
        ({"op_type": "D", "link_origin": "A"}, True, "automatic delete is still applied"),
        ({"op_type": "I", "link_origin": "A"}, False, "automatic insert is owned elsewhere"),
        ({"op_type": "U"}, False, "updates arrive as a delete/insert pair"),
        ({"op_type": "X"}, False, "unknown operation"),
        ({"order_id": None}, False, "unresolvable order"),
    ],
)
def test_relevance_filter(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    overrides: dict[str, Any],
    expected: bool,
    reason: str,
) -> None:
    sink = make_sink(recorder, sink_settings)

    assert sink.is_relevant({**change_event, **overrides}) is expected, reason


@pytest.mark.parametrize("value", [None, {}, {"op_type": "I"}])
def test_malformed_payloads_are_rejected(
    recorder: Recorder, sink_settings: SinkSettings, value: Any
) -> None:
    sink = make_sink(recorder, sink_settings)

    assert sink.is_relevant(value) is False


# ---------------------------------------------------------------------------
# Skipping without losing data
# ---------------------------------------------------------------------------


def test_skipped_event_is_acknowledged_when_nothing_is_pending(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink = make_sink(recorder, sink_settings)
    irrelevant = FakeMessage({**change_event, "op_type": "U"})

    assert sink.handle_message(irrelevant) is False
    assert sink.consumer.commits == [{"asynchronous": False}]
    assert sink.skipped == 1


def test_skipped_event_is_not_acknowledged_while_a_batch_is_pending(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """Committing here would acknowledge buffered records that are not applied yet."""
    sink = make_sink(recorder, sink_settings)
    sink.buffer(change_event)

    sink.handle_message(FakeMessage({**change_event, "op_type": "U"}))

    assert sink.consumer.commits == []
    assert sink.buffered == 1


def test_transport_errors_are_skipped(recorder: Recorder, sink_settings: SinkSettings) -> None:
    sink = make_sink(recorder, sink_settings)

    assert sink.handle_message(FakeMessage(error="partition eof")) is False
    assert sink.buffered == 0


# ---------------------------------------------------------------------------
# Flush triggers
# ---------------------------------------------------------------------------


def test_batch_size_triggers_a_flush(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    """batch_size is 3, so three relevant events produce exactly one flush."""
    messages = [FakeMessage(change_event) for _ in range(3)]
    sink = make_sink(recorder, sink_settings, poll_results=messages)

    sink.run()

    assert sink.flushes == 1
    assert sink.buffered == 0


def test_empty_poll_flushes_a_partial_batch(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    """A quiet topic must not leave records sitting in memory indefinitely."""
    sink = make_sink(recorder, sink_settings)
    sink.buffer(change_event)

    applied = sink.on_idle_poll()

    assert applied == 1
    assert sink.buffered == 0
    assert recorder.index("warehouse.commit") < recorder.index("kafka.commit")


def test_empty_poll_with_an_empty_buffer_commits_nothing(
    recorder: Recorder, sink_settings: SinkSettings, no_sleep: None
) -> None:
    sink = make_sink(recorder, sink_settings)

    assert sink.on_idle_poll() == 0
    assert recorder.events == []
    assert sink.consumer.commits == []


def test_partial_batch_is_flushed_by_the_idle_poll_inside_the_loop(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    """Two events then silence: batch_size is never reached, the idle poll flushes."""
    poll_results = [FakeMessage(change_event), FakeMessage(change_event), None]
    sink = make_sink(recorder, sink_settings, poll_results=poll_results)

    sink.run()

    assert sink.flushes == 1
    assert sink.buffered == 0


def test_irrelevant_events_do_not_trigger_a_flush(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    automatic = {**change_event, "link_origin": "A"}
    poll_results = [FakeMessage(automatic) for _ in range(5)]
    sink = make_sink(recorder, sink_settings, poll_results=poll_results)

    sink.run()

    assert sink.flushes == 0
    assert sink.skipped == 5


# ---------------------------------------------------------------------------
# Loop lifecycle and recovery
# ---------------------------------------------------------------------------


def test_run_subscribes_before_polling(
    recorder: Recorder, sink_settings: SinkSettings, no_sleep: None
) -> None:
    sink = make_sink(recorder, sink_settings)

    sink.run()

    assert sink.consumer.subscriptions == [["cdc.test.orders"]]
    assert recorder.events[0] == "kafka.subscribe"


def test_keyboard_interrupt_closes_cleanly(
    recorder: Recorder, sink_settings: SinkSettings, no_sleep: None
) -> None:
    sink = make_sink(recorder, sink_settings, poll_results=[KeyboardInterrupt])

    sink.run()

    assert sink.consumer.closed is True
    assert sink.warehouse.closed is True


def test_runtime_error_resubscribes_and_keeps_going(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    """A detached consumer is recovered by resubscribing, not by dying."""
    poll_results = [
        FakeMessage(change_event),
        RuntimeError("Consumer closed"),
        FakeMessage(change_event),
        KeyboardInterrupt,
    ]
    sink = make_sink(recorder, sink_settings, poll_results=poll_results)

    sink.run()

    assert sink.consumer.subscriptions == [["cdc.test.orders"], ["cdc.test.orders"]]
    assert recorder.count("kafka.subscribe") == 2


def test_runtime_error_discards_the_uncommitted_batch(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    """Offsets were never committed for those records, so Kafka will replay them.

    Keeping the in-memory copy as well would apply them twice within one run.
    """
    poll_results = [FakeMessage(change_event), RuntimeError("Consumer closed"), KeyboardInterrupt]
    sink = make_sink(recorder, sink_settings, poll_results=poll_results)

    sink.run()

    assert sink.buffered == 0
    assert sink.flushes == 0
    assert "kafka.commit" not in recorder


def test_stop_exits_the_loop_after_the_current_iteration(
    recorder: Recorder,
    sink_settings: SinkSettings,
    change_event: dict[str, Any],
    no_sleep: None,
) -> None:
    sink = make_sink(recorder, sink_settings)

    class StoppingMessage(FakeMessage):
        """Asks the sink to stop while its payload is being read."""

        def value(self) -> Any:
            sink.stop()
            return super().value()

    sink.consumer.poll_results = [
        StoppingMessage(change_event),
        FakeMessage(change_event),
    ]

    sink.run()

    assert sink.consumer.polls == 1
    assert sink.consumer.closed is True
