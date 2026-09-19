"""The flush must run all six upsert steps, in order, on one cursor."""

from __future__ import annotations

from typing import Any

from cdc_sink.config import SinkSettings
from cdc_sink.sink import CdcWarehouseSink
from tests.conftest import FakeConsumer, FakeWarehouse, Recorder

#: Fragment identifying each step in the order the flush must run them.
EXPECTED_STEPS: list[str] = [
    "insert into customers_reference",  # 1 backfill catalog customers
    "create temporary table stage_order_events",  # 2 stage against current state
    "insert into order_customer_candidates_history",  # 3 candidate history
    "insert into order_customer_candidates (",  # 4 current candidates
    "insert into order_customer_links_history",  # 5 link history
    "when 'd' then 1",  # 6 ordered replay
]


def build(recorder: Recorder, settings: SinkSettings) -> CdcWarehouseSink:
    return CdcWarehouseSink(
        consumer=FakeConsumer(recorder=recorder),
        warehouse=FakeWarehouse(recorder=recorder),  # type: ignore[arg-type]
        settings=settings,
        topic="cdc.test.orders",
    )


def test_all_six_steps_run_in_order(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink = build(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    texts = [sql.lower() for sql in sink.warehouse.last_cursor.sql_texts]
    positions = []
    for fragment in EXPECTED_STEPS:
        matches = [index for index, sql in enumerate(texts) if fragment in sql]
        assert matches, f"step not executed: {fragment}"
        positions.append(matches[0])

    assert positions == sorted(positions), "the six steps did not run in order"


def test_the_whole_batch_uses_a_single_cursor(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """One cursor per batch means one transaction per batch."""
    sink = build(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    assert len(sink.warehouse.cursors) == 1
    assert sink.warehouse.last_cursor.closed is True


def test_each_batch_gets_its_own_transaction(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    sink = build(recorder, sink_settings)

    for _ in range(2):
        sink.buffer(change_event)
        sink.flush()

    assert len(sink.warehouse.cursors) == 2
    assert sink.warehouse.commits == 2


def test_batch_values_reach_the_warehouse_as_parameters(
    recorder: Recorder, sink_settings: SinkSettings, change_event: dict[str, Any]
) -> None:
    """No business value may be interpolated into the statement text."""
    sink = build(recorder, sink_settings)
    sink.buffer(change_event)

    sink.flush()

    cursor = sink.warehouse.last_cursor
    for sql in cursor.sql_texts:
        assert "ORD-0001" not in sql
        assert "CUST-00001" not in sql
    assert "ORD-0001" in cursor.all_params()
    assert "CUST-00001" in cursor.all_params()


def test_catalog_source_id_from_settings_is_used(
    recorder: Recorder, change_event: dict[str, Any]
) -> None:
    settings = SinkSettings(batch_size=1, catalog_source_id=77, default_link_score=9)
    sink = build(recorder, settings)
    sink.buffer(change_event)

    sink.flush()

    params = sink.warehouse.last_cursor.all_params()
    assert 77 in params
    assert 9 in params
