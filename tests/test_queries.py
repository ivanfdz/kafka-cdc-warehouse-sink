"""Tests for the six upsert steps.

These run against a recording cursor rather than a database, so what is asserted
is the shape of the SQL and the parameters: that values are bound rather than
interpolated, that the steps read the staging table, and that step 6 replays
operations in the right order with a delete in front of every insert.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import pytest

from cdc_sink import queries
from cdc_sink.hashing import entity_hash, surrogate_id
from tests.conftest import FakeCursor

CATALOG_SOURCE_ID = 12
LINK_SCORE = 2


def records(count: int = 2) -> list[dict[str, Any]]:
    return [
        {
            "order_id": f"ORD-{index:04d}",
            "customer_ref": f"CUST-{index:05d}",
            "op_type": "I",
            "op_timestamp": "2026-01-05 08:00:00",
        }
        for index in range(1, count + 1)
    ]


#: Matches "%s" optionally followed by an explicit "::cast".
PLACEHOLDER = re.compile(r"%s(?:::(\w+))?")

TIMESTAMP_TEXT = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def assert_parameters_align(sql: str, params: Sequence[Any]) -> None:
    """Check that each bound value matches the cast of its own placeholder.

    Parameters are positional, so a statement that mixes its own placeholders
    with those of the inlined batch is easy to get wrong: the values still count
    correctly while landing one position out. Comparing each value against the
    cast next to its placeholder catches that, which a count comparison cannot.
    """
    casts = PLACEHOLDER.findall(sql)
    assert len(casts) == len(params), f"{len(casts)} placeholders but {len(params)} parameters"

    for position, (cast, value) in enumerate(zip(casts, params, strict=True)):
        if cast == "timestamp":
            assert isinstance(value, str) and TIMESTAMP_TEXT.match(value), (
                f"parameter {position} is bound to a ::timestamp placeholder "
                f"but is not a timestamp: {value!r}"
            )
        elif cast == "int":
            assert isinstance(value, int), (
                f"parameter {position} is bound to an ::int placeholder "
                f"but is not an integer: {value!r}"
            )
        elif cast == "varchar":
            assert value is None or isinstance(value, str), (
                f"parameter {position} is bound to a ::varchar placeholder "
                f"but is not text: {value!r}"
            )


def staged_row(order_id: str, op_type: str, created_at: str) -> tuple:
    """A row in the layout returned by SELECT_STAGED_OPERATIONS."""
    return (
        1,
        order_id,
        CATALOG_SOURCE_ID,
        surrogate_id(["catalog", "CUST-00001", None]),
        LINK_SCORE,
        -1,
        "manual",
        created_at,
        created_at,
        op_type,
    )


# ---------------------------------------------------------------------------
# The inlined batch relation
# ---------------------------------------------------------------------------


def test_batch_relation_binds_every_value() -> None:
    sql, params = queries.batch_relation(records(3))

    assert sql.count("%s") == 3 * len(queries.BASE_COLUMNS)
    assert len(params) == 3 * len(queries.BASE_COLUMNS)
    assert sql.count("UNION ALL") == 2


def test_batch_relation_does_not_interpolate_values() -> None:
    """A customer reference must never appear in the statement text."""
    sql, params = queries.batch_relation(records(2))

    assert "CUST-00001" not in sql
    assert "ORD-0001" not in sql
    assert "CUST-00001" in params


def test_batch_relation_casts_every_column() -> None:
    enriched = queries.enrich_with_customer_keys(records(1))

    sql, _params = queries.batch_relation(enriched, queries.EXTENDED_COLUMNS)

    for column in queries.EXTENDED_COLUMNS:
        assert f"AS {column}" in sql
    assert "::timestamp" in sql


def test_batch_relation_rejects_an_empty_batch() -> None:
    with pytest.raises(ValueError, match="empty batch"):
        queries.batch_relation([])


# ---------------------------------------------------------------------------
# Surrogate keys
# ---------------------------------------------------------------------------


def test_customer_keys_are_deterministic() -> None:
    first = queries.enrich_with_customer_keys(records(1))
    second = queries.enrich_with_customer_keys(records(1))

    assert first[0]["customer_id"] == second[0]["customer_id"]
    assert first[0]["customer_hash"] == second[0]["customer_hash"]


def test_customer_keys_differ_per_reference() -> None:
    enriched = queries.enrich_with_customer_keys(records(2))

    assert enriched[0]["customer_id"] != enriched[1]["customer_id"]


def test_customer_key_matches_the_documented_recipe() -> None:
    enriched = queries.enrich_with_customer_keys(records(1))
    parts = ["catalog", "CUST-00001", None]

    assert enriched[0]["customer_id"] == surrogate_id(parts)
    assert enriched[0]["customer_hash"] == entity_hash(parts)


def test_entity_hash_ignores_null_like_parts() -> None:
    assert entity_hash(["catalog", "CUST-1", None]) == entity_hash(["catalog", "CUST-1", "unknown"])


# ---------------------------------------------------------------------------
# Step 1
# ---------------------------------------------------------------------------


def test_step_one_is_guarded_by_not_exists() -> None:
    cursor = FakeCursor()

    queries.insert_missing_customers(cursor, records(2), CATALOG_SOURCE_ID)

    sql, params = cursor.statements[0]
    assert "INSERT INTO customers_reference" in sql
    assert "NOT EXISTS" in sql
    # The source id appears twice in the text: in the projection and in the guard.
    assert params[0] == CATALOG_SOURCE_ID
    assert params[-1] == CATALOG_SOURCE_ID


def test_step_one_groups_the_batch_before_inserting() -> None:
    """A batch mentioning the same new customer twice must insert one row."""
    cursor = FakeCursor()

    queries.insert_missing_customers(cursor, records(2), CATALOG_SOURCE_ID)

    sql, _params = cursor.statements[0]
    assert "GROUP BY" in sql
    assert "MIN(batch.op_timestamp)" in sql


def test_step_one_passes_the_extended_column_set() -> None:
    cursor = FakeCursor()

    queries.insert_missing_customers(cursor, records(2), CATALOG_SOURCE_ID)

    _sql, params = cursor.statements[0]
    # Two records times six columns, plus the two source-id parameters.
    assert len(params) == 2 * len(queries.EXTENDED_COLUMNS) + 2


def test_step_one_parameters_line_up_with_their_placeholders() -> None:
    """Regression guard: the step's own placeholders bracket the inlined batch."""
    cursor = FakeCursor()

    queries.insert_missing_customers(cursor, records(3), CATALOG_SOURCE_ID)

    sql, params = cursor.statements[0]
    assert_parameters_align(sql, params)


# ---------------------------------------------------------------------------
# Step 2
# ---------------------------------------------------------------------------


def test_step_two_drops_then_recreates_the_staging_table() -> None:
    cursor = FakeCursor()

    queries.stage_batch(cursor, records(2), CATALOG_SOURCE_ID, LINK_SCORE)

    assert cursor.sql_texts[0].strip().startswith("DROP TABLE IF EXISTS")
    assert "CREATE TEMPORARY TABLE stage_order_events" in cursor.sql_texts[1]


def test_step_two_restricts_orders_to_operational_sources() -> None:
    cursor = FakeCursor()

    queries.stage_batch(cursor, records(1), CATALOG_SOURCE_ID, LINK_SCORE)

    create_sql = cursor.sql_texts[1]
    assert "sources_reference" in create_sql
    # The literal percent signs are doubled because parameters are bound.
    assert "'%%operational%%'" in create_sql


def test_step_two_left_joins_the_customer_side() -> None:
    """A brand new customer must not drop the event; the join stays outer."""
    cursor = FakeCursor()

    queries.stage_batch(cursor, records(1), CATALOG_SOURCE_ID, LINK_SCORE)

    create_sql = cursor.sql_texts[1]
    assert "LEFT JOIN customers_reference" in create_sql
    assert "JOIN orders_reference" in create_sql


def test_step_two_binds_the_score_first_and_the_source_id_last() -> None:
    cursor = FakeCursor()

    queries.stage_batch(cursor, records(2), CATALOG_SOURCE_ID, LINK_SCORE)

    _sql, params = cursor.statements[1]
    assert params[0] == LINK_SCORE
    assert params[-1] == CATALOG_SOURCE_ID
    assert len(params) == 2 + 2 * len(queries.BASE_COLUMNS)


def test_step_two_parameters_line_up_with_their_placeholders() -> None:
    cursor = FakeCursor()

    queries.stage_batch(cursor, records(3), CATALOG_SOURCE_ID, LINK_SCORE)

    sql, params = cursor.statements[1]
    assert_parameters_align(sql, params)


def test_batch_relation_parameters_line_up_with_their_placeholders() -> None:
    enriched = queries.enrich_with_customer_keys(records(3))

    sql, params = queries.batch_relation(enriched, queries.EXTENDED_COLUMNS)

    assert_parameters_align(sql, params)


# ---------------------------------------------------------------------------
# Steps 3 to 5
# ---------------------------------------------------------------------------


def test_step_three_only_records_inserts() -> None:
    cursor = FakeCursor()

    queries.append_candidate_history(cursor)

    sql, params = cursor.statements[0]
    assert "INSERT INTO order_customer_candidates_history" in sql
    assert "WHERE op_type = 'I'" in sql
    assert params == [queries.ACTION_INSERT]


def test_step_four_clears_previous_candidates_first() -> None:
    cursor = FakeCursor()

    queries.upsert_candidates(cursor)

    assert "DELETE FROM order_customer_candidates" in cursor.sql_texts[0]
    assert "INSERT INTO order_customer_candidates" in cursor.sql_texts[1]


def test_step_five_records_both_operations() -> None:
    cursor = FakeCursor()

    queries.append_link_history(cursor)

    sql, params = cursor.statements[0]
    assert "INSERT INTO order_customer_links_history" in sql
    assert "WHERE op_type IN ('D', 'I')" in sql
    assert params == [queries.ACTION_DELETE, queries.ACTION_INSERT]


def test_history_steps_read_only_the_staging_table() -> None:
    """Steps 3 to 5 must not re-join the reference tables; step 2 already did."""
    for step in (
        queries.append_candidate_history,
        queries.upsert_candidates,
        queries.append_link_history,
    ):
        cursor = FakeCursor()
        step(cursor)
        for sql in cursor.sql_texts:
            assert queries.STAGE_TABLE in sql
            assert "orders_reference" not in sql
            assert "customers_reference" not in sql


# ---------------------------------------------------------------------------
# Step 6
# ---------------------------------------------------------------------------


def test_step_six_orders_deletes_before_inserts() -> None:
    assert "ORDER BY" in queries.SELECT_STAGED_OPERATIONS
    assert "WHEN 'D' THEN 1" in queries.SELECT_STAGED_OPERATIONS
    assert "WHEN 'I' THEN 2" in queries.SELECT_STAGED_OPERATIONS


def test_step_six_deletes_before_every_insert() -> None:
    """Delete-then-insert is what makes redelivery converge instead of duplicate."""
    cursor = FakeCursor(fetch_rows=[staged_row("ORD-0001", "I", "2026-01-05 08:00:00")])

    applied = queries.apply_link_current_state(cursor)

    assert applied == 1
    # First statement is the SELECT, then the DELETE, then the INSERT.
    assert "DELETE FROM order_customer_links" in cursor.sql_texts[1]
    assert "INSERT INTO order_customer_links" in cursor.sql_texts[2]


def test_step_six_emits_only_a_delete_for_a_delete() -> None:
    cursor = FakeCursor(fetch_rows=[staged_row("ORD-0001", "D", "2026-01-05 08:00:00")])

    applied = queries.apply_link_current_state(cursor)

    assert applied == 1
    assert len(cursor.statements_matching("delete from order_customer_links")) == 1
    assert cursor.statements_matching("insert into order_customer_links") == []


def test_step_six_replays_a_delete_insert_pair_in_order() -> None:
    cursor = FakeCursor(
        fetch_rows=[
            staged_row("ORD-0001", "D", "2026-01-05 08:00:00"),
            staged_row("ORD-0001", "I", "2026-01-05 08:00:01"),
        ]
    )

    applied = queries.apply_link_current_state(cursor)

    assert applied == 2
    operations = [
        "delete" if "DELETE" in sql else "insert" if "INSERT" in sql else "select"
        for sql in cursor.sql_texts
    ]
    assert operations == ["select", "delete", "delete", "insert"]


def test_step_six_binds_row_values() -> None:
    cursor = FakeCursor(fetch_rows=[staged_row("ORD-0001", "I", "2026-01-05 08:00:00")])

    queries.apply_link_current_state(cursor)

    delete_sql, delete_params = cursor.statements[1]
    assert "ORD-0001" not in delete_sql
    assert delete_params == [1, "ORD-0001"]
    _insert_sql, insert_params = cursor.statements[2]
    assert insert_params[1] == "ORD-0001"
    assert insert_params[4] == queries.ACTION_INSERT


def test_step_six_skips_unexpected_operations() -> None:
    cursor = FakeCursor(fetch_rows=[staged_row("ORD-0001", "X", "2026-01-05 08:00:00")])

    applied = queries.apply_link_current_state(cursor)

    assert applied == 0
    assert cursor.sql_texts == [queries.SELECT_STAGED_OPERATIONS]


def test_step_six_on_an_empty_staging_table_is_a_no_op() -> None:
    cursor = FakeCursor(fetch_rows=[])

    assert queries.apply_link_current_state(cursor) == 0
