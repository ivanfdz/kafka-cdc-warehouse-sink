"""The six-step transactional upsert applied to each batch.

Every function here executes one step against a cursor that belongs to the
caller's open transaction. None of them commits. The caller commits once, after
step 6, so a batch is either fully visible in the warehouse or not visible at
all.

Design notes
------------
No temporary staging table is created for the incoming batch itself. The batch
is inlined as a derived table built from ``SELECT ... UNION ALL SELECT ...``,
which avoids the catalog write, the vacuum pressure and the extra round trips a
real temp table costs on an analytical warehouse for a few hundred rows.

Every inlined value is passed as a bound parameter, never interpolated into the
statement text. The statement text is constant for a given batch size; only the
parameter list changes.

The SQL is limited to constructs that work both on PostgreSQL and on
PostgreSQL-compatible analytical warehouses: no ``ON CONFLICT``, no ``MERGE``,
no window functions in DML, and ``CURRENT_TIMESTAMP`` rather than vendor
specific clock functions.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from .hashing import entity_hash, surrogate_id

logger = logging.getLogger(__name__)

#: Name of the per-session staging table written by step 2.
STAGE_TABLE = "stage_order_events"

#: Logical name of the canonical customer catalog inside the reference table.
CATALOG_SOURCE_NAME = "catalog"

#: Audit actions written to the history tables.
ACTION_INSERT = "CDC_INSERT"
ACTION_DELETE = "CDC_DELETE"

BASE_COLUMNS: tuple[str, ...] = ("order_id", "customer_ref", "op_type", "op_timestamp")
EXTENDED_COLUMNS: tuple[str, ...] = BASE_COLUMNS + ("customer_id", "customer_hash")

_COLUMN_CASTS: dict[str, str] = {
    "order_id": "varchar",
    "customer_ref": "varchar",
    "op_type": "varchar",
    "op_timestamp": "timestamp",
    "customer_id": "varchar",
    "customer_hash": "varchar",
}


def batch_relation(
    records: Sequence[dict[str, Any]],
    columns: Sequence[str] = BASE_COLUMNS,
) -> tuple[str, list[Any]]:
    """Build the batch as a parameterised derived table.

    Args:
        records: Buffered change records.
        columns: Columns to project, in order.

    Returns:
        A ``(sql, params)`` pair. ``sql`` is a parenthesised derived table with
        one placeholder per value and ``params`` is the matching flat list.

    Raises:
        ValueError: If ``records`` is empty. An empty batch must never reach the
            warehouse, and an empty ``UNION ALL`` is not valid SQL.
    """
    if not records:
        raise ValueError("Cannot build a batch relation from an empty batch")

    projection = ", ".join(f"%s::{_COLUMN_CASTS[column]} AS {column}" for column in columns)
    branches = [f"    SELECT {projection}"] * len(records)
    sql = "(\n" + "\n    UNION ALL\n".join(branches) + "\n)"

    params: list[Any] = []
    for record in records:
        params.extend(record[column] for column in columns)
    return sql, params


def enrich_with_customer_keys(
    records: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach deterministic catalog keys to every record in the batch.

    The surrogate key is derived from the source name and the external customer
    reference, so the same reference always resolves to the same catalog row.
    """
    enriched: list[dict[str, Any]] = []
    for record in records:
        parts = [CATALOG_SOURCE_NAME, record["customer_ref"], None]
        enriched.append(
            {
                **record,
                "customer_id": surrogate_id(parts),
                "customer_hash": entity_hash(parts),
            }
        )
    return enriched


def insert_missing_customers(
    cursor: Any,
    records: Sequence[dict[str, Any]],
    catalog_source_id: int,
) -> int:
    """Step 1: backfill catalog customers the warehouse has never seen.

    A change event can reference a customer that no upstream load has delivered
    yet. Inserting a placeholder row first keeps the joins in step 2 inner and
    keeps the pipeline from dropping the event.

    The batch is grouped before insertion so a batch that mentions the same new
    customer twice still inserts one row, and ``NOT EXISTS`` makes the step
    idempotent across retries.
    """
    enriched = enrich_with_customer_keys(records)
    relation, params = batch_relation(enriched, EXTENDED_COLUMNS)

    sql = f"""
        INSERT INTO customers_reference (
            id, customer_hash, external_customer_id, source_id, source_name,
            customer_name, country_code, resolution_state, created_at, updated_at
        )
        SELECT
            deduplicated.customer_id,
            deduplicated.customer_hash,
            deduplicated.customer_ref,
            %s::int AS source_id,
            '{CATALOG_SOURCE_NAME}' AS source_name,
            '' AS customer_name,
            '' AS country_code,
            'PENDING' AS resolution_state,
            deduplicated.first_seen_at AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM (
            SELECT
                batch.customer_id AS customer_id,
                batch.customer_hash AS customer_hash,
                batch.customer_ref AS customer_ref,
                MIN(batch.op_timestamp) AS first_seen_at
            FROM {relation} AS batch
            GROUP BY batch.customer_id, batch.customer_hash, batch.customer_ref
        ) AS deduplicated
        WHERE NOT EXISTS (
            SELECT 1
            FROM customers_reference existing
            WHERE existing.source_id = %s
              AND existing.id = deduplicated.customer_id
        )
    """
    # Parameters are positional, so they must be listed in the order their
    # placeholders appear in the statement text: the source id in the projection
    # first, then the inlined batch, then the source id in the NOT EXISTS guard.
    cursor.execute(sql, [catalog_source_id, *params, catalog_source_id])
    return cursor.rowcount


def stage_batch(
    cursor: Any,
    records: Sequence[dict[str, Any]],
    catalog_source_id: int,
    link_score: int,
) -> int:
    """Step 2: resolve the batch against current warehouse state.

    Produces a session-scoped staging table holding one row per change event,
    already carrying the warehouse surrogate keys of both sides of the link.
    Steps 3 to 6 read only this table, so the expensive joins run once per batch
    instead of once per step.
    """
    relation, params = batch_relation(records, BASE_COLUMNS)

    cursor.execute(f"DROP TABLE IF EXISTS {STAGE_TABLE}")
    sql = f"""
        CREATE TEMPORARY TABLE {STAGE_TABLE} AS
        SELECT DISTINCT
            orders.id                AS order_id,
            orders.source_id         AS order_source_id,
            orders.source_name       AS order_source_name,
            orders.order_status      AS order_status,
            customers.id             AS customer_id,
            customers.source_id      AS customer_source_id,
            customers.source_name    AS customer_source_name,
            %s::int                  AS link_score,
            -1                       AS link_rating,
            'manual'                 AS link_method,
            batch.customer_ref       AS source_customer_ref,
            batch.op_timestamp       AS created_at,
            CURRENT_TIMESTAMP        AS updated_at,
            batch.op_type            AS op_type
        FROM {relation} AS batch
        JOIN orders_reference orders
            ON orders.id = batch.order_id
           AND orders.source_id IN (
                SELECT id
                FROM sources_reference
                WHERE source_type LIKE '%%operational%%'
           )
        LEFT JOIN customers_reference customers
            ON customers.source_id = %s
           AND customers.external_customer_id = batch.customer_ref
    """
    # Text order again: the score in the projection, the inlined batch, then the
    # source id in the customer join.
    cursor.execute(sql, [link_score, *params, catalog_source_id])
    return cursor.rowcount


def append_candidate_history(cursor: Any) -> int:
    """Step 3: append every new link proposal to the candidate history.

    Append-only. This table is the audit trail: it keeps rejected and superseded
    proposals that the current-state tables no longer show.
    """
    sql = f"""
        INSERT INTO order_customer_candidates_history (
            order_source_id, order_id, customer_source_id, customer_id,
            link_score, link_rating, link_method,
            audit_action, audit_action_status, created_at, updated_at
        )
        SELECT
            order_source_id, order_id, customer_source_id, customer_id,
            link_score, link_rating, link_method,
            %s AS audit_action, 'DONE' AS audit_action_status,
            created_at, updated_at
        FROM {STAGE_TABLE}
        WHERE op_type = 'I'
    """
    cursor.execute(sql, [ACTION_INSERT])
    return cursor.rowcount


def upsert_candidates(cursor: Any) -> int:
    """Step 4: refresh the current candidate links.

    Any previous proposal for the same order is removed first, so the table
    always reflects the latest proposal set rather than accumulating rows.
    """
    cursor.execute(
        f"""
        DELETE FROM order_customer_candidates
        USING {STAGE_TABLE} staged
        WHERE staged.op_type = 'I'
          AND staged.order_source_id = order_customer_candidates.order_source_id
          AND staged.order_id = order_customer_candidates.order_id
        """
    )
    sql = f"""
        INSERT INTO order_customer_candidates (
            order_source_id, order_id, customer_source_id, customer_id,
            link_score, link_rating, link_method,
            audit_action, audit_action_status, created_at, updated_at
        )
        SELECT
            order_source_id, order_id, customer_source_id, customer_id,
            link_score, link_rating, link_method,
            %s AS audit_action, 'DONE' AS audit_action_status,
            created_at, updated_at
        FROM {STAGE_TABLE}
        WHERE op_type = 'I'
    """
    cursor.execute(sql, [ACTION_INSERT])
    return cursor.rowcount


def append_link_history(cursor: Any) -> int:
    """Step 5: append the confirmed-link audit trail.

    Unlike step 3 this covers deletes as well, so the history reconstructs the
    full lifecycle of a link, not just its creations.
    """
    sql = f"""
        INSERT INTO order_customer_links_history (
            order_source_id, order_id, customer_source_id, customer_id,
            audit_action, audit_action_at, audit_action_status,
            link_score, link_rating, link_method, created_at, updated_at
        )
        SELECT
            order_source_id, order_id, customer_source_id, customer_id,
            CASE op_type
                WHEN 'D' THEN %s
                WHEN 'I' THEN %s
            END AS audit_action,
            CURRENT_TIMESTAMP AS audit_action_at,
            'DONE' AS audit_action_status,
            link_score, link_rating, link_method, created_at, updated_at
        FROM {STAGE_TABLE}
        WHERE op_type IN ('D', 'I')
    """
    cursor.execute(sql, [ACTION_DELETE, ACTION_INSERT])
    return cursor.rowcount


#: Statement that reads the staged batch in source-transaction order.
#:
#: Ordering matters. Within one batch the same order can be unlinked and
#: relinked, and applying those two events out of order leaves the wrong final
#: state. Deletes sort before inserts at an identical timestamp because the
#: source emits an update as a delete/insert pair.
SELECT_STAGED_OPERATIONS = f"""
    SELECT
        order_source_id, order_id, customer_source_id, customer_id,
        link_score, link_rating, link_method, created_at, updated_at, op_type
    FROM {STAGE_TABLE}
    ORDER BY
        created_at,
        CASE op_type WHEN 'D' THEN 1 WHEN 'I' THEN 2 ELSE 3 END
"""

DELETE_LINK = """
    DELETE FROM order_customer_links
    WHERE order_source_id = %s
      AND order_id = %s
"""

INSERT_LINK = """
    INSERT INTO order_customer_links (
        order_source_id, order_id, customer_source_id, customer_id,
        audit_action, audit_action_at, audit_action_status,
        link_score, link_rating, link_method, created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, CURRENT_TIMESTAMP, 'DONE',
        %s, %s, %s, %s, %s
    )
"""


def apply_link_current_state(cursor: Any) -> int:
    """Step 6: replay the staged operations onto the current-state table.

    This step is row-at-a-time on purpose. A set-based statement cannot express
    "apply these operations in this order" when the batch contains several
    operations for the same key, and collapsing them first would lose the
    intermediate audit rows that steps 3 and 5 already wrote.

    An insert is always preceded by a delete for the same order. That makes the
    step idempotent, so redelivery after a crash converges on the same state
    instead of creating duplicates.
    """
    cursor.execute(SELECT_STAGED_OPERATIONS)
    rows = cursor.fetchall()

    applied = 0
    for row in rows:
        (
            order_source_id,
            order_id,
            customer_source_id,
            customer_id,
            link_score,
            link_rating,
            link_method,
            created_at,
            updated_at,
            op_type,
        ) = row

        if op_type not in ("D", "I"):
            logger.warning("Skipping staged row with unexpected op_type %r", op_type)
            continue

        cursor.execute(DELETE_LINK, [order_source_id, order_id])
        if op_type == "I":
            cursor.execute(
                INSERT_LINK,
                [
                    order_source_id,
                    order_id,
                    customer_source_id,
                    customer_id,
                    ACTION_INSERT,
                    link_score,
                    link_rating,
                    link_method,
                    created_at,
                    updated_at,
                ],
            )
        applied += 1
    return applied
