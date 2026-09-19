"""The sink loop: buffer change events, apply them, then advance offsets.

Delivery semantics
------------------
The sink is at-least-once with no partial application. The order of operations
in :meth:`CdcWarehouseSink.flush` is the whole point of this component:

1. Apply the six upsert steps on one warehouse transaction.
2. Commit the warehouse transaction.
3. Only then commit the Kafka offsets.

If the process dies between 2 and 3 the batch is redelivered and reapplied. That
is safe because step 1 of the upsert is guarded by ``NOT EXISTS`` and step 6
deletes before it inserts, so reapplying a batch converges on the same state.

If the order were reversed, a crash between the offset commit and the warehouse
commit would silently drop the batch. Committing offsets last turns a possible
data loss into a possible duplicate, and the upsert is built to absorb the
duplicate.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from . import queries
from .config import SinkSettings
from .warehouse import WarehouseManager

logger = logging.getLogger(__name__)

#: Fields a change event must carry to be processable at all.
REQUIRED_FIELDS = frozenset({"op_type", "link_origin", "order_id"})

#: Operations the sink applies. Updates are ignored because the CDC connector
#: emits them as a delete followed by an insert.
APPLIED_OPERATIONS = frozenset({"I", "D"})

#: Link origin produced by the automated resolution pipeline. Those links are
#: already written by that pipeline, so replaying them here would be redundant.
AUTOMATIC_ORIGIN = "A"


def _to_utc_timestamp(epoch_millis: Any) -> str:
    """Render a millisecond epoch as a naive UTC timestamp string."""
    moment = datetime.fromtimestamp(int(epoch_millis) / 1000, tz=timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S")


class CdcWarehouseSink:
    """Buffer change events and apply them to the warehouse in batches.

    Attributes:
        pending_records: Records buffered since the last successful flush.
        buffered: Size of ``pending_records``, kept as a counter for readability
            in the loop.
        skipped: Events discarded by the relevance filter since startup.
        flushes: Number of successful flushes since startup.
    """

    def __init__(
        self,
        consumer: Any,
        warehouse: WarehouseManager,
        settings: SinkSettings,
        topic: str,
    ) -> None:
        self.consumer = consumer
        self.warehouse = warehouse
        self.settings = settings
        self.topic = topic
        self.pending_records: list[dict[str, Any]] = []
        self.skipped = 0
        self.flushes = 0
        self._running = False

    @property
    def buffered(self) -> int:
        """Number of records waiting to be applied."""
        return len(self.pending_records)

    def subscribe(self) -> None:
        """Subscribe to the configured CDC topic."""
        logger.info("Subscribing to topic %s", self.topic)
        self.consumer.subscribe([self.topic])

    def is_relevant(self, value: dict[str, Any] | None) -> bool:
        """Decide whether a change event should be applied to the warehouse.

        Rejects malformed payloads, automatic link creations owned by another
        pipeline, updates (delivered as delete/insert pairs) and events with no
        resolvable order.
        """
        if not value:
            return False
        if not REQUIRED_FIELDS.issubset(value):
            logger.warning(
                "Discarding event missing required fields: %s",
                sorted(REQUIRED_FIELDS.difference(value)),
            )
            return False
        if value["order_id"] is None:
            return False
        if value["op_type"] not in APPLIED_OPERATIONS:
            return False
        is_automatic_insert = value["op_type"] == "I" and value["link_origin"] == AUTOMATIC_ORIGIN
        return not is_automatic_insert

    def buffer(self, value: dict[str, Any]) -> dict[str, Any]:
        """Normalise a change event and add it to the pending batch."""
        record = {
            "order_id": value["order_id"],
            "customer_ref": value.get("customer_ref"),
            "channel_id": value.get("channel_id"),
            "status": value.get("status"),
            "event_date": value.get("event_date"),
            "link_origin": value["link_origin"],
            "op_type": value["op_type"],
            "op_timestamp": _to_utc_timestamp(value["op_ts"]),
        }
        self.pending_records.append(record)
        logger.debug("Buffered record %s of %s", self.buffered, self.settings.batch_size)
        return record

    def flush(self) -> int:
        """Apply the pending batch, commit the warehouse, then commit Kafka.

        Returns:
            The number of records applied.

        Raises:
            Exception: Anything the warehouse raises is re-raised after a
                rollback. Kafka offsets are left untouched in that case, so the
                batch is redelivered.
        """
        if not self.pending_records:
            return 0

        batch_size = len(self.pending_records)
        logger.info("Flushing %s records", batch_size)

        try:
            with self.warehouse.cursor() as cursor:
                logger.info("Step 1: backfill missing catalog customers")
                queries.insert_missing_customers(
                    cursor, self.pending_records, self.settings.catalog_source_id
                )

                logger.info("Step 2: stage the batch against current state")
                queries.stage_batch(
                    cursor,
                    self.pending_records,
                    self.settings.catalog_source_id,
                    self.settings.default_link_score,
                )

                logger.info("Step 3: append candidate history")
                queries.append_candidate_history(cursor)

                logger.info("Step 4: refresh current candidates")
                queries.upsert_candidates(cursor)

                logger.info("Step 5: append confirmed link history")
                queries.append_link_history(cursor)

                logger.info("Step 6: replay operations onto current links")
                queries.apply_link_current_state(cursor)

            # The warehouse transaction is committed first. Until this returns,
            # nothing in the batch is visible and nothing has been acknowledged.
            logger.info("Committing warehouse transaction")
            self.warehouse.commit()
        except Exception:
            logger.exception("Batch failed, rolling back and keeping offsets")
            self.warehouse.rollback()
            raise

        # Offsets are advanced only now, after the data is durable.
        logger.info("Committing Kafka offsets")
        self.consumer.commit(asynchronous=False)

        self.pending_records = []
        self.flushes += 1
        return batch_size

    def on_idle_poll(self) -> int:
        """Handle a poll that returned nothing.

        A partially filled batch must not wait for more traffic, otherwise a
        quiet topic would hold records in memory indefinitely. An idle poll is
        therefore a flush trigger in its own right.
        """
        applied = 0
        if self.pending_records:
            logger.info("Poll returned nothing, flushing partial batch")
            applied = self.flush()
        else:
            logger.debug("Poll returned nothing and no records are pending")
        time.sleep(self.settings.sleep_interval_seconds)
        return applied

    def handle_message(self, message: Any) -> bool:
        """Process one polled message.

        Returns:
            True if the message was buffered, False if it was discarded.
        """
        error = message.error()
        if error is not None:
            logger.warning("Skipping message with transport error: %s", error)
            return False

        value = message.value()
        if not self.is_relevant(value):
            self.skipped += 1
            # Discarded events still occupy offsets. They can be acknowledged
            # immediately, but only while no batch is pending: committing here
            # with records buffered would acknowledge data that is not yet in
            # the warehouse.
            if not self.pending_records:
                self.consumer.commit(asynchronous=False)
            return False

        self.buffer(value)
        return True

    def run(self) -> None:
        """Consume until interrupted.

        The loop is intentionally flat: poll, filter, buffer, flush. The only
        recovery path is a resubscribe, which covers the case where the client
        has been detached from its partitions and ``poll`` starts raising
        ``RuntimeError``.
        """
        self.subscribe()
        self._running = True
        polled = 0

        while self._running:
            try:
                message = self.consumer.poll(self.settings.poll_timeout_seconds)
                if message is None:
                    self.on_idle_poll()
                    continue

                polled += 1
                logger.debug("Polled message %s", polled)
                self.handle_message(message)

                if self.buffered >= self.settings.batch_size:
                    self.flush()
            except KeyboardInterrupt:
                logger.info("Interrupted, shutting down")
                break
            except RuntimeError:
                # Raised by the client when the consumer is no longer usable.
                # Offsets stay where they are, so resubscribing replays anything
                # that was buffered but not committed.
                logger.warning("Consumer raised RuntimeError, resubscribing")
                self.pending_records = []
                self.subscribe()

        self.close()

    def stop(self) -> None:
        """Ask the loop to exit after the current iteration."""
        self._running = False

    def close(self) -> None:
        """Release the consumer and the warehouse connection."""
        logger.info(
            "Closing sink after %s flushes and %s skipped events",
            self.flushes,
            self.skipped,
        )
        self.consumer.close()
        self.warehouse.close()
