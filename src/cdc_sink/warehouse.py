"""Long-lived warehouse connection used as the transactional boundary.

One connection is opened at startup and reused for every batch, because the
warehouses this sink targets are slow to authenticate and a per-batch connect
would dominate the latency budget.

Autocommit stays off. A batch is applied as a single transaction and the caller
decides when to commit, which is what allows Kafka offsets to be advanced only
after the warehouse has durably accepted the batch.
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PgConnection

from .config import WarehouseSettings

logger = logging.getLogger(__name__)


class WarehouseManager:
    """Manage a single reusable warehouse connection.

    The driver speaks the PostgreSQL wire protocol, which also covers
    PostgreSQL-compatible analytical warehouses such as Amazon Redshift. The SQL
    in ``queries.py`` is restricted to the intersection of both dialects.
    """

    def __init__(self, settings: WarehouseSettings) -> None:
        self.settings = settings
        self._connection: PgConnection | None = None

    def connect(self) -> PgConnection:
        """Open a new connection with autocommit disabled."""
        logger.info(
            "Connecting to warehouse %s:%s/%s as %s",
            self.settings.host,
            self.settings.port,
            self.settings.database,
            self.settings.user,
        )
        conn = psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=self.settings.connect_timeout,
            application_name=self.settings.application_name,
            options=f"-c search_path={self.settings.schema}",
        )
        conn.autocommit = False
        return conn

    @property
    def connection(self) -> PgConnection:
        """Return the live connection, opening one on first use."""
        if self._connection is None or self._connection.closed:
            self._connection = self.connect()
        return self._connection

    def reconnect(self) -> PgConnection:
        """Drop the current connection and open a fresh one."""
        self.close()
        self._connection = self.connect()
        return self._connection

    def cursor(self) -> Any:
        """Return a new cursor on the live connection."""
        return self.connection.cursor()

    def commit(self) -> None:
        """Commit the open transaction."""
        self.connection.commit()

    def rollback(self) -> None:
        """Roll back the open transaction, ignoring a dead connection."""
        if self._connection is not None and not self._connection.closed:
            self._connection.rollback()

    def close(self) -> None:
        """Close the connection if it is still open."""
        if self._connection is not None and not self._connection.closed:
            self._connection.close()
        self._connection = None

    def __enter__(self) -> WarehouseManager:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
