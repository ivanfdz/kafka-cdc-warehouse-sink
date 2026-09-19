"""Entry point: wire the configuration, the clients and the sink loop."""

from __future__ import annotations

import logging
import sys

from .config import ConfigurationError, Settings
from .kafka_manager import KafkaConsumerManager
from .sink import CdcWarehouseSink
from .warehouse import WarehouseManager

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"

logger = logging.getLogger("cdc_sink")


def configure_logging(level: str) -> None:
    """Configure root logging once, before any client is built."""
    logging.basicConfig(level=level, format=LOG_FORMAT)


def main() -> int:
    """Build the pipeline and run it until interrupted."""
    try:
        settings = Settings.from_env()
    except ConfigurationError as exc:
        logging.basicConfig(level="INFO", format=LOG_FORMAT)
        logger.error("Invalid configuration: %s", exc)
        return 2

    configure_logging(settings.log_level)
    logger.info(
        "Starting sink for environment %s, topic %s, group %s, batch size %s",
        settings.environment,
        settings.kafka.consumer_topic,
        settings.kafka.consumer_group,
        settings.sink.batch_size,
    )

    consumer_manager = KafkaConsumerManager(settings.kafka)
    warehouse = WarehouseManager(settings.warehouse)
    warehouse.connect()

    sink = CdcWarehouseSink(
        consumer=consumer_manager.consumer,
        warehouse=warehouse,
        settings=settings.sink,
        topic=settings.kafka.consumer_topic,
    )
    sink.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
