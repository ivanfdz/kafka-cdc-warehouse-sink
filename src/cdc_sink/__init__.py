"""Kafka CDC to analytical warehouse sink.

A consumer that reads Avro change-data-capture events from Kafka, batches them,
and applies each batch to an analytical warehouse inside a single transaction.
Kafka offsets are committed only after the warehouse transaction commits.
"""

__version__ = "1.0.0"

__all__ = ["__version__"]
