"""Deterministic surrogate keys for warehouse reference rows.

The warehouse uses content-derived surrogate keys so that two independent runs
of the pipeline produce the same identifier for the same business entity. That
is what makes the reference backfill in step 1 of the upsert idempotent.

MD5 is used as a fast, fixed-width, non-cryptographic fingerprint. It is never
used for authentication or integrity checks.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from hashlib import md5

_NULL_LIKE = frozenset({"", "none", "null", "unknown", "n/a"})


def surrogate_id(parts: Sequence[object]) -> str:
    """Return a stable 32-character identifier for an ordered list of parts.

    Every part takes part in the key, including empty ones, so the identifier
    is sensitive to the exact position of each value.
    """
    payload = "|".join("" if part is None else str(part) for part in parts)
    return md5(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


def entity_hash(parts: Iterable[object]) -> str:
    """Return a stable fingerprint that ignores null-like parts.

    Used to detect that two reference rows describe the same entity even when
    one of the source systems leaves optional attributes blank.
    """
    meaningful = [str(part) for part in parts if str(part).strip().lower() not in _NULL_LIKE]
    return md5(str(meaningful).encode("utf-8"), usedforsecurity=False).hexdigest()
