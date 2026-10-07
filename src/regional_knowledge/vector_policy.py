"""Required/optional vector-space policy for publication and indexing.

BGE is the measured production retrieval space. E5 remains available for
diagnostics and explicit fusion experiments, but it is not a default publication
gate and its local runtime must not block book activation.
"""
from __future__ import annotations

import os

_ALLOWED = {"bge", "e5"}


def required_vector_spaces() -> tuple[str, ...]:
    raw = os.getenv("RKB_REQUIRED_VECTOR_SPACES", "bge")
    values = tuple(dict.fromkeys(part.strip().lower() for part in raw.split(",") if part.strip()))
    if not values or any(value not in _ALLOWED for value in values):
        raise RuntimeError("invalid RKB_REQUIRED_VECTOR_SPACES")
    if "bge" not in values:
        raise RuntimeError("BGE must remain a required production vector space")
    return values


def index_vector_spaces() -> tuple[str, ...]:
    required = set(required_vector_spaces())
    if os.getenv("RKB_INDEX_E5_DIAGNOSTIC") == "1":
        required.add("e5")
    # BGE first: a diagnostic E5 outage must never delay durable BGE progress.
    return tuple(space for space in ("bge", "e5") if space in required)


def missing_required(*, e5_missing: int, bge_missing: int) -> bool:
    required = set(required_vector_spaces())
    return ("bge" in required and bge_missing > 0) or (
        "e5" in required and e5_missing > 0
    )
