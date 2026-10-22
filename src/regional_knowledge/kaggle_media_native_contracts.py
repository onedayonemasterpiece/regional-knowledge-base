"""Native-safe immutable media read DTO and runtime binding identity.

Keep resident Kaggle reader/worker free of Dev MCP, ReadAuthority, SourceReadGate,
OAuth and server modules. Only stdlib imports. The Dev output gate uses the
SAME class, but performs independent audience, source and issuance checks.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import json

def canonical_media(value: Any) -> bytes:
    try:
        return json.dumps(value,sort_keys=True,separators=(",",":"),
                          ensure_ascii=False,allow_nan=False).encode("utf-8")
    except (TypeError,ValueError,OverflowError,RecursionError):
        raise ValueError("invalid_native_media_proof") from None

def native_binding_key(binding: Any) -> tuple:
    fields=("run_id","runtime_incarnation","connection_generation",
            "database_generation","min_data_seq","provider_version")
    values=tuple(getattr(binding,name,None) for name in fields)
    if (any(type(values[i]) is not str or not 1 <= len(values[i].encode()) <= 256
            for i in (0,1,3,5)) or type(values[2]) is not int or values[2]<1
            or type(values[4]) is not int or values[4]<0):
        raise ValueError("trusted native runtime binding required")
    return values

@dataclass(frozen=True)
class NativeMediaSnapshot:
    """Internal, bounded, one-illustration native PG readback from SQL015+014.

    The existing media reader's PUBLIC row has data with crop_binding removed.
    The owner must additionally resolve PRIVATE source/page/region/crop/availability
    from the same canonical PG transaction; do not substitute model JSON.
    """

    source_id: str
    illustration_id: str
    source_revision: int
    eligibility_revision: int
    accepted: bool
    withdrawn: bool
    original_sha256: str
    origin_original_sha256: str
    media_candidate_sha256: str
    media_origin_sha256: str
    item: dict[str, Any] = field(repr=False)
    manifest_page: dict[str, Any] = field(repr=False)
    manifest_region: dict[str, Any] = field(repr=False)
    public_row: dict[str, Any] = field(repr=False)
    archive_state: dict[str, Any] = field(repr=False)
    archive_chat_id: int = field(repr=False)
    archive_topic_id: int = field(repr=False)
    archive_scope: str = field(repr=False)
    native_reader_sha256: str
    database_generation: str
    observed_data_seq: int
    observed_projection_seq: int
    observed_visibility_epoch: int

