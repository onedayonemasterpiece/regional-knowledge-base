"""Versioned media-only WSS request/result on the EXISTING shared read channel.

Model parsing checks shape, not actor/lease authorization. SharedReadAdmission
and the native worker recheck those with existing ReadAuthority/current owner.
Image bytes are capped at 160 KiB before base64 (~219 KiB), preserving the
existing 256 KiB aggregate JSON/ASGI response budget; larger images DENY,
never truncate, shrink silently, upload or create another byte transport.
"""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Annotated, Literal, Self
from uuid import UUID
from pydantic import Field, model_validator

from .kaggle_protocol import (
    Contract, ReadIdentity, TrustedActorContext, PROTOCOL_VERSION, Identifier,
    Nonnegative, RESULT_MAX_BYTES,
)
from .kaggle_media_native_contracts import NativeMediaSnapshot, canonical_media

MEDIA_MAX_INLINE_BYTES=160*1024
MEDIA_MAX_NATIVE_SNAPSHOT_BYTES=96*1024
MediaDigest=Annotated[str,Field(pattern=r"^[0-9a-f]{64}$")]


def canonical_uuid(value):
    if type(value) is not str or str(UUID(value))!=value:
        raise ValueError("canonical source/illustration identity required")
    return value


class IllustrationReadRequest(ReadIdentity):
    type: Literal["ILLUSTRATION_READ"]="ILLUSTRATION_READ"
    protocol_version: Literal[2]=PROTOCOL_VERSION
    operation: Literal["illustration_read"]="illustration_read"
    operation_version: Literal[1]=1
    action: Literal["snapshot","observe","pixels"]
    source_id: Identifier
    illustration_id: Identifier
    expected_crop_sha256: MediaDigest | None=None
    database_generation: Identifier
    binding_run_id: Identifier
    binding_runtime_incarnation: Identifier
    provider_version: Identifier
    archive_scope: Identifier
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int,Field(gt=0,le=2000)]
    compute_claim: Identifier | None=None

    @model_validator(mode="after")
    def request_scope(self)->Self:
        canonical_uuid(self.source_id);canonical_uuid(self.illustration_id)
        if ((self.action=="snapshot") != (self.expected_crop_sha256 is None)):
            raise ValueError("exact crop SHA only for observe/pixels")
        return self


class IllustrationReadResult(ReadIdentity):
    type: Literal["ILLUSTRATION_READ_RESULT"]="ILLUSTRATION_READ_RESULT"
    protocol_version: Literal[2]=PROTOCOL_VERSION
    status: Literal["ok","unavailable","invalid_query","cancelled","denied"]
    reason: Literal["not_ready","deadline_exceeded","disconnected","caller_cancelled",
                    "invalid_input","access_denied","source_changed",
                    "archive_unavailable","size_bound","internal_error"] | None=None
    action: Literal["snapshot","observe","pixels"]
    source_id: Identifier
    illustration_id: Identifier
    crop_sha256: MediaDigest | None=None
    snapshot: dict | None=None
    pixels_b64: Annotated[str,Field(max_length=218456)] | None=None
    mime_type: Literal["image/png","image/jpeg","image/webp"] | None=None
    pixels_sha256: MediaDigest | None=None
    observed_data_seq: Nonnegative | None=None
    observed_projection_seq: Nonnegative | None=None
    observed_visibility_epoch: Nonnegative | None=None
    observed_database_generation: Identifier | None=None

    @model_validator(mode="after")
    def strict_media_result(self)->Self:
        canonical_uuid(self.source_id);canonical_uuid(self.illustration_id)
        if self.status != "ok":
            if (self.reason is None or any(getattr(self,f) is not None for f in (
                    "snapshot","pixels_b64","mime_type","pixels_sha256","crop_sha256"))):
                raise ValueError("unavailable media cannot carry private proof")
            return self
        if self.reason is not None or any(getattr(self,k) is None for k in (
                "observed_data_seq","observed_projection_seq",
                "observed_visibility_epoch","observed_database_generation",
                "crop_sha256")):
            raise ValueError("complete current native media result required")
        if self.action in ("snapshot","observe"):
            if (type(self.snapshot) is not dict or self.pixels_b64 is not None
                    or self.mime_type is not None or self.pixels_sha256 is not None
                    or len(canonical_media(self.snapshot))>MEDIA_MAX_NATIVE_SNAPSHOT_BYTES):
                raise ValueError("snapshot media result bound")
            obj=NativeMediaSnapshot(**self.snapshot)
            if (obj.source_id!=self.source_id or obj.illustration_id!=self.illustration_id
                    or obj.database_generation!=self.observed_database_generation
                    or obj.observed_data_seq!=self.observed_data_seq
                    or obj.observed_projection_seq!=self.observed_projection_seq
                    or obj.observed_visibility_epoch!=self.observed_visibility_epoch):
                raise ValueError("snapshot media identity differs")
            crop=obj.item.get("crop_binding") if type(obj.item) is dict else None
            if type(crop) is not dict or hashlib.sha256(canonical_media(crop)).hexdigest()!=self.crop_sha256:
                raise ValueError("exact image crop differs")
        else:
            if (self.snapshot is not None or self.mime_type is None
                    or self.pixels_sha256 is None or type(self.pixels_b64) is not str):
                raise ValueError("media bytes require exact digest and mime")
            try:
                raw=base64.b64decode(self.pixels_b64,validate=True)
            except (ValueError,base64.binascii.Error):
                raise ValueError("invalid native image base64") from None
            if (not 1<=len(raw)<=MEDIA_MAX_INLINE_BYTES
                    or base64.b64encode(raw).decode("ascii") != self.pixels_b64
                    or hashlib.sha256(raw).hexdigest()!=self.pixels_sha256):
                raise ValueError("image bytes hash or size differs")
        return self
