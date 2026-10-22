"""Execute scoped illustration reads on the existing native PG + Telegram owners.

One admitted WSS resource / existing reader pool; never a new IAM, downloader,
Telegram session, transport or provider send. No owner installed -> unavailable.
Exact native/local archive proof is never serialized as WSS authority; each
observe/pixels operation freshly verifies source+original/crop+current locator.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
from types import SimpleNamespace

from .kaggle_media_native_contracts import canonical_media
from .kaggle_media_wss_contracts import (
    IllustrationReadRequest, IllustrationReadResult,
    MEDIA_MAX_INLINE_BYTES,
)
from .kaggle_public_illustration_native_read import NativePublicIllustrationReader, READ_SQL_SHA256


def _failure(request, reason="not_ready"):
    return IllustrationReadResult(
        request_id=request.request_id,
        connection_generation=request.connection_generation,
        turn_generation=request.turn_generation,
        action=request.action,source_id=request.source_id,
        illustration_id=request.illustration_id,
        status="unavailable",reason=reason)


async def execute_illustration_read(request, *, acquire, archive,
                                    binding_current, lease_current):
    """Worker-only: trusted exact native context, not caller-provided permission."""
    if type(request) is not IllustrationReadRequest or not callable(lease_current):
        raise TypeError("typed native media read and current WSS resource required")
    if archive is None or not callable(binding_current):
        return _failure(request)
    binding=SimpleNamespace(
        run_id=request.binding_run_id,
        runtime_incarnation=request.binding_runtime_incarnation,
        connection_generation=request.connection_generation,
        database_generation=request.database_generation,
        min_data_seq=request.min_data_seq,
        provider_version=request.provider_version)
    def actual_current(b):
        return (b is binding and lease_current() is True
                and binding_current(b) is True
                and archive.current() is True)
    if (request.archive_scope != archive.archive_scope
            or getattr(archive, "topic_id",None) != 4
            or getattr(archive,"chat_id",None) is None
            or actual_current(binding) is not True):
        return _failure(request)
    reader=NativePublicIllustrationReader(
        acquire=acquire,binding_current=actual_current,
        archive_chat_id=archive.chat_id,archive_scope=archive.archive_scope)
    if reader.native_reader_sha256 != READ_SQL_SHA256:
        return _failure(request)
    try:
        native=await reader(request.source_id,request.illustration_id,
                            request.actor.person_id,binding)
        if not actual_current(binding):
            return _failure(request)
        crop=native.item["crop_binding"]
        crop_sha=hashlib.sha256(canonical_media(crop)).hexdigest()
        if request.action!="snapshot" and request.expected_crop_sha256!=crop_sha:
            return _failure(request,"source_changed")
        fields=dict(request_id=request.request_id,
            connection_generation=request.connection_generation,
            turn_generation=request.turn_generation,action=request.action,
            source_id=request.source_id,illustration_id=request.illustration_id,
            crop_sha256=crop_sha,
            observed_data_seq=native.observed_data_seq,
            observed_projection_seq=native.observed_projection_seq,
            observed_visibility_epoch=native.observed_visibility_epoch,
            observed_database_generation=native.database_generation,
            status="ok")
        if request.action=="snapshot":
            from dataclasses import asdict
            return IllustrationReadResult(**fields,snapshot=asdict(native))
        proof=await archive.read_opaque(native,request.actor.person_id)
        try:
            if not actual_current(binding) or not archive.issuance_current(proof):
                return _failure(request)
            if request.action=="observe":
                from dataclasses import asdict
                return IllustrationReadResult(**fields,snapshot=asdict(native))
            pixels=await archive.read_issued_bytes(proof)
            if not actual_current(binding) or not archive.issuance_current(proof):
                return _failure(request)
            if (pixels.proof is not proof or type(pixels.data) is not bytes
                    or not 1<=len(pixels.data)<=MEDIA_MAX_INLINE_BYTES
                    or hashlib.sha256(pixels.data).hexdigest()!=pixels.sha256):
                return _failure(request,"size_bound")
            # Recheck native source AFTER image bytes await so a concurrent
            # withdrawal/accepted revision change cannot produce pixels.
            current=await reader(request.source_id,request.illustration_id,
                                 request.actor.person_id,binding)
            if not actual_current(binding) or current != native:
                return _failure(request,"source_changed")
            return IllustrationReadResult(**fields,
                pixels_b64=base64.b64encode(pixels.data).decode("ascii"),
                mime_type=pixels.mime_type,pixels_sha256=pixels.sha256)
        finally:
            archive.release_issued(proof)
    except asyncio.CancelledError:
        raise
    except (ValueError,TypeError,LookupError,PermissionError,TimeoutError,KeyError,AttributeError):
        return _failure(request,"archive_unavailable")
    except Exception:
        # Native/private client or PG errors must not reach the public frame.
        return _failure(request,"internal_error")
