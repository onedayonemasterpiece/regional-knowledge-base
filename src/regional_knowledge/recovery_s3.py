"""Native S3 conditional transport for the existing recovery journal.

This adapter reuses an existing S3 client and controller authority. SDK shape
support is checked locally; real bucket conditional semantics still need proof.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, TypeVar

from .recovery_journal import JournalError, JournalLimit, StoredObject, StoreUnavailable


class ConditionalApiUnavailable(JournalError):
    """The installed client cannot express both required native conditions."""


WriterGuard = Callable[[str], AbstractAsyncContextManager[None]]
_T = TypeVar("_T")


def _provider_error(exc: Exception) -> tuple[str, int | None]:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return "", None
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    code = error.get("Code", "") if isinstance(error, Mapping) else ""
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, Mapping) else None
    return code if isinstance(code, str) else "", status if type(status) is int else None


def _missing(exc: Exception) -> bool:
    code, status = _provider_error(exc)
    return ((code == "NoSuchKey" and status in (None, 404))
            or (status == 404 and code in ("", "404", "NotFound", "NoSuchKey")))


def _condition_failed(exc: Exception) -> bool:
    code, status = _provider_error(exc)
    return ((status == 412 and code in ("", "412", "PreconditionFailed"))
            or (code == "PreconditionFailed" and status is None))


async def _blocking(call: Callable[[], _T]) -> _T:
    """A cancelled waiter must not release a fence before its thread finishes."""
    task = asyncio.create_task(asyncio.to_thread(call))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break  # Retrieve the sanitized exception below.
    if cancelled:
        if not task.cancelled():
            task.exception()  # Do not leave a late worker exception unobserved.
        raise asyncio.CancelledError
    return task.result()


class S3ConditionalRecoveryStore:
    """Restricted adapter around S3ObjectStore.client and .bucket.

    writer_guard must be the existing authoritative async generation lock. It
    validates generation on entry and excludes revocation/generation transitions
    until exit. An async precheck without held authority is not sufficient.
    """

    def __init__(self, *, client: Any, bucket: str, prefix: str,
                 writer_guard: WriterGuard) -> None:
        if not callable(writer_guard):
            raise ValueError("an authoritative async writer guard is required")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("existing storage bucket is required")
        if (not isinstance(prefix, str) or not prefix.endswith("/")
                or prefix.startswith("/") or any(ord(c) < 32 for c in prefix)
                or any(part in ("", ".", "..") for part in prefix[:-1].split("/"))):
            raise ValueError("an explicit restricted object prefix ending in '/' is required")
        try:
            members = client.meta.service_model.operation_model("PutObject").input_shape.members
            supported = "IfMatch" in members and "IfNoneMatch" in members
        except Exception:
            supported = False
        if not supported:
            raise ConditionalApiUnavailable("client lacks required native PutObject conditions")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.writer_guard = writer_guard

    def _key(self, key: str) -> None:
        if (not isinstance(key, str) or not key.startswith(self.prefix)
                or len(key) == len(self.prefix) or any(ord(c) < 32 for c in key)
                or any(part in (".", "..", "") for part in key.split("/"))):
            raise ValueError("object key is outside the restricted recovery namespace")

    async def read(self, key: str, *, max_bytes: int) -> StoredObject | None:
        self._key(key)
        if type(max_bytes) is not int or max_bytes < 0:
            raise ValueError("a finite nonnegative read bound is required")

        def load() -> StoredObject | None:
            try:
                result = self.client.get_object(Bucket=self.bucket, Key=key)
            except Exception as exc:
                if _missing(exc):
                    return None
                raise StoreUnavailable("recovery object read unavailable") from None
            body = result.get("Body") if isinstance(result, Mapping) else None
            try:
                length = result.get("ContentLength")
                token = result.get("ETag")
                if type(length) is not int or length < 0:
                    raise StoreUnavailable("recovery object length is unproved")
                if length > max_bytes:
                    raise JournalLimit("recovery object exceeds read bound")
                if not isinstance(token, str) or not token or any(ord(c) < 32 for c in token):
                    raise StoreUnavailable("recovery object version is unproved")
                chunks = []
                size = 0
                while size <= max_bytes:
                    chunk = body.read(max_bytes + 1 - size)
                    if not isinstance(chunk, bytes):
                        raise StoreUnavailable("recovery object stream is invalid")
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        raise JournalLimit("recovery object exceeds read bound")
                    chunks.append(chunk)
                if size != length:
                    raise StoreUnavailable("recovery object length mismatch")
                return StoredObject(b"".join(chunks), token)
            except (JournalLimit, StoreUnavailable):
                raise
            except Exception:
                raise StoreUnavailable("recovery object stream unavailable") from None
            finally:
                if body is not None:
                    try:
                        body.close()
                    except Exception:
                        pass

        return await _blocking(load)

    def _put(self, key: str, data: bytes, condition: dict[str, str]) -> bool:
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data,
                                   ContentType="application/json", **condition)
            return True
        except Exception as exc:
            if _condition_failed(exc):
                return False
            raise StoreUnavailable("conditional recovery write unavailable") from None

    async def put_if_absent(self, key: str, data: bytes) -> bool:
        self._key(key)
        if not isinstance(data, bytes):
            raise ValueError("immutable payload must be exact bytes")
        return await _blocking(lambda: self._put(key, data, {"IfNoneMatch": "*"}))

    async def compare_and_swap(self, key: str, data: bytes, *, expected_token: str | None,
                               writer_generation: str) -> bool:
        self._key(key)
        if not isinstance(data, bytes) or not isinstance(writer_generation, str) or not writer_generation:
            raise ValueError("exact bytes and writer generation are required")
        if expected_token is not None and (
                not isinstance(expected_token, str) or not expected_token
                or any(ord(c) < 32 for c in expected_token)):
            raise ValueError("expected version must be an opaque native ETag")
        condition = {"IfNoneMatch": "*"} if expected_token is None else {"IfMatch": expected_token}
        async with self.writer_guard(writer_generation):
            return await _blocking(lambda: self._put(key, data, condition))
