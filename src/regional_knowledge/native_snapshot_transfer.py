"""Scoped direct snapshot HTTP; no storage credentials or controller imports."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import time
from urllib.parse import urlsplit


class SnapshotTransferError(RuntimeError):
    pass


async def _blocking(call, *args):
    task = asyncio.create_task(asyncio.to_thread(call, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        if not task.cancelled(): task.exception()
        raise asyncio.CancelledError
    return task.result()


def origin(url):
    try:
        p = urlsplit(url)
        if (type(url) is not str or len(url) > 8192 or p.scheme != 'https'
                or not p.hostname or p.username or p.password or p.fragment
                or p.port not in (None, 443) or '\\' in url
                or any(ord(c) < 33 or ord(c) > 126 for c in url)):
            raise ValueError
        return 'https://' + p.hostname.lower()
    except (ValueError, TypeError):
        raise SnapshotTransferError('snapshot_transfer_https_required') from None


@dataclass(frozen=True)
class SnapshotTicket:
    method: str
    operation_ref: str
    key: str
    size: int
    sha256: str
    expires_at: float
    url: str = field(repr=False)
    headers: tuple[tuple[str, str], ...] = field(repr=False)
    upload_kind: str = 's3'

    def __post_init__(self):
        origin(self.url)
        if (self.method not in ('PUT', 'GET') or type(self.size) is not int
                or not 1 <= self.size <= 8 * 1024**3
                or type(self.expires_at) not in (int, float) or not math.isfinite(self.expires_at)
                or any(type(x) is not str or len(x) != 64 or set(x) - set('0123456789abcdef')
                       for x in (self.operation_ref, self.sha256))
                or type(self.key) is not str or not 1 <= len(self.key) <= 1024
                or any(part in ('', '.', '..') for part in self.key.split('/'))
                or any(ord(c) < 32 for c in self.key)
                or type(self.headers) is not tuple or not 1 <= len(self.headers) <= 4):
            raise SnapshotTransferError('snapshot_transfer_ticket_invalid')
        names = set()
        for pair in self.headers:
            if (type(pair) is not tuple or len(pair) != 2
                    or any(type(s) is not str or not 1 <= len(s) <= 1024
                           or any(ord(c) < 32 or ord(c) > 126 for c in s) for s in pair)
                    or pair[0].lower() in names):
                raise SnapshotTransferError('snapshot_transfer_ticket_invalid')
            names.add(pair[0].lower())
        if self.upload_kind not in ('s3', 'kaggle-dataset') or (self.method != 'PUT' and self.upload_kind != 's3'):
            raise SnapshotTransferError('snapshot_transfer_upload_kind')
        expected = ({'content-length', 'content-type'} if self.upload_kind == 'kaggle-dataset' else
                    {'content-length', 'content-type', 'if-none-match'} if self.method == 'PUT' else {'if-match'})
        if names != expected:
            raise SnapshotTransferError('snapshot_transfer_headers_invalid')
        h = {k.lower(): v for k, v in self.headers}
        if self.method == 'PUT' and (h['content-length'] != str(self.size)
                or h['content-type'] != 'application/octet-stream'
                or self.upload_kind == 's3' and h['if-none-match'] != '*'):
            raise SnapshotTransferError('snapshot_transfer_headers_invalid')
        if self.upload_kind == 'kaggle-dataset' and origin(self.url) not in {
                'https://storage.googleapis.com','https://www.googleapis.com'}:
            raise SnapshotTransferError('snapshot_transfer_dataset_origin')

    def wire(self):
        value = dict(method=self.method, operation_ref=self.operation_ref, key=self.key,
            size=self.size, sha256=self.sha256, expires_at=self.expires_at,
            url=self.url, headers=[list(pair) for pair in self.headers])
        if self.upload_kind != 's3':value['upload_kind'] = self.upload_kind
        return value

    @classmethod
    def from_wire(cls, value):
        fields = {'method', 'operation_ref', 'key', 'size', 'sha256', 'expires_at', 'url', 'headers'}
        if (type(value) is not dict or set(value) not in (fields, fields | {'upload_kind'}) or type(value['headers']) is not list
                or any(type(pair) is not list for pair in value['headers'])):
            raise SnapshotTransferError('snapshot_transfer_ticket_invalid')
        return cls(**{**value, 'headers': tuple(tuple(pair) for pair in value['headers'])})


class NativeSnapshotTransfer:
    def __init__(self, *, allowed_origins, max_bytes, timeout_seconds=600, clock=time.time):
        if (type(allowed_origins) is not frozenset or not 1 <= len(allowed_origins) <= 4
                or any(origin(s) != s for s in allowed_origins)
                or type(max_bytes) is not int or not 1 <= max_bytes <= 8 * 1024**3
                or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 900):
            raise ValueError('explicit bounded snapshot transfer deployment required')
        self.allowed_origins, self.max_bytes = allowed_origins, max_bytes
        self.timeout_seconds, self.clock = timeout_seconds, clock

    def _check(self, ticket, method, operation_key):
        if (type(ticket) is not SnapshotTicket or ticket.method != method
                or ticket.operation_ref != hashlib.sha256(operation_key.encode()).hexdigest()
                or ticket.size > self.max_bytes or ticket.expires_at <= self.clock()
                or origin(ticket.url) not in self.allowed_origins):
            raise SnapshotTransferError('snapshot_transfer_scope_or_expiry')
        return min(self.timeout_seconds, ticket.expires_at - self.clock())

    @staticmethod
    def _path(path):
        path = Path(path)
        if (not path.is_absolute() or not path.is_relative_to('/kaggle/temp')
                or '..' in path.parts or path.parent.resolve(strict=True) != path.parent):
            raise SnapshotTransferError('snapshot_transfer_private_path_required')
        return path

    async def upload(self, ticket, path, *, operation_key):
        import aiohttp
        from yarl import URL
        seconds = self._check(ticket, 'PUT', operation_key)
        path = self._path(path)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size != ticket.size:
                raise SnapshotTransferError('snapshot_transfer_local_bytes')
            def verify():
                h = hashlib.sha256()
                while chunk := os.read(fd, 1024 * 1024): h.update(chunk)
                if h.hexdigest() != ticket.sha256:
                    raise SnapshotTransferError('snapshot_transfer_local_bytes')
                os.lseek(fd, 0, os.SEEK_SET)
            await _blocking(verify)
            seconds = self._check(ticket, 'PUT', operation_key)
            async def chunks():
                while chunk := await _blocking(os.read, fd, 1024 * 1024):
                    yield chunk
            async with aiohttp.ClientSession(trust_env=False, cookie_jar=aiohttp.DummyCookieJar(),
                    auto_decompress=False, timeout=aiohttp.ClientTimeout(total=seconds)) as session:
                async with session.put(URL(ticket.url, encoded=True), data=chunks(),
                        headers=dict(ticket.headers), allow_redirects=False) as response:
                    if response.status not in (200, 201, 204):
                        raise SnapshotTransferError('snapshot_transfer_upload_unknown')
                    version = response.headers.get('ETag')
                    if not version and ticket.upload_kind=='kaggle-dataset':
                        # Kaggle's real signed JSON upload endpoint is
                        # www.googleapis.com. Its object generation may be in
                        # the bounded JSON receipt rather than an ETag header.
                        receipt=bytearray()
                        while len(receipt)<=16384:
                            chunk=await response.content.read(min(4096,16385-len(receipt)))
                            if not chunk:break
                            receipt.extend(chunk)
                        if len(receipt)<=16384:
                            try:
                                value=json.loads(receipt);generation=value.get('generation')
                                if (type(generation) is str and 1<=len(generation)<=128 and generation.isdecimal()
                                        and str(value.get('size',ticket.size))==str(ticket.size)):
                                    version='gcs-generation:'+generation
                            except (ValueError,TypeError,AttributeError):pass
                    if not version or len(version) > 512:
                        raise SnapshotTransferError('snapshot_transfer_upload_unknown')
                    return version
        except (OSError, asyncio.TimeoutError, aiohttp.ClientError):
            raise SnapshotTransferError('snapshot_transfer_upload_unknown') from None
        finally:
            os.close(fd)

    async def download(self, ticket, path, *, operation_key):
        import aiohttp
        from yarl import URL
        seconds = self._check(ticket, 'GET', operation_key)
        path = self._path(path)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        success = False
        try:
            used = 0; digest = hashlib.sha256()
            async with aiohttp.ClientSession(trust_env=False, cookie_jar=aiohttp.DummyCookieJar(),
                    auto_decompress=False, timeout=aiohttp.ClientTimeout(total=seconds)) as session:
                async with session.get(URL(ticket.url, encoded=True), headers=dict(ticket.headers),
                        allow_redirects=False) as response:
                    if (response.status != 200 or response.content_length != ticket.size
                            or response.headers.get('ETag') != {k.lower(): v for k, v in ticket.headers}['if-match']
                            or response.headers.get('Content-Encoding', 'identity').lower() != 'identity'):
                        raise SnapshotTransferError('snapshot_transfer_download_identity')
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        used += len(chunk)
                        if used > ticket.size:
                            raise SnapshotTransferError('snapshot_transfer_download_bytes')
                        digest.update(chunk)
                        view = memoryview(chunk)
                        while view:
                            view = view[os.write(fd, view):]
            if used != ticket.size or digest.hexdigest() != ticket.sha256:
                raise SnapshotTransferError('snapshot_transfer_download_bytes')
            os.fsync(fd); success = True
            return path
        except (OSError, asyncio.TimeoutError, aiohttp.ClientError):
            raise SnapshotTransferError('snapshot_transfer_download_unavailable') from None
        finally:
            os.close(fd)
            if not success: path.unlink(missing_ok=True)
