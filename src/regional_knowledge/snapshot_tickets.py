"""Controller-only presigning over the existing restricted recovery S3 client."""
from __future__ import annotations

import hashlib
import time

from .kaggle_pg_snapshot_native import BlobRef, SnapshotError
from .native_snapshot_transfer import SnapshotTicket, origin
from .pg_snapshot_recovery import S3SnapshotTransport
from .recovery_s3 import S3ConditionalRecoveryStore, _blocking


class SnapshotTicketIssuer:
    def __init__(self, store, *, current, allowed_origins, max_single_bytes,
                 ttl_seconds=600, clock=time.time):
        if (not isinstance(store, S3ConditionalRecoveryStore) or not callable(current)
                or type(allowed_origins) is not frozenset or not allowed_origins
                or any(origin(s) != s for s in allowed_origins)
                or type(max_single_bytes) is not int or not 1 <= max_single_bytes <= 8 * 1024**3
                or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 900):
            raise TypeError('existing scoped S3 and explicit transfer bounds required')
        self.store, self.current = store, current
        self.allowed_origins, self.max_single_bytes = allowed_origins, max_single_bytes
        self.ttl_seconds, self.clock = ttl_seconds, clock

    def _scope(self, binding, operation_key, size, sha256):
        if (not self.current(binding) or type(operation_key) is not str
                or not 1 <= len(operation_key.encode()) <= 256
                or type(size) is not int or not 1 <= size <= self.max_single_bytes
                or type(sha256) is not str or len(sha256) != 64 or set(sha256) - set('0123456789abcdef')):
            raise SnapshotError('snapshot_ticket_scope_unavailable')
        operation_ref = hashlib.sha256(operation_key.encode()).hexdigest()
        key = self.store.prefix + 'snapshots/blobs/' + operation_ref + '/' + sha256
        self.store._key(key)
        return operation_ref, key

    async def _sign(self, binding, method, operation_ref, key, size, sha256, params, headers):
        # Scoped immutable orphan bytes cannot promote a snapshot. All canonical
        # effects still hold the existing current writer fence and remote CAS.
        generation=getattr(binding,'writer_generation',None) or getattr(self.store.writer_guard,'writer_generation',None)
        if type(generation) is not str:raise SnapshotError('snapshot_ticket_writer_unavailable')
        async with self.store.writer_guard(generation):
            if not self.current(binding): raise SnapshotError('snapshot_ticket_scope_unavailable')
            url = await _blocking(lambda: self.store.client.generate_presigned_url(
                'put_object' if method == 'PUT' else 'get_object',
                Params={'Bucket': self.store.bucket, 'Key': key, **params},
                ExpiresIn=self.ttl_seconds, HttpMethod=method))
            if not self.current(binding) or origin(url) not in self.allowed_origins:
                raise SnapshotError('snapshot_ticket_origin_or_generation')
            return SnapshotTicket(method, operation_ref, key, size, sha256,
                self.clock() + self.ttl_seconds, url, tuple(headers.items()))

    async def issue_upload(self, binding, operation_key, *, size, sha256):
        operation_ref, key = self._scope(binding, operation_key, size, sha256)
        return await self._sign(binding, 'PUT', operation_ref, key, size, sha256,
            {'ContentLength': size, 'ContentType': 'application/octet-stream', 'IfNoneMatch': '*'},
            {'Content-Length': str(size), 'Content-Type': 'application/octet-stream', 'If-None-Match': '*'})

    async def issue_download(self, binding, operation_key, ref):
        operation_ref, _ = self._scope(binding, operation_key, ref.size, ref.sha256)
        self.store._key(ref.key)
        if not ref.key.startswith(self.store.prefix + 'snapshots/blobs/'):
            raise SnapshotError('snapshot_ticket_blob_scope')
        return await self._sign(binding, 'GET', operation_ref, ref.key, ref.size, ref.sha256,
            {'IfMatch': ref.native_version}, {'If-Match': ref.native_version})

    async def reconcile_upload(self, binding, operation_key, *, size, sha256):
        _, key = self._scope(binding, operation_key, size, sha256)
        def head():
            try:
                result = self.store.client.head_object(Bucket=self.store.bucket, Key=key)
                if result.get('ContentLength') != size: raise ValueError
                return BlobRef(key, size, sha256, result['ETag'])
            except Exception:
                raise SnapshotError('snapshot_upload_unresolved') from None
        ref = await _blocking(head)
        await S3SnapshotTransport(self.store).verify(ref)
        if not self.current(binding): raise SnapshotError('snapshot_ticket_scope_unavailable')
        return ref
