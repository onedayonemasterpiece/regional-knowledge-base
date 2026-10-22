"""Kaggle Dataset upload grants on the existing recovery-phase channel."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time

from .kaggle_dataset_publisher import DatasetPublicationError, DatasetUpload
from .native_snapshot_transfer import SnapshotTicket


class DatasetSnapshotTicketIssuer:
    def __init__(self, queue, publisher, *, current, writer_guard, clock=time.time):
        self.queue, self.publisher = queue, publisher
        self.current, self.guard, self.clock = current, writer_guard, clock
        with queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_dataset_blob_upload(
                operation_ref TEXT PRIMARY KEY, binding TEXT NOT NULL,
                name TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL,
                state TEXT NOT NULL, grant_json TEXT, expires REAL NOT NULL)''')

    async def issue_upload(self, binding, operation_key, *, size, sha256):
        ref = hashlib.sha256(operation_key.encode()).hexdigest()
        # Like my-data-hub checkpoint_provider_file_name: Kaggle expands
        # archive-looking names. The native tar bytes stay opaque in Dataset.
        grant, expires = await self.reserve(binding, operation_key, name='pg-' + ref + '.blob', size=size, sha256=sha256)
        return SnapshotTicket('PUT', ref, 'dataset-blobs/' + ref + '/' + grant.name, size, sha256,
            expires, grant.url, (('Content-Length', str(size)), ('Content-Type', 'application/octet-stream')),
            upload_kind='kaggle-dataset')

    async def reserve(self, binding, operation_key, *, name, size, sha256):
        if (not self.current(binding) or type(operation_key) is not str
                or not 1 <= len(operation_key.encode()) <= 256):
            raise DatasetPublicationError('dataset_capture_writer_changed')
        ref = hashlib.sha256(operation_key.encode()).hexdigest()
        owner = json.dumps([binding.run_id, binding.runtime_incarnation], separators=(',', ':'))
        DatasetUpload(name, size, sha256, 'validation', 'https://storage.googleapis.com/validation')
        generation = getattr(binding, 'writer_generation', None) or getattr(self.guard, 'writer_generation', None)
        if type(generation) is not str:
            raise DatasetPublicationError('dataset_capture_writer_unavailable')
        async with self.guard(generation):
            if not self.current(binding):
                raise DatasetPublicationError('dataset_capture_writer_changed')
            with self.queue.connect() as db:
                row = db.execute('SELECT * FROM rkb_dataset_blob_upload WHERE operation_ref=?', (ref,)).fetchone()
                if row is not None:
                    if (row['binding'], row['name'], row['size'], row['sha256']) != (owner, name, size, sha256):
                        raise DatasetPublicationError('dataset_capture_operation_changed')
                    if row['grant_json'] is None or row['expires'] <= self.clock():
                        raise DatasetPublicationError('dataset_capture_grant_unresolved')
                    grant = DatasetUpload(**json.loads(row['grant_json']))
                    expires = row['expires']
                else:
                    expires = self.clock() + 600
                    db.execute('INSERT INTO rkb_dataset_blob_upload VALUES(?,?,?,?,?,?,?,?)',
                        (ref, owner, name, size, sha256, 'reserving', None, expires))
            if row is None:
                grant = await asyncio.to_thread(self.publisher.reserve,
                    name=name, size=size, sha256=sha256, now=self.clock())
                # This queue is private control state, not a report or artifact.
                with self.queue.connect() as db:
                    db.execute("UPDATE rkb_dataset_blob_upload SET state='reserved',grant_json=? WHERE operation_ref=?",
                        (json.dumps(dict(name=grant.name, size=grant.size, sha256=grant.sha256,
                            token=grant.token, url=grant.url), separators=(',', ':')), ref))
            if not self.current(binding):
                raise DatasetPublicationError('dataset_capture_writer_changed')
            return grant, expires

    def uploaded_file(self, blob):
        """Metadata lookup after the native capture result; does not claim Saved."""
        parts = blob.key.split('/')
        if len(parts) != 3 or parts[0] != 'dataset-blobs':
            raise DatasetPublicationError('dataset_capture_blob_unknown')
        with self.queue.connect(write=False) as db:
            row = db.execute('SELECT * FROM rkb_dataset_blob_upload WHERE operation_ref=?', (parts[1],)).fetchone()
        if row is None or (row['name'], row['size'], row['sha256']) != (parts[2], blob.size, blob.sha256):
            raise DatasetPublicationError('dataset_capture_blob_changed')
        return DatasetUpload(**json.loads(row['grant_json']))

    async def issue_download(self,binding,operation_key,ref):
        from .kaggle_dataset_base import DatasetCheckpoint
        if ref.key.startswith('dataset-handlers/'):
            return await self._handler_download(binding,operation_key,ref)
        checkpoint=getattr(self,'download_checkpoint',None)
        if (type(checkpoint) is not DatasetCheckpoint or not self.current(binding)
                or (ref.key.rsplit('/',1)[-1],ref.size,ref.sha256)!=(
                    checkpoint.archive.name,checkpoint.archive.size,checkpoint.archive.sha256)):
            raise DatasetPublicationError('dataset_download_checkpoint_binding')
        generation=getattr(binding,'writer_generation',None) or self.guard.writer_generation
        async with self.guard(generation):
            url,etag=await asyncio.to_thread(self.publisher.snapshot_download_link,
                checkpoint.archive,version=checkpoint.dataset_version)
            if not self.current(binding):raise DatasetPublicationError('dataset_capture_writer_changed')
            return SnapshotTicket('GET',hashlib.sha256(operation_key.encode()).hexdigest(),ref.key,
                ref.size,ref.sha256,self.clock()+600,url,(('If-Match',etag),))

    async def _handler_download(self,binding,operation_key,ref):
        from .kaggle_dataset_base import DatasetFile
        from .kaggle_dataset_publisher import HANDLER_DATASET_SLUG,KaggleDatasetPublisher
        from .kaggle_warm_read import MAX_MODULE_BYTES
        parts=ref.key.split('/')
        if (not self.current(binding) or len(parts)!=4 or parts[1]!=HANDLER_DATASET_SLUG
                or re.fullmatch(r'[1-9][0-9]{0,8}',parts[2]) is None
                or re.fullmatch('handler-'+re.escape(ref.sha256)+r'\.blob',parts[3]) is None
                or not 1<=ref.size<=MAX_MODULE_BYTES):
            raise DatasetPublicationError('dataset_handler_artifact_scope')
        publisher=KaggleDatasetPublisher(self.publisher.api,
            provider_ref=self.publisher.owner+'/'+HANDLER_DATASET_SLUG,owner=self.publisher.owner)
        generation=getattr(binding,'writer_generation',None) or self.guard.writer_generation
        async with self.guard(generation):
            url,etag=await asyncio.to_thread(publisher.handler_download_link,
                DatasetFile(parts[3],ref.size,ref.sha256),version=int(parts[2]))
            if not self.current(binding) or etag!=ref.native_version:
                raise DatasetPublicationError('dataset_handler_artifact_changed')
            return SnapshotTicket('GET',hashlib.sha256(operation_key.encode()).hexdigest(),
                ref.key,ref.size,ref.sha256,self.clock()+600,url,(('If-Match',etag),))
