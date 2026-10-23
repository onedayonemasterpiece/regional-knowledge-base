"""One-time nonempty PG checkpoint into Kaggle, using the existing restore owner.

This seed preserves the accepted prefix. Publication is an intermediate receipt;
only the subsequent native Dataset restore can establish product readiness.
"""
from dataclasses import dataclass
from contextlib import asynccontextmanager
import asyncio
import base64
import http.client
import json
import logging
from urllib.parse import urlsplit

from .kaggle_dataset_publisher import KaggleDatasetPublisher, DatasetPublicationError, DatasetUpload
from .kaggle_dataset_tickets import DatasetSnapshotTicketIssuer
from .kaggle_pg_snapshot_native import BlobRef, SnapshotError, encode, sha, validate_versions

_LOG = logging.getLogger(__name__)
MAX_JOURNAL_CONTROL_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class DatasetSeedConfig:
    provider_ref: str
    operation_key: str
    versions: dict

    def __post_init__(self):
        validate_versions(self.versions)
        if type(self.operation_key) is not str or not 1 <= len(self.operation_key.encode()) <= 256:
            raise ValueError('stable Dataset seed operation required')

    @classmethod
    def from_wire(cls, value):
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise ValueError('exact Dataset seed configuration required')
        return cls(**value)


def _put_control(grant, data):
    limit=MAX_JOURNAL_CONTROL_BYTES if grant.name=='journal-control.json' else 1024*1024
    if len(data) > limit or (len(data), sha(data)) != (grant.size, grant.sha256):
        raise DatasetPublicationError('dataset_control_upload_bytes_changed')
    parsed = urlsplit(grant.url)
    connection = http.client.HTTPSConnection(parsed.hostname, timeout=30)
    try:
        path = parsed.path + ('?' + parsed.query if parsed.query else '')
        connection.request('PUT', path, body=data,
            headers={'Content-Length':str(len(data)), 'Content-Type':'application/octet-stream'})
        response = connection.getresponse()
        if response.status not in (200, 201, 204):
            raise DatasetPublicationError('dataset_control_upload_pending')
    except (OSError, http.client.HTTPException):
        raise DatasetPublicationError('dataset_control_upload_pending') from None
    finally:
        connection.close()


class SourceDatasetSeed:
    def __init__(self, startup, config, publisher):
        self.startup, self.config, self.publisher = startup, config, publisher
        self.queue = startup.config.queue
        self.tickets = DatasetSnapshotTicketIssuer(self.queue, publisher,
            current=startup.binding_current, writer_guard=startup.config.writer_owner)
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_dataset_seed(
                operation_key TEXT PRIMARY KEY, provider_ref TEXT NOT NULL,
                state TEXT NOT NULL, package_json TEXT, version INTEGER)''')
            row = db.execute('SELECT * FROM rkb_dataset_seed WHERE operation_key=?', (config.operation_key,)).fetchone()
            if row is None:
                db.execute("INSERT INTO rkb_dataset_seed VALUES(?,?,'pending',NULL,NULL)",
                    (config.operation_key, config.provider_ref))
            elif row['provider_ref'] != config.provider_ref:
                raise DatasetPublicationError('dataset_seed_operation_changed')

    def published(self):
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT state FROM rkb_dataset_seed WHERE operation_key=?', (self.config.operation_key,)).fetchone()
        return row is not None and row['state']=='published'

    def _prefix_ready(self,proof):
        return (proof is not None and proof.current() and not proof.pending
            and proof.accepted_sequence==proof.applied_sequence
            and proof.accepted_sequence==self.startup.config.spool._source_covered_sequence)

    def _control(self,physical,proof):return physical._control(proof)

    async def _prepare_prefix(self,proof):pass

    # The one-time first seed still owns the full intake barrier. Only the
    # work-cycle subclass permits concurrent application under a physical
    # WAL-consistent backup, with a short protected prefix selection below.
    concurrent_capture=False

    @asynccontextmanager
    async def capture_guard(self,binding):
        async with self.startup.config.spool._lock:
            yield

    @asynccontextmanager
    async def prefix_guard(self,binding):
        yield

    async def _capture(self,physical,bridge,binding,sequence):
        return await physical.native.capture(binding,versions=dict(self.config.versions),expected_sequence=sequence)

    async def _journal_control(self):
        """One-time bounded legacy recovery metadata, excluding physical blobs."""
        store=self.startup.config.spool.journal.store
        def keys():
            result=[];cursor=None
            for _ in range(10):
                options=dict(Bucket=store.bucket,Prefix=store.prefix,MaxKeys=1000)
                if cursor:options['ContinuationToken']=cursor
                page=store.client.list_objects_v2(**options)
                for item in page.get('Contents',[]):
                    key=item['Key']
                    if '/snapshots/' not in key:
                        if item['Size']>1024*1024:raise SnapshotError('dataset_seed_journal_object_budget')
                        result.append(key)
                if not page.get('IsTruncated'):return sorted(result)
                cursor=page['NextContinuationToken']
            raise SnapshotError('dataset_seed_journal_inventory_budget')
        records=[];used=0
        for key in await asyncio.to_thread(keys):
            obj=await store.read(key,max_bytes=1024*1024)
            if obj is None:raise SnapshotError('dataset_seed_journal_object_missing')
            used+=len(obj.data)
            if used>12*1024*1024:raise SnapshotError('dataset_seed_journal_control_budget')
            records.append(dict(key=key,token=obj.token,data_b64=base64.b64encode(obj.data).decode()))
        data=encode(dict(codec='rkb.dataset-journal.v1',objects=records))
        if len(data)>MAX_JOURNAL_CONTROL_BYTES:raise SnapshotError('dataset_seed_journal_control_budget')
        return data

    async def capture_once(self, binding):
        with self.queue.connect(write=False) as db:
            row = db.execute('SELECT * FROM rkb_dataset_seed WHERE operation_key=?', (self.config.operation_key,)).fetchone()
        if row['state'] == 'published':
            return row['version']
        if row['package_json'] is not None:
            package = json.loads(row['package_json'])
        else:
            restorer = self.startup.restorer
            if restorer is None or not restorer.complete or restorer.physical is None:
                raise SnapshotError('dataset_seed_requires_verified_native_restore')
            physical = restorer.physical
            bridge = restorer.dispatch
            # Restore used its original S3 tickets once; capture now uploads
            # directly to Kaggle. No provider credential reaches the notebook.
            bridge.snapshot_tickets = self.tickets
            async with self.capture_guard(binding):
                proof = self.startup.coverage
                await self._prepare_prefix(proof)
                # Read the old accepted/applied boundary and its compact source
                # heads under ONE short spool exclusion. Release it BEFORE the
                # physical pg_basebackup and Dataset publication so later
                # accepted commands can advance while this prefix is saved.
                async with self.prefix_guard(binding):
                    if proof is not self.startup.coverage or not self._prefix_ready(proof):
                        raise SnapshotError('dataset_seed_requires_drained_prefix')
                    if await self.startup.config.spool.journal.head()!=proof.head:
                        raise SnapshotError('dataset_seed_capture_prefix_changed')
                    control = self._control(physical,proof)
                descriptor = await self._capture(physical,bridge,binding,proof.applied_sequence)
                if (descriptor['database_generation'] != proof.database_generation
                        or descriptor['writer_generation'] != self.startup.config.writer_generation
                        or descriptor['applied_sequence'] != proof.applied_sequence
                        or descriptor['versions'] != self.config.versions
                        or not self.startup.binding_current(binding)
                        or (not self.concurrent_capture and
                            await self.startup.config.spool.journal.head()!=proof.head)):
                    raise SnapshotError('dataset_seed_capture_prefix_changed')
                archive = self.tickets.uploaded_file(BlobRef(**descriptor['blob']))
                control_ref = dict(key=self.startup.config.spool.journal.prefix+'/source-base-control/'+sha(control),
                    sha256=sha(control), length=len(control))
                manifest = encode(dict(descriptor, codec='rkb.pg-physical-base.v1',
                    stream=self.startup.config.spool.journal.stream,
                    journal_head=proof.head.reference.wire() if proof.head else None, source_control=control_ref))
                # Only bounded recovery metadata is uploaded from Dev.
                files = [archive]
                journal_control=await self._journal_control()
                for name, data in (('base-manifest.json', manifest),
                        ('source-control.json', control), ('journal-control.json',journal_control)):
                    grant, _expires = await self.tickets.reserve(binding, self.config.operation_key+':'+name,
                        name=name, size=len(data), sha256=sha(data))
                    await asyncio.to_thread(_put_control, grant, data)
                    files.append(grant)
                previous = await asyncio.to_thread(self.publisher.observe)
                package = dict(files=[dict(name=f.name, size=f.size, sha256=f.sha256, token=f.token, url=f.url) for f in files],
                    previous_version=previous, base_sequence=proof.applied_sequence,
                    database_generation=proof.database_generation, manifest_sha256=sha(manifest))
                with self.queue.connect() as db:
                    db.execute("UPDATE rkb_dataset_seed SET state='publishing',package_json=? WHERE operation_key=?",
                        (json.dumps(package, separators=(',', ':')), self.config.operation_key))
                # Intent is committed before the create/version effect.
                version = await asyncio.to_thread(self.publisher.finalize,
                    files=tuple(files), operation=self.config.operation_key, previous_version=previous)
                return self._published(version, package)
        expected = 1 if package['previous_version'] is None else package['previous_version']+1
        if not await asyncio.to_thread(self.publisher.reconcile,
                files=tuple(DatasetUpload(**f) for f in package['files']),
                operation=self.config.operation_key, expected_version=expected):
            raise DatasetPublicationError('dataset_seed_publication_pending')
        return self._published(expected, package)

    def _published(self, version, package):
        with self.queue.connect() as db:
            db.execute("UPDATE rkb_dataset_seed SET state='published',version=? WHERE operation_key=?",
                (version, self.config.operation_key))
        _LOG.info('dataset_seed_published provider_ref=%s version=%s base_sequence=%s manifest_sha256=%s native_restore_pending=true',
            self.config.provider_ref, version, package['base_sequence'], package['manifest_sha256'])
        return version
