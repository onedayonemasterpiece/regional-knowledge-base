from contextlib import contextmanager, asynccontextmanager
from dataclasses import replace
import sqlite3
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_publisher import DatasetPublicationError, DatasetUpload
from regional_knowledge.kaggle_dataset_tickets import DatasetSnapshotTicketIssuer
from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
from regional_knowledge.native_snapshot_transfer import SnapshotTicket, SnapshotTransferError


class Queue:
    def __init__(self, path):self.path = path

    @contextmanager
    def connect(self, write=True):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        try:
            with db:yield db
        finally:db.close()


@pytest.mark.asyncio
async def test_existing_channel_ticket_replays_same_grant_and_scope(tmp_path):
    queue = Queue(tmp_path/'queue.sqlite')
    binding = NS(run_id='run', runtime_incarnation='inc', writer_generation='writer')
    calls = []
    class Publisher:
        def reserve(self, **value):
            calls.append(value)
            return DatasetUpload(value['name'], value['size'], value['sha256'], 'opaque',
                'https://storage.googleapis.com/blob?private=grant')
    @asynccontextmanager
    async def guard(generation):
        assert generation == 'writer'
        yield
    issuer = DatasetSnapshotTicketIssuer(queue, Publisher(), current=lambda b:b is binding,
        writer_guard=guard, clock=lambda:100)
    first = await issuer.issue_upload(binding, 'capture', size=123, sha256='a'*64)
    assert first.upload_kind == 'kaggle-dataset'
    assert 'If-None-Match' not in dict(first.headers)
    assert SnapshotTicket.from_wire(first.wire()) == first
    assert first == await issuer.issue_upload(binding, 'capture', size=123, sha256='a'*64)
    assert len(calls) == 1
    blob = BlobRef(first.key, first.size, first.sha256, 'native-etag')
    assert issuer.uploaded_file(blob).token == 'opaque'
    with pytest.raises(DatasetPublicationError, match='operation_changed'):
        await issuer.issue_upload(binding, 'capture', size=124, sha256='a'*64)
    with pytest.raises(SnapshotTransferError, match='dataset_origin'):
        replace(first, url='https://foreign.example/blob')
    with pytest.raises(SnapshotTransferError, match='headers_invalid'):
        replace(first, upload_kind='s3')


@pytest.mark.asyncio
async def test_lost_reservation_response_does_not_allocate_another_blob(tmp_path):
    queue = Queue(tmp_path/'queue.sqlite')
    binding = NS(run_id='run', runtime_incarnation='inc', writer_generation='writer')
    calls = []
    class Publisher:
        def reserve(self, **value):
            calls.append(value)
            raise DatasetPublicationError('dataset_blob_reservation_unknown')
    @asynccontextmanager
    async def guard(generation):yield
    issuer = DatasetSnapshotTicketIssuer(queue, Publisher(), current=lambda b:True, writer_guard=guard)
    with pytest.raises(DatasetPublicationError, match='reservation_unknown'):
        await issuer.issue_upload(binding, 'capture', size=123, sha256='a'*64)
    with pytest.raises(DatasetPublicationError, match='grant_unresolved'):
        await issuer.issue_upload(binding, 'capture', size=123, sha256='a'*64)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', [None, 'archive', 'base_sequence', 'database_generation', 'provider_receipt'])
async def test_download_survives_prune_before_saved_promotion(tmp_path, changed):
    import json
    from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint, DatasetFile
    from regional_knowledge.kaggle_pg_snapshot_native import encode, sha
    queue = Queue(tmp_path/'queue.sqlite')
    binding = NS(run_id='run', runtime_incarnation='inc', writer_generation='writer')
    files = [DatasetFile(n, 123, str(i)*64) for i,n in enumerate(
        ('base-manifest.json','pg-archive.blob','source-control.json','journal-control.json'), 1)]
    saved = DatasetCheckpoint('owner','rkb-pg',510,'database',87,*files)
    operation = 'dataset-prune:'+sha(encode(['owner/rkb-pg',510]))
    package = dict(base_sequence=87,database_generation='database',files=[dict(f.wire(),
        token='private',url='https://storage.googleapis.com/blob') for f in files])
    if changed == 'archive':package['files'][1]['sha256']='a'*64
    if changed == 'base_sequence':package['base_sequence']=88
    if changed == 'database_generation':package['database_generation']='other'
    calls=[]
    class Publisher:
        ref='owner/rkb-pg'
        def reconcile(self, **kw):
            assert kw['operation']==operation and kw['expected_version']==511
            return changed != 'provider_receipt'
        def snapshot_download_link(self, file, *, version):
            calls.append(version)
            if version==510:raise DatasetPublicationError('dataset_archive_link_unavailable')
            return 'https://storage.googleapis.com/blob','etag'
    @asynccontextmanager
    async def guard(generation):yield
    issuer=DatasetSnapshotTicketIssuer(queue,Publisher(),current=lambda b:b is binding,writer_guard=guard)
    issuer.download_checkpoint=saved
    with queue.connect() as db:
        db.execute('CREATE TABLE rkb_dataset_seed(operation_key TEXT,provider_ref TEXT,state TEXT,package_json TEXT,version INTEGER)')
        db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',(operation,'owner/rkb-pg','published',json.dumps(package),511))
    ref=BlobRef('dataset-blobs/operation/pg-archive.blob',123,files[1].sha256,'etag')
    if changed:
        with pytest.raises(DatasetPublicationError):await issuer.issue_download(binding,'prepare',ref)
        assert 511 not in calls
    else:
        result=await issuer.issue_download(binding,'prepare',ref)
        assert result.sha256==files[1].sha256 and calls==[511]
        assert issuer.download_checkpoint==saved  # Physical verification still owns promotion.
