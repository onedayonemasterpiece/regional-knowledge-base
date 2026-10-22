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
async def test_reviewed_handler_blob_uses_own_dataset_without_checkpoint_change(tmp_path,monkeypatch):
    from regional_knowledge.kaggle_dataset_publisher import KaggleDatasetPublisher
    queue=Queue(tmp_path/'queue.sqlite')
    binding=NS(run_id='run',runtime_incarnation='inc',writer_generation='writer')
    calls=[]
    @asynccontextmanager
    async def guard(generation):
        assert generation=='writer'
        yield
    def link(self,file,*,version):
        calls.append((self.owner,self.slug,file.name,version))
        return 'https://storage.googleapis.com/handler?private=grant','handler-etag'
    monkeypatch.setattr(KaggleDatasetPublisher,'handler_download_link',link,raising=False)
    issuer=DatasetSnapshotTicketIssuer(queue,NS(owner='fixture-owner',api=object()),
        current=lambda b:b is binding,writer_guard=guard,clock=lambda:100)
    original_checkpoint=object();issuer.download_checkpoint=original_checkpoint
    name='handler-'+('a'*64)+'.blob'
    blob=BlobRef('dataset-handlers/rkb-read-handler-artifacts/7/'+name,123,'a'*64,'handler-etag')
    ticket=await issuer.issue_download(binding,'read-handler-reviewed',blob)
    assert (ticket.key,ticket.size,ticket.sha256)==(blob.key,blob.size,blob.sha256)
    assert dict(ticket.headers)=={'If-Match':'handler-etag'}
    assert calls==[('fixture-owner','rkb-read-handler-artifacts',name,7)]
    assert issuer.download_checkpoint is original_checkpoint
    with pytest.raises(DatasetPublicationError):
        await issuer.issue_download(NS(run_id='other'),'read-handler-reviewed',blob)
    with pytest.raises(DatasetPublicationError):
        await issuer.issue_download(binding,'changed-etag',replace(blob,native_version='other'))
    for key in ('dataset-handlers/rkb-pg-checkpoints/7/'+name,
                'dataset-handlers/rkb-read-handler-artifacts/7/sql-'+('a'*64)+'.blob'):
        with pytest.raises(DatasetPublicationError):
            await issuer.issue_download(binding,'wrong-scope',replace(blob,key=key))


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
