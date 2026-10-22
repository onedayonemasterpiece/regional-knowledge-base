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
