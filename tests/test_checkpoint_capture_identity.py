from contextlib import asynccontextmanager
from types import SimpleNamespace as NS

import pytest

from test_kaggle_dataset_tickets import Queue
from regional_knowledge.kaggle_dataset_checkpoint import _WorkCycleCapture
from regional_knowledge.kaggle_dataset_publisher import DatasetUpload
from regional_knowledge.kaggle_dataset_tickets import DatasetSnapshotTicketIssuer
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch


@pytest.mark.asyncio
async def test_unpublished_capture_reuses_one_runtime_but_not_its_successor(tmp_path):
    """Real capture adapter/ticket ledger, with only native/provider I/O fake."""
    queue = Queue(tmp_path / 'queue.sqlite')
    bindings = [NS(run_id='old-run', runtime_incarnation='old-inc', writer_generation='writer'),
                NS(run_id='new-run', runtime_incarnation='new-inc', writer_generation='writer')]
    active = [bindings[0]]
    reserved = []

    class Publisher:
        def reserve(self, **values):
            reserved.append(values)
            return DatasetUpload(values['name'], values['size'], values['sha256'],
                                 'opaque-' + values['sha256'], 'https://storage.googleapis.com/fixture')

    @asynccontextmanager
    async def guard(generation):
        assert generation == 'writer'
        yield

    tickets = DatasetSnapshotTicketIssuer(queue, Publisher(),
        current=lambda binding: binding is active[0], writer_guard=guard, clock=lambda: 100.)
    bridge = RecoveryPhaseDispatch.__new__(RecoveryPhaseDispatch)
    bridge.identity = ('old-run', 'old-nonce', 'old-inc')
    digest = ['a' * 64]

    async def request(binding, phase, arguments, *, operation_key):
        assert phase == 'capture' and arguments['expected_sequence'] == 7
        # This is the existing deterministic capture_upload child request.
        return await tickets.issue_upload(binding, operation_key + ':upload',
                                          size=20, sha256=digest[0])

    bridge.request = request
    batch = _WorkCycleCapture.__new__(_WorkCycleCapture)
    batch.config = NS(operation_key='dataset-checkpoint:same-prefix', versions={'schema_version': 16})
    first = await batch._capture(None, bridge, bindings[0], 7)
    assert first == await batch._capture(None, bridge, bindings[0], 7)
    assert len(reserved) == 1

    active[0] = bindings[1]
    bridge.identity = ('new-run', 'new-nonce', 'new-inc')
    digest[0] = 'b' * 64  # New physical archive, same accepted logical prefix.
    second = await batch._capture(None, bridge, bindings[1], 7)
    assert second == await batch._capture(None, bridge, bindings[1], 7)
    assert first.key != second.key and len(reserved) == 2
    assert batch.config.operation_key == 'dataset-checkpoint:same-prefix'
    with queue.connect(write=False) as db:
        rows = db.execute('SELECT binding,sha256 FROM rkb_dataset_blob_upload ORDER BY rowid').fetchall()
    assert [(row['binding'], row['sha256']) for row in rows] == [
        ('["old-run","old-inc"]', 'a' * 64), ('["new-run","new-inc"]', 'b' * 64)]
