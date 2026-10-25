"""Unexpected native handler faults leave the existing warmed worker alive.

Synthetic authenticated WSS peer only: no provider, PostgreSQL, notebook,
model download, new result protocol or synthetic canonical receipt.
"""
import asyncio
import json

import pytest

from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError
from regional_knowledge.kaggle_snapshot_channel import SnapshotTicketChannel
from regional_knowledge.recovery_journal import StaleWriter
from test_bge_shared_worker import Peer, query, result
from test_bge_wss_worker import CONFIG, module
from test_kaggle_bootstrap import candidate


def dispatch(worker, *, phase, job_id='native-job', generation=1):
    return dict(type='DISPATCH',id=job_id,claim='original-claim',
        kind='recovery_phase',run_id=CONFIG['run_id'],
        launch_nonce=CONFIG['launch_nonce'],
        runtime_incarnation=worker.incarnation,phase=phase,
        operation_key='original-operation',request_sha256='a'*64,
        arguments={'private_query':'secret-query-from-authenticated-model'},
        database_generation='original-db',writer_generation='original-writer',
        compute_claim='original-compute',dispatch_generation=generation)






@pytest.mark.asyncio
@pytest.mark.parametrize('error_type',[PermissionError,StaleWriter])
async def test_native_authority_fences_are_not_downgraded_to_unknown(capsys,error_type):
    transport=module('bge_wss_transport')
    async def read(frame,*,encode):return result(frame)
    async def forbidden(frame):
        raise error_type('private stale authority or writer binding')
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda text:[1.]+[0.]*1023,lambda:{},
        query_handler=read,recovery_handler=forbidden)
    with pytest.raises(error_type):
        await worker._encode(dispatch(worker,phase='verify'))
    assert 'native_job_outcome_unresolved' not in capsys.readouterr().out
    assert worker.pending is None


@pytest.mark.asyncio
async def test_native_handler_cancellation_remains_cancellation(capsys):
    transport=module('bge_wss_transport')
    async def read(frame,*,encode):return result(frame)
    async def cancel_handler(frame):
        raise asyncio.CancelledError
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda text:[1.]+[0.]*1023,lambda:{},
        query_handler=read,recovery_handler=cancel_handler)
    with pytest.raises(asyncio.CancelledError):
        await worker._encode(dispatch(worker,phase='verify'))
    assert 'native_job_outcome_unresolved' not in capsys.readouterr().out
    assert worker.pending is None
