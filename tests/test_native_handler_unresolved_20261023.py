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


class AttachedHandler:
    def __init__(self, calls, error_type):
        self.calls=calls
        self.error_type=error_type
        self.generations=[]

    def attached(self,generation):
        self.generations.append(generation)

    def detached(self,generation):
        pass

    async def __call__(self,frame):
        self.calls.append((frame['id'],frame['phase']))
        raise self.error_type('private import path token=secret-native-value')


@pytest.mark.asyncio
@pytest.mark.parametrize('phase,has_ticket', [
    ('verify',False),
    ('capture',True),
    ('source_history',False),
    ('original_pages',False),
])
@pytest.mark.parametrize('error_type',[ModuleNotFoundError,SnapshotError])
async def test_unexpected_native_handler_error_preserves_warm_worker_and_read(
        capsys,monkeypatch,phase,has_ticket,error_type):
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.0001)
    peer=Peer();stop=asyncio.Event();calls=[];loads=[];closes=[]
    history=AttachedHandler(calls,error_type)
    original=AttachedHandler(calls,error_type)

    async def read(frame,*,encode):
        assert await encode(frame['query'])==[1.]+[0.]*1023
        return result(frame)

    async def recovery(frame):
        calls.append((frame['id'],frame['phase']))
        raise error_type('private module file token=secret-native-value')

    async def prepare(encoder,**identity):
        return PreparedRuntime(read,candidate(),source_history_handler=history,
            original_intake_handler=original)

    def load():
        loads.append('loaded')
        return lambda text:[1.]+[0.]*1023

    async def close():
        closes.append('closed')

    ticket=SnapshotTicketChannel(timeout_seconds=2) if has_ticket else None
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},
        prepare_runtime=prepare,recovery_handler=recovery,snapshot_channel=ticket,
        connector=peer.connect,close_runtime=close,stop=stop,
        heartbeat_seconds=.01,maximum_seconds=6)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        frame=dispatch(worker,phase=phase)
        await peer.put(frame)
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:
            f['resource_kind']=='document' and f['resource_id']=='native-job')
        assert worker.job is not None and worker.job['id']=='native-job'
        assert worker.pending is None
        assert not peer.frames('RECOVERY_PHASE_RESULT')
        assert calls==[('native-job',phase)]
        assert peer.sessions==1 and not task.done()
        if ticket is not None:
            assert ticket._assignment is None

        # Duplicate of the unresolved *same* job cannot reexecute.
        await peer.put(frame)
        await peer.put(query(request='healthy-read',claim='independent-read-slot'))
        read_result=await peer.wait('QUERY_RESULT',predicate=lambda f:
            f['request_id']=='healthy-read')
        assert read_result['status']=='degraded'
        assert calls==[('native-job',phase)]
        assert worker.job is not None and worker.pending is None
        assert not peer.frames('RECOVERY_PHASE_RESULT')
        assert loads==['loaded'] and peer.sessions==1 and not task.done()

        captured=capsys.readouterr()
        events=[]
        for line in captured.out.splitlines():
            try:
                item=json.loads(line)
            except ValueError:
                continue
            if item.get('event')=='native_job_outcome_unresolved':
                events.append(item)
        assert events==[dict(event='native_job_outcome_unresolved',
            job_id='native-job',kind='recovery_phase',phase=phase,
            error_class=error_type.__name__)]
        assert 'secret-native-value' not in captured.out+captured.err
        assert 'secret-query-from-authenticated-model' not in captured.out+captured.err
        assert 'private import path' not in captured.out+captured.err
        assert 'arguments' not in captured.out+captured.err
        assert 'exception' not in captured.out+captured.err
    finally:
        stop.set()
        await asyncio.wait_for(task,3)
    assert closes==['closed']


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
