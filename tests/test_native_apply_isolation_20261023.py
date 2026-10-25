"""P0 command failure isolation (offline fake wire only).

No Kaggle, native PostgreSQL, code provider, model, or domain data is launched.
The existing test_kaggle_apply real-PG fixture separately proves that a failed
apply transaction rolls back effect, ledger and cursor; these checks exercise
the retained worker/phase lifetime boundary around that proved failure type.
"""
import asyncio
from contextlib import asynccontextmanager

import pytest

from regional_knowledge.kaggle_apply import ApplyExecutionError, ApplyUnknown
from regional_knowledge.kaggle_recovery_phases import WorkerRecoveryPhases
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory, SnapshotError, encode, sha
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from test_kaggle_recovery_phases import database
from test_kaggle_bootstrap import candidate
from test_bge_shared_worker import Peer, query, result
from test_bge_wss_worker import CONFIG, module


def factory(identity):
    native = object.__new__(NativeSnapshotFactory)
    native.database = database()
    native.identity = identity
    native._current = lambda binding: None
    native._owned_candidates = set()
    native._capabilities = set()
    return native


def phase(identity, kind, key, *, generation=1, arguments=None):
    args = {} if arguments is None else arguments
    return dict(type='DISPATCH', protocol_version=2, connection_generation=generation,
        id='job-' + key, claim='claim-' + key, kind='recovery_phase',
        run_id=identity[0], launch_nonce=identity[1],
        runtime_incarnation=identity[2],
        database_generation=database().database_generation,
        writer_generation=database().writer_generation,
        phase=kind, operation_key=key, arguments=args,
        request_sha256=sha(encode(dict(phase=kind, arguments=args))),
        compute_claim='slot-' + key, dispatch_generation=generation)


@pytest.mark.asyncio
async def test_known_post_ready_sql_rollback_is_fixed_rejected_not_fatal_or_duplicate():
    identity=('fixture-run','fixture-nonce','fixture-incarnation')
    native=factory(identity)
    handler=WorkerRecoveryPhases(factory=native,download=lambda *_:None,
        workspace=lambda *_:None)
    handler._initialized=object()  # Actual warm run already READY.
    attempts=[];rolled_back=[]
    @asynccontextmanager
    async def fake_transaction():
        try:
            yield
        except ApplyExecutionError:
            rolled_back.append(True)
            raise
    async def execute(phase_name,args,key,binding,frame):
        attempts.append(key)
        if key=='bad':
            async with fake_transaction():
                raise ApplyExecutionError('private synthetic SQL AmbiguousColumn')
        return dict(status='ok', phase=phase_name)
    handler._execute=execute
    failed=phase(identity,'apply','bad')
    first=await handler(failed)
    assert first['value']==dict(status='rejected',reason='native_apply_rolled_back')
    assert rolled_back==[True]
    assert await handler(failed)==first  # Exact cached rejection, not rerun.
    good=await handler(phase(identity,'verify','next'))
    assert good['value']=={'status':'ok','phase':'verify'}
    assert attempts==['bad','next']
    assert handler._initialization_error is None


@pytest.mark.asyncio
async def test_unknown_commit_and_revoked_authority_remain_distinct_from_rejected():
    identity=('fixture-run','fixture-nonce','fixture-incarnation')
    native=factory(identity)
    handler=WorkerRecoveryPhases(factory=native,download=lambda *_:None,
        workspace=lambda *_:None)
    handler._initialized=object()
    attempts=[]
    async def execute(phase_name,args,key,binding,frame):
        attempts.append(key)
        if key=='unknown':raise ApplyUnknown('commit ACK absent')
        raise PermissionError('writer/ACL revoked')
    handler._execute=execute
    unknown=phase(identity,'apply','unknown')
    with pytest.raises(ApplyUnknown):await handler(unknown)
    with pytest.raises(ApplyUnknown):
        await handler(unknown)
    with pytest.raises(PermissionError):
        await handler(phase(identity,'apply','authority'))
    assert attempts==['unknown','unknown','authority']
    assert not handler._results


@pytest.mark.asyncio
async def test_same_wss_run_survives_native_sql_failure_then_read_and_next_command(monkeypatch):
    transport=module('bge_wss_transport')
    peer=Peer();stop=asyncio.Event();executed=[];loads=[];closes=[]
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.0001)
    phases=[]
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,*,run_id,launch_nonce,runtime_incarnation):
        native=factory((run_id,launch_nonce,runtime_incarnation))
        handler=WorkerRecoveryPhases(factory=native,download=lambda *_:None,workspace=lambda *_:None)
        handler._initialized=object()
        async def execute(phase_name,args,key,binding,frame):
            executed.append(key)
            if key=='bad':raise ApplyExecutionError('synthetic PG rolled back')
            return dict(status='ok')
        handler._execute=execute
        phases.append(handler)
        return PreparedRuntime(read,candidate())
    async def native_command(frame):
        return await phases[0](frame)
    def load():loads.append(True);return lambda text:[1.]+[0.]*1023
    async def close():closes.append(True)
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda: {},
        connector=peer.connect,prepare_runtime=prepare,recovery_handler=native_command,
        close_runtime=close,heartbeat_seconds=.01,maximum_seconds=5,stop=stop)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        identity=(CONFIG['run_id'],CONFIG['launch_nonce'],worker.incarnation)
        await peer.put(phase(identity,'apply','bad'))
        failed=await peer.wait('RECOVERY_PHASE_RESULT')
        assert failed['receipt']['value']==dict(status='rejected',reason='native_apply_rolled_back')
        await peer.put(dict(type='RESULT_ACK',job_id='job-bad',claim='claim-bad',status='accepted'))
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_id']=='job-bad')
        await peer.put(query(request='after-fault'))
        out=await peer.wait('QUERY_RESULT',predicate=lambda f:f['request_id']=='after-fault')
        assert out['status']=='degraded'
        await peer.put(phase(identity,'verify','good'))
        success=await peer.wait('RECOVERY_PHASE_RESULT',
            predicate=lambda f:f['job_id']=='job-good')
        assert success['receipt']['value']=={'status':'ok'}
        await peer.put(dict(type='RESULT_ACK',job_id='job-good',claim='claim-good',status='accepted'))
        assert executed==['bad','good'] and loads==[True] and peer.sessions==1
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task,3)
    assert closes==[True]
