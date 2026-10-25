"""Source-only warm-worker regressions: no Kaggle/PG/provider/model effects."""
import asyncio
import json
import sys

import pytest

from regional_knowledge.bge_contract import SPACE
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from regional_knowledge.kaggle_target_launch import MODULES
from regional_knowledge.recovery_journal import StaleWriter
from test_bge_shared_worker import Peer, query, result
from test_bge_wss_worker import CONFIG, module
from test_kaggle_bootstrap import candidate


# A real >512-word input under the accepted 40k-character dispatch cap;
# no tokenizer or model is loaded in this synthetic fixture.
OVERLONG_TEXT = ' '.join(['fixturetoken'] * 513)


def document(worker, kind='document'):
    assert len(OVERLONG_TEXT) < 40000
    return dict(type='DISPATCH', id='unresolved-job', claim='same-claim',
        kind=kind, space=SPACE, texts=[OVERLONG_TEXT],
        compute_claim='original-compute', dispatch_generation=1)


def source_command(worker):
    return dict(type='DISPATCH', id='unresolved-job', claim='same-claim',
        kind='source_command', run_id=CONFIG['run_id'],
        launch_nonce=CONFIG['launch_nonce'], runtime_incarnation=worker.incarnation,
        command_sha256='a'*64, command_b64='e30=', accepted={},
        handler_sha256='b'*64, database_generation='original-db',
        writer_generation='original-writer',
        compute_claim='original-compute', dispatch_generation=1)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['document'])
async def test_unexpected_durable_job_failure_keeps_same_warm_worker_and_reads(
        kind, capsys, monkeypatch):
    transport=module('bge_wss_transport')
    monkeypatch.setattr(transport.random, 'uniform', lambda *_: .0001)
    peer=Peer();stop=asyncio.Event();calls=[];loads=[];closes=[]
    secret='private page text bearer-token=not-for-logs'

    def native(text):
        if len(text.split()) > 512:
            calls.append(('encode',text))
            raise ValueError('more than 512 tokens: '+secret)
        return [1.]+[0.]*1023

    async def read(frame, *, encode):
        assert await encode(frame['query']) == [1.]+[0.]*1023
        return result(frame)

    async def source(frame):
        calls.append(('source',frame['id']))
        raise ModuleNotFoundError('secret model path: '+secret)

    async def prepare(encoder,**identity):
        return PreparedRuntime(read,candidate(),source_handler=source)

    def load():
        loads.append('loaded')
        return native

    async def close():
        closes.append('closed')

    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},
        prepare_runtime=prepare,connector=peer.connect,close_runtime=close,
        stop=stop,heartbeat_seconds=.01,maximum_seconds=6)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        frame=document(worker) if kind=='document' else source_command(worker)
        await peer.put(frame)
        await peer.wait('RESOURCE_RELEASED', predicate=lambda f:
            f['resource_id']=='unresolved-job' and f['compute_claim']=='original-compute')
        # Releasing a compute permit no longer means the durable attempt ran.
        await asyncio.wait_for(asyncio.shield(worker.encoding),1)
        assert worker.job is not None and worker.job['claim']=='same-claim'
        assert worker.pending is None and not peer.frames('RESULT')
        assert not peer.frames('SOURCE_COMMAND_RESULT')
        assert len(calls)==1 and not task.done()
        await peer.put(frame)  # Exact duplicate is not permission to reexecute.
        await peer.put(query(request='still-serving',claim='fresh-read'))
        reply=await peer.wait('QUERY_RESULT',predicate=lambda f:
            f['request_id']=='still-serving')
        assert reply['status']=='degraded'
        assert len(calls)==1 and loads==['loaded']
        assert worker.pending is None and worker.job['id']=='unresolved-job'
        assert peer.sessions==1 and not task.done()
        assert not peer.frames('RESULT') and not peer.frames('SOURCE_COMMAND_RESULT')
        output=capsys.readouterr().out
        assert secret not in output
        events=[json.loads(line) for line in output.splitlines()
                if line.startswith('{"event":')]
        events=[event for event in events if event['event']!='worker_bootstrap']
        assert events==[dict(event='native_job_outcome_unresolved',
            job_id='unresolved-job',kind=kind,phase=None,
            error_class='ValueError' if kind=='document' else 'ModuleNotFoundError')]
        assert secret not in json.dumps(events)
    finally:
        stop.set()
        await asyncio.wait_for(task,3)
    assert closes==['closed']


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['document','query'])
async def test_unexpected_legacy_vector_error_uses_unresolved_boundary(kind,capsys):
    transport=module('bge_wss_transport')
    worker=transport.BgeWssTransport(CONFIG, lambda:None, lambda:{})
    def bad(_):
        raise ValueError('private over-512-token text')
    worker.encoder=bad
    await worker._encode(dict(kind=kind,id='legacy-job',claim='claim',texts=['secret']))
    assert worker.pending is None
    assert json.loads(capsys.readouterr().out)==dict(event='native_job_outcome_unresolved',
        job_id='legacy-job',kind=kind,phase=None,error_class='ValueError')




@pytest.mark.asyncio
async def test_oversized_vector_is_not_published_as_success(capsys):
    transport=module('bge_wss_transport')
    worker=transport.BgeWssTransport(CONFIG,lambda:None,lambda:{})
    worker.encoder=lambda _: [1.]*100000  # Far beyond channel RESULT bound.
    await worker._encode(dict(kind='document',id='oversize-vector',claim='claim',
        texts=['fixture document']))
    assert worker.pending is None
    assert json.loads(capsys.readouterr().out)==dict(event='native_job_outcome_unresolved',
        job_id='oversize-vector',kind='document',phase=None,error_class='ValueError')


@pytest.mark.asyncio
@pytest.mark.parametrize('error_type',[PermissionError,StaleWriter,asyncio.CancelledError])
async def test_source_authority_and_cancel_still_propagate(error_type,capsys):
    transport=module('bge_wss_transport')
    async def blocked(_):
        raise error_type('secret authority bytes')
    async def read(frame,*,encode):
        return result(frame)
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:None,lambda:{},query_handler=read)
    worker.source_handler=blocked
    with pytest.raises(error_type):
        await worker._encode(dict(kind='source_command',id='authority-job',claim='claim'))
    assert worker.pending is None
    assert capsys.readouterr().out==''


@pytest.mark.asyncio
async def test_true_stop_is_not_swallowed_as_job_unknown(capsys):
    transport=module('bge_wss_transport')
    async def stop(_):
        raise transport.BgeChannelStopped('explicit stop')
    worker=transport.BgeWssTransport(CONFIG,lambda:None,lambda:{})
    worker.recovery_handler=stop
    with pytest.raises(transport.BgeChannelStopped):
        await worker._encode(dict(kind='recovery_phase',id='stop-job',claim='claim'))
    assert worker.pending is None and capsys.readouterr().out==''


@pytest.mark.asyncio
async def test_missing_optional_product_telemetry_and_overlapping_reads_do_not_kill_writer(
        monkeypatch):
    # This is the actual generated Kaggle module allowlist. No optional module.
    assert 'product_telemetry' not in MODULES
    monkeypatch.setitem(sys.modules,'regional_knowledge.product_telemetry',None)
    transport=module('bge_wss_transport')
    peer=Peer();stop=asyncio.Event();calls=[];drains=[]

    async def read(frame,*,encode):
        calls.append(frame['request_id'])
        assert await encode(frame['query']) == [1.]+[0.]*1023
        return result(frame)

    def drain(frame):
        drains.append(frame['request_id'])
        raise RuntimeError('private diagnostic sink failure')

    async def prepare(encoder,**identity):
        return PreparedRuntime(read,candidate(),read_event_drain=drain)

    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda text:[1.]+[0.]*1023,lambda:{},prepare_runtime=prepare,
        connector=peer.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=6)
    # Deterministically reproduce the old writer's occupied-export branch
    # without ever importing or starting notebook product telemetry.
    blocked_export=asyncio.create_task(asyncio.Event().wait())
    worker._read_telemetry_export=blocked_export
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        for index in range(2):
            request=f'read-{index}'
            await peer.put(query(request=request,claim=f'claim-{index}'))
            out=await peer.wait('QUERY_RESULT',predicate=lambda f:f['request_id']==request)
            assert out['status']=='degraded'
        assert calls==['read-0','read-1']
        assert drains==[]  # No new worker export despite native drain callback.
        assert worker._read_telemetry_export is blocked_export
        assert not blocked_export.done()
        assert peer.sessions==1 and not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task,3)
        blocked_export.cancel()
        await asyncio.gather(blocked_export,return_exceptions=True)
