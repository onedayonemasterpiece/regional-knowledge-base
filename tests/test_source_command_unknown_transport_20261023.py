"""An uncertain native source result must not kill the warmed read process.

Real PG commit/ledger behavior belongs to the existing PG17 tests. This test
exercises the actual existing WSS native-result/compute/replay boundary only.
No provider, notebook, model, database or remote controller is started.
"""
import asyncio

import pytest

from regional_knowledge.kaggle_apply import ApplyUnknown, ApplyExecutionError
from regional_knowledge.kaggle_bootstrap import PreparedRuntime
from test_bge_shared_worker import Peer, query, result
from test_bge_wss_worker import CONFIG, module
from test_kaggle_bootstrap import candidate


@pytest.mark.asyncio
@pytest.mark.parametrize('error_type',[ApplyUnknown,ApplyExecutionError])
async def test_source_without_committed_receipt_keeps_warm_run_and_original_job(monkeypatch,error_type):
    transport=module('bge_wss_transport')
    peer=Peer();stop=asyncio.Event();calls=[];loads=[];closes=[]
    monkeypatch.setattr(transport.random,'uniform',lambda *_:.0001)

    async def read(frame,*,encode):
        assert await encode(frame['query'])==[1.]+[0.]*1023
        return result(frame)

    async def source(frame):
        calls.append((frame['id'],frame['claim'],frame['command_sha256']))
        raise error_type('synthetic failed apply without canonical committed receipt')

    async def prepare(encoder,**identity):
        return PreparedRuntime(read,candidate(),source)

    def load():
        loads.append('load')
        return lambda _: [1.]+[0.]*1023

    async def close():
        closes.append('closed')

    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},
        prepare_runtime=prepare,connector=peer.connect,close_runtime=close,
        stop=stop,heartbeat_seconds=.01,maximum_seconds=5)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        original=dict(type='DISPATCH',id='uncertain-job',claim='same-claim',kind='source_command',
            run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],
            runtime_incarnation=worker.incarnation,command_sha256='a'*64,
            command_b64='e30=',accepted={},handler_sha256='b'*64,
            database_generation='original-db',writer_generation='original-writer',
            compute_claim='same-compute',dispatch_generation=1)
        await peer.put(original)

        # A completed *compute* releases capacity, not the source command's
        # accepted/unknown durable outcome. There is no canonical receipt.
        await peer.wait('RESOURCE_RELEASED',predicate=lambda f:
            f['resource_id']=='uncertain-job' and f['compute_claim']=='same-compute')
        assert worker.job is not None and worker.job['id']=='uncertain-job'
        assert worker.pending is None and not peer.frames('SOURCE_COMMAND_RESULT')
        assert calls==[('uncertain-job','same-claim','a'*64)]
        assert not task.done()

        await peer.put(original)  # Exact replay must not reexecute or settle.
        await peer.put(query(request='read-with-source-unknown'))
        read_result=await peer.wait('QUERY_RESULT',predicate=lambda f:
            f['request_id']=='read-with-source-unknown')
        assert read_result['status']=='degraded'
        assert calls==[('uncertain-job','same-claim','a'*64)]
        assert worker.pending is None and not peer.frames('SOURCE_COMMAND_RESULT')
        assert peer.sessions==1 and not task.done()

        # Reconnect changes the authenticated channel, not the retained
        # command identity, original unknown result or native incarnation.
        await peer.disconnect()
        await peer.wait('READY',generation=2)
        await peer.put(original,generation=2)
        await peer.put(query(generation=2,request='read-after-reconnect'),generation=2)
        final=await peer.wait('QUERY_RESULT',generation=2,predicate=lambda f:
            f['request_id']=='read-after-reconnect')
        assert final['status']=='degraded'
        assert calls==[('uncertain-job','same-claim','a'*64)]
        assert not peer.frames('SOURCE_COMMAND_RESULT')
        assert worker.incarnation==original['runtime_incarnation']
        assert peer.sessions==2 and not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task,3)
    assert loads==['load'] and closes==['closed']
