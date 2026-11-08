"""Fictional native job replay; no provider, database, corpus or model."""
import asyncio
import json

import pytest

from test_bge_wss_worker import CONFIG, module


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['source_command', 'recovery_phase'])
@pytest.mark.parametrize('changed', [None, 'payload'])
async def test_waiting_native_assignment_before_ready_ack_keeps_exact_payload(kind, changed):
    transport=module('bge_wss_transport')
    async def prepare():pass
    worker=transport.BgeWssTransport({**CONFIG,'shared_reads':True},lambda:None,lambda:{},
        prepare_runtime=prepare)
    worker.encoder=lambda _: [1.]+[0.]*1023
    worker.loading=asyncio.get_running_loop().create_future();worker.loading.set_result(None)
    # Pretend the resident model/runtime has already been prepared on a prior attachment.
    worker.prepare_runtime=None
    job=dict(type='DISPATCH',protocol_version=2,connection_generation=2,id='fictional-native',
        claim='same-claim',kind=kind,compute_claim='same-compute',dispatch_generation=2,
        run_id=CONFIG['run_id'],launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,
        command_b64='eyJmaWN0aW9uYWwiOnRydWV9',command_sha256='a'*64,accepted=True,
        handler_sha256='b'*64,database_generation='fictional-db',writer_generation='fictional-writer')
    if kind=='recovery_phase':job.update(phase='snapshot_status',operation_key='fixture-op',request_sha256='a'*64,arguments={})
    worker.waiting_job=job
    executed=[];outgoing=[];incoming=asyncio.Queue()
    async def handle(value):executed.append(value);return {'fixture':'ok'}
    worker.source_handler=handle;worker.recovery_handler=handle
    replay=dict(job)
    if changed:replay['command_sha256' if kind=='source_command' else 'request_sha256']='c'*64
    class Socket:
        async def send(self, raw):
            frame=json.loads(raw);outgoing.append(frame)
            if frame['type']=='ATTACH':await incoming.put(dict(type='WELCOME',provider_version='fixture'))
            if frame['type']=='READY':
                await incoming.put(replay)
                await incoming.put(dict(type='READY_ACK',status='ready'))
            if frame['type'] in ('SOURCE_COMMAND_RESULT','RECOVERY_PHASE_RESULT'):worker.stop.set()
        async def receive(self):
            return json.dumps({'protocol_version':2,'connection_generation':2,**await incoming.get()})
    try:
        if changed:
            with pytest.raises(ValueError,match='replayed waiting assignment payload mismatch'):
                await asyncio.wait_for(worker._session(Socket()),1)
            assert executed==[]
        else:
            await asyncio.wait_for(worker._session(Socket()),1)
            assert executed==[job]
            assert sum(f['type'].endswith('_RESULT') for f in outgoing)==1
    finally:
        if worker.encoding is not None:await asyncio.gather(worker.encoding,return_exceptions=True)
