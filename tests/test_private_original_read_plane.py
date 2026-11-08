"""Fictional original metadata on the existing ephemeral read plane."""
import asyncio
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_protocol import decode_envelope, encode_envelope
from regional_knowledge.kaggle_source_read import SourceReadRequest, execute_source_read
from regional_knowledge.kaggle_snapshot_io import SnapshotIo
from regional_knowledge.original_intake import canonical, digest
from test_kaggle_original_intake import native, acquire, SOURCE, INGEST


def request(**changes):
    values=dict(request_id='private-page',connection_generation=1,turn_generation=0,
        operation='original_page_io',source_id=SOURCE,page_start=0,limit=1,
        original=dict(ingestion_id=INGEST,handle='retained',original_sha256='a'*64,
            cursor=dict(block=0,offset=0,text=0)),
        actor=dict(person_id='owner',audience='personal',auth_epoch=1,
            visibility_epoch=0,context_ref='owner-context'),min_data_seq=0,deadline_ms=2000)
    values.update(changes)
    return SourceReadRequest.model_validate(values)


@pytest.mark.asyncio
async def test_original_read_has_no_pg_or_forward_wait_and_roundtrips(native):
    _,record=await acquire(native)
    native.handler._records[record['handle']]['verified']={'retained':True}
    native.handler.snapshot_io=SnapshotIo(factory=SimpleNamespace(_current=lambda binding:None,
        limits=SimpleNamespace(timeout_seconds=10)),tickets=None,transfer=None)
    req=request(original=dict(ingestion_id=INGEST,handle=record['handle'],
        original_sha256=record['sha256'],cursor=dict(block=0,offset=0,text=0)))
    assert decode_envelope(encode_envelope(req))==req
    blocked=asyncio.Event();order=[]
    async def text():
        order.append('writer-start');await blocked.wait();order.append('writer-commit')
    writer=asyncio.create_task(text());await asyncio.sleep(0)
    def forbidden():raise AssertionError('private originals must not acquire PG')
    async def read(q):
        return await execute_source_read(q,acquire=forbidden,
            original_page=lambda q:native.handler.read_page(q,lease_current=lambda:True))
    try:
        result=await read(req)
        assert result.status=='ok' and result.records[0]['status']=='pending'
        assert result.observed_data_seq is None and result.observed_projection_seq is None
        assert decode_envelope(encode_envelope(result))==result
        handle=result.records[0]['handle']
        await native.handler.snapshot_io.operations[handle].task
        status=await read(req.model_copy(update={'original':req.original.model_copy(update={'io_handle':handle})}))
        assert status.records[0]['status']=='complete'
        assert status.records[0]['value']['original_sha256']==record['sha256']
        assert order==['writer-start']
    finally:
        blocked.set();await writer;await native.handler.snapshot_io.aclose()
    assert order==['writer-start','writer-commit']


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['source','ingestion','sha','foreign_handle','dataset_handle','revoked','generation'])
async def test_original_read_rejects_foreign_scope_and_expired_lease(native,fault):
    from regional_knowledge.original_intake import OriginalIntakeError
    _,record=await acquire(native)
    native.handler._records[record['handle']]['verified']={'retained':True}
    io=native.handler.snapshot_io=SnapshotIo(factory=SimpleNamespace(_current=lambda binding:None,
        limits=SimpleNamespace(timeout_seconds=10)),tickets=None,transfer=None)
    req=request(original=dict(ingestion_id=INGEST,handle=record['handle'],
        original_sha256=record['sha256'],cursor=dict(block=0,offset=0,text=0)))
    try:
        result=await native.handler.read_page(req,lease_current=lambda:True)
        selector=req.original.model_dump()
        if fault=='source':req=req.model_copy(update={'source_id':'40000000-0000-0000-0000-000000000004'})
        elif fault=='ingestion':selector['ingestion_id']='40000000-0000-0000-0000-000000000004'
        elif fault=='sha':selector['original_sha256']='b'*64
        elif fault=='foreign_handle':selector['io_handle']='b'*64
        elif fault=='dataset_handle':
            args=dict(handle=record['handle'],original_sha256=record['sha256'],index=0,cursor=selector['cursor'])
            key=digest(canonical(dict(phase='original_pages',args=args)))
            io.operations[result['handle']].phase='snapshot'
            selector['io_handle']=digest(key.encode())
        elif fault=='generation':req=req.model_copy(update={'connection_generation':2})
        req=req.model_copy(update={'original':type(req.original).model_validate(selector)})
        with pytest.raises(OriginalIntakeError):
            await native.handler.read_page(req,lease_current=lambda:fault!='revoked')
    finally:await io.aclose()


@pytest.mark.parametrize('change',[dict(operation='source_catalog',query='public title',page_start=None),
    dict(query='cannot mix selector'),dict(page_end=0),dict(limit=2),dict(min_data_seq=9),
    dict(original=dict(ingestion_id=INGEST,handle='retained',original_sha256='a'*64,cursor={'block':0}))])
def test_private_selector_cannot_be_used_as_public_query(change):
    with pytest.raises(ValueError):request(**change)


@pytest.mark.asyncio
async def test_public_gate_never_accepts_private_original(env,auth):
    from test_source_read_gate import setup
    from regional_knowledge.source_read_gate import SourceGateDenied
    gate,authority,binding=await setup(env,auth)
    with pytest.raises(SourceGateDenied,match='owned intake'):
        await gate.dispatch(authority,request(),binding)


@pytest.mark.asyncio
@pytest.mark.parametrize('revoke',[False,True])
async def test_private_read_delivery_uses_existing_authority_and_no_pg_frontier(state,revoke):
    import time
    from regional_knowledge.kaggle_source_read import SourceReadResult
    from regional_knowledge.participant_contracts import ParticipantDisable
    from test_participants import change
    from test_bge_admission import query, release
    s=state;actor=query(s.context,'projection').actor
    req=request(actor=actor,connection_generation=s.generation)
    ticket=await s.a.begin_read(s.context,req,dialog_ref='owner-dialog',deadline=time.monotonic()+1.9)
    work=await s.q.next_channel_work(s.run,s.token,s.generation);s.a.mark_work_sent(work)
    result=SourceReadResult(request_id=req.request_id,connection_generation=s.generation,turn_generation=0,
        operation=req.operation,status='ok',records=({'status':'pending','handle':'a'*64},),
        observed_data_seq=None,observed_projection_seq=None)
    if revoke:change(s.auth.p,ParticipantDisable(disabled=True,reason='fictional revoke before return'))
    assert await s.a.finish_read(s.run,s.token,s.generation,result) is not revoke
    if revoke:assert ticket.result.cancelled()
    else:assert (await ticket.result).records==result.records
    await release(s,work)

from test_control_spool import env
from test_read_authority import auth
from test_participants import people
from test_bge_admission import state


@pytest.mark.asyncio
async def test_real_transport_private_read_completes_during_source_text_without_reordering(native):
    from regional_knowledge.kaggle_bootstrap import PreparedRuntime
    from test_kaggle_target_runtime import candidate
    from test_bge_shared_worker import Peer
    from test_bge_wss_worker import CONFIG,module
    _,record=await acquire(native)
    native.handler._records[record['handle']]['verified']={'retained':True}
    io=native.handler.snapshot_io=SnapshotIo(factory=SimpleNamespace(_current=lambda binding:None,
        limits=SimpleNamespace(timeout_seconds=10)),tickets=None,transfer=None)
    peer=Peer();stop=asyncio.Event();started=asyncio.Event();finish=asyncio.Event();order=[]
    async def read(frame,*,encode,lease_current=None):
        req=decode_envelope(__import__('json').dumps(frame).encode())
        def forbidden():raise AssertionError('private original must not acquire PG')
        result=await execute_source_read(req,acquire=forbidden,
            original_page=lambda q:native.handler.read_page(q,lease_current=lease_current))
        return __import__('json').loads(encode_envelope(result))
    async def source(frame):
        order.append('start:'+frame['id']);started.set();await finish.wait()
        order.append('commit:'+frame['id']);return {'receipt':'synthetic'}
    async def prepare(*args,**kwargs):return PreparedRuntime(read,candidate(),source_handler=source)
    worker=module('bge_wss_transport').BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda _:None,lambda:{},prepare_runtime=prepare,
        connector=peer.connect,heartbeat_seconds=.01,maximum_seconds=5,stop=stop)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        await peer.put(dict(type='DISPATCH',id='text-a',claim='text-claim',kind='source_command',
            compute_claim='writer-claim',dispatch_generation=1,run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,
            database_generation='fixture-db',writer_generation='fixture-writer',
            command_b64='e30=',command_sha256='a'*64,accepted={},handler_sha256='b'*64))
        await asyncio.wait_for(started.wait(),1)
        req=request(original=dict(ingestion_id=INGEST,handle=record['handle'],original_sha256=record['sha256'],
            cursor=dict(block=0,offset=0,text=0)),compute_claim='read-claim')
        await peer.put(req.model_dump())
        result=await peer.wait('SOURCE_READ_RESULT')
        assert result['operation']=='original_page_io' and result['status']=='ok'
        assert result['records'][0]['status']=='pending' and not peer.frames('SOURCE_COMMAND_RESULT')
        assert order==['start:text-a'] and not finish.is_set()
        finish.set();await peer.wait('SOURCE_COMMAND_RESULT')
        assert order==['start:text-a','commit:text-a']
    finally:
        finish.set();stop.set();await asyncio.wait_for(task,3);await io.aclose()


@pytest.mark.asyncio
async def test_private_retry_walks_existing_attempt_keys_across_requests_without_replacing_tasks(native):
    from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
    from regional_knowledge.original_intake import OriginalIntakeError
    _,record=await acquire(native)
    native.handler._records[record['handle']]['verified']={'retained':True}
    io=native.handler.snapshot_io=SnapshotIo(factory=SimpleNamespace(_current=lambda binding:None,
        limits=SimpleNamespace(timeout_seconds=10)),tickets=None,transfer=None)
    original=native.handler._render_original_page;calls=[]
    async def fail_once(*args):
        calls.append('render')
        if len(calls)==1:raise OriginalIntakeError('fictional_transient_read')
        return await original(*args)
    native.handler._render_original_page=fail_once
    bridge=object.__new__(RecoveryPhaseDispatch)
    bridge.current=lambda binding:True;bridge.bind_attachment=lambda binding:None
    bridge.identity=('run','nonce','inc');bridge.timeout=2;bridge.queue=None  # No durable job may be admitted by this path.
    args=dict(handle=record['handle'],original_sha256=record['sha256'],index=0,cursor=dict(block=0,offset=0,text=0))
    root=digest(canonical(dict(phase='original_pages',args=args)))
    async def page_read(args,handle,attempt=0):
        await asyncio.sleep(0)
        req=request(original=dict(ingestion_id=INGEST,handle=args['handle'],original_sha256=args['original_sha256'],
            cursor=args['cursor'],io_handle=handle,attempt=attempt))
        return await native.handler.read_page(req,lease_current=lambda:True)
    try:
        first=await bridge.request(None,'original_pages',args,operation_key=root,page_read=page_read)
        assert first=={'error':'fictional_transient_read','retryable':True} and calls==['render']
        old=io.operations[digest(root.encode())]
        second=await bridge.request(None,'original_pages',args,operation_key=root,page_read=page_read)
        assert second['original_sha256']==record['sha256'] and calls==['render','render']
        assert io.operations[digest(root.encode())] is old and old.task.result()==first
        assert set(io.operations)=={digest(root.encode()),digest((root+':retry:1').encode())}
        assert await bridge.request(None,'original_pages',args,operation_key=root,page_read=page_read)==second
        assert calls==['render','render']
    finally:await io.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('revoke',[False,True])
async def test_modern_page_poll_renews_expired_outer_authority_but_denies_revoked_owner(intake,monkeypatch,revoke):
    import json
    from mcp.server.mcpserver.exceptions import ToolError
    from regional_knowledge.kaggle_source_read import SourceReadResult
    from test_kaggle_source_importer import start
    s=intake;book=await start(s)
    resolver=s.importer.read_authority;resolver.clock=lambda:s.t.auth.now[0]
    s.importer.native_intake=SimpleNamespace(spool=s.spool)
    original_pages=s.originals.pages;seen=[]
    async def accepted(*args):return None
    monkeypatch.setattr('regional_knowledge.source_page_progress.read_accepted_model_page',accepted)
    async def begin(authority,query,**kwargs):
        await resolver.recheck(authority)
        assert authority.expires_at>resolver.clock() and query.operation=='original_page_io'
        seen.append(query)
        future=asyncio.get_running_loop().create_future()
        future.set_result(SourceReadResult(request_id=query.request_id,connection_generation=query.connection_generation,
            turn_generation=query.turn_generation,operation=query.operation,status='ok',
            records=({'status':'pending','handle':'a'*64},),observed_data_seq=None,observed_projection_seq=None))
        return SimpleNamespace(result=future)
    monkeypatch.setattr(s.t.queue.shared_reads,'begin_read',begin)
    async def slow_pages(context,ingestion_id,cursor=None,batch_size=1,*,page_read):
        # Existing slow-page contract: outer context expires while the render lives.
        s.t.auth.now[0]+=resolver.ttl_seconds+1
        if revoke:
            await s.t.auth.provider.revoke_token(await s.t.auth.provider.load_access_token(s.principal.access_token))
        receipt=s.originals.receipts[ingestion_id]
        await page_read(dict(handle='owned-retained-original',original_sha256=receipt.sha256,index=0,
            cursor=dict(block=0,offset=0,text=0)),None)
        return await original_pages(context,ingestion_id,cursor,batch_size)
    s.originals.pages=slow_pages
    if revoke:
        with pytest.raises(ToolError) as caught:
            await s.importer.book_pages(principal=s.principal,ingestion_id=book.ingestion_id)
        assert json.loads(str(caught.value))['reason']=='access_denied' and not seen
    else:
        result=await s.importer.book_pages(principal=s.principal,ingestion_id=book.ingestion_id)
        assert result.pages[0].data==b'synthetic-jpeg' and len(seen)==1

from test_kaggle_source_importer import intake
from test_kaggle_public_backend import target
