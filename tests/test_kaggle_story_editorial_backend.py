"""Real OAuth/spool/admission; fixed native context and vector task are synthetic."""
import asyncio
import base64
import json
from types import SimpleNamespace
import pytest
import pytest_asyncio
from mcp.server.mcpserver.exceptions import ToolError
from regional_knowledge.kaggle_apply import HandlerRegistry,ApplyReceipt
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
from regional_knowledge.kaggle_import_tokenizer import HfImportTokenCounter
from regional_knowledge.kaggle_story_editorial_backend import KaggleStoryEditorial,canonical,digest
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_read_gate import SourceReadGate
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_source_commands import AcceptedSourceDispatch
from regional_knowledge.kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
from regional_knowledge.kaggle_story_read import story_item
from regional_knowledge.bge_contract import SPACE
from test_kaggle_public_backend import target,work,release
from test_kaggle_editorial_contracts import record
from test_story_editorial import operation,STORY
from test_participants import people
from test_read_authority import auth
from test_recovery_journal import ConditionalMemoryStore


@pytest_asyncio.fixture
async def editorial(target,tmp_path):
    t=target
    with t.queue.connect() as db:
        db.execute('DELETE FROM jobs')
        row=db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',(t.run,)).fetchone()
        values=[]
        for raw in row:
            value=json.loads(raw);value.update(dict(zip(('schema_revision','function_revision','executor_revision'),DOMAIN_REVISIONS.wire())))
            values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',(*values,t.run))
    spool=ControlSpool(t.queue,t.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='editorial-controller-test'),
        HandlerRegistry([current_handler(route,check_sha256='a'*64,apply_sha256='b'*64) for route in ('source.import.v4','story.edit.v1','story.access.v1','source.withdraw.v1')]))
    def current(binding):return binding==current_binding(t.queue)
    gate=SourceReadGate(t.auth.resolver,spool,binding_current=current)
    spool._source_anchor=SimpleNamespace(database_generation='synthetic-db',current=lambda:True)
    tokenizer=HfImportTokenCounter(tmp_path/'no-model-loaded');counts=[]
    async def count(texts):counts.append(tuple(texts));return [10]*len(texts)
    tokenizer.counts=count
    startup=SimpleNamespace(config=SimpleNamespace(queue=t.queue,spool=spool,participants=t.auth.p.registry,writer_generation='writer-1'),
        gate=gate,authority=t.auth.resolver,backend=t.backend,binding_current=current)
    startup.source_dispatch=AcceptedSourceDispatch(spool,binding_current=lambda **fields:True)
    owner=KaggleStoryEditorial(startup,tokenizer=tokenizer)
    yield SimpleNamespace(t=t,workflow=owner,spool=spool,gate=gate,counts=counts,startup=startup)
    await owner.aclose();await startup.source_dispatch.aclose();spool.close()


async def context(s,task,role='editor',*,second_role=None,second_revision=None):
    async def respond(role,revision=None):
        w=await work(s.t)
        assert type(w.query) is EditorialReadRequest and w.query.view=='edit_context'
        r=record();r.update(view='edit_context',role=role,variant_refs=[r['default_variant_ref']])
        if revision is not None:r.update(revision=revision,current_revision=revision)
        result=EditorialReadResult(request_id=w.query.request_id,connection_generation=s.t.generation,turn_generation=0,status='ok',
            record=r,evidence=(story_item(r),),observed_data_seq=5,observed_projection_seq=5,observed_visibility_epoch=1,observed_database_generation='synthetic-db')
        assert await s.t.admission.finish_read(s.t.run,s.t.token,s.t.generation,result)
        await release(s.t,w)
    await respond(role)
    for _ in range(300):
        if task.done():return await task
        if s.t.admission._reads:
            await respond(second_role or role,second_revision)
            return await task
        await asyncio.sleep(.001)
    pytest.fail('editorial task neither completed nor acquired fresh preflight')


@pytest.mark.asyncio
async def test_editor_can_encode_only_exact_authorized_document_then_save_accepted_bytes_once(editorial):
    s=editorial;t=s.t;actor=t.auth.p.other.subject
    rogue=t.queue.enqueue(actor,'unrelated-body',['Not an authorized Story body'],kind='document')
    args=dict(story_id=STORY,expected_revision=1,idempotency_key='exact-story-edit',operation=operation())
    first=await context(s,asyncio.create_task(s.workflow.mutate(t.principal,**args)))
    assert first['status']=='processing' and first['reason']=='exact_narrative_vector_pending'
    assert s.counts==[(operation().body,)]
    assert actor not in t.admission._documents
    w=await t.queue.next_channel_work(t.run,t.token,t.generation)
    assert w.kind=='document' and w.document_payload['id']!=rogue
    assert tuple(w.document_payload['texts'])==(operation().body,)
    t.admission.mark_work_sent(w);p=w.document_payload
    assert t.queue.complete(t.run,t.token,p['id'],p['claim'],SPACE,[[1.]+[0.]*1023],{},connection_generation=t.generation)=='accepted'
    await release(t,w)
    second=await context(s,asyncio.create_task(s.workflow.mutate(t.principal,**args)))
    assert second['status']=='processing' and not second['remote_outcome_durable']
    entry=await s.spool.get(second['operation_id']);c=entry.command;doc=json.loads(c.payload)
    assert entry.accepted and c.person_id==actor and doc['operation']['body']==operation().body
    assert doc['embedding']==[1.]+[0.]*1023 and c.schema_revision==DOMAIN_REVISIONS.schema
    assert t.queue.result(actor,rogue)['state']=='pending'
    before=len(s.counts)
    again=await s.workflow.mutate(t.principal,**args)
    assert again['operation_id']==second['operation_id'] and len(s.counts)==before
    with pytest.raises(ToolError,match='idempotency_conflict'):
        await s.workflow.mutate(t.principal,**dict(args,operation=operation(body='Changed exact body.')))
    from regional_knowledge.kaggle_source_commands import receipt_wire
    w=await t.queue.next_channel_work(t.run,t.token,t.generation);t.admission.mark_work_sent(w)
    assert w.document_payload['kind']=='source_command'
    inner=canonical(dict(story_id=STORY,revision=2,variant_id=operation().variant_id,variant_revision=2,
        body_sha256=digest(operation().body.encode())))
    outcome=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=c.operation_key,command_sha256=entry.command_sha256,
        cursor=c.sequence,status='applied',ids=dict(c.ids),request_id=c.request_id,person_id=c.person_id,
        route=c.route,logical_time=c.logical_time,before=list(DOMAIN_REVISIONS.wire()),after=list(DOMAIN_REVISIONS.wire()),
        handler_sha256=entry.handler_sha256,result_b64=base64.b64encode(inner).decode(),result_sha256=digest(inner)))
    native=ApplyReceipt('synthetic-db',c.operation_key,c.sequence,entry.command_sha256,'applied',outcome,digest(outcome),False)
    await release(t,w)
    entered,finish=asyncio.Event(),asyncio.Event();publish=s.spool.publish_outcome
    async def blocked(*args,**kwargs):entered.set();await finish.wait();return await publish(*args,**kwargs)
    s.spool.publish_outcome=blocked
    response=dict(type='SOURCE_COMMAND_RESULT',protocol_version=2,connection_generation=t.generation,
        job_id=w.document_payload['id'],claim=w.document_payload['claim'],receipt=receipt_wire(native),run_id=t.run,
        launch_nonce=t.nonce,runtime_incarnation='incarnation-a',compute_claim=w.compute_claim,dispatch_generation=w.dispatch_generation)
    assert await s.startup.source_dispatch.receive(response,run_id=t.run,token=t.token,generation=t.generation) is None
    await asyncio.wait_for(entered.wait(),2)
    assert (await s.workflow.mutate(t.principal,**args))['status']=='processing'
    finish.set()
    for _ in range(100):
        ack=await s.startup.source_dispatch.poll_settlement(run_id=t.run,token=t.token,generation=t.generation)
        if ack:break
        await asyncio.sleep(.01)
    assert ack['status']=='accepted'
    final=await s.workflow.mutate(t.principal,**args)
    assert final['status']=='completed' and final['remote_outcome_durable'] and final['variant_revision']==2
    assert final['body_sha256']==digest(operation().body.encode()) and len(s.counts)==before
    assert (await s.spool.get(c.operation_key)).command.encode()==c.encode()



@pytest.mark.asyncio
async def test_native_viewer_is_denied_before_tokenization_or_any_document_job(editorial):
    s=editorial
    task=asyncio.create_task(s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
        idempotency_key='viewer-denied',operation=operation()))
    with pytest.raises(ToolError,match='access_denied'):await context(s,task,role='viewer')
    assert not s.counts
    with s.t.queue.connect(write=False) as db:assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0


@pytest.mark.asyncio
async def test_only_manager_can_stage_exact_access_command_without_bge(editorial):
    s=editorial
    args=dict(story_id=STORY,expected_revision=1,idempotency_key='grant-editor-same',
        grantee_user_id=s.t.auth.p.owner.subject,capability='editor')
    denied=asyncio.create_task(s.workflow.mutate(s.t.principal,**args))
    with pytest.raises(ToolError,match='access_denied'):await context(s,denied,role='editor')
    assert not s.counts
    value=await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,**args)),role='manager')
    entry=await s.spool.get(value['operation_id'])
    assert entry.command.route=='story.access.v1' and entry.accepted
    assert json.loads(entry.command.payload)['grantee_user_id']==s.t.auth.p.owner.subject
    assert not s.counts and not s.workflow._documents


@pytest.mark.asyncio
async def test_copied_invalid_variant_never_counts_or_enqueues(editorial):
    s=editorial
    malformed=operation().model_copy(update={'body':'x'*4001})
    with pytest.raises(ToolError,match='invalid_input'):
        await s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
            idempotency_key='bad-copied-variant',operation=malformed)
    assert not s.counts
    with s.t.queue.connect(write=False) as db:assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0


@pytest.mark.asyncio
async def test_expired_source_proof_cannot_dispatch_retained_story_document(editorial):
    s=editorial
    await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
        idempotency_key='expire-before-document',operation=operation())))
    job=next(iter(s.workflow._documents))
    s.spool.invalidate_source_history()
    with pytest.raises(PermissionError):await s.workflow.authorize_document(job)
    assert await s.t.queue.next_channel_work(s.t.run,s.t.token,s.t.generation) is None
    s.workflow._release_document(job)
    assert not s.workflow._documents and job not in s.t.admission._editorial_documents


@pytest.mark.asyncio
async def test_existing_mcp_story_edit_routes_through_native_context_and_same_document_guard(editorial,monkeypatch):
    from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
    from regional_knowledge.server import build_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from starlette.requests import Request
    from test_kaggle_service_startup import HistoryAdapter
    s=editorial;t=s.t
    startup=TargetServiceStartup(TargetServiceConfig(t.auth.provider,t.auth.p.registry,t.queue,s.spool,
        HistoryAdapter(),'writer-1',lambda **v:v['database_generation']=='synthetic-db',
        destination_ref=s.workflow.destination.destination_ref))
    # Retain the tested workflow object; native original acquisition is outside
    # this isolated editorial entry-point test.
    startup.editorial=s.workflow;s.workflow.startup=startup;s.workflow.gate=startup.gate
    startup.routes.update({'story.edit':startup.backend.story_edit,'story.access':startup.backend.story_access})
    startup.binding=current_binding(t.queue);startup.coverage=s.spool._source_anchor;startup.running=True
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:t.principal)
    monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy fallback'))
    server=build_server(target_startup=startup)
    tools={tool.name for tool in server._tool_manager.list_tools()}
    assert {'story_edit','story_access','story_get','story_history','story_variants'}<=tools
    req=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
        'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
    ctx=SimpleNamespace(request_context=SimpleNamespace(request=req,request_id='story-edit-mcp-fixture'))
    task=asyncio.create_task(server.call_tool('story_edit',dict(story_id=STORY,expected_revision=1,
        operations=[operation().model_dump(mode='json')],idempotency_key='actual-mcp-story-edit'),ctx))
    output=await context(s,task)
    assert 'exact_narrative_vector_pending' in str(output)
    issued=req.scope[_SCOPE_KEY][1]
    assert issued.gate is s.workflow.output_gate and issued.gate.issuance_current(issued.gated.ticket)
    issued.release()
    work_item=await t.queue.next_channel_work(t.run,t.token,t.generation)
    assert work_item.kind=='document' and tuple(work_item.document_payload['texts'])==(operation().body,)
    t.admission.mark_work_sent(work_item);await release(t,work_item)


@pytest.mark.asyncio
async def test_slow_exact_counter_uses_new_proof_without_extending_original_deadline(editorial):
    import time
    s=editorial;now=[time.monotonic()];s.gate.clock=lambda:now[0]
    proofs=[];preflight=s.workflow._preflight
    async def observe(*args):
        proof=await preflight(*args);proofs.append(proof);return proof
    s.workflow._preflight=observe
    async def slow_counter(texts):
        assert len(proofs)==1
        original=proofs[0]['gated'].ticket
        now[0]+=45
        assert not s.gate.issuance_current(original)
        s.counts.append(tuple(texts));return [10]
    s.workflow.tokenizer.counts=slow_counter
    result=await context(s,asyncio.create_task(s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
        idempotency_key='slow-protected-counter',operation=operation())))
    assert result['reason']=='exact_narrative_vector_pending' and len(proofs)==2
    assert not s.gate.issuance_current(proofs[0]['gated'].ticket)
    assert s.gate.issuance_current(proofs[1]['gated'].ticket)
    document=await s.t.queue.next_channel_work(s.t.run,s.t.token,s.t.generation)
    assert document.kind=='document' and tuple(document.document_payload['texts'])==(operation().body,)


@pytest.mark.asyncio
async def test_fresh_post_count_preflight_detects_concurrent_story_revision_before_document(editorial):
    s=editorial
    task=asyncio.create_task(s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
        idempotency_key='changed-after-count',operation=operation()))
    with pytest.raises(ToolError,match='revision_conflict'):
        await context(s,task,second_revision=2)
    assert s.counts==[(operation().body,)] and not s.workflow._documents
    with s.t.queue.connect(write=False) as db:assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0
