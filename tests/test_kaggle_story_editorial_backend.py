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
from regional_knowledge.source_read_gate import SourceReadGate,SourceGateExpired
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
            value=json.loads(raw);value.update(schema_version=11)
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


async def context(s,task,role='editor',*,second_role=None,second_revision=None,first_revision=None):
    async def respond(role,revision=None):
        w=await work(s.t)
        assert type(w.query) is EditorialReadRequest and w.query.view=='edit_context'
        r=record();r.update(view='edit_context',role=role,variant_refs=[r['default_variant_ref']])
        if revision is not None:r.update(revision=revision,current_revision=revision)
        result=EditorialReadResult(request_id=w.query.request_id,connection_generation=s.t.generation,turn_generation=0,status='ok',
            record=r,evidence=(story_item(r),),observed_data_seq=5,observed_projection_seq=5,observed_visibility_epoch=1,observed_database_generation='synthetic-db')
        assert await s.t.admission.finish_read(s.t.run,s.t.token,s.t.generation,result)
        await release(s.t,w)
    await respond(role,first_revision)
    # Real OAuth/spool operations can outlive the old300ms poll on a busy CI
    # runner. Observe the actual task/admission events; do not assume the
    # first reply is a causal barrier for journal acceptance or next preflight.
    deadline=asyncio.get_running_loop().time()+3
    active=[True]
    def completed(_):
        if active[0]:s.t.admission.wake()
    task.add_done_callback(completed)
    try:
        while asyncio.get_running_loop().time()<deadline:
            if task.done():return await task
            if s.t.admission._reads:
                await respond(second_role or role,second_revision)
                return await asyncio.wait_for(asyncio.shield(task),
                    max(.001,deadline-asyncio.get_running_loop().time()))
            await s.t.admission.wait_channel_work(min(.1,
                max(0,deadline-asyncio.get_running_loop().time())))
        pytest.fail('editorial task neither completed nor acquired fresh preflight')
    finally:
        active[0]=False
        task.remove_done_callback(completed)


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
@pytest.mark.parametrize('phase',['binding','dispatch_binding','reply_binding','expired_permit'])
async def test_expired_editorial_runtime_is_unavailable_not_access_denied(editorial,monkeypatch,caplog,phase):
    s=editorial
    if phase=='binding':
        monkeypatch.setattr(s.startup,'binding_current',lambda binding:False)
    elif phase=='dispatch_binding':
        monkeypatch.setattr(s.gate,'binding_current',lambda binding:False)
    else:
        finish=s.gate.finish;now=s.gate.clock()
        async def expire(permit,result):
            if phase=='expired_permit':monkeypatch.setattr(s.gate,'clock',lambda:now+3)
            else:monkeypatch.setattr(s.gate,'binding_current',lambda binding:False)
            return await finish(permit,result)
        monkeypatch.setattr(s.gate,'finish',expire)
    task=asyncio.create_task(s.workflow.mutate(s.t.principal,story_id=STORY,expected_revision=1,
        idempotency_key='expired-runtime',operation=operation()))
    with pytest.raises(ToolError) as caught:
        if phase in ('binding','dispatch_binding'):await task
        else:await context(s,task)
    assert json.loads(str(caught.value))==dict(status='unavailable',reason='source_context_changed')
    assert 'story_editorial_failure' in caplog.text and 'error_site=' in caplog.text
    assert operation().body not in caplog.text
    assert not s.counts
    with s.t.queue.connect(write=False) as db:assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==0


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
    with pytest.raises(SourceGateExpired):await s.workflow.authorize_document(job)
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
    from pydantic import ValidationError
    for operations in ([dict(op='set_metadata',title='Corrected title')],
                       [operation().model_dump(mode='json')]*2):
        with pytest.raises(ToolError) as rejected:
            await server.call_tool('story_edit',dict(story_id=STORY,expected_revision=1,
                operations=operations,idempotency_key='unsupported-mcp-story-edit'),ctx)
        assert isinstance(rejected.value.__cause__,ValidationError), 'unsupported native input must fail SDK validation before dispatch'
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


@pytest.mark.asyncio
@pytest.mark.parametrize('applied', [False, True])
async def test_story_edit_waits_for_packet_apply_but_not_dataset_checkpoint(editorial, applied):
    from regional_knowledge.source_batch import resolved_command, CODEC
    from regional_knowledge.kaggle_native_source_bindings import native_source_handler
    from test_control_spool import receipt
    from test_source_stage import INGESTION, REQUEST
    s=editorial;t=s.t
    with t.queue.connect() as db:
        row=db.execute('SELECT expected_json,observed_json FROM bge_runtime_expectations WHERE run_id=?',(t.run,)).fetchone()
        values=[]
        for raw in row:
            value=json.loads(raw);value.update(schema_version=14)
            values.append(json.dumps(value))
        db.execute('UPDATE bge_runtime_expectations SET expected_json=?,observed_json=? WHERE run_id=?',(*values,t.run))
    s.spool.registry=HandlerRegistry([
        native_source_handler(route,check_sha256='a'*64,apply_sha256='b'*64)
        for route in ('source.activate.v1','story.edit.v1','story.access.v1','source.withdraw.v1','source.batch.v1')
    ])
    packet=resolved_command(source_id=record()['source']['source_id'],
        payload=dict(codec=CODEC,ingestion_id=INGESTION,batch_index=0,batch_count=1,
            last_batch_sha256='a'*64,materializer_sha256='f'*64),
        sequence=1,operation_key='pending-dataset-packet',person_id=t.auth.p.owner.subject,
        request_id=REQUEST,logical_time='2026-10-25T12:00:00Z')
    frozen,data,selected=s.spool._validate(packet,'synthetic-db')
    await s.spool._write(s.spool._stage,frozen,data,selected,'synthetic-db')
    await s.spool.seal(t.auth.p.admin,packet.operation_key,
        predecessor=None,writer_generation='writer-1')
    if applied:
        await s.spool.record_apply(packet.operation_key,receipt(packet,database='synthetic-db'))
    assert (await s.spool.scope_state())['pending']
    assert (await s.spool.read_facts(None)).pending is not applied
    assert (await s.spool.get(packet.operation_key)).outcome_state=='NOT_SENT'

    args=dict(story_id=STORY,expected_revision=1,idempotency_key='edit-after-packet',operation=operation())
    first=await context(s,asyncio.create_task(s.workflow.mutate(t.principal,**args)))
    assert first['reason']=='exact_narrative_vector_pending'
    vector=await t.queue.next_channel_work(t.run,t.token,t.generation)
    assert vector.kind=='document'
    t.admission.mark_work_sent(vector);payload=vector.document_payload
    assert t.queue.complete(t.run,t.token,payload['id'],payload['claim'],SPACE,
        [[1.]+[0.]*1023],{},connection_generation=t.generation)=='accepted'
    await release(t,vector)
    edit=asyncio.create_task(s.workflow.mutate(t.principal,**args))
    if not applied:
        with pytest.raises(ToolError,match='prior_write_pending'):
            await context(s,edit)
    else:
        result=await context(s,edit)
        entry=await s.spool.get(result['operation_id'])
        assert entry.accepted and entry.command.route=='story.edit.v1'
        assert entry.command.sequence==packet.sequence+1
        assert (await s.spool.get(packet.operation_key)).outcome_state=='NOT_SENT'

@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['applied_not_saved','apply_unknown','writer_changed','head_rewritten','grant_revoked'])
async def test_next_story_edit_requires_current_accepted_applied_prefix_not_dataset_save(editorial,phase):
    """Real SQLite spool + conditional journal + Dataset cursor; no live PG or BGE."""
    from regional_knowledge.kaggle_domain_bindings import resolved_current_story_edit_command
    from test_control_spool import receipt
    s=editorial;t=s.t;binding=current_binding(t.queue)
    source=record()['source']
    first=resolved_current_story_edit_command(
        sequence=1,operation_key='previous-story-edit',request_id='previous-editorial',
        person_id=t.auth.p.other.subject,logical_time='2026-10-30T10:00:00Z',
        source_id=source['source_id'],story_id=STORY,expected_revision=1,
        source_revision=source['content_revision'],eligibility_revision=source['eligibility_revision'],
        operation=operation(),embedding=[1.]+[0.]*1023,input_tokens=10)
    await s.spool.stage_story_editorial(t.auth.p.personal,first,database_generation='synthetic-db')
    await s.spool.seal(t.auth.p.personal,first.operation_key,
        predecessor=None,writer_generation='writer-1')
    if phase!='apply_unknown':
        await s.spool.record_apply(first.operation_key,receipt(first,database='synthetic-db'))
    else:
        await s.spool.mark_apply_sent(first.operation_key)
    original=await s.spool.get(first.operation_key)
    assert original.accepted is not None and original.outcome_state=='NOT_SENT'
    assert (await s.spool.scope_state())['pending']
    assert (await s.spool.read_facts(None)).pending
    current=[True]
    s.startup.coverage=await s.spool.dataset_cursor(binding,control=dict(sequence=0,heads=[]),
        current=lambda candidate:current[0] and candidate==binding)
    assert s.startup.coverage.accepted_sequence==1
    assert s.startup.coverage.applied_sequence==(0 if phase=='apply_unknown' else 1)
    args=dict(story_id=STORY,expected_revision=2,idempotency_key='following-edit',
        operation=operation(variant_id=None,expected_variant_revision=None))
    first_call=await context(s,asyncio.create_task(s.workflow.mutate(t.principal,**args)),
        first_revision=2,second_revision=2)
    assert first_call['reason']=='exact_narrative_vector_pending'
    w=await t.queue.next_channel_work(t.run,t.token,t.generation)
    assert w.kind=='document'
    t.admission.mark_work_sent(w);payload=w.document_payload
    assert t.queue.complete(t.run,t.token,payload['id'],payload['claim'],SPACE,
        [[1.]+[0.]*1023],{},connection_generation=t.generation)=='accepted'
    await release(t,w)
    if phase=='writer_changed':current[0]=False
    if phase=='head_rewritten':
        store=s.spool.journal.store;head_key=s.spool.journal.head_key
        store.write(head_key,store.objects[head_key].data)  # same manifest, changed CAS token
    attempt=asyncio.create_task(s.workflow.mutate(t.principal,**args))
    if phase!='applied_not_saved':
        with pytest.raises((ToolError,PermissionError),match='prior_write_pending|source_context_changed|access_denied'):
            await context(s,attempt,first_revision=2,second_revision=2,
                second_role='viewer' if phase=='grant_revoked' else None)
        assert (await s.spool.scope_state())['sequence']==1
        return
    outcome=await context(s,attempt,first_revision=2,second_revision=2)
    second=await s.spool.get(outcome['operation_id'])
    assert second.command.sequence==2 and second.accepted is not None
    assert second.command.route=='story.edit.v1'
    assert second.command.ids['story_id']==STORY
    assert second.outcome_state=='NOT_SENT' and not outcome['remote_outcome_durable']
    assert (await s.spool.get(first.operation_key)).outcome_state=='NOT_SENT', 'Saved requires remote receipt'
    again=await s.workflow.mutate(t.principal,**args)
    assert again['operation_id']==outcome['operation_id']
    assert (await s.spool.scope_state())['sequence']==2
