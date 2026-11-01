"""Real canonical auth/spool/journal/dispatch, synthetic native reply only."""
import asyncio
from types import SimpleNamespace

import pytest
from regional_knowledge.kaggle_cases_backend import KaggleCasesBackend
from regional_knowledge.kaggle_source_commands import AcceptedSourceDispatch
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.disagreement_native import DEPENDENCIES
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.disagreement_contracts import CaseCreate
from test_case_read_gate import setup,header,result,command
from test_control_spool import env
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people,change
from test_disagreement_native import CASE,create


async def backend(e,a,monkeypatch,*,current11=False):
    for capability in ('case_contribute','personal_conclusion','maintain_shared_summary'):
        change(a.p,ParticipantGrant(capability=capability,granted=True,reason='Synthetic Case workflow'))
    gate,authority,b,anchor=await setup(e,a)
    a.resolver.release(authority)
    if current11:
        e.spool.registry=HandlerRegistry([current_handler(route,check_sha256='1'*64,apply_sha256='2'*64) for route in DEPENDENCIES])
    e.q.configure_shared_reads(a.resolver)
    bridge=AcceptedSourceDispatch(e.spool,binding_current=lambda **kwargs:True)
    startup=SimpleNamespace(config=SimpleNamespace(queue=e.q,spool=e.spool,participants=e.p.registry,writer_generation='writer-1'),
        authority=a.resolver,backend=SimpleNamespace(destination=a.destination),reads_current=lambda:True,
        binding_current=lambda current:current is b,source_dispatch=bridge)
    value=KaggleCasesBackend(startup,case_gate=gate)
    monkeypatch.setattr('regional_knowledge.kaggle_cases_backend.current_binding',lambda queue:b)
    principal=SimpleNamespace(access_token=a.tokens.access_token)
    return value,principal,b


@pytest.mark.asyncio
@pytest.mark.parametrize('current11',[False,True])
async def test_normal_create_remote_accepts_once_same_spool_and_queues_exact_native_command(env,auth,monkeypatch,current11):
    value,principal,b=await backend(env,auth,monkeypatch,current11=current11)
    with capture_read_output() as outputs:
        first=await value.disagreement_edit('create','stable-case-create',principal,command=create())
        assert first['status']=='pending' and not first['saved'] and first['intent_remote_accepted']
        assert len(outputs)==1
        await outputs[0].gate.refresh(outputs[0].gated)
        assert outputs[0].gate.issuance_current(outputs[0].gated.ticket)
        outputs[0].release()
    stored=await env.spool.get(first['operation_key'])
    assert stored.command.person_id==auth.p.other.subject and stored.command.ids['case_id']==first['case_id']
    assert stored.command.schema_revision==(DOMAIN_REVISIONS.schema if current11 else 'rkb-v4-sql001-010-v1')
    assert b'case_read' not in stored.command.payload # capabilities never become caller payload grants
    with capture_read_output() as outputs:
        again=await value.disagreement_edit('create','stable-case-create',principal,command=create())
        assert again['command_sha256']==first['command_sha256'] and again['sequence']==first['sequence']
        outputs[0].release()
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==1
    changed=create().model_copy(update={'question':'A different synthetic question?'})
    with pytest.raises(Exception,match='operation_key_conflict'):
        await value.disagreement_edit('create','stable-case-create',principal,command=changed)


@pytest.mark.asyncio
async def test_operation_status_returns_durable_rejection_with_native_empty_result(env,auth,monkeypatch):
    import hashlib
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_case_contracts import canonical
    value,principal,binding=await backend(env,auth,monkeypatch)
    with capture_read_output() as outputs:
        accepted=await value.disagreement_edit('create','rejected-empty-result',principal,command=create())
        outputs[0].release()
    entry=await env.spool.get(accepted['operation_key']);c=entry.command
    body=canonical(dict(command_sha256=entry.command_sha256,person_id=c.person_id,
        status='rejected_precondition',result_b64=''))
    await env.spool.mark_apply_sent(c.operation_key)
    await env.spool.record_apply(c.operation_key,ApplyReceipt(entry.database_generation,c.operation_key,
        c.sequence,entry.command_sha256,'rejected_precondition',body,hashlib.sha256(body).hexdigest(),False))
    await env.spool.publish_outcome(c.operation_key,writer_generation='writer-1')
    with capture_read_output() as outputs:
        rejected=await value.disagreement_edit('operation_status',c.operation_key,principal,case_id=c.ids['case_id'])
        assert rejected['status']=='rejected' and rejected['saved'] is False and rejected['result'] is None
        assert rejected['outcome_durability']=='DURABLE' and rejected['sequence']==c.sequence
        await outputs[0].gate.refresh(outputs[0].gated)
        assert outputs[0].gate.issuance_current(outputs[0].gated.ticket)
        outputs[0].release()
    assert (await env.spool.get(c.operation_key)).apply_receipt.outcome==body
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==1
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
@pytest.mark.parametrize('native_status',['applied','rejected_precondition'])
async def test_verified_case_terminal_requests_early_dataset_saved_after_native_receipt(
        env,auth,monkeypatch,native_status):
    import base64
    import hashlib
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_case_contracts import canonical
    from regional_knowledge.kaggle_service_startup import TargetServiceStartup

    value,principal,binding=await backend(env,auth,monkeypatch)
    with capture_read_output() as outputs:
        accepted=await value.disagreement_edit('create','case-checkpoint-regression',
            principal,command=create())
        assert accepted['status']=='pending' and not accepted['saved']
        outputs[0].release()
    entry=await env.spool.get(accepted['operation_key'])
    assert entry.accepted is not None and entry.apply_state=='NOT_SENT'
    calls=[]
    startup=SimpleNamespace(config=SimpleNamespace(queue=env.q),
        dataset_checkpoints=SimpleNamespace(kick=lambda binding,*,planned_stop=False:
            calls.append((binding,planned_stop))))
    control={'sequence':0,'heads':[]}
    async def cursor():
        startup.coverage=await env.spool.dataset_cursor(binding,control=control,
            current=lambda candidate:candidate is binding)
        return startup.coverage
    await cursor()
    TargetServiceStartup._kick_dataset_checkpoint(startup,binding,dataset=True)
    assert calls==[(binding,False)]  # accepted is not native APPLIED

    await env.spool.mark_apply_sent(entry.command.operation_key)
    assert (await cursor()).pending  # UNKNOWN is not permission to force Saved
    TargetServiceStartup._kick_dataset_checkpoint(startup,binding,dataset=True)
    assert calls[-1]==(binding,False)
    command=entry.command
    outcome=canonical(dict(command_sha256=entry.command_sha256,person_id=command.person_id,
        status=native_status,result_b64=base64.b64encode(
            canonical(dict(case_id=command.ids['case_id'],object_id=command.ids['object_id']))).decode()))
    receipt=ApplyReceipt(entry.database_generation,command.operation_key,command.sequence,
        entry.command_sha256,native_status,outcome,hashlib.sha256(outcome).hexdigest(),False)
    await env.spool.record_apply(command.operation_key,receipt)
    applied=await env.spool.get(command.operation_key)
    assert applied.outcome_state=='NOT_SENT' and applied.remote_outcome is None
    assert applied.apply_state==('APPLIED' if native_status=='applied' else 'REJECTED_PRECONDITION')
    assert (await cursor()).applied_sequence==command.sequence
    TargetServiceStartup._kick_dataset_checkpoint(startup,binding,dataset=True)
    assert calls[-1]==(binding,True)  # terminal APPLIED or REJECTED_PRECONDITION
    # Even with a terminal Case, the history-only path keeps its old cadence.
    TargetServiceStartup._kick_dataset_checkpoint(startup,binding,dataset=False)
    assert calls[-1]==(binding,False)
    await env.spool.publish_outcome(command.operation_key,writer_generation='writer-1')
    assert (await env.spool.get(command.operation_key)).outcome_state=='DURABLE'
    await cursor()
    TargetServiceStartup._kick_dataset_checkpoint(startup,binding,dataset=True)
    assert calls[-1]==(binding,False)  # no repeat after exact durable outcome


@pytest.mark.asyncio
async def test_case_evidence_after_dataset_restore_uses_native_role_and_exact_local_suffix(
        env,auth,monkeypatch):
    import base64
    import hashlib
    from uuid import UUID
    from regional_knowledge.control_spool import SourceCursor
    from regional_knowledge.disagreement_contracts import EvidenceAdd
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_case_contracts import CaseReadRequest,CaseReadResult,canonical
    from regional_knowledge.kaggle_protocol import TrustedActorContext
    from test_disagreement_native import SOURCE,PAGE,REGION

    value,principal,binding=await backend(env,auth,monkeypatch,current11=True)
    async def commit_native(output):
        entry=await env.spool.get(output['operation_key']);command=entry.command
        raw=canonical(dict(case_id=command.ids['case_id'],object_id=command.ids['object_id']))
        body=canonical(dict(command_sha256=entry.command_sha256,person_id=command.person_id,
            status='applied',result_b64=base64.b64encode(raw).decode()))
        receipt=ApplyReceipt('db-source',command.operation_key,command.sequence,
            entry.command_sha256,'applied',body,hashlib.sha256(body).hexdigest(),False)
        await env.spool.record_apply(command.operation_key,receipt)
        await env.spool.publish_outcome(command.operation_key,writer_generation='writer-1')
        return command
    with capture_read_output() as outputs:
        created=await value.disagreement_edit('create','case-base-rev1',principal,command=create())
        outputs[0].release()
    original=await commit_native(created)
    case_id=original.ids['case_id']
    live=[True]
    control=dict(sequence=1,heads=[])
    async def restore():
        cursor=await env.spool.dataset_cursor(binding,control=control,
            current=lambda candidate:live[0] and candidate is binding)
        value.startup.coverage=cursor
        value.startup.binding=binding
        assert type(cursor) is SourceCursor and cursor.current()
        return cursor
    first=await restore()
    assert first.applied_sequence==first.accepted_sequence==1
    assert not hasattr(first,'case_state')
    # An unrelated accepted/applied command is retained as the local suffix,
    # not installed as another synthetic CaseHistory projection.
    with capture_read_output() as outputs:
        next_page=await value.disagreement_edit('create','next-accepted-local-suffix',
            principal,command=create())
        outputs[0].release()
    await commit_native(next_page)
    current=await restore()
    assert current.applied_sequence==current.accepted_sequence==2 and not current.pending
    assert not hasattr(current,'case_state')

    candidate=dict(source_id=SOURCE,content_revision=1,eligibility_revision=1)
    roles=[4]
    observations=[]
    async def native(authority,b,*,case_id,section,candidate_source_id=None,**unused):
        assert section=='edit_context' and case_id==original.ids['case_id']
        assert candidate_source_id==SOURCE
        request=CaseReadRequest(request_id='synthetic-edit-context',connection_generation=b.connection_generation,
            turn_generation=0,case_id=case_id,section='edit_context',candidate_source_id=candidate_source_id,
            actor=TrustedActorContext(person_id=authority.person_id,audience=value.destination.key,
                auth_epoch=authority.auth_revision,visibility_epoch=0),min_data_seq=0,deadline_ms=2000)
        permit=await value.gate.dispatch(authority,request,b)
        record=dict(case_id=case_id,case_revision=1,acl_revision=1,event_seq=1,case_role=roles[0],
            section='edit_context',source_dependencies=[],candidate_dependency=candidate)
        reply=CaseReadResult(request_id=request.request_id,connection_generation=request.connection_generation,
            turn_generation=request.turn_generation,status='ok',record_b64=base64.b64encode(canonical(record)).decode(),
            observed_data_seq=current.applied_sequence,observed_projection_seq=current.applied_sequence,
            observed_visibility_epoch=1,observed_database_generation='db-source')
        gated=await value.gate.finish(permit,reply)
        assert gated.result.status=='ok'
        observations.append(gated.ticket)
        return gated
    monkeypatch.setattr(value,'_native',native)
    evidence=EvidenceAdd(document_id=UUID(SOURCE),source_revision=1,source_sha256='a'*64,
        page_id=UUID(PAGE),region_id=UUID(REGION),text_start=0,quote='Synthetic exact source quote.',
        context='Synthetic original source line.',position_ids=['phase-one'],relation='supports',
        speaker='Archive author',attribution='direct_statement')
    with capture_read_output() as outputs:
        added=await value.disagreement_edit('add_evidence','evidence-after-dataset-restore',
            principal,case_id=case_id,command=evidence)
        assert added['status']=='pending' and added['intent_remote_accepted'] and not added['saved']
        assert added['sequence']==3
        held=outputs[0]
        assert held.gate is value.output_gate and held.gate.issuance_current(held.gated.ticket)
        await held.gate.refresh(held.gated)
        assert held.gate.issuance_current(held.gated.ticket)
        env.spool._source_version=object()
        assert not held.gate.issuance_current(held.gated.ticket)
        held.release()
    accepted=await env.spool.get(added['operation_key'])
    assert accepted.seal_state=='ACCEPTED' and accepted.apply_state=='NOT_SENT'
    assert accepted.command.ids['case_id']==case_id
    assert len(observations)>=2

    roles[0]=1  # a verified PG viewer is not a Case contributor
    with pytest.raises(Exception,match='access_denied'):
        await value.disagreement_edit('add_evidence','viewer-cannot-append-evidence',
            principal,case_id=case_id,command=evidence)
    live[0]=False
    roles[0]=4
    with pytest.raises(Exception,match='case_history_unavailable|prior_write_pending'):
        await value.disagreement_edit('add_evidence','stale-dataset-cannot-append-evidence',
            principal,case_id=case_id,command=evidence)
    live[0]=True
    await env.spool.mark_apply_sent(added['operation_key'])
    with pytest.raises(Exception,match='prior_write_pending'):
        await value.disagreement_edit('add_evidence','unknown-apply-cannot-append-evidence',
            principal,case_id=case_id,command=evidence)
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM rkb_control_operations").fetchone()[0]==3


@pytest.mark.asyncio
async def test_case_create_does_not_require_unrelated_next_page_history_refresh(env,auth,monkeypatch):
    value,principal,b=await backend(env,auth,monkeypatch)
    # Fresh PG binding and authenticated Case actor, but unrelated source
    # history maintenance is stale while another model page is underway.
    value.startup.reads_current=lambda:False
    with capture_read_output() as outputs:
        first=await value.disagreement_edit('create','case-during-next-page',
            principal,command=create())
        assert first['status']=='pending' and first['saved'] is False
        assert len(outputs)==1
        outputs[0].release()
    with capture_read_output() as outputs:
        again=await value.disagreement_edit('create','case-during-next-page',
            principal,command=create())
        assert again['operation_key']==first['operation_key']
        assert again['status']=='pending' and not again['saved']
        outputs[0].release()
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_case_write_after_accepted_unfinished_other_page_keeps_canonical_predecessor(
        env,auth,monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError
    value,principal,b=await backend(env,auth,monkeypatch)
    original=env.spool.scope_state
    mode={"unknown":False}

    async def accepted_other_page_state(*args,**kwargs):
        scope=await original(*args,**kwargs)
        # Existing journal/accepted_sequence remain exact. The unrelated
        # packet has an outcome not yet durable; no false Saved is inferred.
        return {**scope,"pending":True,"pending_count":1,
                "unknown":mode["unknown"],"unknown_count":int(mode["unknown"])}

    monkeypatch.setattr(env.spool,"scope_state",accepted_other_page_state)
    with capture_read_output() as output:
        accepted=await value.disagreement_edit('create','case-after-accepted-page',
            principal,command=create())
        assert accepted["status"]=="pending" and not accepted["saved"]
        output[0].release()
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1

    mode["unknown"]=True
    with pytest.raises(ToolError,match='prior_write_pending'):
        await value.disagreement_edit('create','case-while-unknown-source',
            principal,command=create())
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_read_retains_current_gate_until_actual_public_output_and_checks_revocation(env,auth,monkeypatch):
    value,principal,b=await backend(env,auth,monkeypatch)
    async def native(authority,binding,**selector):
        from test_case_read_gate import request
        permit=await value.gate.dispatch(authority,request(),binding)
        return await value.gate.finish(permit,result(header()))
    monkeypatch.setattr(value,'_native',native)
    with capture_read_output() as outputs:
        output=await value.disagreement_get(CASE,principal)
        assert output['case_id']==CASE and output['status']=='ok' and len(outputs)==1
        change(auth.p,ParticipantGrant(capability='case_read',granted=False,reason='Synthetic revoked before output'))
        with pytest.raises(PermissionError):await outputs[0].gate.refresh(outputs[0].gated)
        outputs[0].release()


@pytest.mark.parametrize('incorrect_binding',[False,True])
@pytest.mark.asyncio
async def test_case_contribution_review_projection_follows_owner_unlink_without_changing_native_record(
        env,auth,monkeypatch,incorrect_binding):
    """The PG record stays immutable; only the current owner review marker changes."""
    from regional_knowledge.disagreement_contracts import ContributionAdd
    from regional_knowledge.participant_contracts import AccountUnlink
    from test_participants import bind,change
    from test_case_read_gate import result,header
    from test_disagreement_native import OBJECT

    value,principal,_=await backend(env,auth,monkeypatch)
    linked=bind(auth.p)
    actor=auth.p.other.subject
    contribution=ContributionAdd(kind='opinion',original_text='Exact sourced synthetic observation.')
    native_attribution=dict(person_id=actor,recorded_by=actor,route='expert',
        link_id=linked.link_id,link_revision=linked.link_revision,human_reading_confirmed=False)
    native_payload=dict(contribution.model_dump(mode='json'),attribution=native_attribution,
        human_reading_confirmed=False,withdrawn=False)
    native_item=dict(contribution_id=OBJECT,version=1,actor_id=actor,recorded_by=actor,
        event_seq=1,payload=native_payload,logical_time='2026-10-21T10:00:00Z')
    source=header()
    source.update(section='contributions',items=[native_item],has_more=False,next_after_seq=None)
    native=result(source)
    assert value._render_case(native)['items'][0]['attribution_review_required'] is False
    change(auth.p,AccountUnlink(link_id=linked.link_id,expected_link_revision=2,
        reason='Synthetic link correction',incorrect_binding=incorrect_binding))
    output=value._render_case(native)
    assert output['items'][0]['attribution_review_required'] is incorrect_binding
    assert output['items'][0]['payload']==native_payload
    assert native.record==source
    # Source identity and immutable native evidence have not been rewritten.
    with auth.p.corpus.connect() as db:
        row=db.execute('SELECT attribution_review_required FROM rkb_external_identities WHERE id=?',
                       (linked.link_id,)).fetchone()
        assert bool(row[0]) is incorrect_binding


@pytest.mark.asyncio
async def test_case_contribution_review_is_recomputed_at_final_output_gate(env,auth,monkeypatch):
    """An account correction after native read must be visible at MCP issuance."""
    from regional_knowledge.disagreement_contracts import ContributionAdd
    from regional_knowledge.participant_contracts import AccountUnlink,ParticipantUpsert
    from test_participants import bind,change
    from test_case_read_gate import request,result,header
    from test_disagreement_native import OBJECT

    value,principal,binding=await backend(env,auth,monkeypatch)
    # The reader owns the Case, but this contribution belongs to another
    # verified participant. Changing the author's link must not invalidate
    # the reader's own OAuth context or leave a stale attribution label.
    auth.p.registry.change(auth.p.admin,auth.p.stranger.subject,0,
        ParticipantUpsert(name='Independent synthetic contributor',reason='Test policy projection'),
        'create-independent-contributor')
    expert=bind(auth.p,person=auth.p.stranger.subject,user='U222')
    actor=auth.p.stranger.subject
    source=header()
    source.update(section='contributions',items=[dict(
        contribution_id=OBJECT,version=1,actor_id=actor,recorded_by=actor,
        event_seq=1,logical_time='2026-10-21T10:00:00Z',
        payload=dict(ContributionAdd(kind='opinion',original_text='Immutable expert contribution.').model_dump(mode='json'),
            withdrawn=False,human_reading_confirmed=False,
            attribution=dict(person_id=actor,recorded_by=actor,route='expert',
                link_id=expert.link_id,link_revision=expert.link_revision,human_reading_confirmed=False)))],
        has_more=False,next_after_seq=None)
    immutable=result(source)
    before_bytes=immutable.record_b64

    async def native(authority,binding,**selector):
        permit=await value.gate.dispatch(authority,request(section='contributions'),binding)
        return await value.gate.finish(permit,immutable)
    monkeypatch.setattr(value,'_native',native)

    with capture_read_output() as outputs:
        before=await value.disagreement_get(CASE,principal,section='contributions')
        assert before['items'][0]['attribution_review_required'] is False
        assert len(outputs)==1
        call=outputs[0]
        # Same native result stays issued to the same user; policy changes now.
        change(auth.p,AccountUnlink(link_id=expert.link_id,expected_link_revision=2,
            reason='Wrong attribution on old link',incorrect_binding=True),person=actor)
        call.gated=await call.gate.refresh(call.gated)
        assert call.gate.issuance_current(call.gated.ticket)
        final=call.render(call.gated.result)
        assert final['items'][0]['attribution_review_required'] is True
        assert call.gated.result.record_b64==before_bytes
        call.release()


@pytest.mark.asyncio
async def test_operation_output_fence_closes_on_control_write_even_after_async_refresh(env,auth,monkeypatch):
    value,principal,b=await backend(env,auth,monkeypatch)
    with capture_read_output() as outputs:
        await value.disagreement_edit('create','fenced-create',principal,command=create())
        call=outputs[0];await call.gate.refresh(call.gated)
        assert call.gate.issuance_current(call.gated.ticket)
        env.spool._source_version=object()
        assert not call.gate.issuance_current(call.gated.ticket)
        call.release()


@pytest.mark.asyncio
async def test_normal_mcp_case_tools_use_typed_shared_read_and_accepted_dispatch(target,monkeypatch):
    from starlette.requests import Request
    from regional_knowledge.server import build_server
    from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
    from regional_knowledge.control_spool import ControlSpool
    from regional_knowledge.case_history import project_cases
    from regional_knowledge.kaggle_case_contracts import CaseReadRequest,CaseReadResult
    from regional_knowledge.kaggle_public_backend import current_binding
    from regional_knowledge.recovery_journal import RecoveryJournal
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from test_kaggle_service_startup import HistoryAdapter
    from test_recovery_journal import ConditionalMemoryStore
    from test_kaggle_public_backend import work,release
    from test_case_read_gate import command
    s=target
    change(s.auth.p,ParticipantGrant(capability='case_contribute',granted=True,reason='Synthetic MCP create'))
    registry=HandlerRegistry([current_handler(route,check_sha256='1'*64,apply_sha256='2'*64) for route in DEPENDENCIES])
    spool=ControlSpool(s.queue,s.auth.p.registry,RecoveryJournal(ConditionalMemoryStore(),stream='case-mcp-fixture'),registry)
    adapter=HistoryAdapter()
    startup=TargetServiceStartup(TargetServiceConfig(s.auth.provider,s.auth.p.registry,s.queue,spool,adapter,
        'writer-1',lambda **values:values['database_generation']=='synthetic-db'))
    first=command(s.auth.p.other.subject,'create',create())
    projection=project_cases([(first,None,None)],{1:'applied'})
    anchor=SimpleNamespace(database_generation='synthetic-db',current=lambda:True,case_state=projection,pending=())
    spool._source_anchor=anchor;startup.binding=current_binding(s.queue);startup.coverage=anchor;startup.running=True
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.principal)
    server=build_server(target_startup=startup)
    try:
        tools={t.name:t for t in server._tool_manager.list_tools()}
        assert {'disagreement_search','disagreement_get','disagreement_edit',
                'disagreement_contribute','disagreement_conclusion'}<=tools.keys()
        assert tools['disagreement_search'].annotations.read_only_hint
        assert tools['disagreement_get'].annotations.read_only_hint
        def request_context(identity):
            request=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
                'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
            return request,SimpleNamespace(request_context=SimpleNamespace(request=request,request_id=identity))
        request,context=request_context('case-mcp-read')
        task=asyncio.create_task(server.call_tool('disagreement_get',{'case_id':CASE},context))
        dispatched=await work(s)
        assert type(dispatched.query) is CaseReadRequest
        assert dispatched.query.actor.person_id==s.auth.p.other.subject
        answer=result(header()).model_copy(update=dict(request_id=dispatched.query.request_id,
            connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
            observed_database_generation='synthetic-db'))
        assert await s.admission.finish_read(s.run,s.token,s.generation,answer)
        output=await task
        assert CASE in str(output)
        issued=request.scope[_SCOPE_KEY][1]
        assert issued.gate is startup.case_gate and issued.gate.issuance_current(issued.gated.ticket)
        issued.release();await release(s,dispatched)
        # Actual registered MCP Case subject tool uses the same admitted
        # CASE_READ transport and actor authority, not a separate endpoint.
        request,context=request_context('case-mcp-search')
        task=asyncio.create_task(server.call_tool('disagreement_search',
            {'query':'Who founded the harbour?','place':'old town','limit':5},context))
        dispatched=await work(s)
        assert type(dispatched.query) is CaseReadRequest
        assert dispatched.query.section=='search'
        assert dispatched.query.actor.person_id==s.auth.p.other.subject
        assert dispatched.query.search_query=='Who founded the harbour?'
        import base64
        from regional_knowledge.kaggle_case_contracts import canonical
        case_record=dict(section='search',items=[dict(case_id=CASE,question='Who founded the harbour?',
            context='Exact attributed dispute',state='open',case_revision=1,acl_revision=1,event_seq=1,
            source_dependencies=[])],
            has_more=False,next_after_case=CASE)
        native=CaseReadResult(request_id=dispatched.query.request_id,
            connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
            status='ok',record_b64=base64.b64encode(canonical(case_record)).decode(),
            observed_data_seq=7,observed_projection_seq=7,observed_visibility_epoch=1,
            observed_database_generation='synthetic-db')
        assert await s.admission.finish_read(s.run,s.token,s.generation,native)
        output=await task
        assert 'Who founded the harbour?' in str(output) and CASE in str(output)
        issued=request.scope[_SCOPE_KEY][1]
        assert issued.gate is startup.case_gate and issued.gate.issuance_current(issued.gated.ticket)
        issued.release();await release(s,dispatched)
        request,context=request_context('case-mcp-create')
        output=await server.call_tool('disagreement_edit',dict(action='create',operation_key='normal-mcp-create',
            command=create().model_dump(mode='json')),context)
        assert 'pending' in str(output) and 'saved' in str(output)
        issued=request.scope[_SCOPE_KEY][1]
        assert issued.gate is startup.cases_backend.output_gate and issued.gate.issuance_current(issued.gated.ticket)
        issued.release()
        with s.queue.connect(write=False) as db:
            assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
    finally:
        await startup.source_dispatch.aclose();spool.close()


@pytest.mark.asyncio
async def test_contribution_private_draft_and_selected_publication_preserve_exact_resolved_inputs(env,auth,monkeypatch):
    import base64,hashlib,json
    from regional_knowledge.case_history import project_cases
    from regional_knowledge.disagreement_contracts import ContributionAdd,PersonalDraft,DraftPublication
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_case_contracts import canonical
    from test_disagreement_native import OBJECT
    from test_case_read_gate import request
    value,principal,b=await backend(env,auth,monkeypatch,current11=True)
    accepted=[]
    # Only the native observations/apply receipts are synthetic here. Real PG
    # tests independently verify source evidence, selected text and private ACL.
    async def native(authority,binding,**selector):
        state=env.spool._source_anchor.case_state.cases[CASE]
        q=request(section='edit_context');permit=await value.gate.dispatch(authority,q,binding)
        record=header();record.update(section='edit_context',case_revision=state['revision'],event_seq=state['event_seq'],candidate_dependency=None)
        return await value.gate.finish(permit,result(record))
    monkeypatch.setattr(value,'_native',native)
    # Seed an actually accepted Case create through this same public workflow.
    with capture_read_output() as calls:
        first=await value.disagreement_edit('create','workflow-create',principal,command=create())
        calls[0].release()
    async def settle(output):
        entry=await env.spool.get(output['operation_key']);c=entry.command
        outcome=canonical(dict(command_sha256=entry.command_sha256,person_id=c.person_id,status='applied',
            result_b64=base64.b64encode(canonical(dict(case_id=c.ids['case_id'],object_id=c.ids['object_id']))).decode()))
        receipt=ApplyReceipt('db-source',c.operation_key,c.sequence,entry.command_sha256,'applied',outcome,hashlib.sha256(outcome).hexdigest(),False)
        await env.spool.record_apply(c.operation_key,receipt)
        assert value._reply(await env.spool.get(c.operation_key))['saved'] is False
        await env.spool.publish_outcome(c.operation_key,writer_generation='writer-1')
        accepted.append((c,None,None))
        projection=project_cases(accepted,{x[0].sequence:'applied' for x in accepted})
        # Match this test's fixed requested Case ID to the server-created ID.
        return c,projection
    c,projection=await settle(first)
    actual_case=c.ids['case_id']
    # Use actual Case request/header IDs while retaining all canonical gates.
    async def native(authority,binding,**selector):
        state=env.spool._source_anchor.case_state.cases[actual_case]
        q=request(section='edit_context').model_copy(update={'case_id':actual_case})
        permit=await value.gate.dispatch(authority,q,binding)
        record=header();record.update(case_id=actual_case,section='edit_context',case_revision=state['revision'],
            event_seq=state['event_seq'],candidate_dependency=None)
        return await value.gate.finish(permit,result(record))
    monkeypatch.setattr(value,'_native',native)
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,case_state=projection,pending=())
    contribution=ContributionAdd(kind='argument',original_text='Exact independently authored argument.',interpretation='',
        evidence_ids=[],position_ids=['phase-one'],reply_to=None)
    with capture_read_output() as calls:
        output=await value.disagreement_contribute(actual_case,'workflow-contribution',contribution,principal)
        calls[0].release()
    c,projection=await settle(output)
    assert json.loads(c.payload)['command']['original_text']==contribution.original_text
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,case_state=projection,pending=())
    draft=PersonalDraft(text='PRIVATE PREFIX. Exact selected opinion. PRIVATE SUFFIX.',based_on_seq=2,
        evidence_ids=[],position_ids=['phase-one'])
    with capture_read_output() as calls:
        output=await value.disagreement_conclusion(actual_case,'draft_save',principal,operation_key='workflow-draft',
            command=draft,draft_id=OBJECT,expected_version=0)
        calls[0].release()
    c,projection=await settle(output)
    assert json.loads(c.payload)['command']['text']==draft.text
    assert projection.cases[actual_case]['event_seq']==2
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,case_state=projection,pending=())
    start=draft.text.index('Exact');end=start+len('Exact selected opinion.')
    selected=DraftPublication(text_start=start,text_end=end,kind='opinion',evidence_ids=[],position_ids=['phase-one'])
    with capture_read_output() as calls:
        output=await value.disagreement_conclusion(actual_case,'draft_publish',principal,operation_key='workflow-publish',
            command=selected,draft_id=OBJECT,expected_version=1)
        calls[0].release()
    c,projection=await settle(output)
    assert json.loads(c.payload)['command']==json.loads(selected.model_dump_json())
    assert c.ids['object_id']==OBJECT and c.ids['result_id']!=OBJECT
    assert b'PRIVATE PREFIX' not in c.payload and b'PRIVATE SUFFIX' not in c.payload
    assert projection.cases[actual_case]['drafts'][OBJECT]['version']==1
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,case_state=projection,pending=())
    with capture_read_output() as calls:
        final=await value.disagreement_edit('operation_status',output['operation_key'],principal)
        assert final['saved'] and final['status']=='saved' and final['outcome_durability']=='DURABLE'
        calls[0].release()


def test_viewer_private_draft_scope_matches_native_and_verified_history():
    from regional_knowledge.case_history import project_cases
    from regional_knowledge.disagreement_contracts import CaseGrant,PersonalDraft
    from regional_knowledge.case_read_gate import CaseReadGate
    from test_case_read_gate import command
    from test_disagreement_native import OBJECT
    from uuid import UUID
    owner='00000000-0000-0000-0000-000000000011'
    viewer='00000000-0000-0000-0000-000000000012'
    create_case=command(owner,'create',create())
    grant=command(owner,'grant',CaseGrant(grantee=UUID(viewer),capability='viewer',reason='Synthetic Case viewer'),seq=2,case_revision=1)
    draft=command(viewer,'draft_save',PersonalDraft(text='Own private analysis as an authorized viewer.',based_on_seq=2,
        evidence_ids=[],position_ids=['phase-one']),seq=3)
    projection=project_cases([(create_case,None,None),(grant,None,None)],{1:'applied',2:'applied'})
    anchor=SimpleNamespace(case_state=projection,current=lambda:True)
    spool=SimpleNamespace(_source_anchor=anchor)
    gate=object.__new__(CaseReadGate);gate.spool=spool
    backend=object.__new__(KaggleCasesBackend);backend.spool=spool;backend.gate=gate
    backend.destination=SimpleNamespace(key='mcp:personal:synthetic')
    authority=SimpleNamespace(person_id=viewer,auth_revision=1)
    backend._scope(authority,draft) # native draft_save requires viewer, not contributor
    after=project_cases([(draft,None,None)],{3:'applied'},projection)
    assert after.cases[CASE]['drafts'][OBJECT]==dict(actor=viewer,version=1)
    assert after.cases[CASE]['event_seq']==2 # private save cannot enter shared history
    # The same viewer still cannot publish a shared contribution.
    from regional_knowledge.disagreement_contracts import DraftPublication
    publication=command(viewer,'draft_publish',DraftPublication(text_start=0,text_end=3,kind='opinion',
        evidence_ids=[],position_ids=['phase-one']),seq=4,object_version=1)
    spool._source_anchor=SimpleNamespace(case_state=after,current=lambda:True)
    with pytest.raises(PermissionError):backend._scope(authority,publication)
    with pytest.raises(ValueError,match='role history'):project_cases([(publication,None,None)],{4:'applied'},after)


@pytest.mark.asyncio
async def test_grant_revoke_between_restriction_lookup_and_source_facts_cannot_become_output_witness(env,auth,monkeypatch):
    from regional_knowledge.case_history import project_cases
    from regional_knowledge.disagreement_contracts import CaseGrant,ContributionAdd
    from regional_knowledge.contracts import Principal
    from regional_knowledge.participants import RequestAuthority
    from test_case_read_gate import command
    from uuid import UUID
    value,principal,b=await backend(env,auth,monkeypatch)
    actor=auth.p.other.subject;owner=auth.p.owner.subject
    first=command(owner,'create',create())
    manager=command(owner,'grant',CaseGrant(grantee=UUID(actor),capability='manager',reason='Synthetic manager'),seq=2,case_revision=1)
    projection=project_cases([(first,None,None),(manager,None,None)],{1:'applied',2:'applied'})
    env.spool._source_anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,
        case_state=projection,pending=(),applied_sequence=2)
    authority=await auth.resolver.resolve_oauth(auth.tokens.access_token,destination=auth.destination)
    context=RequestAuthority(Principal(subject=actor,client_id=authority.client_id,issuer=authority.issuer,access_token=''),
        route=authority.route,link_id=authority.link_id,link_revision=authority.link_revision)
    operation=command(actor,'contribute',ContributionAdd(kind='opinion',original_text='Synthetic own opinion.',
        interpretation='',evidence_ids=[],position_ids=['phase-one'],reply_to=None),seq=3)
    revoke=command(actor,'grant',CaseGrant(grantee=UUID(actor),capability=None,reason='Revoke own manager role'),seq=4,case_revision=2)
    original=value.gate._facts;changed=False
    async def facts_after_revoke(*args):
        nonlocal changed
        assert not changed
        changed=True
        await env.spool.stage_domain_command(context,revoke,database_generation='db-source')
        return await original(*args)
    monkeypatch.setattr(value.gate,'_facts',facts_after_revoke)
    try:
        with pytest.raises(Exception,match='prior_write_pending'):
            await value._check(authority,context,b,operation)
        assert changed
    finally:auth.resolver.release(authority)


async def collected_operation(e,a,monkeypatch,*,remote=True):
    """Real durable journal + compact receipt, collected raw intent, reopened spool.

    Only the worker's native SQL receipt is synthetic; it crosses the actual
    Case gate. PG reader/receipt provenance is covered by native fixtures.
    """
    import base64,hashlib
    from regional_knowledge.control_spool import ControlSpool
    from regional_knowledge.case_history import project_cases
    from regional_knowledge.kaggle_apply import ApplyReceipt
    from regional_knowledge.kaggle_case_contracts import canonical
    value,principal,b=await backend(e,a,monkeypatch,current11=True)
    with capture_read_output() as calls:
        pending=await value.disagreement_edit('create','collected-create',principal,command=create())
        calls[0].release()
    entry=await e.spool.get(pending['operation_key']);c=entry.command
    native_result=dict(case_id=c.ids['case_id'],object_id=c.ids['object_id'],case_revision=1,event_seq=1,
        acl_revision=1,version=1,action='create')
    result_bytes=canonical(native_result)
    outcome=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=c.operation_key,command_sha256=entry.command_sha256,
        cursor=c.sequence,status='applied',ids=dict(c.ids),request_id=c.request_id,person_id=c.person_id,
        route=c.route,logical_time=c.logical_time,before=list(DOMAIN_REVISIONS.wire()),after=list(DOMAIN_REVISIONS.wire()),
        handler_sha256=entry.handler_sha256,result_b64=base64.b64encode(result_bytes).decode(),
        result_sha256=hashlib.sha256(result_bytes).hexdigest()))
    receipt=ApplyReceipt('db-source',c.operation_key,c.sequence,entry.command_sha256,'applied',outcome,hashlib.sha256(outcome).hexdigest(),False)
    await e.spool.record_apply(c.operation_key,receipt)
    if remote:await e.spool.publish_outcome(c.operation_key,writer_generation='writer-1')
    projection=project_cases([(c,None,None)],{c.sequence:'applied'})
    registry=e.spool.registry
    # Simulate the raw retention boundary only after known native result. No
    # production collector policy is changed by this fixture.
    with e.spool._db(write=True) as db:db.execute('DELETE FROM rkb_control_operations WHERE operation_key=?',(c.operation_key,))
    e.spool.close()
    e.spool=ControlSpool(e.q,e.p.registry,e.journal,registry)
    # Rebuild the backend, not a RAM receipt cache, after the real DB reopen.
    gate,authority,b,anchor=await setup(e,a)
    a.resolver.release(authority);e.spool.registry=registry
    anchor.case_state=projection;anchor.applied_sequence=c.sequence
    startup=value.startup;startup.config.spool=e.spool;startup.binding_current=lambda current:current is b
    value=KaggleCasesBackend(startup,case_gate=gate)
    monkeypatch.setattr('regional_knowledge.kaggle_cases_backend.current_binding',lambda queue:b)
    native_record=dict(case_id=c.ids['case_id'],source_dependencies=[],result=native_result,
        operation_key=c.operation_key,sequence=c.sequence,command_sha256=entry.command_sha256,
        request_id=c.request_id,state='applied',outcome_durability='native_only')
    async def native(authority,binding,**selector):
        from test_case_read_gate import request
        q=request(section='operation',operation_key=selector['operation_key']).model_copy(update={'case_id':selector['case_id']})
        permit=await value.gate.dispatch(authority,q,binding)
        # Real native function returns no receipt for another actor/key/Case.
        if (authority.person_id!=c.person_id or selector['operation_key']!=c.operation_key
                or selector['case_id']!=c.ids['case_id']):raise PermissionError('native receipt actor scope')
        gated=await value.gate.finish(permit,result(native_record))
        if gated.result.status!='ok':
            value.gate.release(gated.ticket)
            from regional_knowledge.kaggle_cases_backend import failure
            raise failure(gated.result.reason,gated.result.status)
        return gated
    monkeypatch.setattr(value,'_native',native)
    return value,principal,c,native_record


@pytest.mark.asyncio
async def test_collected_operation_status_reopens_queue_and_proves_remote_saved_without_effect(env,auth,monkeypatch):
    value,principal,c,native=await collected_operation(env,auth,monkeypatch)
    version=env.store.version
    with capture_read_output() as calls:
        recovered=await value.disagreement_edit('operation_status',c.operation_key,principal,case_id=c.ids['case_id'])
        assert recovered['saved'] and recovered['status']=='saved' and recovered['outcome_durability']=='DURABLE'
        assert recovered['result']==native['result'] and recovered['command_sha256']==native['command_sha256']
        assert len(calls)==1 and calls[0].gate is value.output_gate
        await calls[0].gate.refresh(calls[0].gated)
        assert calls[0].gate.issuance_current(calls[0].gated.ticket)
        calls[0].release()
    assert env.store.version==version # reads only: no remote CAS/publication
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM rkb_control_receipts').fetchone()[0]==1
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
    # The owner/client never saw case_id when the create response was lost.
    # The actual raw control operation has been collected: recovery must use
    # the same native PG outcome and the compact durable receipt, not the spool.
    with capture_read_output() as calls:
        recovered_without_id=await value.disagreement_edit('operation_status',c.operation_key,principal)
        assert recovered_without_id==recovered
        assert recovered_without_id['result']['case_id']==c.ids['case_id']
        assert len(calls)==1
        calls[0].release()
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['native_only','missing_remote','remote_mismatch','native_mismatch','command_mismatch','wrong_case','wrong_key','revoked_case','participant_revoke'])
async def test_collected_operation_recovery_never_infers_saved_from_unknown_or_wrong_scope(env,auth,monkeypatch,fault):
    import hashlib
    from regional_knowledge.kaggle_case_contracts import canonical
    value,principal,c,native=await collected_operation(env,auth,monkeypatch,remote=fault!='native_only')
    remote_key=env.journal.prefix+'/outcomes/'+hashlib.sha256(c.operation_key.encode()).hexdigest()
    key=c.operation_key;case_id=c.ids['case_id']
    if fault=='missing_remote':del env.store.objects[remote_key]
    if fault=='remote_mismatch':env.store.write(remote_key,b'{}')
    if fault=='command_mismatch':
        env.store.write(env.journal.prefix+'/commands/'+hashlib.sha256(key.encode()).hexdigest(),b'{}')
    if fault=='native_mismatch':native['result']={'case_id':case_id,'object_id':case_id,'action':'forged'}
    if fault=='wrong_case':case_id=CASE
    if fault=='wrong_key':key='different-original-operation'
    if fault=='revoked_case':
        data=env.spool._source_anchor.case_state.cases
        data[case_id]['owner']=auth.p.owner.subject
        env.spool._source_anchor.case_state=type(env.spool._source_anchor.case_state)(canonical(data))
    if fault=='participant_revoke':change(auth.p,ParticipantGrant(capability='case_read',granted=False,reason='Synthetic revoke'))
    version=env.store.version
    with capture_read_output() as calls:
        with pytest.raises(Exception):await value.disagreement_edit('operation_status',key,principal,case_id=case_id)
        assert not calls
    assert env.store.version==version
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==0


def test_collected_noncreate_retry_compares_cas_revision_and_draft_id_before_replay():
    # These envelopes are immutable accepted commands from the same native
    # constructors used for PG apply; the raw control row has been collected.
    # A matching payload does not excuse mismatched expected CAS versions/IDs.
    from uuid import UUID
    from regional_knowledge.disagreement_contracts import CaseRevise,PersonalDraft
    from test_disagreement_native import command as case_command,CASE,OBJECT
    revise=CaseRevise(question='Which date is correct?',context='Exact original context.',
        classification='candidate discrepancy',rationale='Synthetic reason.',reason='Review date.')
    historical=case_command('revise',revise,case_revision=1)
    key=historical.operation_key
    check=KaggleCasesBackend._verify_collected_retry
    check(historical,key,'revise',revise,CASE,OBJECT,1,0)
    with pytest.raises(Exception,match='operation_key_conflict'):
        check(historical,key,'revise',revise,CASE,OBJECT,2,0)
    with pytest.raises(Exception,match='operation_key_conflict'):
        check(historical,key,'revise',revise,CASE,'00000000-0000-0000-0000-000000000099',1,0)
    draft=PersonalDraft(text='Original private source-based argument.',based_on_seq=1,
        evidence_ids=[],position_ids=[])
    saved=case_command('draft_save',draft,object_version=1)
    key=saved.operation_key
    check(saved,key,'draft_save',draft,CASE,OBJECT,0,1)
    with pytest.raises(Exception,match='operation_key_conflict'):
        check(saved,key,'draft_save',draft,CASE,'00000000-0000-0000-0000-000000000099',0,1)
    with pytest.raises(Exception,match='operation_key_conflict'):
        check(saved,key,'draft_save',draft,CASE,OBJECT,0,2)
    assert UUID(CASE) != UUID(OBJECT)  # Explicitly distinct case and private draft identities.


@pytest.mark.asyncio
async def test_repeat_create_after_raw_gc_returns_exact_saved_case_without_new_effect(env,auth,monkeypatch):
    # The caller retries the original CREATE because the first response was lost.
    # The raw operation has really been deleted, leaving only compact receipt,
    # sealed remote command/outcome and native PG receipt.
    value,principal,c,native=await collected_operation(env,auth,monkeypatch)
    before=env.store.version
    with capture_read_output() as calls:
        recovered=await value.disagreement_edit('create','collected-create',principal,command=create())
        assert recovered['status']=='saved' and recovered['saved']
        assert recovered['result']==native['result']
        assert recovered['case_id']==c.ids['case_id']
        assert len(calls)==1
        calls[0].release()
    assert env.store.version==before  # No second remote intent/outcome write.
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM rkb_control_receipts').fetchone()[0]==1
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
    # Reusing the same idempotency key with different model-authored input
    # must not return the prior success as though it were the new request.
    mismatched=create().model_copy(update={'question':'Different synthetic event?'})
    with capture_read_output() as calls:
        with pytest.raises(Exception,match='operation_key_conflict'):
            await value.disagreement_edit('create','collected-create',principal,command=mismatched)
        assert not calls
    assert env.store.version==before


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['native_only','missing_remote','wrong_key'])
async def test_collected_create_without_case_id_never_infers_saved_from_unproven_outcome(env,auth,monkeypatch,fault):
    import hashlib
    value,principal,c,_=await collected_operation(env,auth,monkeypatch,remote=fault!='native_only')
    key=c.operation_key
    if fault=='missing_remote':
        remote_key=env.journal.prefix+'/outcomes/'+hashlib.sha256(key.encode()).hexdigest()
        del env.store.objects[remote_key]
    if fault=='wrong_key':key='not-the-original-create-operation'
    before=env.store.version
    with capture_read_output() as calls:
        with pytest.raises(Exception):
            await value.disagreement_edit('operation_status',key,principal)
        assert not calls
    assert env.store.version==before
    with env.q.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_collected_receipt_is_not_recoverable_by_global_owner_as_other_actor(env,auth,monkeypatch):
    from test_read_authority import issue
    value,principal,c,native=await collected_operation(env,auth,monkeypatch)
    owner_tokens=await issue(auth.provider)
    version=env.store.version
    with capture_read_output() as calls:
        with pytest.raises(Exception,match='access_denied'):
            await value.disagreement_edit('operation_status',c.operation_key,
                SimpleNamespace(access_token=owner_tokens.access_token),case_id=c.ids['case_id'])
        # Omitting case_id must not turn a global owner into the actual creator.
        with pytest.raises(Exception,match='access_denied'):
            await value.disagreement_edit('operation_status',c.operation_key,
                SimpleNamespace(access_token=owner_tokens.access_token))
        assert not calls
    assert env.store.version==version


@pytest.mark.asyncio
async def test_recovered_saved_receipt_stays_closed_after_case_revoke_at_final_output(env,auth,monkeypatch):
    from regional_knowledge.kaggle_case_contracts import canonical
    value,principal,c,native=await collected_operation(env,auth,monkeypatch)
    with capture_read_output() as calls:
        output=await value.disagreement_edit('operation_status',c.operation_key,principal,case_id=c.ids['case_id'])
        assert output['saved']
        data=env.spool._source_anchor.case_state.cases
        data[c.ids['case_id']]['owner']=auth.p.owner.subject
        env.spool._source_anchor.case_state=type(env.spool._source_anchor.case_state)(canonical(data))
        with pytest.raises(PermissionError):await calls[0].gate.refresh(calls[0].gated)
        assert not calls[0].gate.issuance_current(calls[0].gated.ticket)
        calls[0].release()


@pytest.mark.asyncio
async def test_collected_receipt_current_source_withdrawal_blocks_recovery(env,auth,monkeypatch):
    from regional_knowledge.kaggle_case_contracts import canonical
    from test_control_spool import SOURCE,withdraw
    value,principal,c,native=await collected_operation(env,auth,monkeypatch)
    data=env.spool._source_anchor.case_state.cases
    data[c.ids['case_id']]['sources'][SOURCE]=1
    env.spool._source_anchor.case_state=type(env.spool._source_anchor.case_state)(canonical(data))
    native['source_dependencies']=[dict(source_id=SOURCE,content_revision=1,eligibility_revision=1)]
    env.spool.registry=HandlerRegistry([*env.spool.registry.handlers.values(),*env.registry.handlers.values()])
    await env.spool.stage_source_withdraw(env.p.admin,withdraw(env.p.owner.subject,sequence=2),database_generation='db-source')
    version=env.store.version
    with capture_read_output() as calls:
        with pytest.raises(Exception,match='prior_write_pending|evidence_withdrawn|case_history_unavailable'):
            await value.disagreement_edit('operation_status',c.operation_key,principal,case_id=c.ids['case_id'])
        assert not calls
    assert env.store.version==version
    with env.q.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM jobs WHERE kind='source_command'").fetchone()[0]==1
