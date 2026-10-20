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
    from regional_knowledge.kaggle_case_contracts import CaseReadRequest
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
        assert {'disagreement_get','disagreement_edit','disagreement_contribute','disagreement_conclusion'}<=tools.keys()
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
