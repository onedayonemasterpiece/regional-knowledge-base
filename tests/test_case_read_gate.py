"""Actual Participants/spool fences with compact synthetic verified history."""
import base64
from types import SimpleNamespace
from uuid import UUID

import pytest
from regional_knowledge.case_history import project_cases
from regional_knowledge.case_read_gate import CaseReadGate
from regional_knowledge.disagreement_native import resolved_case_command,case_handler,DEPENDENCIES
from regional_knowledge.disagreement_contracts import CaseGrant,PersonalDraft
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_case_contracts import CaseReadRequest,CaseReadResult,canonical
from regional_knowledge.source_read_gate import SourceGateDenied
from regional_knowledge.participant_contracts import ParticipantGrant
from test_control_spool import env
from test_read_authority import auth
from test_participants import people,change
from test_source_read_gate import binding
from test_disagreement_native import CASE,OBJECT,create


def command(actor,action,value,*,seq=1,case_revision=0,object_version=0):
    return resolved_case_command(sequence=seq,operation_key='case-'+str(seq),request_id='case-request-'+str(seq),
        person_id=actor,logical_time='2026-10-20T18:00:00Z',case_id=CASE,object_id=CASE if action=='create' else OBJECT,
        action=action,command=value,source_dependencies=[],case_revision=case_revision,object_version=object_version,
        attribution=dict(person_id=actor,recorded_by=actor,route='personal',link_id=None,link_revision=None,human_reading_confirmed=False))


async def setup(e,a,commands=None):
    e.spool.registry=HandlerRegistry([*e.registry.handlers.values(),*[case_handler(route,check_sha256='1'*64,apply_sha256='2'*64) for route in DEPENDENCIES]])
    commands=commands or [command(a.p.other.subject,'create',create())]
    projection=project_cases([(c,None,None) for c in commands],{c.sequence:'applied' for c in commands})
    anchor=SimpleNamespace(database_generation='db-source',current=lambda:True,case_state=projection,pending=())
    e.spool._source_anchor=anchor
    authority=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    b=binding();gate=CaseReadGate(a.resolver,e.spool,binding_current=lambda current:current is b)
    return gate,authority,b,anchor


def request(**changes):
    return CaseReadRequest(request_id='case-read',connection_generation=1,turn_generation=1,case_id=CASE,
        min_data_seq=0,deadline_ms=2000,actor=dict(person_id='00000000-0000-0000-0000-000000000001',
            audience='untrusted',auth_epoch=0,visibility_epoch=0),**changes)


def result(record):
    return CaseReadResult(request_id='case-read',connection_generation=1,turn_generation=1,status='ok',
        record_b64=base64.b64encode(canonical(record)).decode(),observed_data_seq=7,observed_projection_seq=7,
        observed_visibility_epoch=0,observed_database_generation='db-source')


def header(**changes):
    return dict(case_id=CASE,case_revision=1,acl_revision=1,event_seq=1,case_role=4,section='case',source_dependencies=[],**changes)


@pytest.mark.asyncio
async def test_empty_case_has_real_independent_acl_without_fake_source_dependencies(env,auth):
    gate,authority,b,anchor=await setup(env,auth)
    permit=await gate.dispatch(authority,request(),b)
    assert permit.query.actor.person_id==authority.person_id
    gated=await gate.finish(permit,result(header()))
    assert gated.result.status=='ok' and gated.result.record['source_dependencies']==[]
    assert gate.issuance_current(gated.ticket)
    env.spool._source_version=object()  # unrelated history refresh is not Case ACL
    assert gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert refreshed.result.status=='ok'


@pytest.mark.asyncio
async def test_missing_coverage_and_forged_native_acl_are_closed_without_private_payload(env,auth):
    gate,authority,b,anchor=await setup(env,auth)
    permit=await gate.dispatch(authority,request(),b)
    bad=header();bad['case_role']=0  # native PG can never grant anonymous Case access
    gated=await gate.finish(permit,result(bad))
    assert gated.result.status=='unavailable' and gated.result.record is None and gated.result.reason=='internal_error'
    env.spool._source_anchor=None
    permit=await gate.dispatch(authority,request(),b)
    gated=await gate.finish(permit,result(header()))
    assert gated.result.status=='ok' and gated.result.record is not None


@pytest.mark.asyncio
async def test_private_draft_requires_current_personal_capability_and_exact_author(env,auth):
    change(auth.p,ParticipantGrant(capability='personal_conclusion',granted=True,reason='synthetic draft'))
    actor=auth.p.other.subject
    commands=[command(actor,'create',create()),command(actor,'draft_save',PersonalDraft(text='Synthetic private analysis.',based_on_seq=1,evidence_ids=[],position_ids=['phase-one']),seq=2)]
    gate,authority,b,anchor=await setup(env,auth,commands)
    permit=await gate.dispatch(authority,request(section='draft_get',draft_id=OBJECT),b)
    import json
    record=header();record.update(section='draft_get',draft_id=OBJECT,version=1,current_version=1,
        visibility='personal',payload=json.loads(commands[1].payload)['command'])
    gated=await gate.finish(permit,result(record));assert gated.result.status=='ok'
    # Private output still requires the personal capability at refresh;
    # the controller cannot infer draft authority from stale history.
    change(auth.p,ParticipantGrant(capability='personal_conclusion',granted=False,reason='synthetic revoke'))
    with pytest.raises(PermissionError):await gate.refresh(gated)


def test_case_projection_rejects_forged_grant_actor_and_preserves_private_event_boundary():
    actor='00000000-0000-0000-0000-000000000001';other='00000000-0000-0000-0000-000000000007'
    first=command(actor,'create',create())
    draft=command(actor,'draft_save',PersonalDraft(text='Private.',based_on_seq=1,evidence_ids=[],position_ids=['phase-one']),seq=2)
    state=project_cases([(first,None,None),(draft,None,None)],{1:'applied',2:'applied'})
    assert state.cases[CASE]['event_seq']==1 and state.cases[CASE]['drafts'][OBJECT]['version']==1
    bad=command(other,'grant',CaseGrant(grantee=UUID(other),capability='manager',reason='Not authorized'),seq=3,case_revision=1)
    with pytest.raises(ValueError,match='role history'):project_cases([(bad,None,None)],{3:'applied'},state)


@pytest.mark.asyncio
async def test_native_extra_private_payload_and_duplicate_draft_paths_are_rejected(env,auth):
    gate,authority,b,anchor=await setup(env,auth)
    permit=await gate.dispatch(authority,request(),b)
    bad=header();bad['unrelated_private_draft']='Must never cross a Case header'
    gated=await gate.finish(permit,result(bad))
    assert gated.result.reason=='internal_error' and gated.result.record is None


@pytest.mark.asyncio
@pytest.mark.parametrize('shape',[
    'valid','missing_page','extra_page','missing_region','extra_region',
])
async def test_nonempty_native_case_evidence_preserves_exact_page_region_keysets(
        env,auth,shape):
    from regional_knowledge.disagreement_contracts import EvidenceAdd
    from regional_knowledge.kaggle_case_contracts import validate_case_record
    from regional_knowledge.source_manifest import PAGE_KEYS,REGION_KEYS
    from test_disagreement_native import SOURCE,PAGE,REGION

    quote='The medieval gate was rebuilt in 1799.'
    page=dict(page_id=PAGE,physical_page_index=1,printed_page_number='12',
        width=612.0,height=792.0,page_receipt_sha256='a'*64,
        image_sha256='b'*64,native_text_sha256='c'*64,native_blocks_sha256='d'*64,
        preview_render=dict(policy='rkb.pdf-preview.v1',width=612,height=792,
            scale=1.0,max_edge=792,jpeg_quality=75,byte_limit=256*1024,preview_only=True))
    region=dict(region_id=REGION,page_id=PAGE,kind='body',
        bbox=dict(left=50,top=50,right=400,bottom=300),reading_order=0,column_id=None,
        text_sha256='e'*64,native_block_index=0,native_block_sha256='f'*64,
        start_offset=0,end_offset=len(quote))
    assert set(page)==set(PAGE_KEYS.split()) and set(region)==set(REGION_KEYS.split())
    evidence=EvidenceAdd(document_id=UUID(SOURCE),source_revision=1,source_sha256='a'*64,
        page_id=UUID(PAGE),region_id=UUID(REGION),text_start=0,quote=quote,
        context='Synthetic source line observed on the native page.',
        position_ids=['phase-one'],relation='supports',speaker='Archive author',
        attribution='direct_statement').model_dump(mode='json')
    evidence.update(text_end=len(quote),verification='accepted_text_match',
        visual_confirmation=None,physical_page_index=1,
        page_provenance=page,region_provenance=region)
    if shape=='missing_page':del page['image_sha256']
    elif shape=='extra_page':page['unexpected']='not permitted'
    elif shape=='missing_region':del region['native_block_sha256']
    elif shape=='extra_region':region['unexpected']='not permitted'
    record=header()
    record.update(section='evidence',items=[dict(evidence_id=OBJECT,version=1,
        actor_id=auth.p.other.subject,event_seq=2,payload=evidence,
        logical_time='2026-10-24T00:00:00Z')],has_more=False,next_after_seq=None)
    q=request(section='evidence')
    if shape=='valid':
        assert validate_case_record(q,record) is record
    else:
        with pytest.raises(ValueError,match='exact native evidence provenance required'):
            validate_case_record(q,record)
    gate,authority,b,_=await setup(env,auth)
    permit=await gate.dispatch(authority,q,b)
    gated=await gate.finish(permit,result(record))
    if shape=='valid':
        assert gated.result.status=='ok' and gated.result.record==record
        assert gate.issuance_current(gated.ticket)
    else:
        assert gated.result.status=='unavailable'
        assert gated.result.reason=='internal_error' and gated.result.record is None
