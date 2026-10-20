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
    env.spool._source_version=object()
    assert not gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert refreshed.result.status=='ok' # re-read same current independent policy


@pytest.mark.asyncio
async def test_missing_coverage_and_forged_native_acl_are_closed_without_private_payload(env,auth):
    gate,authority,b,anchor=await setup(env,auth)
    permit=await gate.dispatch(authority,request(),b)
    bad=header();bad['acl_revision']=99
    gated=await gate.finish(permit,result(bad))
    assert gated.result.status=='unavailable' and gated.result.record is None and gated.result.reason=='internal_error'
    env.spool._source_anchor=None
    permit=await gate.dispatch(authority,request(),b)
    gated=await gate.finish(permit,result(header()))
    assert gated.result.reason=='prior_write_pending' and gated.result.record is None


@pytest.mark.asyncio
async def test_private_draft_requires_current_personal_capability_and_exact_author(env,auth):
    change(auth.p,ParticipantGrant(capability='personal_conclusion',granted=True,reason='synthetic draft'))
    actor=auth.p.other.subject
    commands=[command(actor,'create',create()),command(actor,'draft_save',PersonalDraft(text='Synthetic private analysis.',based_on_seq=1,evidence_ids=[],position_ids=['phase-one']),seq=2)]
    gate,authority,b,anchor=await setup(env,auth,commands)
    permit=await gate.dispatch(authority,request(section='draft_get',draft_id=OBJECT),b)
    record=header();record.update(section='draft_get',draft_id=OBJECT,version=1,current_version=1,payload={'text':'Synthetic private analysis.'})
    gated=await gate.finish(permit,result(record));assert gated.result.status=='ok'
    # Current Case managers do not inherit another author's private draft.
    data=anchor.case_state.cases;data[CASE]['drafts'][OBJECT]['actor']=auth.p.owner.subject
    anchor.case_state=type(anchor.case_state)(canonical(data))
    with pytest.raises(PermissionError):await gate.refresh(gated)


def test_case_projection_rejects_forged_grant_actor_and_preserves_private_event_boundary():
    actor='00000000-0000-0000-0000-000000000001';other='00000000-0000-0000-0000-000000000007'
    first=command(actor,'create',create())
    draft=command(actor,'draft_save',PersonalDraft(text='Private.',based_on_seq=1,evidence_ids=[],position_ids=['phase-one']),seq=2)
    state=project_cases([(first,None,None),(draft,None,None)],{1:'applied',2:'applied'})
    assert state.cases[CASE]['event_seq']==1 and state.cases[CASE]['drafts'][OBJECT]['version']==1
    bad=command(other,'grant',CaseGrant(grantee=UUID(other),capability='manager',reason='Not authorized'),seq=3,case_revision=1)
    with pytest.raises(ValueError,match='role history'):project_cases([(bad,None,None)],{3:'applied'},state)
