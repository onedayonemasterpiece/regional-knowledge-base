"""Output races over real canonical authority/spool; native adapter is synthetic."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_protocol import (
    Query, QueryFilters, QueryResult, TrustedActorContext, EvidenceItem,
    ItemDependencies, SourceDependency,
)
from regional_knowledge.source_read_gate import SourceReadGate, SourceGateDenied, DispatchPermit
from test_control_spool import env, SOURCE, OTHER, stage, accept
from test_read_authority import auth
from test_bge_admission import state
from test_participants import people, change
from regional_knowledge.participant_contracts import ParticipantDisable


def binding():
    return SimpleNamespace(run_id='run',runtime_incarnation='inc',connection_generation=1,
        database_generation='db-source',min_data_seq=0,provider_version='v1')


def query(scope=None):
    return Query(request_id='read',connection_generation=1,turn_generation=1,query='history',
        min_data_seq=0,deadline_ms=10000,filters=QueryFilters(source_ids=scope),
        actor=TrustedActorContext(person_id='untrusted',audience='public',auth_epoch=0,visibility_epoch=0))


def item(*sources,id='passage',**changes):
    return EvidenceItem(id=id,kind='source',text='synthetic evidence',dependencies=ItemDependencies(
        source_ids=sources,variant_refs=(),grant_refs=(),source_revisions=tuple(
            SourceDependency(source_id=s,content_revision=1,eligibility_revision=1) for s in sources)),**changes)


def result(*items,**changes):
    return QueryResult(request_id='read',connection_generation=1,turn_generation=1,status='ok',mode='hybrid',
        observed_data_seq=0,observed_projection_seq=0,observed_database_generation='db-source',
        observed_visibility_epoch=0,
        evidence=items,**changes)


async def setup(e,a):
    authority=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    b=binding()
    # Unit-only capability fixture; native verification is tested independently.
    anchor=SimpleNamespace(database_generation='db-source',current=lambda:True)
    e.spool._source_anchor=anchor
    gate=SourceReadGate(a.resolver,e.spool,binding_current=lambda current:current is b)
    return gate,authority,b


@pytest.mark.asyncio
async def test_uncovered_empty_spool_is_not_complete(env,auth):
    gate,authority,b=await setup(env,auth)
    env.spool.invalidate_source_history()
    permit=await gate.dispatch(authority,query(),b)
    with pytest.raises(SourceGateDenied):await gate.finish(permit,result())


@pytest.mark.asyncio
async def test_native_evidence_free_failure_preserves_reason_without_inventing_pending(env,auth):
    gate,authority,b=await setup(env,auth)
    env.spool.invalidate_source_history()
    permit=await gate.dispatch(authority,query(),b)
    failure=QueryResult(request_id='read',connection_generation=1,turn_generation=1,
        status='unavailable',mode=None,reason='deadline_exceeded',
        observed_data_seq=None,observed_projection_seq=None)
    gated=await gate.finish(permit,failure)
    assert gated.result is failure and not gated.result.prior_write_pending
    assert gate.issuance_current(gated.ticket)
    deadline=gate._tickets[gated.ticket].deadline
    refreshed=await gate.refresh(gated)
    assert gate._tickets[refreshed.ticket].deadline==deadline


@pytest.mark.asyncio
async def test_broad_topk_omits_pending_A_still_degraded_but_explicit_B_unaffected(env,auth):
    gate,authority,b=await setup(env,auth)
    await stage(env)
    for evidence in ((),(item(OTHER),)):
        permit=await gate.dispatch(authority,query(),b)
        gated=await gate.finish(permit,result(*evidence))
        assert gated.result.status=='degraded' and gated.result.prior_write_pending
        assert gated.result.evidence==evidence and gate.issuance_current(gated.ticket)
        gate.release(gated.ticket)
    permit=await gate.dispatch(authority,query((OTHER,)),b)
    gated=await gate.finish(permit,result(item(OTHER)))
    assert gated.result.status=='ok' and not gated.result.prior_write_pending
    assert permit.query.actor.person_id==authority.person_id
    assert permit.query.min_data_seq==0


@pytest.mark.asyncio
async def test_whole_cross_source_item_and_all_metadata_continuations_are_removed(env,auth):
    gate,authority,b=await setup(env,auth)
    permit=await gate.dispatch(authority,query(),b)
    await stage(env)
    gated=await gate.finish(permit,result(item(SOURCE,OTHER,continuation='private-tail'),item(OTHER,id='safe'),
        continuation='unsafe-whole-result'))
    assert [i.id for i in gated.result.evidence]==['safe']
    assert gated.result.continuation is None and gated.result.reason=='prior_write_pending'


@pytest.mark.asyncio
async def test_final_synchronous_fence_and_refresh_recheck_current_control(env,auth):
    gate,authority,b=await setup(env,auth)
    permit=await gate.dispatch(authority,query(),b)
    gated=await gate.finish(permit,result(item(SOURCE)))
    assert gate.issuance_current(gated.ticket)
    await stage(env)
    assert not gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert refreshed.result.evidence==() and refreshed.result.prior_write_pending
    assert not gate.issuance_current(gated.ticket) and gate.issuance_current(refreshed.ticket)
    b.runtime_incarnation='new-incarnation'
    assert not gate.issuance_current(refreshed.ticket)


@pytest.mark.asyncio
async def test_copied_permit_or_replaced_authority_is_not_capability(env,auth):
    gate,authority,b=await setup(env,auth)
    permit=await gate.dispatch(authority,query(),b)
    with pytest.raises(SourceGateDenied):await gate.finish(DispatchPermit(permit.query),result())
    change(auth.p,ParticipantDisable(disabled=True,reason='synthetic revoke'))
    with pytest.raises(PermissionError):await gate.finish(permit,result(item(OTHER)))


@pytest.mark.asyncio
async def test_unsupported_grant_variant_and_non_source_fail_closed(env,auth):
    gate,authority,b=await setup(env,auth)
    unsupported=item(SOURCE).model_copy(update={'dependencies':item(SOURCE).dependencies.model_copy(
        update={'grant_refs':('private-grant',)})})
    permit=await gate.dispatch(authority,query(),b)
    gated=await gate.finish(permit,result(unsupported))
    assert gated.result.status=='denied' and not gated.result.evidence
    with pytest.raises(SourceGateDenied):
        await gate.dispatch(authority,query().model_copy(update={'filters':QueryFilters(kinds=('story',))}),b)


@pytest.mark.asyncio
async def test_capacity_deadline_release_and_explicit_invalidation(env,auth):
    gate,authority,b=await setup(env,auth)
    now=[0.0]; gate.clock=lambda:now[0]; gate.max_retained=1
    permit=await gate.dispatch(authority,query(),b)
    with pytest.raises(SourceGateDenied):await gate.dispatch(authority,query(),b)
    now[0]=11
    with pytest.raises(SourceGateDenied):await gate.finish(permit,result())
    permit=await gate.dispatch(authority,query(),b)
    gated=await gate.finish(permit,result())
    gate.invalidate()
    assert not gate.issuance_current(gated.ticket)


@pytest.mark.asyncio
async def test_gate_projection_enters_real_shared_admission_and_abandon_frees_permit(env,state):
    import time
    b=binding();b.connection_generation=state.generation
    gate=SourceReadGate(state.auth.resolver,env.spool,binding_current=lambda current:current is b,
        max_retained=1)
    permit=await gate.dispatch(state.context,query(),b)
    assert permit.query.actor.audience==state.context.destination.key
    ticket=await state.a.begin_read(state.context,permit.query,dialog_ref='gate-dialog',deadline=time.monotonic()+1.9)
    state.a.cancel_read(ticket)
    gate.abandon(permit)
    replacement=await gate.dispatch(state.context,query(),b)
    gate.abandon(replacement)


@pytest.mark.asyncio
async def test_resolved_import_intake_uses_existing_spool_but_is_not_a_restriction(env,auth,monkeypatch):
    import sys
    from regional_knowledge.kaggle_apply import DependencySpec,HandlerRegistry,HandlerSpec
    from regional_knowledge.control_spool import SpoolDenied
    from test_control_spool import handler
    dep=DependencySpec('source.import.v1','public_source',frozenset({'source_id'}),'shared',False)
    withdrawal=handler()
    imported=replace(withdrawal,route='source.import.v1',dependency=dep,precondition_keys=frozenset({'absent_source'}))
    env.spool.registry=HandlerRegistry([withdrawal,imported])
    def validate(command):
        assert command.ids=={'source_id':SOURCE} and command.expected_revisions=={'absent_source':'true'}
    monkeypatch.setitem(sys.modules,'regional_knowledge.source_import',SimpleNamespace(
        SOURCE_IMPORT='source.import.v1',IMPORT_DEPENDENCY=dep,IMPORT_PRECONDITIONS=frozenset({'absent_source'}),
        validate_import_command=validate))
    command=replace(env.cmd,route='source.import.v1',expected_revisions={'absent_source':'true'},payload=b'{}')
    with pytest.raises(SpoolDenied):await env.spool.stage_source_withdraw(env.p.admin,command,database_generation='db-source')
    await env.spool.stage_source_import(env.p.admin,command,database_generation='db-source')
    facts=await env.spool.read_facts(None,(SOURCE,))
    assert facts.pending and not facts.changes[0].restriction
    assert not (await env.spool.barriers_for((SOURCE,))).barriers[0].restriction_active
    sealed=await env.spool.seal(env.p.admin,command.operation_key,predecessor=None,writer_generation='writer-1')
    assert sealed.manifest.last_sequence==1
