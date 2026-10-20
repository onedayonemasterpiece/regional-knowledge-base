"""Exact real journal/spool/OAuth boundary with synthetic native coverage fixture."""
import base64
from dataclasses import replace
import hashlib
from types import SimpleNamespace
import time

import pytest

from regional_knowledge.geo_manifest_commands import GEO_REVISIONS,geo_manifest_handler,plan_geo_manifest
from regional_knowledge.geo_migration import GeoMigrationManifest,canonical
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.geo_read_gate import GeoReadGate
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_geo_contracts import GeoReadResult,GeoReadRecord
from regional_knowledge.participant_contracts import ParticipantDisable
from regional_knowledge.source_history_verifier import _Coverage
from regional_knowledge.source_read_gate import SourceGateDenied,binding_key
from test_control_spool import env,receipt
from test_read_authority import auth
from test_participants import people,change
from test_geo_migration import snapshot,uid
from test_kaggle_geo_read import request


async def setup(e,a,*,durable=True,binding=None):
    e.registry=HandlerRegistry([geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256='1'*64,apply_sha256='2'*64)])
    e.spool.registry=e.registry
    manifest=build_geo_whitelist(**snapshot()).require_ready()
    manifest=GeoMigrationManifest.from_wire(dict(manifest.wire(),items=[dict(item.wire(),owner_person_id=a.p.other.subject) for item in manifest.items])).require_ready()
    resolved=plan_geo_manifest(manifest).commands(first_sequence=1,operation_key='geo-fixture',request_id='geo-request',
        person_id=e.p.owner.subject,logical_time='2026-10-20T18:00:00Z')
    generation=binding.database_generation if binding is not None else 'db-source'
    for c in resolved:
        await e.spool.stage_domain_command(e.p.admin,c,database_generation=generation)
        await e.spool.seal(e.p.admin,c.operation_key,writer_generation='writer-1',predecessor=await e.journal.head())
        await e.spool.record_apply(c.operation_key,receipt(c,database=generation))
        if durable:await e.spool.publish_outcome(c.operation_key,writer_generation='writer-1')
    b=binding or SimpleNamespace(run_id='run',runtime_incarnation='inc',connection_generation=2,database_generation='db-source',min_data_seq=0,provider_version='1')
    # Unit-only actual _Coverage identity: native catalog/ledger verifier has
    # separate guarded integration tests. No caller can inject this through API.
    v=SimpleNamespace(clock=time.monotonic,binding_current=lambda current:binding_key(current)==binding_key(b),
        registry_hash='fixture-registry',_registry_hash=lambda:'fixture-registry',_routes_current=lambda:True)
    anchor=_Coverage(generation,v,b,binding_key(b),time.monotonic()+30,'fixture',None,len(resolved),len(resolved))
    v._coverage=anchor;e.spool._source_anchor=anchor
    gate=GeoReadGate(a.resolver,e.spool,binding_current=lambda current:binding_key(current)==binding_key(b))
    authority=await a.resolver.resolve_oauth(a.tokens.access_token,destination=a.destination)
    keys=tuple(c.operation_key for c in resolved)
    if durable:assert await gate.establish(keys,binding=b)==manifest.sha256
    return gate,authority,b,manifest,resolved,keys


def result(manifest,resolved,*,matched=False):
    item=next(i for i in manifest.items if i.key=='place:'+uid(10));last=resolved[-1]
    record=GeoReadRecord(item_key=item.key,wire_b64=base64.b64encode(canonical(item.wire())).decode(),
        matched_wire_b64=base64.b64encode(canonical(item.wire())).decode() if matched else None,
        manifest_id=manifest.sha256,activation_sequence=last.sequence,activation_operation_key=last.operation_key,
        activation_command_sha256=hashlib.sha256(last.encode()).hexdigest())
    return GeoReadResult(request_id='geo-read',connection_generation=2,turn_generation=3,status='ok',records=(record,),
        observed_data_seq=last.sequence,observed_projection_seq=last.sequence,observed_visibility_epoch=0,
        observed_database_generation='db-source')


@pytest.mark.asyncio
async def test_current_personal_scope_exact_manifest_bytes_and_final_output_refresh(env,auth):
    gate,authority,b,manifest,resolved,_=await setup(env,auth)
    permit=await gate.dispatch(authority,request(min_data_seq=0),b)
    assert permit.query.actor.person_id==auth.p.other.subject
    gated=await gate.finish(permit,result(manifest,resolved))
    assert gated.result.evidence==() and gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert not gate.issuance_current(gated.ticket) and gate.issuance_current(refreshed.ticket)
    gate.release(refreshed.ticket)


@pytest.mark.asyncio
async def test_import_installation_fence_requires_exact_durable_operation_list_and_current_native_prefix(env,auth):
    gate,authority,b,manifest,resolved,keys=await setup(env,auth)
    assert gate.installation_current(b,keys)
    for other in ((),keys[:-1],tuple(reversed(keys)),('different-installation',*keys[1:])):
        assert not gate.installation_current(b,other)
    assert not gate.installation_current(SimpleNamespace(**dict(vars(b),database_generation='other-db')),keys)
    env.spool.invalidate_source_history()
    assert not gate.installation_current(b,keys)
    gate.invalidate()
    assert not gate.installation_current(b,keys)


@pytest.mark.asyncio
@pytest.mark.parametrize('change_type',['body','owner','activation','name'])
async def test_forged_or_unmatched_native_item_is_never_issued(env,auth,change_type):
    gate,authority,b,manifest,resolved,_=await setup(env,auth)
    q=request(entity_id=None,name='forged name',min_data_seq=0) if change_type=='name' else request(min_data_seq=0)
    permit=await gate.dispatch(authority,q,b);value=result(manifest,resolved,matched=change_type=='name')
    record=value.records[0]
    if change_type in ('body','owner'):
        from regional_knowledge.kaggle_geo_contracts import decode_item
        item,_=decode_item(record.wire_b64)
        if change_type=='owner':item=item.model_copy(update={'owner_person_id':env.p.owner.subject})
        else:item=item.model_copy(update={'payload':item.payload.model_copy(update={'canonical_label':'forged historical identity'})})
        record=record.model_copy(update={'wire_b64':base64.b64encode(canonical(item.wire())).decode()})
    if change_type=='activation':record=record.model_copy(update={'activation_command_sha256':'f'*64})
    value=value.model_copy(update={'records':(record,)})
    with pytest.raises(SourceGateDenied):await gate.finish(permit,value)


@pytest.mark.asyncio
async def test_native_history_invalidation_and_current_grant_revoke_close_delayed_output(env,auth):
    gate,authority,b,manifest,resolved,_=await setup(env,auth)
    permit=await gate.dispatch(authority,request(min_data_seq=0),b);gated=await gate.finish(permit,result(manifest,resolved))
    env.spool.invalidate_source_history()
    assert not gate.issuance_current(gated.ticket)
    with pytest.raises(SourceGateDenied):await gate.refresh(gated)
    # A revoked participant is checked before any new dispatch, independently
    # of native coverage or owner-copy metadata.
    change(auth.p,ParticipantDisable(disabled=True,reason='revoked exact response'))
    with pytest.raises(PermissionError):await gate.dispatch(authority,request(min_data_seq=0),b)


@pytest.mark.asyncio
async def test_unknown_remote_outcome_and_missing_native_history_cannot_establish(env,auth):
    gate,authority,b,manifest,resolved,keys=await setup(env,auth,durable=False)
    with pytest.raises(SourceGateDenied,match='not durable'):await gate.establish(keys,binding=b)
    assert gate._manifest is None
    for c in resolved:await env.spool.publish_outcome(c.operation_key,writer_generation='writer-1')
    env.spool.invalidate_source_history()
    with pytest.raises(SourceGateDenied,match='native accepted manifest'):await gate.establish(keys,binding=b)


@pytest.mark.asyncio
async def test_remote_outcome_readback_must_match_exact_recorded_bytes(env,auth):
    gate,authority,b,manifest,resolved,keys=await setup(env,auth)
    last=await env.spool.get(keys[-1]);stored=env.store.objects[last.remote_outcome.key]
    # Existing test-store type preserves its provider token; only bytes corrupt.
    env.store.objects[last.remote_outcome.key]=replace(stored,data=b'changed remote receipt')
    with pytest.raises(SourceGateDenied,match='remote outcome differs'):await gate.establish(keys,binding=b)
