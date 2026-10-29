"""Story reads preserve accepted narrative and the existing authority fences."""
import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from regional_knowledge.kaggle_protocol import StoryReadRequest,StoryReadResult,QueryFilters,decode_envelope,encode_envelope
from regional_knowledge.kaggle_story_read import story_item
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.source_import_v2 import STORY_REVISIONS,source_import_v2_handler
from regional_knowledge.source_read_gate import SourceReadGate,SourceGateDenied
from test_source_import_v2 import payload,STORY,VARIANT
from test_participants import people
from test_source_read_gate import env,auth,setup,binding,stage,SOURCE,OTHER
from test_kaggle_pg_search import FixtureConnection
from test_kaggle_worker_query import pinned
from test_kaggle_public_backend import target,work,settle,release
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.read_authority import ReadAuthorityError


def record(source=SOURCE,view='get'):
    doc=payload();story=doc['semantic']['stories'][0];variant={k:v for k,v in story['default_variant'].items() if k!='embedding'}
    return dict(story_id=STORY,revision=1,title=story['title'],summary=story['summary'],material_type=story['material_type'],
        body=variant['body'],body_sha256=hashlib.sha256(variant['body'].encode()).hexdigest(),narrative_availability='available',
        default_variant_ref=dict(variant_id=VARIANT,revision=1),variant=variant,bibliography=doc['bibliography'],
        source=dict(source_id=source,content_revision=1,eligibility_revision=1),finding_ids=story['finding_ids'],view=view,
        entries=[],next_revision=None)


def request(**changes):
    value=dict(request_id='read',connection_generation=1,turn_generation=1,story_id=STORY,deadline_ms=1900,
        min_data_seq=0,actor=dict(person_id='untrusted',audience='public',auth_epoch=0,visibility_epoch=0))
    return StoryReadRequest(**dict(value,**changes))


def result(row=None):
    row=row or record()
    return StoryReadResult(request_id='read',connection_generation=1,turn_generation=1,status='ok',
        evidence=(story_item(row),),record=row,observed_data_seq=0,observed_projection_seq=0,
        observed_visibility_epoch=0,observed_database_generation='db-source')


def enable(spool):
    spool.registry=HandlerRegistry([*spool.registry.handlers.values(),source_import_v2_handler(
        revisions=STORY_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)])


@pytest.mark.asyncio
async def test_story_gate_rechecks_withdrawal_and_drops_all_body_history_metadata(env,auth):
    gate,authority,b=await setup(env,auth);enable(env.spool)
    permit=await gate.dispatch(authority,request(),b)
    gated=await gate.finish(permit,result())
    assert gated.result.record['body']!=gated.result.record['summary'] and gate.issuance_current(gated.ticket)


@pytest.mark.asyncio
async def test_exact_story_B_unaffected_by_pending_A_and_tampered_record_fails_closed(env,auth):
    gate,authority,b=await setup(env,auth);enable(env.spool);await stage(env)
    permit=await gate.dispatch(authority,request(),b)
    gated=await gate.finish(permit,result(record(OTHER)))
    assert gated.result.status=='ok'
    permit=await gate.dispatch(authority,request(),b)
    unsafe=result(record(OTHER));changed=dict(unsafe.record,body='fabricated body')
    gated=await gate.finish(permit,unsafe.model_copy(update={'record':changed}))
    assert gated.result.status=='unavailable' and gated.result.record is None and not gated.result.evidence


@pytest.mark.asyncio
async def test_story_without_exact_installed_semantics_denied(env,auth):
    gate,authority,b=await setup(env,auth)
    with pytest.raises(SourceGateDenied):await gate.dispatch(authority,request(),b)


@pytest.mark.asyncio
async def test_worker_story_read_never_counts_or_runs_model_and_preserves_exact_record():
    row=record()
    class Conn(FixtureConnection):
        async def fetchone(self):
            return (row,) if 'read_story_v1' in self.statement else (0,0,0,True,True,'db-source')
    connection=Conn()
    @asynccontextmanager
    async def acquire():yield connection
    async def forbidden(_):raise AssertionError('exact Story read reached BGE')
    handler=KaggleWorkerQuery(acquire=acquire,tokenizer=pinned(forbidden))
    wire=await handler(request(compute_claim='actual-permit').model_dump(mode='json'),encode=forbidden)
    actual=decode_envelope(json.dumps(wire).encode())
    assert type(actual) is StoryReadResult and actual.record==row and actual.evidence[0].text==row['body']
    assert connection.cleaned


@pytest.mark.asyncio
async def test_public_story_roundtrip_uses_same_admission_and_final_output_authority(target):
    s=target
    async def read():
        with capture_read_output() as calls:output=await s.backend.story_get(STORY,s.principal)
        return output,calls
    task=asyncio.create_task(read());dispatched=await work(s)
    assert type(dispatched.query) is StoryReadRequest
    row=record();native=result(row).model_copy(update={'request_id':dispatched.query.request_id,
        'connection_generation':s.generation,'turn_generation':dispatched.query.turn_generation,
        'observed_data_seq':5,'observed_projection_seq':5})
    assert await s.admission.finish_read(s.run,s.token,s.generation,native)
    output,calls=await task
    assert output['body']==row['body'] and output['default_variant_ref']==row['default_variant_ref']
    authority=s.gate.permits[0].authority
    assert await s.auth.resolver.recheck(authority)
    calls[0].release()
    with pytest.raises(ReadAuthorityError):await s.auth.resolver.recheck(authority)
    await release(s,dispatched)


@pytest.mark.asyncio
async def test_trusted_adapter_keeps_identity_authority_and_timing_goal_without_oauth_resolution(target,monkeypatch):
    import time
    s=target
    authority=await s.auth.resolver.resolve_oauth(s.principal.access_token,destination=s.backend.destination)
    async def forbidden(*args,**kwargs):raise AssertionError('trusted receiver re-resolved bearer')
    monkeypatch.setattr(s.auth.resolver,'resolve_oauth',forbidden)
    async def read():
        with capture_read_output() as calls:
            output=await s.backend.search_adapter('gate',authority=authority,request_id='adapter-turn-7',turn_generation=7,
                min_data_seq=8,deadline_monotonic=time.monotonic()+1.7,destination=authority.destination)
        return output,calls
    task=asyncio.create_task(read());dispatched=await work(s)
    assert dispatched.query.request_id!='adapter-turn-7'
    assert (dispatched.query.turn_generation,dispatched.query.min_data_seq)==(7,8)
    assert dispatched.query.deadline_ms==2000  # Search timing goal, not caller authority expiry.
    from regional_knowledge.kaggle_protocol import QueryResult
    native=QueryResult(request_id=dispatched.query.request_id,connection_generation=s.generation,
        turn_generation=7,status='degraded',mode='hybrid',reason='bounded_retrieval',
        observed_data_seq=8,observed_projection_seq=8,evidence=())
    assert await s.admission.finish_read(s.run,s.token,s.generation,native)
    output,calls=await task
    assert output.status=='degraded' and await s.auth.resolver.recheck(authority)
    calls[0].release()
    with pytest.raises(ReadAuthorityError):await s.auth.resolver.recheck(authority)
    await release(s,dispatched)


def test_story_read_accepts_same_maximum_audience_and_language_as_accepted_import():
    from test_source_import_v2 import command
    value=payload();variant=value['semantic']['stories'][0]['default_variant']
    variant['audience']='a'*500;variant['language']='x'*24
    accepted=command(value)
    assert accepted.payload
    row=record();row['variant']['audience']=variant['audience'];row['variant']['language']=variant['language']
    actual=decode_envelope(encode_envelope(result(row)))
    assert actual.record['variant']['audience']==variant['audience']
    assert actual.record['variant']['language']==variant['language']
