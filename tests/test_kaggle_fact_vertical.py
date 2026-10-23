"""Facts keep the existing admission/current output authority; no model or SQL."""
import asyncio
from contextlib import asynccontextmanager

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_fact_contracts import FactReadRequest, FactReadResult
from regional_knowledge.kaggle_fact_read import _record
from regional_knowledge.kaggle_protocol import encode_envelope,decode_envelope
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.source_import_v3 import FACT_REVISIONS,source_import_v3_handler
from regional_knowledge.source_read_gate import SourceGateDenied
from regional_knowledge.read_issuance import capture_read_output
from test_kaggle_fact_read import row,request,Native
from test_source_read_gate import env,auth,setup,stage,SOURCE,OTHER
from test_participants import people
from test_kaggle_public_backend import target,work,release
from test_kaggle_worker_query import pinned


def result(**changes):
    record,item,_=_record(row(),'calendar')
    data=dict(request_id='fact-call',connection_generation=2,turn_generation=3,status='ok',
        records=(record,),evidence=(item,),observed_data_seq=0,observed_projection_seq=0,
        observed_visibility_epoch=0,observed_database_generation='db-source')
    return FactReadResult(**dict(data,**changes))


def enable(spool):
    spool.registry=HandlerRegistry([*spool.registry.handlers.values(),source_import_v3_handler(
        revisions=FACT_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)])


@pytest.mark.asyncio
async def test_gate_clears_all_fact_records_cursor_and_source_metadata_after_pending_withdrawal(env,auth):
    gate,authority,binding=await setup(env,auth);enable(env.spool)
    q=request(connection_generation=1,turn_generation=1,min_data_seq=0)
    permit=await gate.dispatch(authority,q,binding)
    native=result(connection_generation=1,turn_generation=1)
    # Fixture control source eligibility is1, unlike standalone reader fixture7.
    item=native.evidence[0]
    dependency=item.dependencies.source_revisions[0].model_copy(update={'eligibility_revision':1})
    native=native.model_copy(update={'evidence':(item.model_copy(update={'dependencies':item.dependencies.model_copy(
        update={'source_revisions':(dependency,)})}),)})
    gated=await gate.finish(permit,native)
    assert gated.result.records and gate.issuance_current(gated.ticket)
    await stage(env)
    assert not gate.issuance_current(gated.ticket)
    closed=await gate.refresh(gated)
    assert closed.result.reason=='evidence_withdrawn'
    assert closed.result.records==closed.result.evidence==() and closed.result.next_cursor is None
    assert decode_envelope(encode_envelope(closed.result)).status=='unavailable'


@pytest.mark.asyncio
async def test_schema7_registry_cannot_authorize_new_fact_wire(env,auth):
    gate,authority,binding=await setup(env,auth)
    with pytest.raises(SourceGateDenied):
        await gate.dispatch(authority,request(connection_generation=1,min_data_seq=0),binding)


@pytest.mark.asyncio
async def test_worker_fixed_fact_read_never_counts_or_encodes_and_preserves_decimal():
    native=Native([row(action='observations')])
    async def forbidden(_):raise AssertionError('fact read reached resident BGE')
    handler=KaggleWorkerQuery(acquire=native.acquire,tokenizer=pinned(forbidden))
    q=request(action='observations',calendar=None,month=None,day=None,
        metric_key='population_count',compute_claim='current-shared-resource')
    actual=decode_envelope(__import__('json').dumps(await handler(q.model_dump(mode='json'),encode=forbidden)).encode())
    assert type(actual) is FactReadResult and actual.records[0]['fact']['value']=='12.50'


@pytest.mark.asyncio
async def test_actual_queue_fact_dispatch_and_final_output_capture_use_same_read_authority(target):
    s=target
    async def read():
        with capture_read_output() as calls:
            output=await s.backend.story_calendar(s.principal,month=2,day=29,calendar='julian')
        return output,calls
    task=asyncio.create_task(read());dispatched=await work(s)
    assert type(dispatched.query) is FactReadRequest and dispatched.query.actor.person_id==s.auth.p.other.subject
    native=result(request_id=dispatched.query.request_id,connection_generation=s.generation,
        turn_generation=dispatched.query.turn_generation,observed_data_seq=5,observed_projection_seq=5)
    assert await s.admission.finish_read(s.run,s.token,s.generation,native)
    output,calls=await task
    assert output['items'][0]['fact']['calendar']=='julian'
    assert len(calls)==1 and not s.gate.released
    assert s.admission.status()['state']=='busy'  # Result is not actual compute release.
    calls[0].release();await release(s,dispatched)


@pytest.mark.asyncio
async def test_actual_worker_transport_fact_branch_uses_existing_read_slot_and_release():
    from test_bge_shared_worker import running
    calls=[]
    async def handler(frame,*,encode):
        calls.append(frame['request_id'])
        return result(request_id=frame['request_id'],connection_generation=frame['connection_generation'],
            turn_generation=frame['turn_generation']).model_dump(mode='json')
    def forbidden(_):raise AssertionError('fact transport encoded text')
    async with running(handler,forbidden) as (peer,task,loads):
        q=request(connection_generation=1,min_data_seq=0,compute_claim='native-fact-resource')
        await peer.put(q.model_dump(mode='json'))
        observed=await peer.wait('FACT_READ_RESULT')
        assert observed['records'][0]['fact']['calendar']=='julian'
        released=await peer.wait('RESOURCE_RELEASED')
        assert released['resource_kind']=='read' and released['compute_claim']=='native-fact-resource'
        assert calls==[q.request_id] and loads==['loaded']
