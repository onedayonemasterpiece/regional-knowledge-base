"""Typed fixed graph reads, no model or local native effect."""
from dataclasses import replace
from uuid import UUID

import pytest

from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
from regional_knowledge.kaggle_protocol import decode_envelope,encode_envelope
from regional_knowledge.kaggle_query import PinnedTokenizer
from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest,execute_source_graph_read
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from test_kaggle_fact_read import Native
from test_source_graph import payload
from test_source_import import SOURCE


def request(**changes):
    value,_=payload();node=value['source_graph']['entities'][0]
    args=dict(request_id='graph-read',connection_generation=2,turn_generation=3,entity_id=node['entity_id'],
        min_data_seq=9,deadline_ms=2000,compute_claim='actual-read-claim',
        actor=dict(person_id='verified-person',audience='personal',auth_epoch=1,visibility_epoch=4))
    args.update(changes);return SourceGraphReadRequest.model_validate(args)


def row(index=0):
    value,_=payload();node=value['source_graph']['entities'][index]
    return UUID(SOURCE),1,7,node['key'],node,node['mention']['evidence']['exact_quote'],'Recorded source citation'


def native(rows=()):return Native(rows,revision=(DOMAIN_REVISIONS.schema,DOMAIN_REVISIONS.function))


@pytest.mark.parametrize('changes',[dict(entity_id=None),dict(entity_id='fake'),dict(limit=4),dict(action='sql'),dict(action='list'),
    dict(action='poi_facts',source_id=SOURCE),dict(action='mentions',cursor='x'*1601)])
def test_fixed_small_scope(changes):
    with pytest.raises(ValueError):request(**changes)


@pytest.mark.asyncio
async def test_native_source_graph_retrieval_is_indexed_with_real_source_revisions():
    n=native([row()]);r=await execute_source_graph_read(request(),acquire=n.acquire)
    assert r.status=='ok' and r.records[0]['data']['kind'] in ('person','organization','event','historical_thread','poi_ref')
    assert r.evidence[0].kind=='entity' and r.evidence[0].dependencies.source_ids==(SOURCE,)
    assert r.evidence[0].dependencies.source_revisions[0].eligibility_revision==7
    assert type(decode_envelope(encode_envelope(r))) is type(r)
    assert [params for sql,params in n.calls if 'read_source_graph_v1' in sql]==[(request().entity_id,None,'entity',None,None,4)]


@pytest.mark.asyncio
async def test_worker_graph_branch_never_counts_or_encodes_and_pairs_exact_type():
    n=native([row()])
    async def forbidden(*_):raise AssertionError('graph read must not encode/count')
    worker=KaggleWorkerQuery(acquire=n.acquire,tokenizer=PinnedTokenizer(forbidden,REVISION,TOKENIZER_SHA256,SPACE))
    result=await worker(request().model_dump(mode='json'),encode=forbidden)
    assert result['type']=='SOURCE_GRAPH_READ_RESULT' and result['status']=='ok'
    assert result['records'][0]['entity_id']==request().entity_id


@pytest.mark.asyncio
async def test_wrong_entity_old_schema_or_changed_cursor_actor_refuses_payload():
    n=native([row(1)]);r=await execute_source_graph_read(request(),acquire=n.acquire)
    assert r.reason=='internal_error' and not r.records
    n=native([row(),row()]);first=await execute_source_graph_read(request(limit=1),acquire=n.acquire)
    n.calls.clear()
    changed=await execute_source_graph_read(request(limit=1,cursor=first.next_cursor,
        actor=request().actor.model_copy(update={'auth_epoch':2})),acquire=n.acquire)
    assert changed.reason=='invalid_input' and not changed.records
    assert not any('read_source_graph_v1' in sql for sql,_ in n.calls)
    n.revision=('rkb-v4-sql001-009-v1','rkb-v4-source-story-editorial-v4')
    assert (await execute_source_graph_read(request(),acquire=n.acquire)).reason=='not_ready'
