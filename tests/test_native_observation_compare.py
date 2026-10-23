"""Exact pair reads, conservative semantics and the existing final output gate."""
import asyncio
import copy
import json
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from regional_knowledge.kaggle_fact_contracts import FactReadRequest,FactReadResult
from regional_knowledge.kaggle_fact_read import execute_fact_read
from regional_knowledge.kaggle_protocol import encode_envelope,decode_envelope
from regional_knowledge.observation_comparison import render_comparison
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.source_read_gate import SourceGateDenied
from test_kaggle_fact_read import Native,row,request,ASSERTION,SOURCE
from test_kaggle_fact_vertical import enable
from test_source_read_gate import setup,env,auth,stage,OTHER
from test_participants import people
from test_kaggle_public_backend import target,work,release
from test_kaggle_story_editorial_backend import editorial

STORY='40000000-0000-0000-0000-000000000001'
LEFT=f'{ASSERTION}:o0'
RIGHT=f'{ASSERTION}:o1'


def compare_request(**changes):
    return request(**dict(dict(action='observation_compare',calendar=None,month=None,day=None,
        story_id=STORY,left_id=LEFT,right_id=RIGHT,limit=2),**changes))


class PairNative(Native):
    def __init__(self,**kwargs):
        super().__init__([row(1,action='observations'),row(0,action='observations')],**kwargs)
        self.story=dict(story_id=STORY,revision=3,finding_ids=[ASSERTION],
            source=dict(source_id=SOURCE,content_revision=1,eligibility_revision=7))

    async def fetchone(self):
        if 'read_story_v1' in self.statement or 'read_editorial_story_v1' in self.statement:
            return (self.story,)
        return await super().fetchone()


@pytest.mark.parametrize('changes',[
    dict(left_id=RIGHT),dict(left_id='caller SQL'),dict(right_id=ASSERTION+':o8'),
    dict(story_id=STORY.upper().replace('4','A',1)),dict(limit=3),dict(cursor='cursor'),
    dict(metric_key='population_count'),dict(source_id=SOURCE),dict(story_id=None),
])
def test_pair_requires_exact_distinct_ids_without_page_or_metric_filters(changes):
    with pytest.raises(ValueError):compare_request(**changes)
    with pytest.raises(ValueError):request(story_id=STORY)


@pytest.mark.asyncio
@pytest.mark.parametrize('revision',[(f'rkb-v4-sql001-{version:03d}-v1','audit-only') for version in (8,9,11,14,17,18)])
async def test_pair_is_one_snapshot_two_primary_keys_and_no_encoding(revision):
    native=PairNative(revision=revision)
    result=await execute_fact_read(compare_request(),acquire=native.acquire)
    assert result.status=='ok' and [r['id'] for r in result.records]==[LEFT,RIGHT]
    assert result.records[0]['story_revision']==3
    assert result.records[0]['fact']['value']=='12.50'
    assert decode_envelope(encode_envelope(result))==result
    pair=[call for call in native.calls if 'FROM rkb_v4.source_observations' in call[0]]
    assert len(pair)==1 and pair[0][1]==(ASSERTION,0,ASSERTION,1,SOURCE)
    assert 'd.assertion_revision,d.ordinal) IN' in pair[0][0]
    assert sum('REPEATABLE READ READ ONLY' in call[0] for call in native.calls)==1
    assert not any('dense_candidates' in sql or 'embedding' in sql for sql,_ in native.calls)


@pytest.mark.asyncio
async def test_fixed_worker_comparison_never_counts_or_encodes():
    from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
    from test_kaggle_worker_query import pinned
    async def forbidden(_):raise AssertionError('comparison reached model or tokenizer')
    handler=KaggleWorkerQuery(acquire=PairNative().acquire,tokenizer=pinned(forbidden))
    query=compare_request(compute_claim='existing-read-slot')
    result=await handler(query.model_dump(mode='json'),encode=forbidden)
    assert result['status']=='ok' and len(result['records'])==2


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['story_missing','wrong_story','outside_findings','one_missing','wrong_source'])
async def test_pair_never_exposes_half_or_foreign_story_observations(change):
    native=PairNative()
    if change=='story_missing':native.story=None
    elif change=='wrong_story':native.story['story_id']=SOURCE
    elif change=='outside_findings':native.story['finding_ids']=[]
    elif change=='one_missing':native.rows=native.rows[:1]
    else:native.story['source']['source_id']=STORY
    result=await execute_fact_read(compare_request(),acquire=native.acquire)
    assert result.status!='ok' and not result.records and not result.evidence
    if change in ('story_missing','wrong_story','outside_findings'):
        assert not any('FROM rkb_v4.source_observations' in sql for sql,_ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('change,verdict',[
    ({'metric_key':'species_count'},'different_metrics'),
    ({'unit_code':'species'},'different_units'),
    ({'subject_label':'Another village'},'unverified_subject_identity'),
    ({'method':'unknown'},'incompatible_or_unknown_method'),
    ({'period_text':'unknown','period_start_year':None,'period_end_year':None},'period_not_confirmed'),
    ({},'manual_identity_and_semantic_review_required'),
])
async def test_comparison_preserves_legacy_no_arithmetic_policy(change,verdict):
    native=PairNative();right=list(native.rows[0]);right[9]=dict(right[9],**change);native.rows[0]=tuple(right)
    result=await execute_fact_read(compare_request(),acquire=native.acquire)
    output=render_comparison(result)
    assert output['verdict']==verdict
    assert output['comparable'] is False and output['difference'] is None and output['ratio'] is None
    assert output['left']['value_decimal']=='12.50' and output['right']['fact']==right[9]
    assert output['left']['source_dependencies'][0]['source_id']==SOURCE
    assert output['left']['proofs'][0]['original_excerpt'].endswith('1900.')
    assert output['left']['bibliography']=='Exact source citation'


@pytest.mark.asyncio
async def test_current_gate_drops_entire_pair_after_pending_withdrawal(env,auth):
    gate,authority,binding=await setup(env,auth);enable(env.spool)
    native=PairNative(frontier=(0,0,0,True,False,'db-source'))
    query=compare_request(connection_generation=1,turn_generation=1,min_data_seq=0)
    permit=await gate.dispatch(authority,query,binding)
    result=await execute_fact_read(permit.query,acquire=native.acquire)
    gated=await gate.finish(permit,result)
    assert len(gated.result.records)==2 and gate.issuance_current(gated.ticket)
    await stage(env)
    assert not gate.issuance_current(gated.ticket)
    refreshed=await gate.refresh(gated)
    assert not refreshed.result.records and not refreshed.result.evidence
    assert refreshed.result.reason=='prior_write_pending'
    with pytest.raises(ValueError):render_comparison(refreshed.result)


@pytest.mark.asyncio
async def test_explicit_story_B_pair_is_unaffected_by_source_A_pending(env,auth):
    gate,authority,binding=await setup(env,auth);enable(env.spool)
    native=PairNative(frontier=(0,0,0,True,False,'db-source'))
    from uuid import UUID
    native.story['source']['source_id']=OTHER
    native.rows=[tuple([r[0],UUID(OTHER),*r[2:]]) for r in native.rows]
    query=compare_request(connection_generation=1,turn_generation=1,min_data_seq=0)
    permit=await gate.dispatch(authority,query,binding)
    await stage(env)
    result=await execute_fact_read(permit.query,acquire=native.acquire)
    gated=await gate.finish(permit,result)
    assert gated.result.status=='ok' and len(gated.result.records)==2


@pytest.mark.asyncio
async def test_gate_rejects_forged_pair_selector_before_output(env,auth):
    gate,authority,binding=await setup(env,auth);enable(env.spool)
    query=compare_request(connection_generation=1,turn_generation=1,min_data_seq=0)
    permit=await gate.dispatch(authority,query,binding)
    result=await execute_fact_read(permit.query,acquire=PairNative(frontier=(0,0,0,True,False,'db-source')).acquire)
    records=copy.deepcopy(result.records);records[1]['story_id']=SOURCE
    malformed=result.model_copy(update={'records':records})
    gated=await gate.finish(permit,malformed)
    assert gated.result.reason=='internal_error' and not gated.result.records


@pytest.mark.asyncio
async def test_existing_queue_and_final_renderer_keep_pair_authority_until_issuance(target):
    s=target
    async def read():
        with capture_read_output() as calls:
            output=await s.backend.story_observation_compare(STORY,LEFT,RIGHT,s.principal)
        return output,calls
    task=asyncio.create_task(read());dispatch=await work(s)
    assert type(dispatch.query) is FactReadRequest and dispatch.query.action=='observation_compare'
    native=PairNative(frontier=(5,5,0,True,False,'synthetic-db'))
    result=await execute_fact_read(dispatch.query,acquire=native.acquire)
    assert await s.admission.finish_read(s.run,s.token,s.generation,result)
    output,calls=await task
    assert output['verdict']=='manual_identity_and_semantic_review_required' and output['ratio'] is None
    assert len(calls)==1 and calls[0].render(calls[0].gated.result)['left']['value_decimal']=='12.50'
    calls[0].release();await release(s,dispatch)


@pytest.mark.asyncio
async def test_actual_mcp_registration_dispatches_native_pair_and_refresh_removes_both(editorial,monkeypatch):
    from regional_knowledge.kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
    from regional_knowledge.kaggle_public_backend import current_binding
    from regional_knowledge.server import build_server
    from regional_knowledge.read_issuance import _SCOPE_KEY
    from test_kaggle_service_startup import HistoryAdapter
    s=editorial;t=s.t
    startup=TargetServiceStartup(TargetServiceConfig(t.auth.provider,t.auth.p.registry,t.queue,s.spool,
        HistoryAdapter(),'writer-1',lambda **v:v['database_generation']=='synthetic-db',
        destination_ref=s.workflow.destination.destination_ref))
    startup.binding=current_binding(t.queue);startup.coverage=s.spool._source_anchor;startup.running=True
    assert startup.routes['source.observation_compare']==startup.backend.story_observation_compare
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:t.principal)
    monkeypatch.setattr('regional_knowledge.server.backend_from_env',lambda:pytest.fail('legacy corpus fallback'))
    server=build_server(target_startup=startup)
    assert 'story_observation_compare' in {tool.name for tool in server._tool_manager.list_tools()}
    req=Request({'type':'http','method':'POST','path':'/mcp','headers':[],
        'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
    ctx=SimpleNamespace(request_context=SimpleNamespace(request=req,request_id='native-comparison-mcp'))
    task=asyncio.create_task(server.call_tool('story_observation_compare',
        dict(story_id=STORY,left_id=LEFT,right_id=RIGHT),ctx))
    dispatch=await work(t)
    result=await execute_fact_read(dispatch.query,
        acquire=PairNative(frontier=(5,5,0,True,False,'synthetic-db')).acquire)
    assert await t.admission.finish_read(t.run,t.token,t.generation,result)
    output=await task
    assert 'manual_identity_and_semantic_review_required' in str(output)
    issued=req.scope[_SCOPE_KEY][1]
    assert issued.gate is startup.gate and issued.gate.issuance_current(issued.gated.ticket)
    s.spool.invalidate_source_history()
    assert not issued.gate.issuance_current(issued.gated.ticket)
    refreshed=await issued.gate.refresh(issued.gated)
    closed=issued.render(refreshed.result)
    assert closed['status']=='unavailable' and not closed['items']
    assert all(key not in closed for key in ('left','right','story_id','verdict','difference','ratio'))
    issued.release();await release(t,dispatch)
