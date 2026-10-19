"""Durable dependency waits, fair resume and conservative computation GC."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
from uuid import uuid4

import pytest

from regional_knowledge.bge_queue import BgeQueue,DONE_QUERY_RETENTION_SECONDS,JOB_SECONDS
from regional_knowledge.bge_contract import SPACE
from regional_knowledge.expert_requests import ExpertRequests,ExpertResult,ExpertRequestError
from regional_knowledge.request_dependencies import RequestDependencies
from regional_knowledge.request_authority import track_resources
from regional_knowledge.participants import ParticipantError
from test_bge_queue import run_details
from test_expert_requests import queue,claim,observation,save_result,expect
from test_expert_executor import sources,surface,client_for,rpc,tool_value


@pytest.fixture
def dependent(queue,tmp_path):
    q=queue
    q.bge=BgeQueue(tmp_path/'bge.sqlite',clock=lambda:q.clock[0].timestamp())
    q.deps=RequestDependencies(q.requests,q.bge)
    return q


def observed(q,lease,context,query='Synthetic dependency query'):
    digest=hashlib.sha256(query.encode()).hexdigest()
    ident=q.bge.enqueue(context.principal.subject,'query:'+digest,[query],
        identity={'query_sha256':digest},consumer=q.deps.consumer(lease['request_id']))
    q.deps.observe(context,ident,query)
    return ident


def checkpoint(q,lease,job,continuation='Retained plan, not a human verification'):
    return q.deps.checkpoint(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],job,continuation)


def complete(q,job):
    run,token=run_details(q.bge);q.bge.heartbeat(run,token,ready=True)
    picked=q.bge.claim(run,token);assert picked['id']==job
    q.bge.complete(run,token,job,picked['claim'],SPACE,[[1]+[0]*1023],{})


def test_900_second_dependency_wait_releases_lease_and_resumes_after_restart(dependent):
    q=dependent;lease,old=claim(q);job=observed(q,lease,old)
    saved=checkpoint(q,lease,job)
    assert checkpoint(q,lease,job)==saved
    expect('checkpoint_conflict',checkpoint,q,lease,job,'Changed plan')
    expect('request_fence_denied',q.people.require,old,'public_search')
    with q.corpus.connect() as db:
        row=db.execute('SELECT state,lease_until FROM rkb_expert_requests WHERE id=?',(lease['request_id'],)).fetchone()
        assert tuple(row)==('dependency_wait',None)
    q.clock[0]+=timedelta(seconds=900)
    q.requests=ExpertRequests(q.people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
    q.deps=RequestDependencies(q.requests,q.bge)
    assert q.requests.claim(q.executor,request_id=lease['request_id'])['state']=='empty'
    # An independent person still starts while this dialog is waiting.
    other,_=claim(q,observation('U2',2))
    assert other['state']=='leased'
    complete(q,job)
    resumed=q.requests.claim(q.executor,request_id=lease['request_id'])
    assert resumed['state']=='leased' and resumed['generation']==lease['generation']+1
    current=q.requests.context(q.executor,resumed['request_id'],resumed['generation'],resumed['execution_token'])
    status=q.requests.get(current,resumed['request_id'])
    assert status['checkpoint']['continuation']=='Retained plan, not a human verification'
    assert status['checkpoint']['main_job_id']==job
    assert status['input_text']=='Synthetic research request 1'
    expect('request_fence_denied',checkpoint,q,lease,job)
    save_result(q,resumed)
    q.deps.reconcile_consumers()
    with pytest.raises(PermissionError):q.bge.result(q.actors[0].subject,job)


def test_only_current_execution_observations_can_authorize_a_checkpoint(dependent):
    q=dependent;lease,ctx=claim(q)
    arbitrary=q.bge.enqueue(q.actors[0].subject,'unobserved',['Synthetic unobserved query'])
    expect('dependency_unobserved',checkpoint,q,lease,arbitrary)
    foreign=q.bge.enqueue(q.actors[1].subject,'foreign',['Synthetic query'],identity={'query_sha256':hashlib.sha256(b'Synthetic query').hexdigest()})
    with pytest.raises(PermissionError):q.deps.observe(ctx,foreign,'Synthetic query')
    document=q.bge.enqueue(q.actors[0].subject,'not-a-query',['Synthetic query'],kind='document',identity={'query_sha256':hashlib.sha256(b'Synthetic query').hexdigest()})
    with pytest.raises(PermissionError):q.deps.observe(ctx,document,'Synthetic query')
    wrong_body=q.bge.enqueue(q.actors[0].subject,'wrong-literal-body',['Different body'],identity={'query_sha256':hashlib.sha256(b'Synthetic query').hexdigest()})
    with pytest.raises(PermissionError):q.deps.observe(ctx,wrong_body,'Synthetic query')
    own=observed(q,lease,ctx)
    with pytest.raises(PermissionError):q.deps.observe(ctx,own,'Different query')
    # A different request from the same person cannot borrow the observation.
    another,_=claim(q,replace(observation(number=2),thread_ts='200.1'))
    expect('dependency_unobserved',checkpoint,q,another,own)


def test_private_checkpoint_context_is_withheld_after_acl_revocation(dependent):
    q=dependent;lease,ctx=claim(q);doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,'content_visibility':'private','rights_status':'unknown'}])
    q.corpus.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':q.actors[0].subject}])
    with q.corpus.connect() as db:track_resources(db,ctx,document_ids=[doc])
    job=observed(q,lease,ctx);checkpoint(q,lease,job)
    with q.corpus.connect() as db:db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    complete(q,job);q.deps.refresh()
    assert q.requests.claim(q.executor)['state']=='empty'
    with q.corpus.connect() as db:
        row=db.execute('SELECT state,input_text FROM rkb_expert_requests WHERE id=?',(lease['request_id'],)).fetchone()
        assert row['state']=='blocked' and row['input_text']=='Synthetic research request 1'
    expect('resource_access_revoked',checkpoint,q,lease,job)


@pytest.mark.parametrize('fault',['missing','identity'])
def test_missing_or_changed_dependency_needs_recovery_not_inference_retry(dependent,fault):
    q=dependent;lease,ctx=claim(q);job=observed(q,lease,ctx);checkpoint(q,lease,job)
    with q.bge.connect() as db:
        runs=[tuple(r) for r in db.execute('SELECT id,launch_state FROM runs')]
        if fault=='missing':db.execute('DELETE FROM jobs WHERE id=?',(job,))
        else:db.execute("UPDATE jobs SET identity='{}' WHERE id=?",(job,))
    assert q.requests.claim(q.executor)['state']=='empty'
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(lease['request_id'],)).fetchone()[0]=='needs_recovery'
    with q.bge.connect() as db:assert [tuple(r) for r in db.execute('SELECT id,launch_state FROM runs')]==runs


def test_completed_dependency_outlives_cache_ttl_until_parent_is_finished(dependent):
    q=dependent;lease,ctx=claim(q);job=observed(q,lease,ctx);checkpoint(q,lease,job);complete(q,job)
    q.clock[0]+=timedelta(seconds=DONE_QUERY_RETENTION_SECONDS+1)
    q.bge.maintenance()
    assert q.bge.result(q.actors[0].subject,job)['state']=='done'
    resumed=q.requests.claim(q.executor)
    assert resumed['request_id']==lease['request_id']
    save_result(q,resumed);q.deps.reconcile_consumers()
    with pytest.raises(PermissionError):q.bge.result(q.actors[0].subject,job)


def test_shared_consumers_and_unmanaged_queries_do_not_lose_work(dependent):
    q=dependent;first,a=claim(q);job=observed(q,first,a)
    second,b=claim(q,replace(observation(number=2),thread_ts='200.1'))
    assert observed(q,second,b)==job
    save_result(q,first);q.deps.reconcile_consumers()
    assert q.bge.result(a.principal.subject,job)['state']=='pending'
    save_result(q,second);q.deps.reconcile_consumers()
    with pytest.raises(PermissionError):q.bge.result(a.principal.subject,job)
    third,c=claim(q,replace(observation(number=3),thread_ts='300.1'))
    query='Bare and managed query';digest=hashlib.sha256(query.encode()).hexdigest()
    bare=q.bge.enqueue(c.principal.subject,'query:'+digest,[query],identity={'query_sha256':digest})
    assert observed(q,third,c,query)==bare
    save_result(q,third);q.deps.reconcile_consumers()
    assert q.bge.result(c.principal.subject,bare)['state']=='pending'


def test_orphan_retirement_preserves_document_and_unknown_launch(dependent):
    q=dependent;lease,ctx=claim(q);job=observed(q,lease,ctx)
    document=q.bge.enqueue(ctx.principal.subject,'document',['Publication text'],kind='document')
    run,token=run_details(q.bge)
    assert q.bge.launch_claim(run)==token
    save_result(q,lease);q.deps.reconcile_consumers()
    assert q.bge.result(ctx.principal.subject,document)['state']=='pending'
    with q.bge.connect() as db:
        assert db.execute('SELECT launch_state FROM runs WHERE id=?',(run,)).fetchone()[0]=='dispatching'
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1
        assert not db.execute('SELECT 1 FROM jobs WHERE id=?',(job,)).fetchone()


@pytest.mark.parametrize('finish',[True,False])
def test_orphan_active_native_claim_finishes_or_expires_without_reclaim(dependent,finish):
    q=dependent;lease,ctx=claim(q);job=observed(q,lease,ctx)
    run,token=run_details(q.bge);q.bge.heartbeat(run,token,ready=True)
    picked=q.bge.claim(run,token);assert picked['id']==job
    save_result(q,lease);q.deps.reconcile_consumers()
    assert q.bge.result(ctx.principal.subject,job)['state']=='claimed'
    if finish:q.bge.complete(run,token,job,picked['claim'],SPACE,[[1]+[0]*1023],{})
    else:q.clock[0]+=timedelta(seconds=JOB_SECONDS+1)
    q.bge.maintenance()
    with q.bge.connect() as db:
        assert not db.execute('SELECT 1 FROM jobs WHERE id=?',(job,)).fetchone()
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1
    with pytest.raises(PermissionError):observed(q,lease,ctx)


def test_concurrent_schema_upgrade_and_managed_intake(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    path=tmp_path/'shared.sqlite'
    def open_queue(_):
        queue=BgeQueue(path)
        return queue.enqueue('actor','same-query',['Synthetic query'])
    with ThreadPoolExecutor(max_workers=4) as pool:ids=list(pool.map(open_queue,range(4)))
    assert len(set(ids))==1


def test_ready_dependency_cannot_bypass_another_persons_fair_turn(dependent):
    q=dependent;lease,ctx=claim(q);job=observed(q,lease,ctx);checkpoint(q,lease,job)
    other=q.requests.accept(observation('U2',2));complete(q,job)
    assert q.requests.claim(q.executor,request_id=lease['request_id'])=={'state':'deferred'}
    assert q.requests.claim(q.executor,request_id=other['request_id'])['state']=='leased'
    assert q.requests.claim(q.executor,request_id=lease['request_id'])['state']=='leased'


def test_bounded_dependency_scan_progress_survives_restart(dependent):
    q=dependent
    for index in range(1,13):
        lease,ctx=claim(q,replace(observation(number=index),thread_ts=str(index)+'.1'))
        job=observed(q,lease,ctx,'Query '+str(index));checkpoint(q,lease,job)
    run,token=run_details(q.bge);q.bge.heartbeat(run,token,ready=True)
    for _ in range(12):
        job=q.bge.claim(run,token)
        q.bge.complete(run,token,job['id'],job['claim'],SPACE,[[1]+[0]*1023],{})
    with q.corpus.connect() as db:q.deps._set_cursor(db,'waiting','')
    q.deps.refresh()
    with q.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests WHERE state='pending'").fetchone()[0]==10
    restarted=RequestDependencies(q.requests,q.bge);restarted.refresh()
    with q.corpus.connect() as db:
        assert db.execute("SELECT count(*) FROM rkb_expert_requests WHERE state='pending'").fetchone()[0]==12
        assert db.execute("SELECT count(*) FROM rkb_expert_request_events WHERE kind='dependency_ready'").fetchone()[0]==12


@pytest.mark.asyncio
async def test_actual_http_source_search_binds_dependency_before_checkpoint(sources,surface,tmp_path,monkeypatch):
    s=sources;q=s.q;q.requests=surface.server.expert_requests
    queue=BgeQueue(tmp_path/'http-bge.sqlite');deps=RequestDependencies(q.requests,queue)
    monkeypatch.setenv('RKB_BGE_ENABLED','1');monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(queue.path))
    accepted=q.requests.accept(observation())
    async with client_for(surface,q.executor) as client:
        rejected=await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{'action':'claim','request_id':accepted['request_id'],'person_id':q.owner.subject}})
        assert rejected['result']['isError']
        leased=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{'action':'claim','request_id':accepted['request_id']}}))
        args={k:leased[k] for k in ('request_id','generation','execution_token')}
        found=tool_value(await rpc(client,'tools/call',{'name':'expert_research','arguments':{**args,'operation':'source_search','query':'Synthetic atlas'}}))
        assert found['main_job_id'] and found['main_state']=='starting'
        receipts=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{**args,'action':'dependencies'}}))
        assert receipts['items'][0]['main_job_id']==found['main_job_id'] and receipts['items'][0]['query']=='Synthetic atlas'
        rejected=await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{**args,'action':'checkpoint_wait',
            'main_job_id':found['main_job_id'],'continuation':'Wait','lease_seconds':3600}})
        assert rejected['result']['isError']
        saved=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{**args,'action':'checkpoint_wait',
            'main_job_id':found['main_job_id'],'continuation':'Wait for exact BGE query, then read sources'}}))
        assert saved['state']=='dependency_wait'
        assert tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{**args,'action':'checkpoint_wait',
            'main_job_id':found['main_job_id'],'continuation':'Wait for exact BGE query, then read sources'}}))==saved
    with q.corpus.connect() as db:assert db.execute('SELECT count(*) FROM rkb_request_bge_dependencies').fetchone()[0]==1
    assert deps.queue.consumer_parents(deps.namespace)==[accepted['request_id']]
    await s.backend.aclose()


@pytest.mark.asyncio
async def test_search_deadline_preserves_receipt_without_repeating_inference(sources,tmp_path,monkeypatch):
    import asyncio,time,threading
    from regional_knowledge.expert_source_scope import current_source_scope
    s=sources;q=s.q
    q.bge=BgeQueue(tmp_path/'deadline-bge.sqlite');q.deps=RequestDependencies(q.requests,q.bge)
    monkeypatch.setenv('RKB_BGE_ENABLED','1');monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(q.bge.path))
    lease,ctx=claim(q)
    gate=threading.Event();started=threading.Event();finished=threading.Event();late=[]
    original=s.executor._complete_source_read
    def hold(*args):
        started.set();gate.wait(4)
        try:return original(*args)
        except (ExpertRequestError,ParticipantError) as error:late.append(error.code);raise
        finally:finished.set()
    monkeypatch.setattr(s.executor,'_complete_source_read',hold)
    try:
        start=time.monotonic()
        with pytest.raises(ExpertRequestError,match='search_deadline'):
            await s.executor.search(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],'Synthetic atlas')
        assert time.monotonic()-start<2.5 and started.is_set() and current_source_scope.get() is None
        # Queue commit survived the lost tool reply, even though its ordinary
        # post-read write never ran. Recovery reads the existing parent receipt.
        receipts=q.deps.list(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
        assert len(receipts['items'])==1
        saved=checkpoint(q,lease,receipts['items'][0]['main_job_id'])
        assert saved['state']=='dependency_wait'
        gate.set();assert await asyncio.to_thread(finished.wait,2)
        assert late==['request_fence_denied']
        with q.bge.connect() as db:
            assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==1
            assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1
    finally:
        gate.set();await asyncio.to_thread(finished.wait,2);await s.backend.aclose()
