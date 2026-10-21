import concurrent.futures
import pytest
from regional_knowledge.bge_queue import BgeQueue,IDLE_SECONDS,ROTATE_SECONDS,DONE_QUERY_RETENTION_SECONDS,DONE_AUDIT_RETENTION_SECONDS
from regional_knowledge.bge_contract import SPACE,validate_vector

def run_details(queue):
    with queue.connect() as db:
        run=dict(db.execute("select * from runs where status='starting' order by started desc limit 1").fetchone())
    return run['id'],(queue.path.parent/'bge-worker-credentials'/run['id']).read_text()

def test_atomic_start_and_durable_replayed_job(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        ids=list(pool.map(lambda _:queue.enqueue('actor','same',['query']),range(10)))
    assert len(set(ids))==1
    with queue.connect() as db:assert db.execute('select count(*) from runs').fetchone()[0]==1
    assert BgeQueue(queue.path).result('actor',ids[0])['state']=='pending'
    with pytest.raises(PermissionError):queue.result('other',ids[0])
    with pytest.raises(ValueError):queue.enqueue('actor','same',['changed'])

def test_useful_lease_heartbeat_does_not_extend_it(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    queue.enqueue('actor','one',['query']);run,token=run_details(queue)
    queue.heartbeat(run,token,ready=True)
    for _ in range(18):
        now[0]+=99;queue.heartbeat(run,token);queue.maintenance()
    assert queue.status()['lease_remaining_seconds']==18
    now[0]+=19
    with pytest.raises(PermissionError):queue.heartbeat(run,token)
    # Pending useful work survives the expired run; it never disappears.
    assert queue.status()['queue_depth']==1

def test_queries_precede_documents_and_lost_worker_result_is_fenced(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    doc=queue.enqueue('actor','doc',['passage'],kind='document')
    query=queue.enqueue('actor','query',['query']);run,token=run_details(queue);queue.heartbeat(run,token,ready=True)
    job=queue.claim(run,token);assert job['id']==query and queue.claim(run,token) is None
    with pytest.raises(ValueError):queue.complete(run,token,query,job['claim'],'e5',[[1]+[0]*1023],{})
    assert queue.complete(run,token,query,job['claim'],SPACE,[[1]+[0]*1023],{})=='accepted'
    assert queue.complete(run,token,query,job['claim'],SPACE,[[1]+[0]*1023],{})=='duplicate'
    document=queue.claim(run,token);assert document['id']==doc
    now[0]+=121
    with pytest.raises(PermissionError):queue.complete(run,token,doc,document['claim'],SPACE,[[1]+[0]*1023],{})
    assert queue.result('actor',doc)['state']=='pending'

def test_rotation_allows_one_serving_one_warming_then_drains(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    queue.enqueue('actor','q',['query']);run,token=run_details(queue);queue.heartbeat(run,token,ready=True)
    # Model continuous useful demand without skipping liveness or idle expiry.
    for step in range(1,ROTATE_SECONDS//60+1):
        now[0]=1000.+step*60
        queue.heartbeat(run,token)
        queue.enqueue('actor',str(step),['query'])
        job=queue.claim(run,token)
        if job:queue.complete(run,token,job['id'],job['claim'],SPACE,[[1]+[0]*1023],{})
    queue.maintenance();successor,new_token=run_details(queue);assert successor!=run
    with queue.connect() as db:assert db.execute("select count(*) from runs where status in ('ready','starting')").fetchone()[0]==2
    queue.heartbeat(successor,new_token,ready=True)
    assert queue.claim(run,token) is None
    queue.maintenance()
    with pytest.raises(PermissionError):queue.claim(run,token)

def test_bge_1024_normalized_space_is_not_e5_or_legacy():
    assert len(validate_vector([1]+[0]*1023))==1024
    for values in ([1]+[0]*383,[1]+[0]*767,[0]*1024,[float('nan')]+[0]*1023):
        with pytest.raises(ValueError):validate_vector(values)

def test_worker_diagnostics_do_not_preserve_arbitrary_private_fields(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite');queue.enqueue('actor','one',['query'])
    run,token=run_details(queue)
    queue.heartbeat(run,token,ready=True,diagnostics={'rss_kib':1024,'token':'private','texts':['private']})
    import json
    with queue.connect() as db:
        assert json.loads(db.execute('select diagnostics from runs where id=?',(run,)).fetchone()[0])=={'rss_kib':1024}


def test_legacy_failure_budget_resets_only_after_proven_ready_and_successful_work(tmp_path):
    """Two NOT SENT incidents must not poison a later independent cycle."""
    now=[1000.]
    queue=BgeQueue(tmp_path/'legacy-success-cycle.sqlite',clock=lambda:now[0])
    document=queue.enqueue('indexer','first',['synthetic passage'],kind='document')
    run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    for _ in range(2):
        token=queue.launch_claim(run,provider_ref='owner/stable-slug')
        assert token is not None
        queue.launch_record(run,'owner/stable-slug',failed=True)
        now[0]+=61
        run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==2
    token=queue.launch_claim(run,provider_ref='owner/stable-slug')
    queue.launch_record(run,'owner/stable-slug',provider_version=7)
    # A mere socket heartbeat without actual READY is insufficient.
    queue.heartbeat(run,token)
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==2
    queue.heartbeat(run,token,ready=True)
    assigned=queue.claim(run,token)
    assert assigned['id']==document
    assert queue.complete(run,token,document,assigned['claim'],SPACE,[[1]+[0]*1023],{})=='accepted'
    assert queue.result('indexer',document)['state']=='done'
    with queue.connect(write=False) as db:
        control=db.execute('select startup_ref,startup_state,startup_failures from control where id=1').fetchone()
        assert tuple(control)==(None,None,0)
    # Independent terminal proof is conceptually external; neither it nor
    # eventual useful-idle expiry is a failed cold startup.
    queue.launch_record(run,'owner/stable-slug',failed=True,provider_version=7)
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==0
    now[0]+=61
    queue.maintenance()
    second=queue.enqueue('indexer','new-cycle',['another passage'],kind='document')
    next_run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    assert next_run!=run
    queue.launch_claim(next_run,provider_ref='owner/stable-slug')
    queue.launch_record(next_run,'owner/stable-slug',failed=True)
    with queue.connect(write=False) as db:
        state=db.execute('select startup_state,startup_failures from control where id=1').fetchone()
    assert state['startup_state'] is None and state['startup_failures']==1
    assert queue.result('indexer',second)['state']=='pending'


def test_legacy_unverified_heartbeat_does_not_reset_failure_budget(tmp_path):
    now=[1000.]
    queue=BgeQueue(tmp_path/'legacy-unverified.sqlite',clock=lambda:now[0])
    queue.enqueue('indexer','doc',['synthetic document'],kind='document')
    run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    for index in range(3):
        token=queue.launch_claim(run,provider_ref='owner/stable-slug')
        if index==2:queue.heartbeat(run,token,ready=False)
        queue.launch_record(run,'owner/stable-slug',failed=True)
        if index!=2:
            now[0]+=61
            run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    with queue.connect(write=False) as db:
        state=db.execute('select startup_state,startup_failures from control where id=1').fetchone()
    assert tuple(state)==('blocked',3)


def test_legacy_document_demand_without_startup_ref_cannot_retry_forever(tmp_path):
    """Source regression for observed active controller not_sent ValueErrors."""
    from regional_knowledge.bge_queue import MAX_STARTUP_FAILURES
    now=[1000.]
    queue=BgeQueue(tmp_path/'legacy-controller.sqlite',clock=lambda:now[0])
    job=queue.enqueue('indexer','owned-document',['synthetic passage'],kind='document')
    with queue.connect(write=False) as db:
        assert tuple(db.execute('select startup_ref,startup_state from control where id=1').fetchone())==(None,None)
    run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    for index in range(MAX_STARTUP_FAILURES):
        assert queue.launch_claim(run,provider_ref='owner/stable-slug') is not None
        queue.launch_record(run,'owner/stable-slug',failed=True)  # exact known NOT SENT
        queue.launch_record(run,'owner/stable-slug',failed=True)  # replay is inert
        with queue.connect(write=False) as db:
            control=db.execute('select startup_ref,startup_state,startup_failures from control where id=1').fetchone()
        assert control['startup_failures']==index+1
        assert control['startup_ref'] is None
        if index<MAX_STARTUP_FAILURES-1:
            assert control['startup_state'] is None
            now[0]+=61
            ready=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
            assert ready!=run
            run=ready
        else:
            assert control['startup_state']=='blocked'
    assert queue.status()['state']=='blocked'
    assert queue.result('indexer',job)['state']=='pending'
    queue.enqueue('indexer','second',['another safe passage'],kind='document')
    now[0]+=600
    assert len(queue.maintenance())==MAX_STARTUP_FAILURES
    assert not any(x['status']=='starting' for x in queue.maintenance())


def test_authorized_cold_search_cannot_reset_legacy_failure_budget(tmp_path):
    from regional_knowledge.bge_queue import MAX_STARTUP_FAILURES
    now=[1000.]
    queue=BgeQueue(tmp_path/'legacy-search.sqlite',clock=lambda:now[0])
    queue.enqueue('indexer','owned-document',['synthetic passage'],kind='document')
    run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    for _ in range(MAX_STARTUP_FAILURES-1):
        assert queue.launch_claim(run,provider_ref='owner/stable-slug') is not None
        queue.launch_record(run,'owner/stable-slug',failed=True)
        now[0]+=61
        run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    first=queue.request_startup()
    assert first['status']=='pending' and first['startup_ref']
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==MAX_STARTUP_FAILURES-1
    queue.launch_claim(run,provider_ref='owner/stable-slug')
    queue.launch_record(run,'owner/stable-slug',failed=True)
    assert queue.request_startup()['status']=='unavailable'
    assert queue.startup_status(first['startup_ref'])['reason']=='startup_retry_exhausted'


def test_startup_failure_cooldown_preserves_jobs_and_does_not_relaunch_ambiguously(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    job=queue.enqueue('actor','one',['query']);run,token=run_details(queue)
    assert queue.launch_claim(run)==token and queue.launch_claim(run) is None
    queue.launch_record(run,'owner/slug',failed=True)
    assert queue.status()['state']=='failed' and queue.result('actor',job)['state']=='pending'
    now[0]+=59;queue.maintenance();assert queue.status()['state']=='failed'
    now[0]+=2;queue.maintenance();assert queue.status()['state']=='starting'
    successor,_=run_details(queue);assert successor!=run

def test_fusion_preserves_independent_alias_diagnostics_and_deduplicates():
    from regional_knowledge.rank_fusion import fuse
    rows=[{'chunk_id':'a','branch':'bge','rank':1},{'chunk_id':'b','branch':'e5','rank':1},{'chunk_id':'b','branch':'bge','rank':2},{'chunk_id':'a','branch':'exact_historical_alias','rank':1,'matched_alias':'Königsberg'}]
    ids,signals=fuse(rows+rows,['bge','e5'])
    assert ids==['b','a'] and len(signals['b'])==2
    ids,signals=fuse(rows,['bge','exact_historical_alias'])
    assert ids[0]=='a' and signals['a'][1]['matched_alias']=='Königsberg'

@pytest.mark.asyncio
async def test_cold_main_returns_fast_evidence_and_actor_bound_pending_job(monkeypatch,tmp_path):
    from regional_knowledge.multilingual_retrieval import main_search
    from regional_knowledge.contracts import Principal,SearchOutput,SearchResult
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(tmp_path/'queue.sqlite'))
    class Backend:
        async def search(self,query,principal,**kwargs):
            assert kwargs['_fast_only'] is True
            return SearchOutput(results=[SearchResult(id='safe',title='title',url='knowledge://evidence/safe')],retrieval_mode='fast_e5')
    actor=Principal(subject='actor',client_id='test',issuer='test',access_token='test')
    result=await main_search(Backend(),'query',actor)
    assert result.main_state=='starting' and result.results[0].id=='safe'
    replay=await main_search(Backend(),'query',actor)
    assert replay.main_job_id==result.main_job_id
    queue=BgeQueue(tmp_path/'queue.sqlite')
    assert queue.result('actor',result.main_job_id)['state']=='pending'
    with queue.connect() as db:
        assert db.execute("select count(*) from jobs where kind='query'").fetchone()[0]==1
    with pytest.raises(PermissionError):queue.result('other',result.main_job_id)
    with pytest.raises(ValueError):await main_search(Backend(),'changed',actor,main_job_id=result.main_job_id)

def test_done_document_job_can_retire_only_after_completion(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    doc=queue.enqueue('actor','doc-retire',['passage'],kind='document')
    assert queue.retire_done_documents('actor',[doc])==0
    run,token=run_details(queue);queue.heartbeat(run,token,ready=True)
    job=queue.claim(run,token);assert job['id']==doc
    queue.complete(run,token,doc,job['claim'],SPACE,[[1]+[0]*1023],{})
    assert queue.retire_done_documents('other',[doc])==0
    assert queue.retire_done_documents('actor',[doc])==1
    with pytest.raises(PermissionError):queue.result('actor',doc)


def test_done_query_results_expire_after_bounded_retention(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    query=queue.enqueue('actor','query-retention',['query'])
    run,token=run_details(queue);queue.heartbeat(run,token,ready=True)
    job=queue.claim(run,token);queue.complete(run,token,query,job['claim'],SPACE,[[1]+[0]*1023],{})
    now[0]+=DONE_QUERY_RETENTION_SECONDS-1;queue.maintenance()
    assert queue.result('actor',query)['state']=='done'
    now[0]+=2;queue.maintenance()
    with pytest.raises(PermissionError):queue.result('actor',query)

def test_done_audit_document_results_expire_without_touching_production_documents(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    audit=queue.enqueue('actor','audit-doc',['passage'],kind='document',identity={'audit':'benchmark','material_sha256':'a'*64,'space':SPACE})
    production=queue.enqueue('actor','prod-doc',['passage'],kind='document',identity={'chunk_id':'chunk','revision':1,'text_sha256':'b'*64})
    run,token=run_details(queue);queue.heartbeat(run,token,ready=True)
    claimed=set()
    for _ in range(2):
        job=queue.claim(run,token);claimed.add(job['id'])
        queue.complete(run,token,job['id'],job['claim'],SPACE,[[1]+[0]*1023],{})
    assert claimed=={audit,production}
    now[0]+=DONE_AUDIT_RETENTION_SECONDS+1;queue.maintenance()
    with pytest.raises(PermissionError):queue.result('actor',audit)
    assert queue.result('actor',production)['state']=='done'


def test_live_heartbeat_keeps_native_durable_ack_claims_until_settled(tmp_path):
    now=[1000.];q=BgeQueue(tmp_path/'native-ack.sqlite',clock=lambda:now[0])
    run=q.request_startup()['run_id'];token=q.launch_claim(run,provider_ref='fixture/stable')
    q.heartbeat(run,token,ready=True)
    with q.connect() as db:
        for kind in ('source_command','recovery_phase','query'):
            db.execute("insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,run_id,claim,lease_until) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                (kind,'fixture',kind,kind,'[]','{}','claimed',now[0],now[0],run,kind+'-claim',1010.))
    now[0]=1005.;q.heartbeat(run,token)
    now[0]=1011.;q.maintenance()
    assert q._document_claim_waiting('source_command','source_command-claim')
    assert q._document_claim_waiting('recovery_phase','recovery_phase-claim')
    with q.connect(write=False) as db:
        assert db.execute("select state from jobs where id='query'").fetchone()[0]=='pending'
    # A stopped heartbeat still leaves a finite ordinary recovery lease.
    now[0]=1190.;q.maintenance()
    with q.connect(write=False) as db:
        assert {r[0] for r in db.execute('select state from jobs')}=={'pending'}
