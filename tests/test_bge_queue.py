import concurrent.futures
import json
import pytest
from regional_knowledge.bge_queue import BgeQueue,IDLE_SECONDS,ROTATE_SECONDS,DONE_QUERY_RETENTION_SECONDS,DONE_AUDIT_RETENTION_SECONDS
from regional_knowledge.bge_contract import SPACE,validate_vector

def run_details(queue):
    with queue.connect() as db:
        run=dict(db.execute("select * from runs where status='starting' order by started desc limit 1").fetchone())
    return run['id'],(queue.path.parent/'bge-worker-credentials'/run['id']).read_text()


@pytest.mark.parametrize('missing',[
    ('bge_retention',),('bge_assigned',),('bge_retention','bge_assigned'),
])
def test_existing_queue_upgrades_missing_maintenance_indexes_without_changing_jobs(tmp_path,missing):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    job=queue.enqueue('actor','retained-command',['accepted input'],kind='document')
    with queue.connect() as db:
        before=dict(db.execute('SELECT * FROM jobs WHERE id=?',(job,)).fetchone())
        for name in missing:db.execute('DROP INDEX '+name)
    reopened=BgeQueue(queue.path)
    with reopened.connect(write=False) as db:
        indexes={row['name'] for row in db.execute('PRAGMA index_list(jobs)')}
        assert {'bge_retention','bge_assigned'}<=indexes
        assert dict(db.execute('SELECT * FROM jobs WHERE id=?',(job,)).fetchone())==before
    assert reopened._schema_current()


@pytest.mark.parametrize('sql,arguments,index,bounds',[
    ("DELETE FROM jobs WHERE kind='query' AND state='done' AND updated<?",
        (1000.,),'bge_retention',('state=?','kind=?','updated<?')),
    ("DELETE FROM jobs WHERE kind='document' AND state='done' AND updated<? "
        "AND json_extract(identity,'$.audit') IS NOT NULL",
        (1000.,),'bge_retention',('state=?','kind=?','updated<?')),
    ("SELECT 1 FROM jobs WHERE run_id=? AND state='claimed' "
        "AND kind IN ('source_command','recovery_phase') LIMIT 1",
        ('retained-run',),'bge_assigned',('run_id=?','state=?')),
])
def test_maintenance_plans_bound_retention_time_and_assigned_run(tmp_path,sql,arguments,index,bounds):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    with queue.connect() as db:
        # Include the serving admission index, which previously won these
        # lookups while leaving retention time/run identity as row filters.
        db.execute('CREATE INDEX bge_admission_actor ON jobs(state,kind,actor,created,id)')
    with queue.connect(write=False) as db:
        plan=' '.join(row['detail'] for row in db.execute('EXPLAIN QUERY PLAN '+sql,arguments))
    assert 'SEARCH jobs USING INDEX '+index in plan or 'SEARCH jobs USING COVERING INDEX '+index in plan
    assert all(bound in plan for bound in bounds),plan

def test_atomic_start_and_durable_replayed_job(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        ids=list(pool.map(lambda _:queue.enqueue('actor','same',['query']),range(10)))
    assert len(set(ids))==1
    with queue.connect() as db:assert db.execute('select count(*) from runs').fetchone()[0]==1
    assert BgeQueue(queue.path).result('actor',ids[0])['state']=='pending'
    with pytest.raises(PermissionError):queue.result('other',ids[0])
    with pytest.raises(ValueError):queue.enqueue('actor','same',['changed'])


@pytest.mark.parametrize('transition',['lease','failure','drain'])
def test_retired_run_finishes_maintenance_bookkeeping_without_inventing_native_result(tmp_path,transition):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    ordinary=queue.enqueue('actor','ordinary',['ordinary work'],kind='document')
    run,_=run_details(queue)
    with queue.connect() as db:
        for name,state in [('pending','pending'),('claimed','pending' if transition=='drain' else 'claimed')]:
            db.execute('INSERT INTO jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,run_id,claim,lease_until) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(name,'maintenance:'+run,name,'recovery_phase','[]','{"operation":"unknown"}',state,
                    1000,1000,run,'retained-claim',2000))
    if transition=='lease':now[0]+=901;queue.maintenance()
    elif transition=='failure':queue.launch_record(run,'owner/notebook',failed=True)
    else:
        with queue.connect() as db:db.execute("UPDATE runs SET status='draining' WHERE id=?",(run,))
        queue.maintenance()
    with queue.connect(write=False) as db:
        rows=db.execute("SELECT state,result,identity,run_id,claim FROM jobs WHERE kind='recovery_phase'").fetchall()
        assert len(rows)==2
        assert all((r['state'],json.loads(r['result']),r['identity'],r['run_id'],r['claim'])==
            ('done',{'value':{'error':'run_expired'}},'{"operation":"unknown"}',run,'retained-claim') for r in rows)
    assert queue.result('actor',ordinary)['state']=='pending'


def test_planned_idle_drain_preserves_provider_reservation_and_pending_work(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite');job=queue.enqueue('actor','work',['query'])
    run,token=run_details(queue)
    queue.launch_claim(run,provider_ref='owner/notebook')
    queue.launch_record(run,'owner/notebook',provider_version=112)
    queue.heartbeat(run,token,ready=True)
    with pytest.raises(PermissionError):
        queue.request_idle_drain(run,provider_ref='owner/notebook',provider_version=113)
    claimed=queue.claim(run,token)
    assert not queue.request_idle_drain(run,provider_ref='owner/notebook',provider_version=112)
    queue.complete(run,token,job,claimed['claim'],SPACE,[[1]+[0]*1023],{})
    pending=queue.enqueue('actor','next',['next query'])
    assert queue.request_idle_drain(run,provider_ref='owner/notebook',provider_version=112)
    assert queue.claim(run,token) is None
    queue.maintenance()
    with queue.connect(write=False) as db:
        row=db.execute('SELECT status,launch_state FROM runs WHERE id=?',(run,)).fetchone()
        assert tuple(row)==('draining','dispatched')
        assert not db.execute('SELECT 1 FROM bge_provider_terminal_receipts WHERE run_id=?',(run,)).fetchone()
    assert queue.result('actor',pending)['state']=='pending'
    assert queue.result('actor',job)['state']=='done'


def test_planned_drain_keeps_attached_current_channel_for_exact_stop(tmp_path):
    now=[1000.];q=BgeQueue(tmp_path/'planned-stop.sqlite',clock=lambda:now[0])
    q.request_startup();run,token=run_details(q)
    q.launch_claim(run,provider_ref='owner/fixture')
    nonce=q.prepare_channel(run)
    q.launch_record(run,'owner/fixture',provider_version=12)
    generation=q.attach_channel(run,token,nonce,'same-runtime')['connection_generation']
    q.heartbeat(run,token,ready=True,connection_generation=generation)
    assert not q.stop_eligible(run,token,generation)
    assert q.request_idle_drain(run,provider_ref='owner/fixture',provider_version=12)
    q.maintenance()
    assert q.channel_status(run,token,generation)['state']=='draining'
    assert q.stop_eligible(run,token,generation)
    q.detach_channel(run,token,generation)
    assert q.needs_dispatched_terminal_probe(run)
    now[0]+=1000  # Neither socket loss nor heartbeat expiry proves compute ended.
    q.maintenance()
    with q.connect(write=False) as db:
        assert db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]==run
        assert db.execute('SELECT status FROM runs WHERE id=?',(run,)).fetchone()[0]=='draining'

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


@pytest.mark.parametrize('claimed_document',[False,True])
@pytest.mark.parametrize('first_observer',['maintenance','channel_status','stop_eligible'])
def test_live_native_idle_timeout_drains_to_stop_without_losing_claim(tmp_path,claimed_document,first_observer):
    now=[1000.];q=BgeQueue(tmp_path/'idle-stop.sqlite',clock=lambda:now[0])
    q.request_startup();run,token=run_details(q)
    q.launch_claim(run,provider_ref='owner/fixture')
    nonce=q.prepare_channel(run)
    q.launch_record(run,'owner/fixture',provider_version=165)
    generation=q.attach_channel(run,token,nonce,'idle-runtime')['connection_generation']
    q.heartbeat(run,token,ready=True,connection_generation=generation)
    job=None
    if claimed_document:
        ident=q.enqueue('actor','long-document',['fixture'],kind='document')
        job=q.claim(run,token,connection_generation=generation)
        assert job['id']==ident
    for _ in range(IDLE_SECONDS//60-1):
        now[0]+=60;q.heartbeat(run,token,connection_generation=generation)
    now[0]+=60
    if first_observer=='maintenance':q.maintenance()
    elif first_observer=='channel_status':q.channel_status(run,token,generation)
    else:assert q.stop_eligible(run,token,generation) is (not claimed_document)
    assert q.channel_status(run,token,generation)['state']=='draining'
    assert q.channel_status(run,token,generation)['ready'] is False
    assert q.claim(run,token,connection_generation=generation) is None
    assert q.stop_eligible(run,token,generation) is (not claimed_document)
    if job:
        assert q.result('actor',ident)['state']=='claimed'
        q.complete(run,token,ident,job['claim'],SPACE,[[1]+[0]*1023],{},connection_generation=generation)
        assert q.result('actor',ident)['state']=='done'
        assert q.stop_eligible(run,token,generation)
    with q.connect(write=False) as db:
        assert db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]==run
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0]==1
        assert not db.execute('SELECT 1 FROM bge_provider_terminal_receipts WHERE run_id=?',(run,)).fetchone()
    # Keeping STOP available does not extend a missing heartbeat or hard age.
    now[0]+=121
    assert not q.stop_eligible(run,token,generation)
    from regional_knowledge.bge_queue import MAX_LIFETIME
    now[0]=1000.+MAX_LIFETIME
    with pytest.raises(PermissionError):q.stop_eligible(run,token,generation)

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
    # Owner policy starts warming by 8:30, leaving at least 30 minutes before nine hours.
    assert 8 * 3600 <= ROTATE_SECONDS <= 8 * 3600 + 30 * 60
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
    assert tuple(state)==(None,3)


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
            assert control['startup_state'] is None
    assert queue.status()['reason']=='startup_retry_exhausted'
    assert queue.result('indexer',job)['state']=='pending'
    # Replaying the old demand cannot silently consume a fourth launch.
    assert queue.enqueue('indexer','owned-document',['synthetic passage'],kind='document')==job
    now[0]+=600
    assert len(queue.maintenance())==MAX_STARTUP_FAILURES
    assert not any(x['status']=='starting' for x in queue.maintenance())
    # A distinct incoming import renews only its own finite startup budget.
    second=queue.enqueue('indexer','second',['another safe passage'],kind='document')
    assert second!=job
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==0
    new_run=next(x['id'] for x in queue.maintenance() if x['status']=='starting')
    assert new_run!=run
    assert queue.launch_claim(new_run,provider_ref='owner/stable-slug') is not None


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
    assert queue.startup_status(first['startup_ref'])['reason']=='startup_retry_exhausted'
    # A distinct import must close the exhausted search locator too.
    fresh=queue.enqueue('indexer','new-import',['new passage'],kind='document')
    assert queue.result('indexer',fresh)['state']=='pending'
    assert queue.startup_status(first['startup_ref'])['reason']=='startup_ref_stale'
    renewed=queue.request_startup()
    assert renewed['status']=='pending'
    assert renewed['startup_ref']!=first['startup_ref']
    assert renewed['run_id']!=run
    assert queue.startup_status(first['startup_ref'])['reason']=='startup_ref_stale'
    with queue.connect(write=False) as db:
        assert db.execute('select startup_failures from control where id=1').fetchone()[0]==0
    assert queue.launch_claim(renewed['run_id'],provider_ref='owner/stable-slug') is not None


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
        for kind in ('document','source_command','recovery_phase','query'):
            db.execute("insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,run_id,claim,lease_until) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                (kind,'fixture',kind,kind,'[]','{}','claimed',now[0],now[0],run,kind+'-claim',1010.))
    now[0]=1005.;q.heartbeat(run,token)
    now[0]=1011.;q.maintenance()
    assert q._document_claim_waiting('source_command','source_command-claim')
    assert q._document_claim_waiting('recovery_phase','recovery_phase-claim')
    assert q._document_claim_waiting('document','document-claim')
    with q.connect(write=False) as db:
        assert db.execute("select state from jobs where id='query'").fetchone()[0]=='pending'
    # A stopped heartbeat still leaves a finite ordinary recovery lease.
    now[0]=1190.;q.maintenance()
    with q.connect(write=False) as db:
        assert {r[0] for r in db.execute('select state from jobs')}=={'pending'}


def test_exact_authenticated_stop_eligibility_old_new_and_nine_hours(tmp_path):
    """Read-only queue predicate, with actual old/new run and claim rows."""
    now = [1000.]
    q = BgeQueue(tmp_path/'stop.sqlite', clock=lambda:now[0])
    with q.connect() as db:
        old = q._new_run(db, now[0])
        db.execute('UPDATE control SET current_run=? WHERE id=1', (old,))
    token = (q.path.parent/'bge-worker-credentials'/old).read_text()
    nonce = q.prepare_channel(old)
    generation = q.attach_channel(old, token, nonce, 'old-incarnation')['connection_generation']
    q.heartbeat(old, token, ready=True, connection_generation=generation)
    assert not q.stop_eligible(old, token, generation)
    with pytest.raises(PermissionError):
        q.stop_eligible(old, 'not-original-token', generation)
    with pytest.raises(PermissionError):
        q.stop_eligible(old, token, generation+1)

    # A queued new version becomes current through the same heartbeat transition.
    with q.connect() as db:
        new = q._new_run(db, now[0])
        db.execute('UPDATE control SET successor=? WHERE id=1', (new,))
    new_token = (q.path.parent/'bge-worker-credentials'/new).read_text()
    new_nonce = q.prepare_channel(new)
    new_generation = q.attach_channel(new,new_token,new_nonce,'new-incarnation')['connection_generation']
    q.heartbeat(new,new_token,ready=True,connection_generation=new_generation)
    assert q.stop_eligible(old, token, generation)
    assert not q.stop_eligible(new,new_token,new_generation)

    # A retained claimed or pending job, including an outcome whose receipt
    # is still unacknowledged, keeps the old worker from receiving STOP.
    with q.connect() as db:
        db.execute("INSERT INTO jobs(id,actor,idempotency,kind,texts,identity,state,created,updated,run_id) "
            "VALUES('stop-job','actor','stop-key','document','[]','{}','claimed',?,?,?)",
            (now[0],now[0],old))
    assert not q.stop_eligible(old,token,generation)
    with q.connect() as db:
        db.execute("UPDATE jobs SET state='pending' WHERE id='stop-job'")
    assert not q.stop_eligible(old,token,generation)
    with q.connect() as db:
        db.execute("DELETE FROM jobs WHERE id='stop-job'")
    assert q.stop_eligible(old,token,generation)

    # The current run is allowed to stop at nine hours, without a successor.
    now[0] = 1000. + 9*3600
    with q.connect() as db:
        db.execute('UPDATE runs SET useful=?,heartbeat=? WHERE id=?',
            (now[0],now[0],new))
    assert q.stop_eligible(new,new_token,new_generation)


def test_request_activity_renews_idle_but_never_worker_heartbeat(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    queue.enqueue('actor','one',['query']);run,token=run_details(queue)
    queue.heartbeat(run,token,ready=True)
    for _ in range(21):
        now[0]+=90
        assert queue.record_request_activity()
        queue.heartbeat(run,token)
    assert queue.status()['lease_remaining_seconds']==IDLE_SECONDS
    now[0]+=121
    assert queue.record_request_activity()
    with pytest.raises(PermissionError):queue.heartbeat(run,token)


def test_execution_clock_starts_at_single_launch_claim_after_preparation(tmp_path):
    now=[1000.];queue=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    run=queue.request_startup()['run_id']
    now[0]+=500
    assert queue.launch_claim(run,provider_ref='fixture/stable')
    with queue.connect(write=False) as db:
        r=db.execute('select * from runs where id=?',(run,)).fetchone()
        assert r['started']==1500.
        assert not queue._run_expired(r,now[0]+899)
        assert queue._run_expired(r,now[0]+900.001)
    now[0]+=100
    assert queue.launch_claim(run,provider_ref='fixture/stable') is None
    with queue.connect(write=False) as db:
        assert db.execute('select started from runs where id=?',(run,)).fetchone()[0]==1500.
