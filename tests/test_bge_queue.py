import concurrent.futures
import pytest
from regional_knowledge.bge_queue import BgeQueue,IDLE_SECONDS,ROTATE_SECONDS
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
    queue=BgeQueue(tmp_path/'queue.sqlite')
    assert queue.result('actor',result.main_job_id)['state']=='pending'
    with pytest.raises(PermissionError):queue.result('other',result.main_job_id)
    with pytest.raises(ValueError):await main_search(Backend(),'changed',actor,main_job_id=result.main_job_id)
