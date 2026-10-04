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
