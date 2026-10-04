"""Controlled acceptance on the owned production BGE queue and real notebooks.

Lease/lifetime boundaries use explicitly accelerated stored timestamps. Provider
unavailability/start failure are injected, not claims of an actual Kaggle outage.
Worker fencing, cold E5 evidence, notebook succession and job recovery are real.
No credentials/private payloads enter the report. Controller is restored finally.
"""
import argparse,asyncio,json,os,subprocess,time,uuid
from pathlib import Path
import httpx
from operator_env import load_service_env
from accept_fast_e5 import principal,distribution,resources
from regional_knowledge.bge_queue import BgeQueue,ROTATE_SECONDS
from regional_knowledge.bge_contract import SPACE
from regional_knowledge.multilingual_retrieval import wait_result
from regional_knowledge.supabase_backend import backend_from_env

CONTROLLER='regional-knowledge-bge-controller.service'
def service(action):subprocess.run(['systemctl','--user',action,CONTROLLER],check=True)
def current(queue):
    with queue.connect() as db:return dict(db.execute('select r.* from runs r join control c on r.id=c.current_run').fetchone())
def token(queue,run):return (queue.path.parent/'bge-worker-credentials'/run['id']).read_text()

async def run(args):
    load_service_env();queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    if str(queue.path)!='/home/dev/.local/state/regional-knowledge-base/bge/queue.sqlite':raise RuntimeError('owned production queue required')
    if queue.status()['state']!='ready' or queue.status()['queue_depth']:raise RuntimeError('ready idle queue required before controlled acceptance')
    backend=backend_from_env();actor=principal();cases=json.loads(args.fixture.read_text())['cases'];output={'controlled_operator_acceptance':True,'accelerated_boundaries':True,'external_embedding_api_calls':0,'e5_before':resources(),'steps':{}}
    def save(name,data):output['steps'][name]=data;args.output.write_text(json.dumps(output,indent=2));print(json.dumps({'step':name,**data}),flush=True)
    async def ready(seconds=240):
        start=time.monotonic()
        while time.monotonic()-start<seconds:
            if queue.status()['state']=='ready':return time.monotonic()-start
            await asyncio.sleep(.25)
        raise TimeoutError('controlled acceptance worker not ready')
    async def cold_requests(count):
        async def one(i):
            start=time.monotonic();result=await backend.search_evidence(cases[i%len(cases)]['query'],actor)
            return {'seconds':time.monotonic()-start,'mode':result.retrieval_mode,'state':result.main_state,'evidence_count':len(result.evidence),'job_id':result.main_job_id,**result.timings}
        rows=await asyncio.gather(*(one(i) for i in range(count)))
        return {'requests':count,'fast_e5':sum(row['mode']=='fast_e5' for row in rows),'evidence_responses':sum(row['evidence_count']>0 for row in rows),'states':{state:sum(row['state']==state for row in rows) for state in ('starting','pending','unavailable','ready')},'initial_seconds':distribution([row['seconds'] for row in rows]),'e5_encoder_seconds':distribution([row['encoder_seconds'] for row in rows if 'encoder_seconds' in row]),'e5_queue_seconds':distribution([row['queue_wait_seconds'] for row in rows if 'queue_wait_seconds' in row]),'rows':rows}
    async def drain(ids):
        for job in ids:
            if await wait_result(queue,actor.subject,job,180) is None:raise TimeoutError('durable acceptance job pending')
    try:
        # Real successor warms beside the serving worker. Queue more work than
        # one cold startup can drain to verify pending jobs across the handoff.
        old=current(queue);old_token=token(queue,old);start=time.monotonic()
        with queue.connect() as db:db.execute('update runs set started=?,useful=?,heartbeat=? where id=?',(time.time()-ROTATE_SECONDS-1,time.time(),time.time(),old['id']))
        queue.maintenance()
        with queue.connect() as db:
            successor=db.execute('select successor from control').fetchone()[0]
            active=db.execute("select count(*) from runs where status in ('ready','starting')").fetchone()[0]
        serving=await backend.search_evidence(cases[0]['query'],actor)
        jobs=[queue.enqueue(actor.subject,'rotation:'+str(uuid.uuid4()),[cases[i%len(cases)]['query']],identity={'acceptance':'rotation'}) for i in range(120)]
        peak=queue.status()['queue_depth'];max_active=active
        while current(queue)['id']!=successor:
            if time.monotonic()-start>240:raise TimeoutError('successor handoff')
            with queue.connect() as db:max_active=max(max_active,db.execute("select count(*) from runs where status in ('ready','starting')").fetchone()[0])
            peak=max(peak,queue.status()['queue_depth']);await asyncio.sleep(.2)
        at_handoff=queue.status()['queue_depth'];new=current(queue)
        await drain(jobs);queue.maintenance()
        save('planned_rotation',{'old_run':old['id'],'successor_run':successor,'artificial_age_seconds':ROTATE_SECONDS+1,'actual_previous_run_age_seconds':time.time()-old['started'],'serving_during_warmup':serving.main_state=='ready','serving_mode':serving.retrieval_mode,'max_serving_plus_warming':max_active,'queued_jobs':len(jobs),'queue_peak':peak,'pending_at_handoff':at_handoff,'handoff_seconds':new['ready_at']-new['started'],'all_jobs_done':all(queue.result(actor.subject,job)['state']=='done' for job in jobs),'drained_old_fenced':True,'successor_diagnostics':json.loads(new['diagnostics'])})
        async with httpx.AsyncClient(timeout=15,trust_env=False) as client:
            revoked=await client.post('https://knowledge.kenigevents.ru/bge-worker/heartbeat',headers={'Authorization':'Bearer '+old_token},json={'run_id':old['id'],'space':SPACE})
            worker_mcp=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token(queue,new),'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':1,'method':'tools/list','params':{}})
        save('credential_boundaries',{'old_worker_heartbeat_http':revoked.status_code,'worker_credential_mcp_http':worker_mcp.status_code})
        if revoked.status_code!=403 or worker_mcp.status_code!=401:raise RuntimeError('worker credential boundary failed')
        # Fence a real claimed job before its worker can install the result.
        old=current(queue);old_token=token(queue,old)
        lost_job=queue.enqueue(actor.subject,'loss:'+str(uuid.uuid4()),[cases[0]['query']],identity={'acceptance':'loss'})
        started=time.monotonic();claimed=None
        while time.monotonic()-started<10:
            with queue.connect() as db:
                row=db.execute('select * from jobs where id=?',(lost_job,)).fetchone()
                if row['state']=='claimed':
                    claimed=dict(row);db.execute('update runs set heartbeat=? where id=?',(time.time()-121,old['id']));queue._expire(db,time.time());break
            await asyncio.sleep(.01)
        if claimed is None:raise RuntimeError('could not inject loss during actual claim')
        pending=queue.result(actor.subject,lost_job)['state'];cold=await cold_requests(5)
        wait=await ready();await drain([lost_job]+[row['job_id'] for row in cold['rows'] if row['job_id']])
        vector=json.loads(args.vectors.read_text())[cases[0]['id']]['bge']
        async with httpx.AsyncClient(timeout=15,trust_env=False) as client:
            late=await client.post('https://knowledge.kenigevents.ru/bge-worker/complete',headers={'Authorization':'Bearer '+old_token},json={'run_id':old['id'],'job_id':lost_job,'claim':claimed['claim'],'space':SPACE,'vectors':[vector],'timings':{'encoder_seconds':1}})
        with queue.connect() as db:attempts=db.execute('select attempt from jobs where id=?',(lost_job,)).fetchone()[0]
        save('mid_job_loss',{'injection':'heartbeat backdated beyond 120s while actual job claimed','requeued_state':pending,'cold_fast':cold,'replacement_ready_wait_seconds':wait,'attempts':attempts,'job_done':queue.result(actor.subject,lost_job)['state']=='done','late_result_http':late.status_code})
        if pending!='pending' or attempts<2 or late.status_code!=403:raise RuntimeError('loss fencing/recovery failed')
        # Actual broker rejects a live notebook at the accelerated idle boundary.
        service('stop');old=current(queue);old_token=token(queue,old)
        with queue.connect() as db:db.execute('update runs set useful=?,heartbeat=? where id=?',(time.time()-1801,time.time(),old['id']))
        stopped=queue.status()
        async with httpx.AsyncClient(timeout=15,trust_env=False) as client:
            idle=await client.post('https://knowledge.kenigevents.ru/bge-worker/heartbeat',headers={'Authorization':'Bearer '+old_token},json={'run_id':old['id'],'space':SPACE})
        save('idle_expiry',{'artificial_idle_seconds':1801,'state':stopped['state'],'queue_depth':stopped['queue_depth'],'live_notebook_heartbeat_http':idle.status_code,'heartbeat_did_not_extend_lease':stopped['state']=='stopped'})
        with queue.connect() as db:before=db.execute('select count(*) from runs').fetchone()[0]
        cold=await cold_requests(10)
        with queue.connect() as db:
            created=db.execute('select count(*) from runs').fetchone()[0]-before
            run=dict(db.execute('select r.* from runs r join control c on r.id=c.current_run').fetchone())
        save('provider_unavailable_duplicate_start',{'injection':'controller stopped to deny provider dispatch; no real provider outage claimed','new_runs_for_10_demands':created,'launch_state':run['launch_state'],'cold_fast':cold})
        if created!=1 or cold['fast_e5']!=10 or idle.status_code!=403:raise RuntimeError('cold start/lease acceptance failed')
        queue.launch_claim(run['id']);queue.launch_record(run['id'],'controlled-startup-failure',failed=True)
        failed=await cold_requests(1)
        save('startup_failure',{'injection':'claimed dispatch marked failed before provider creation','state':queue.status()['state'],'jobs_retained':queue.status()['queue_depth'],'fast':failed})
        with queue.connect() as db:db.execute('update runs set heartbeat=? where id=?',(time.time()-61,run['id']))
        queue.maintenance();service('start');wait=await ready();await drain([row['job_id'] for row in cold['rows']+failed['rows'] if row['job_id']])
        final=current(queue)
        for job in jobs[:3]:
            row=queue.result(actor.subject,job)
            if row['state']!='done':raise RuntimeError('rotation completed job lost')
        with queue.connect() as db:replay=dict(db.execute('select * from jobs where id=?',(jobs[0],)).fetchone())
        same=queue.enqueue(actor.subject,replay['idempotency'],json.loads(replay['texts']),identity=json.loads(replay['identity']))
        with queue.connect() as db:after=dict(db.execute('select * from jobs where id=?',(same,)).fetchone())
        save('completed_query_replay',{'same_job':same==jobs[0],'still_done':after['state']=='done','attempts_unchanged':after['attempt']==replay['attempt'],'result_unchanged':after['result']==replay['result']})
        save('final_recovery',{'ready_wait_seconds':wait,'state':queue.status()['state'],'queue_depth':queue.status()['queue_depth'],'run_id':final['id'],'cpu_diagnostics':json.loads(final['diagnostics']),'all_cold_jobs_done':True})
        output['e5_after']=resources();output['e5_pid_unchanged']=output['e5_before']['MainPID']==output['e5_after']['MainPID'];args.output.write_text(json.dumps(output,indent=2))
    finally:service('start');await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('fixture',type=Path);parser.add_argument('vectors',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--execute',action='store_true',required=True);asyncio.run(run(parser.parse_args()))
