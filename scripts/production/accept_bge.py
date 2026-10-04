"""Actual owner-ACL search/fetch acceptance through the durable Kaggle queue.

Only aggregate numbers belong in Git; per-request evidence is private. A pending
initial response is measured separately from the eventual main result.
"""
import argparse,asyncio,json,os,time
from pathlib import Path
from operator_env import load_service_env
from accept_fast_e5 import principal,distribution,resources
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.supabase_backend import backend_from_env

async def run(args):
    load_service_env();os.environ['RKB_BGE_ENABLED']='1'
    backend=backend_from_env();actor=principal();queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    cases=json.loads(args.fixture.read_text())['cases']
    output={'path':'production database/object store with ordinary owner actor RLS; real Kaggle CPU queue', 'warm_mode':os.environ.get('RKB_BGE_WARM_MODE','bge_lexical'),'external_embedding_api_calls':0,'levels':{},'e5_before':resources()}
    try:
        for users in args.users:
            rows=[]
            async def request(index):
                query=cases[index%len(cases)]['query'];start=time.monotonic()
                try:
                    initial=await backend.search_evidence(query,actor)
                    initial_seconds=time.monotonic()-start;main=initial;polls=0
                    while main.main_state!='ready' and main.main_job_id and time.monotonic()-start<args.deadline:
                        await asyncio.sleep(.1);polls+=1
                        main=await backend.search_evidence(query,actor,main_job_id=main.main_job_id)
                    return {'ok':main.main_state=='ready','initial_mode':initial.retrieval_mode,'initial_state':initial.main_state,'initial_evidence':len(initial.evidence),'initial_seconds':initial_seconds,'main_seconds':time.monotonic()-start,'mode':main.retrieval_mode,'state':main.main_state,'polls':polls,'evidence_count':len(main.evidence),**main.timings}
                except Exception as error:return {'ok':False,'error_type':type(error).__name__,'main_seconds':time.monotonic()-start}
            before=queue.status();peak=before['queue_depth'];stop=asyncio.Event()
            async def sample():
                nonlocal peak
                while not stop.is_set():
                    peak=max(peak,(await asyncio.to_thread(queue.status))['queue_depth']);await asyncio.sleep(.1)
            sampler=asyncio.create_task(sample())
            try:
                for start in range(0,args.requests,users):rows.extend(await asyncio.gather(*(request(i) for i in range(start,min(start+users,args.requests)))))
            finally:stop.set();await sampler
            level={'requests':len(rows),'failures':sum(not row['ok'] for row in rows),'initial_states':{state:sum(row.get('initial_state')==state for row in rows) for state in ('ready','starting','pending','unavailable')},'initial_fast':sum(row.get('initial_mode')=='fast_e5' for row in rows),'queue_peak':peak,'queue_before':before,'queue_after':queue.status(),'seconds':{key:distribution([row[key] for row in rows if key in row]) for key in ('initial_seconds','main_seconds','bge_queue_seconds','bge_encoder_seconds','queue_wait_seconds','encoder_seconds','database_seconds','fusion_metadata_seconds','hydration_seconds','total_seconds')},'rows':rows}
            output['levels'][str(users)]=level;args.output.write_text(json.dumps(output,indent=2));print(json.dumps({'users':users,**{key:value for key,value in level.items() if key!='rows'}}),flush=True)
        output['e5_after']=resources();args.output.write_text(json.dumps(output,indent=2))
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('fixture',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--requests',type=int,default=30);parser.add_argument('--users',type=int,nargs='+',default=[1,5,10]);parser.add_argument('--deadline',type=int,default=120);asyncio.run(run(parser.parse_args()))
