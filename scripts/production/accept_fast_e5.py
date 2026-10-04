"""Operator acceptance against production DB/ACL/object store and local encoder.

Private per-request evidence stays in the managed output directory; only aggregate
numbers belong in the public report. Does not change documents or permissions.
"""
import argparse, asyncio, json, math, os, time
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.contracts import Principal
from regional_knowledge.supabase_backend import backend_from_env

def distribution(values):
    values=sorted(values)
    return {key:values[min(len(values)-1,math.ceil(p*len(values))-1)] for key,p in [('p50',.5),('p95',.95),('max',1)]} if values else {}

def principal():
    return Principal(subject=os.environ['RKB_OWNER_SUBJECT'],client_id=os.environ['RKB_OAUTH_CLIENT_ID'],issuer=os.environ['RKB_AUTH_ISSUER'],access_token='operator-acceptance-local-actor-bridge')

def resources():
    import subprocess
    fields=subprocess.check_output(['systemctl','--user','show','regional-knowledge-e5.service','-p','MainPID','-p','MemoryCurrent','-p','MemoryPeak','-p','ControlGroup'],text=True)
    result=dict(line.split('=',1) for line in fields.splitlines())
    processes=0
    for path in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            args=path.read_bytes().rstrip(b'\0').split(b'\0')
            processes+=args[-2:]==[b'-m',b'regional_knowledge.e5_service']
        except (FileNotFoundError,PermissionError,ProcessLookupError):pass
    result['actual_encoder_processes']=processes
    pid=int(result['MainPID'])
    if pid:
        result['process_status']={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in Path(f'/proc/{pid}/status').read_text().splitlines() if line.startswith(('VmRSS:','VmHWM:','Threads:'))}
        result['pss_kib']=sum(int(line.split()[1]) for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines() if line.startswith('Pss:'))
        cg=Path('/sys/fs/cgroup'+result['ControlGroup'])
        result['cgroup']={name:(cg/name).read_text().strip() for name in ('cpu.max','cpu.stat','memory.max','memory.swap.max','memory.events')}
    return result

async def run(args):
    load_service_env()
    if args.pool_max:os.environ['RKB_DB_POOL_MAX']=str(args.pool_max)
    backend=backend_from_env();actor=principal()
    fixture=json.loads(args.fixture.read_text());questions=fixture['questions']
    output={'path':'production backend search_evidence; owner actor bridge with normal rkb_app ACL; OAuth HTTP smoke measured separately','db_pool_max':os.environ.get('RKB_DB_POOL_MAX','6'),'external_embedding_calls':0,'levels':{},'resources_before':resources()}
    try:
        await backend.search_evidence(questions[0]['query'],actor)
        for users in (1,5,10):
            rows=[];before=resources()
            async def request(index,gate):
                await gate.wait();started=time.monotonic()
                try:
                    result=await backend.search_evidence(questions[index%len(questions)]['query'],actor)
                    return {'index':index,'ok':True,'mode':result.retrieval_mode,'evidence_count':len(result.evidence),**result.timings,'wall_seconds':time.monotonic()-started}
                except Exception as error:
                    return {'index':index,'ok':False,'error_type':type(error).__name__,'wall_seconds':time.monotonic()-started}
            for start in range(0,args.requests,users):
                gate=asyncio.Event();tasks=[asyncio.create_task(request(i,gate)) for i in range(start,min(start+users,args.requests))]
                gate.set();rows.extend(await asyncio.gather(*tasks))
            output['levels'][str(users)]={'requests':len(rows),'failures':sum(not row['ok'] for row in rows),'modes':{mode:sum(row.get('mode')==mode for row in rows) for mode in ('fast_e5','lexical_only')},'seconds':{key:distribution([row[key] for row in rows if key in row]) for key in ('queue_wait_seconds','encoder_seconds','database_seconds','hydration_seconds','total_seconds','wall_seconds')},'resources_before':before,'resources_after':resources(),'rows':rows}
            args.output.write_text(json.dumps(output,indent=2));print(json.dumps({'users':users,**{k:v for k,v in output['levels'][str(users)].items() if k not in ('rows','resources_before','resources_after')}}),flush=True)
        regression=[]
        for question in questions:
            result=await backend.search(question['query'],actor,match_count=20)
            ids=[item.id for item in result.results];groups=question.get('evidence_groups',[])
            ranks=[min((ids.index(item)+1 for item in group if item in ids),default=0) for group in groups]
            regression.append({'id':question['id'],'unanswerable':question.get('unanswerable',False),'ranks':ranks,'ids':ids,'mode':result.retrieval_mode})
        answerable=[row for row in regression if not row['unanswerable']]
        output['regression']={'questions':regression,'answerable':len(answerable),'recall':{str(k):sum(sum(0<rank<=k for rank in row['ranks'])/len(row['ranks']) for row in answerable)/len(answerable) for k in (1,5,10)},'mrr':sum(max((1/rank if rank else 0 for rank in row['ranks']),default=0) for row in answerable)/len(answerable),'multi_complete_at10':sum(all(0<rank<=10 for rank in row['ranks']) for row in answerable if len(row['ranks'])>1),'multi_count':sum(len(row['ranks'])>1 for row in answerable)}
        output['resources_after']=resources();args.output.write_text(json.dumps(output,indent=2))
    finally:await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('fixture',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--requests',type=int,default=60);parser.add_argument('--pool-max',type=int)
    asyncio.run(run(parser.parse_args()))
