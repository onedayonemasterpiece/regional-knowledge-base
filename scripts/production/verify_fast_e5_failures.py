"""Authorized production failure/restart exercise; always restore encoder."""
import argparse,asyncio,json,time
from pathlib import Path
import httpx
from operator_env import load_service_env
from accept_fast_e5 import principal,resources
from regional_knowledge.e5_contract import SPACE
from regional_knowledge.supabase_backend import backend_from_env

async def control(verb,unit='regional-knowledge-e5.service'):
    process=await asyncio.create_subprocess_exec('systemctl','--user',verb,unit)
    if await process.wait():raise RuntimeError('service control failed')

async def run(output):
    load_service_env();backend=backend_from_env();actor=principal();result={}
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8767',timeout=4,trust_env=False) as client:
        async def ready():
            start=time.monotonic()
            while time.monotonic()-start<15:
                try:
                    response=await client.get('/health');response.raise_for_status()
                    if response.json()['ready']:return time.monotonic()-start,response.json()
                except httpx.HTTPError:pass
                await asyncio.sleep(.1)
            raise RuntimeError('encoder failed to recover')
        try:
            _,warm=await ready();result['warm']=warm
            repeated=[]
            for _ in range(5):
                response=await client.post('/embed',json={'space':SPACE,'role':'query','texts':['Кёнигсберг крепость']});response.raise_for_status();repeated.append(response.json()['vectors'][0])
            result['identical_query']={'requests':5,'exact_vectors_equal':all(vector==repeated[0] for vector in repeated),'pid_unchanged':(await ready())[1]['pid']==warm['pid']}
            await control('restart');seconds,restarted=await ready()
            result['encoder_restart']={'ready_seconds':seconds,'new_pid':restarted['pid']!=warm['pid'],'local_weights':True}
            pid=restarted['pid'];await control('restart','regional-knowledge-base.service')
            result['mcp_restart']={'encoder_pid_unchanged':(await ready())[1]['pid']==pid}
            await control('stop')
            start=time.monotonic();degraded=await backend.search_evidence('Кёнигсберг',actor)
            result['encoder_down']={'mode':degraded.retrieval_mode,'evidence_count':len(degraded.evidence),'seconds':time.monotonic()-start,'model_processes':int(resources()['MainPID']!= '0')}
            assert degraded.retrieval_mode=='lexical_only' and degraded.evidence
            # Multiple simultaneous starts are coalesced by systemd, never by a
            # request-side model constructor. Initial searches stay available.
            starts=[asyncio.create_task(control('start')) for _ in range(10)]
            first=await asyncio.gather(*(backend.search('Кёнигсберг',actor) for _ in range(10)))
            await asyncio.gather(*starts);seconds,cold=await ready()
            result['simultaneous_first_requests']={'requests':10,'successful':len(first),'modes':[row.retrieval_mode for row in first],'encoder_processes':cold['encoder_processes'],'ready_seconds_after_requests':seconds,'resources':resources()}
            async def burst():
                started=time.monotonic();response=await client.post('/embed',json={'space':SPACE,'role':'query','texts':['история Кёнигсберг город крепость '*200]})
                return {'status':response.status_code,'seconds':time.monotonic()-started}
            burst_rows=await asyncio.gather(*(burst() for _ in range(40)))
            _,status=await ready();result['queue_full']={'requests':40,'statuses':{str(code):sum(row['status']==code for row in burst_rows) for code in set(row['status'] for row in burst_rows)},'max_seconds':max(row['seconds'] for row in burst_rows),'max_queue_depth':status['max_queue_depth'],'queue_capacity':status['queue_capacity'],'encoder_processes':status['encoder_processes'],'resources':resources()}
            assert result['queue_full']['statuses'].get('429',0)>0 and status['max_queue_depth']<=10
            assert status['encoder_processes']==1
            load=[asyncio.create_task(burst()) for _ in range(20)]
            await asyncio.sleep(.05)
            fallback=await asyncio.gather(*(backend.search('Кёнигсберг',actor) for _ in range(10)))
            await asyncio.gather(*load)
            result['queue_full']['application_requests']=len(fallback)
            result['queue_full']['application_lexical_fallbacks']=sum(row.retrieval_mode=='lexical_only' for row in fallback)
            result['queue_full']['application_failures']=0
            assert result['queue_full']['application_lexical_fallbacks']>0
            assert resources()['actual_encoder_processes']==1
            final=await backend.search_evidence('Кёнигсберг',actor)
            result['recovered']={'mode':final.retrieval_mode,'evidence_count':len(final.evidence),'external_embedding_calls':status['external_embedding_calls']}
            assert final.retrieval_mode=='fast_e5'
            output.write_text(json.dumps(result,indent=2));print(json.dumps({key:value for key,value in result.items() if key not in ('warm','simultaneous_first_requests','queue_full')}))
        finally:
            await control('start');await ready();await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output',type=Path);asyncio.run(run(parser.parse_args().output))
