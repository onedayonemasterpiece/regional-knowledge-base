"""Deployed OAuth MCP search/fetch acceptance; never retain bearer tokens."""
import argparse,asyncio,json,os,time
from pathlib import Path
import httpx
from operator_env import load_service_env
from accept_fast_e5 import distribution
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE

async def run(args):
    load_service_env();provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL'])
    token=provider.store.mutate(lambda state:provider._mint_family(state,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=os.environ['RKB_OWNER_SUBJECT']))
    access=await provider.load_access_token(token.access_token);cases=json.loads(args.fixture.read_text())['cases'];output={'public_http':True,'oauth_verifier':True,'external_embedding_api_calls':0,'levels':{}}
    try:
        async with httpx.AsyncClient(timeout=20,trust_env=False) as client:
            sequence=0
            async def rpc(method,params):
                nonlocal sequence
                sequence+=1
                response=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':sequence,'method':method,'params':params});response.raise_for_status()
                data=json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith('data: '))) if response.headers.get('content-type','').startswith('text/event-stream') else response.json()
                if 'error' in data:raise RuntimeError('MCP RPC error')
                result=data['result']
                if result.get('isError'):raise RuntimeError('MCP tool error')
                return result
            async def search(arguments):
                result=await rpc('tools/call',{'name':'search','arguments':arguments})
                return result.get('structuredContent') or json.loads(result['content'][0]['text'])
            async def fetch(data):
                return await asyncio.gather(*(rpc('tools/call',{'name':'fetch','arguments':{'id':item['id']}}) for item in data.get('results',[])[:3]))
            output['protocol']=(await rpc('initialize',{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'bge-operator-acceptance','version':'1'}}))['protocolVersion']
            output['tools']=[tool['name'] for tool in (await rpc('tools/list',{}))['tools']]
            worker_denied=await client.post('https://knowledge.kenigevents.ru/bge-worker/claim',headers={'Authorization':'Bearer '+token.access_token},json={'run_id':'not-a-worker','space':'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'})
            output['user_token_worker_denied']=worker_denied.status_code==403
            async def request(index):
                query=cases[index%len(cases)]['query'];start=time.monotonic()
                try:
                    initial=await search({'query':query});evidence=await fetch(initial);initial_seconds=time.monotonic()-start;main=initial;polls=0
                    while main.get('main_state')!='ready' and main.get('main_job_id') and time.monotonic()-start<120:
                        await asyncio.sleep(args.poll_seconds);polls+=1;main=await search({'query':query,'main_job_id':main['main_job_id']})
                    if polls:evidence=await fetch(main)
                    signals=[signal['branch'] for item in main.get('results',[]) for signal in item.get('ranking_signals',[])]
                    return {'ok':main.get('main_state')=='ready','initial_seconds':initial_seconds,'main_seconds':time.monotonic()-start,'initial_mode':initial.get('retrieval_mode'),'initial_state':initial.get('main_state'),'mode':main.get('retrieval_mode'),'polls':polls,'evidence_count':len(evidence),'ranking_branches':sorted(set(signals))}
                except Exception as error:return {'ok':False,'error_type':type(error).__name__,'main_seconds':time.monotonic()-start}
            for users in args.users:
                rows=[]
                for start in range(0,args.requests,users):rows.extend(await asyncio.gather(*(request(i) for i in range(start,min(start+users,args.requests)))))
                level={'requests':len(rows),'failures':sum(not row['ok'] for row in rows),'initial_fast':sum(row.get('initial_mode')=='fast_e5' for row in rows),'initial_seconds':distribution([row['initial_seconds'] for row in rows if 'initial_seconds' in row]),'main_seconds':distribution([row['main_seconds'] for row in rows]),'rows':rows};output['levels'][str(users)]=level;args.output.write_text(json.dumps(output,indent=2));print(json.dumps({'http_users':users,**{key:value for key,value in level.items() if key!='rows'}}),flush=True)
    finally:
        if access:await provider.revoke_token(access)
    output['temporary_family_revoked']=await provider.load_access_token(token.access_token) is None;args.output.write_text(json.dumps(output,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('fixture',type=Path);parser.add_argument('output',type=Path);parser.add_argument('--requests',type=int,default=30);parser.add_argument('--users',type=int,nargs='+',default=[1,5,10]);parser.add_argument('--poll-seconds',type=float,default=1);asyncio.run(run(parser.parse_args()))
