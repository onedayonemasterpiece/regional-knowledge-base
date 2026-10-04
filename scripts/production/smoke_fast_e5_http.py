"""Authorized operator HTTP smoke. Short-lived test family is always revoked.

Uses the production OAuth verifier and tools, without storing or printing tokens.
Owner consent UI is outside this server acceptance; no client changes are made.
"""
import argparse,asyncio,json,os,time
from pathlib import Path
import httpx
from operator_env import load_service_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from accept_fast_e5 import distribution

async def smoke(output,fixture=None):
    load_service_env();provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL'])
    token=provider.store.mutate(lambda state:provider._mint_family(state,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=os.environ['RKB_OWNER_SUBJECT']))
    access=await provider.load_access_token(token.access_token)
    try:
        async with httpx.AsyncClient(timeout=15,trust_env=False) as client:
            # Public URL exercises the deployed edge, authentication and server.
            url=os.environ['RKB_RESOURCE_URL']
            async def rpc(method,params,index):
                response=await client.post(url,headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':index,'method':method,'params':params})
                response.raise_for_status()
                if response.headers.get('content-type','').startswith('text/event-stream'):
                    data=json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith('data: ')))
                else:data=response.json()
                if 'error' in data:raise RuntimeError('MCP RPC error')
                return data['result']
            initialized=await rpc('initialize',{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'fast-e5-operator-acceptance','version':'1'}},1)
            listed=await rpc('tools/list',{},2);names=[tool['name'] for tool in listed['tools']]
            started=time.monotonic();search=await rpc('tools/call',{'name':'search','arguments':{'query':'Почему крепость Кёнигсберг получила своё название и какую роль играл король Оттокар?'}},3)
            if search.get('isError'):raise RuntimeError('MCP search failed')
            body=search.get('structuredContent') or json.loads(search['content'][0]['text'])
            if not body.get('results'):raise RuntimeError('MCP search returned no evidence')
            fetched=await rpc('tools/call',{'name':'fetch','arguments':{'id':body['results'][0]['id']}},4)
            if fetched.get('isError'):raise RuntimeError('MCP fetch failed')
            result={'public_http':True,'oauth_verifier':True,'search':True,'fetch':True,'tools':names,'retrieval_mode':body.get('retrieval_mode'),'evidence_count':len(body['results']),'search_plus_fetch_seconds':time.monotonic()-started,'protocol':initialized['protocolVersion']}
            if fixture:
                questions=json.loads(fixture.read_text())['questions'];levels={}
                async def request(index):
                    start=time.monotonic()
                    try:
                        found=await rpc('tools/call',{'name':'search','arguments':{'query':questions[index%len(questions)]['query']}},100+index*4)
                        if found.get('isError'):raise RuntimeError('search failed')
                        data=found.get('structuredContent') or json.loads(found['content'][0]['text'])
                        evidence=await asyncio.gather(*(rpc('tools/call',{'name':'fetch','arguments':{'id':item['id']}},101+index*4+j) for j,item in enumerate(data['results'][:3])))
                        if any(row.get('isError') for row in evidence):raise RuntimeError('fetch failed')
                        return {'ok':True,'mode':data.get('retrieval_mode'),'evidence_count':len(evidence),'total_seconds':time.monotonic()-start}
                    except Exception as error:return {'ok':False,'error_type':type(error).__name__,'total_seconds':time.monotonic()-start}
                for users in (1,5,10):
                    rows=[]
                    for start in range(0,30,users):rows.extend(await asyncio.gather(*(request(index) for index in range(start,start+users))))
                    levels[str(users)]={'requests':len(rows),'failures':sum(not row['ok'] for row in rows),'seconds':distribution([row['total_seconds'] for row in rows]),'fast_e5':sum(row.get('mode')=='fast_e5' for row in rows),'rows':rows}
                    print(json.dumps({'http_users':users,**{key:value for key,value in levels[str(users)].items() if key!='rows'}}),flush=True)
                result['concurrency']=levels
    finally:
        if access:await provider.revoke_token(access)
    assert await provider.load_access_token(token.access_token) is None
    result['temporary_family_revoked']=True
    output.write_text(json.dumps(result,indent=2));print(json.dumps({key:value for key,value in result.items() if key!='concurrency'}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output',type=Path);parser.add_argument('--fixture',type=Path);args=parser.parse_args();asyncio.run(smoke(args.output,args.fixture))
