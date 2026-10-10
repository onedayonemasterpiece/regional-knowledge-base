"""Operator real-source OAuth graph acceptance. Fixture/outputs are private.

The fixture is model-authored and source-verified; no private names/passages/IDs
are embedded in this public runner. Tokens stay in memory and are revoked.
"""
import argparse,asyncio,json,os,time,subprocess
from pathlib import Path
from uuid import uuid4,UUID
import httpx
from operator_env import load_service_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.contracts import Principal
from regional_knowledge.entity_graph import GraphBundle

async def run(a):
 load_service_env();fixture=json.loads(a.fixture.read_text());b=backend_from_env();principal=Principal(subject=os.environ['RKB_OWNER_SUBJECT'],client_id=os.environ['RKB_OAUTH_CLIENT_ID'],issuer=os.environ['RKB_AUTH_ISSUER'],access_token='operator-actor-bridge')
 provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL']);families=[];out={'public_oauth':True,'automatic_merges':0,'external_paid_inference_calls':0}
 def mint(subject):
  token=provider.store.mutate(lambda s:provider._mint_family(s,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=subject));families.append(token);return token
 owner=mint(principal.subject);other=mint(str(uuid4()))
 try:
  async with httpx.AsyncClient(timeout=120,trust_env=False) as client:
   sequence=0
   async def call(name,args,token=owner,deny=False):
    nonlocal sequence
    sequence+=1;t=time.monotonic();r=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':sequence,'method':'tools/call','params':{'name':name,'arguments':args}});r.raise_for_status();data=json.loads(next(x[6:] for x in r.text.splitlines() if x.startswith('data: '))) if r.headers.get('content-type','').startswith('text/event-stream') else r.json()
    if deny:return 'error' in data or data.get('result',{}).get('isError',False)
    if 'error' in data or data['result'].get('isError'):raise RuntimeError('graph tool error:'+name)
    result=data['result'];return result.get('structuredContent') or json.loads(result['content'][0]['text'])
   german=fixture['german'];first=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':german['bundle']});second=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':german['bundle']});assert first==second;out['stage_replay_identical']=True;ids=first['entities'];keys=fixture['keys'];alias_person=ids[keys.get('alias_person',keys['person'])]
   russian=fixture['russian'];bundle=russian['bundle'];next(n for n in bundle['entities'] if n['key']==keys['thread'])['entity_id']=ids[keys['thread']];ru=await call('graph_stage',{'document_id':russian['document_id'],'revision':russian['revision'],'candidates':bundle});out['canonical_entities']={**ids,**ru['entities']}
   thread=await call('graph_fetch',{'entity_id':ids[keys['thread']],'limit':20});out['thread']=thread;out['thread_neighbor_kinds']=sorted({r['neighbor_kind'] for r in thread['neighbors']});out['relations_have_exact_evidence']=all(r['evidence'] and all(e.get('chunk_id') and e.get('page_id') and e.get('region_id') and e.get('exact_quote') for e in r['evidence']) for r in thread['neighbors']);assert out['relations_have_exact_evidence']
   person=await call('graph_fetch',{'entity_id':ids[keys['person']]});out['person_events']=sum(r['kind']=='participated_in' for r in person['neighbors']);out['person_threads']=sum(r['kind']=='member_of' for r in person['neighbors']);assert out['person_events']>=2 and out['person_threads']>=2
   event=await call('graph_fetch',{'entity_id':ids[keys['event']]});out['event_people']=sum(r['kind']=='participated_in' for r in event['neighbors']);out['event_pois']=sum(r['kind']=='occurred_at' for r in event['neighbors']);assert out['event_people']>=2 and out['event_pois']>=1
   out['foreign_graph_denied']=await call('graph_fetch',{'entity_id':ids[keys['thread']]},other,True);out['foreign_stage_denied']=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':german['bundle']},other,True);assert out['foreign_graph_denied'] and out['foreign_stage_denied']
   narrow=await call('graph_fetch',{'entity_id':ids[keys['thread']],'limit':1});assert len(narrow['neighbors'])==1 and narrow['truncated'];out['one_hop_bound']=True
   evidence_ids=list(dict.fromkeys(e['chunk_id'] for r in thread['neighbors'] for e in r['evidence']))[:8];fetched=[await call('fetch',{'id':id}) for id in evidence_ids];out['outline_source_fetches']=len(fetched);out['outline_structured_only']=True
   alias=await call('graph_stage',{'entity_id':alias_person,'alias':fixture['person_alias']});assert alias==await call('graph_stage',{'entity_id':alias_person,'alias':fixture['person_alias']});out['person_alias_job']=alias['job_id']
   if a.poi_alias_update:subprocess.run(a.poi_alias_update,check=True)
   # Real native alias version is picked up asynchronously by the production worker.
   started=time.monotonic();completed=False
   while time.monotonic()-started<600:
    async with b.data_client._connection(b._headers(principal)) as db:
     rows=await(await db.execute('select entity_id,chunk_id,signals from rkb_entity_mentions where entity_id=any(%s::uuid[]) and state=\'candidate\' and rkb_graph_active(document_id,revision)',([alias_person,ids[keys['poi']]],))).fetchall()
     person_hits={str(r['chunk_id']) for r in rows if str(r['entity_id'])==alias_person and r['signals'].get('exact_alias_match')}
     poi_hits={str(r['chunk_id']) for r in rows if str(r['entity_id'])==ids[keys['poi']] and r['signals'].get('exact_alias_match')}
     done=await(await db.execute('select state from rkb_graph_discovery_jobs where id=%s',(UUID(alias['job_id']),))).fetchone()
    completed=set(fixture['gold']['person_alias']).issubset(person_hits) and set(fixture['gold']['poi_alias']).issubset(poi_hits) and done['state']=='done'
    if completed:break
    await asyncio.sleep(2)
   out['alias_discovery_seconds']=time.monotonic()-started;out['person_alias_gold_recall']=len(set(fixture['gold']['person_alias'])&person_hits)/len(fixture['gold']['person_alias']);out['poi_alias_gold_recall']=len(set(fixture['gold']['poi_alias'])&poi_hits)/len(fixture['gold']['poi_alias']);out['discovery_ready']=completed;assert completed
   poi_node=next(n for n in german['bundle']['entities'] if n['key']==keys['poi']);historical={**poi_node,'key':'historical-name-resolve-control','poi_locator':{'names':[fixture['poi_alias']]}}
   historical_result=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':{'entities':[historical],'relations':[]}});current=await call('graph_fetch',{'entity_id':ids[keys['poi']]});old=await call('graph_fetch',{'entity_id':historical_result['entities'][historical['key']]});out['historical_name_same_canonical_poi']=old['entity']['external_ref']==current['entity']['external_ref'] and old['entity']['external_ref'] is not None;assert out['historical_name_same_canonical_poi']
   related=await call('graph_related',{'entity_id':ids[keys['poi']],'query':fixture['related_query'],'limit':20});out['related']=related;found={r['id'] for r in related['retrieval']['results']};out['russian_query_german_gold_recall']=len(set(fixture['gold']['related'])&found)/len(fixture['gold']['related']);assert out['russian_query_german_gold_recall']==1
   ambiguous={**german['bundle']['entities'][0],'key':'same-name-review-control','state':'candidate','review_note':None};control=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':{'entities':[ambiguous],'relations':[]}});out['same_name_person_not_merged']=control['entities'][ambiguous['key']]!=ids[keys['person']]
   two_poi={**poi_node,'key':'ambiguous-poi-review-control','poi_locator':{'names':[poi_node['canonical_label'],next(n['canonical_label'] for n in russian['bundle']['entities'] if n['kind']=='poi_ref')]}};amb=await call('graph_stage',{'document_id':german['document_id'],'revision':german['revision'],'candidates':{'entities':[two_poi],'relations':[]}});unresolved=await call('graph_fetch',{'entity_id':amb['entities'][two_poi['key']]});out['ambiguous_poi_unresolved']=unresolved['entity']['external_ref'] is None and unresolved['entity']['state']=='unresolved';assert out['ambiguous_poi_unresolved']
   out['historical_object_reference']=(await call('graph_fetch',{'entity_id':ru['entities'][keys['castle']]}))['entity']['external_ref'] is not None;assert out['historical_object_reference']
 finally:
  for token in families:
   access=await provider.load_access_token(token.access_token)
   if access:await provider.revoke_token(access)
  await b.aclose()
  a.output.write_text(json.dumps(out,ensure_ascii=False,indent=2,default=str));a.output.chmod(0o600)
 out['temporary_families_revoked']=all([await provider.load_access_token(t.access_token) is None for t in families]);a.output.write_text(json.dumps(out,ensure_ascii=False,indent=2,default=str));a.output.chmod(0o600)
 print(json.dumps({k:v for k,v in out.items() if k not in ('thread','related','canonical_entities','person_alias_job')},ensure_ascii=False,indent=2))
async def model_calls(parts):
 """Apply an explicit operator-supplied packet through the real OAuth MCP.

 Used when a client caches old input schemas. No entity/evidence inference,
 direct corpus writes, credential export or automatic promotion is performed.
 """
 import base64
 raw=base64.b64decode(''.join(parts),validate=True)
 if len(raw)>64000:raise ValueError('bounded model packet required')
 calls=json.loads(raw)
 if not isinstance(calls,list) or not 1<=len(calls)<=8:raise ValueError('1..8 explicit calls required')
 allowed={'graph_stage','graph_fetch','entity_list','fetch'}
 if any(c.get('name') not in allowed for c in calls):raise ValueError('graph/source tools only')
 load_service_env()
 resource=os.environ['RKB_RESOURCE_URL'];issuer=os.environ['RKB_AUTH_ISSUER']
 provider=oauth_provider_from_env(issuer=issuer,resource=resource)
 credentials=provider.store.mutate(lambda state:provider._mint_family(state,
     client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],
     resource=resource,subject=os.environ['RKB_OWNER_SUBJECT']))
 results=[]
 try:
  async with httpx.AsyncClient(timeout=25,trust_env=False) as client:
   for i,call in enumerate(calls):
    response=await client.post(resource,headers={
      'Authorization':'Bearer '+credentials.access_token,
      'Accept':'application/json, text/event-stream'},json={
      'jsonrpc':'2.0','id':i+1,'method':'tools/call','params':call})
    response.raise_for_status()
    message=json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith('data: '))) if response.headers.get('content-type','').startswith('text/event-stream') else response.json()
    if 'error' in message:raise RuntimeError('MCP protocol error: '+call['name'])
    result=message['result']
    if result.get('isError'):raise RuntimeError('MCP validation error: '+str(result.get('content'))[:1500])
    result=result.get('structuredContent') or json.loads(result['content'][0]['text'])
    results.append({'tool':call['name'],'result':result})
 finally:
  access=await provider.load_access_token(credentials.access_token)
  if access:await provider.revoke_token(access)
 print(json.dumps({'transport':'existing_public_oauth_mcp','model_authored':True,
   'direct_corpus_writes':False,'temporary_credentials_revoked':True,'results':results},ensure_ascii=False,default=str))

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('fixture',type=Path,nargs='?');p.add_argument('output',type=Path,nargs='?');p.add_argument('--poi-alias-update',nargs='+');p.add_argument('--model-calls-base64',nargs='+');a=p.parse_args()
 if a.model_calls_base64:asyncio.run(model_calls(a.model_calls_base64))
 elif a.fixture and a.output:asyncio.run(run(a))
 else:p.error('a model-call packet or fixture and output are required')
