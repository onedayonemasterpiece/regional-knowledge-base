"""Real MCP source proof, including shared-page and multi-page controls."""
import argparse,asyncio,base64,json,os,time
from pathlib import Path
import httpx
from operator_env import load_service_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from regional_knowledge.sqlite_corpus import SQLiteCorpus

async def run(args):
 load_service_env();root=args.evidence;product=json.loads((root/'product.json').read_text());corpus=SQLiteCorpus('/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3')
 provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL']);token=provider.store.mutate(lambda s:provider._mint_family(s,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=os.environ['RKB_OWNER_SUBJECT']))
 endpoint=args.endpoint or os.environ['RKB_RESOURCE_URL'];results=[];seq=0
 async with httpx.AsyncClient(timeout=120,trust_env=False) as client:
  async def call(name,values):
   nonlocal seq
   seq+=1;r=await client.post(endpoint,headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json,text/event-stream'},json={'jsonrpc':'2.0','id':seq,'method':'tools/call','params':{'name':name,'arguments':values}});r.raise_for_status();reply=r.json() if 'application/json' in r.headers['content-type'] else json.loads(next(x[6:] for x in r.text.splitlines() if x.startswith('data: ')))
   if 'error' in reply or reply.get('result',{}).get('isError'):raise RuntimeError('MCP '+name+' failed')
   return reply['result']
  try:
   for material in product['materials']:
    if material.get('component'):continue
    chunk=corpus.one('rkb_chunks',material['chunk_ids'][0])
    if material['kind']=='journal_issue':chunk=next(corpus.one('rkb_chunks',i) for i in material['chunk_ids'] if corpus.one('rkb_chunks',i)['metadata'].get('article_id')=='article-a')
    quote=chunk['source_text'];page=0 if material['kind']=='article' else None
    cases=[('cold',quote,page),('warm',quote,page),('wrong_quote','Invented source quotation.',page),('wrong_page',quote,100)]
    for label,q,p in cases:
     t=time.monotonic();reply=await call('source_proof',{'id':chunk['id'],'quote':q,**({'physical_page_index':p} if p is not None else {})});metadata=json.loads(reply['content'][0]['text'])
     images=[c for c in reply['content'] if c['type']=='image']
     results.append({'kind':material['kind'],'case':label,'metadata':metadata,'seconds':time.monotonic()-t,'images':len(images)})
     (root/'proofs.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
     if label.startswith('wrong'):assert metadata['status']!='ok' and not images
     if label in ('cold','warm'):
      assert metadata['status']=='ok' and images
      for i,c in enumerate(images):(root/(material['kind']+'-'+label+'-proof-'+str(i)+'.webp')).write_bytes(base64.b64decode(c['data']))
   print(json.dumps({'successes':sum(r['metadata']['status']=='ok' for r in results),'negative_refusals':sum(r['case'].startswith('wrong') and r['metadata']['status']!='ok' for r in results),'model_calls':sum(r['metadata'].get('model_calls',0) for r in results)},indent=2))
  finally:
   access=await provider.load_access_token(token.access_token)
   if access:await provider.revoke_token(access)
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--endpoint');asyncio.run(run(p.parse_args()))
