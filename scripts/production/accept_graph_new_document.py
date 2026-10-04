"""Operator synthetic new-document transport/discovery acceptance.

Requires the private real-source fixture and a managed retained work directory.
An operator-local attachment starts normal owner-authorized S3 ingestion; the
public OAuth API performs stage/validate/async finalize/status/replay. This does
not test chat attachment URL download. The one-page PDF is explicitly synthetic,
never historical evidence. The exact owned control is archived in finally, not
any existing historical document. All tokens are revoked; outputs stay private.
Use a new --file-id for a fresh test; reusing a finalized archived control cannot
exercise a new activation. Inspect control-render.jpg before historical review.
"""
import asyncio,json,os,time,hashlib,shutil,sys
from pathlib import Path
from uuid import UUID,uuid4,uuid5

from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.contracts import Principal,ChatFile,StagePageInput,StageChunkInput
from regional_knowledge.file_ingress import DownloadedFile
from regional_knowledge.stage_graph import StagedGraph,compile_model_stage
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
import httpx,fitz
import argparse
p=argparse.ArgumentParser();p.add_argument('work_dir',type=Path);p.add_argument('fixture',type=Path);p.add_argument('--file-id',required=True);args=p.parse_args();ROOT=args.work_dir;assert args.file_id.startswith('rkb-graph-control-'), 'dedicated synthetic control file-id required';assert (ROOT/'.artifact.json').exists(), 'managed artifacts directory required'
async def main():
 load_service_env();os.environ['RKB_WORK_DIR']=str(ROOT/'production-work');Path(os.environ['RKB_WORK_DIR']).mkdir(exist_ok=True)
 f=json.loads(args.fixture.read_text());b=backend_from_env();p=Principal(subject=os.environ['RKB_OWNER_SUBJECT'],client_id=os.environ['RKB_OAUTH_CLIENT_ID'],issuer=os.environ['RKB_AUTH_ISSUER'],access_token='operator-actor-bridge')
 provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL']);tokens=[];out={}
 def mint(subject):
  t=provider.store.mutate(lambda s:provider._mint_family(s,client_id=p.client_id,scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=subject));tokens.append(t);return t
 owner=mint(p.subject);other=mint(str(uuid4()));pdf=ROOT/'synthetic-control.pdf';text='SYNTHETIC CONTROL. NOT HISTORICAL EVIDENCE.\n'+f['person_alias']['value']+'\n'+f['poi_alias']+'\nControl thread'
 if not pdf.exists():
  doc=fitz.open();page=doc.new_page();page.insert_text((40,60),text,fontsize=12);doc.save(pdf);doc.close();pdf.chmod(0o600)
 class LocalAttachment:
  async def download(self,url,directory):
   path=Path(directory)/'control.pdf';shutil.copyfile(pdf,path);return DownloadedFile(path=path,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size_bytes=path.stat().st_size)
 b.file_downloader=LocalAttachment();document=None

 try:
  async with httpx.AsyncClient(timeout=120,trust_env=False) as client:
   seq=0
   async def call(name,args,token=owner,deny=False):
    nonlocal seq
    seq+=1;r=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':seq,'method':'tools/call','params':{'name':name,'arguments':args}});r.raise_for_status();d=json.loads(next(x[6:] for x in r.text.splitlines() if x.startswith('data: '))) if r.headers.get('content-type','').startswith('text/event-stream') else r.json()
    error='error' in d or d.get('result',{}).get('isError',False)
    if deny:return error
    if error:raise RuntimeError('tool failed '+name+':'+str(d))
    return d['result'].get('structuredContent') or json.loads(d['result']['content'][0]['text'])
   # Public POI starts actor-private discovery with no seeded entity.
   seed=await call('graph_stage',{'document_id':f['german']['document_id'],'revision':f['german']['revision'],'candidates':f['german']['bundle']})
   poi=await call('graph_fetch',{'entity_id':seed['entities'][f['keys']['poi']]});ref=poi['entity']['external_ref'];job=await call('graph_stage',{'poi_discovery_ref':ref});assert job==await call('graph_stage',{'poi_discovery_ref':ref});out['unseeded_job_replay']=True;out['foreign_job_denied']=await call('graph_fetch',{'discovery_job_id':job['job_id']},other,True);assert out['foreign_job_denied']
   started=time.monotonic()
   while time.monotonic()-started<1200:
    j=await call('graph_fetch',{'discovery_job_id':job['job_id']});
    if j['state']=='done':break
    await asyncio.sleep(3)
   assert j['state']=='done';out['unseeded_readback']=j;out['unseeded_seconds']=time.monotonic()-started
   foreign=await call('graph_stage',{'poi_discovery_ref':ref},other);started=time.monotonic()
   while time.monotonic()-started<600:
    foreign_result=await call('graph_fetch',{'discovery_job_id':foreign['job_id']},other)
    if foreign_result['state']=='done':break
    await asyncio.sleep(2)
   assert foreign_result['state']=='done' and not foreign_result['candidates'];out['public_poi_foreign_corpus_empty']=True
   # Operator attachment ingress only; stage/validate/finalize run over public OAuth.
   start=await b.book_ingest(command='start',principal=p,file=ChatFile(download_url='https://example.org/control.pdf',file_id=args.file_id,mime_type='application/pdf',file_name='synthetic-control.pdf'),ingestion_id=None,cursor=None,payload={'title':'Synthetic graph transport control — not historical evidence','language':'en'})
   ingestion=start.ingestion_id;row=await b._ingestion_row(principal=p,ingestion_id=ingestion);document=str(row['document_id']);revision=int(row['staged_revision']);batch=await b.book_pages(ingestion_id=ingestion,principal=p,cursor=None,batch_size=1);(ROOT/'control-render.jpg').write_bytes(batch.pages[0].data);(ROOT/'control-render.jpg').chmod(0o600)
   pages=[StagePageInput(page_id=str(batch.pages[0].page_id),physical_page_index=0,source_material='visual_reviewed',source_review_note='Operator-created one-page synthetic control; checked rendered heading and all three lines, no historical assertions.',regions=[{'region_key':'body','kind':'body','bbox':{'left':0,'top':0,'right':1000,'bottom':1000},'reading_order':0,'source_text':text}])];chunks=[StageChunkInput(chunk_key='control',title='Synthetic control',region_refs=[{'page_id':pages[0].page_id,'region_key':'body'}])]
   c=compile_model_stage(StagedGraph(revision=revision),document_id=document,revision=revision,pages=pages,chunks=chunks);e={'chunk_id':str(c.chunks[0].chunk_id),'page_id':pages[0].page_id,'region_id':str(c.pages[0].regions[0].region_id),'exact_quote':'Control thread'};bundle={'entities':[{'key':'control-thread','kind':'historical_thread','canonical_label':'Control thread','exact_source_spelling':'Control thread','evidence':e}],'relations':[]}
   args={'command':'stage','ingestion_id':ingestion,'pages':[x.model_dump(mode='json') for x in pages],'chunks':[x.model_dump(mode='json') for x in chunks],'entity_candidates':bundle};await call('book_ingest',args);await call('book_ingest',args);valid=await call('book_ingest',{'command':'validate','ingestion_id':ingestion});assert valid['state']=='ready';await call('book_ingest',{'command':'finalize','ingestion_id':ingestion})
   started=time.monotonic()
   while time.monotonic()-started<300:
    status=await call('book_ingest',{'command':'status','ingestion_id':ingestion})
    if status['state']=='finalized':break
    if status['state']=='failed':raise RuntimeError('control finalize failed')
    await asyncio.sleep(2)
   assert status['state']=='finalized';replay=await call('book_ingest',{'command':'finalize','ingestion_id':ingestion});assert replay['state']=='finalized';out['finalize_replay']=True
   ids=seed['entities'];wanted=[UUID(ids[f['keys']['alias_person']]),UUID(ids[f['keys']['poi']])];started=time.monotonic()
   while time.monotonic()-started<1800:
    async with b.data_client._connection(b._headers(p)) as db:
     hits=await(await db.execute('select entity_id,signals from rkb_entity_mentions where document_id=%s and revision=%s and entity_id=any(%s::uuid[])',(UUID(document),revision,wanted))).fetchall();jobs=await(await db.execute('select state,count(*) as n from rkb_graph_discovery_jobs where document_id=%s and revision=%s group by state',(UUID(document),revision))).fetchall()
    if len({r['entity_id'] for r in hits if r['signals'].get('exact_alias_match')})==2:break
    await asyncio.sleep(3)
   assert len({r['entity_id'] for r in hits if r['signals'].get('exact_alias_match')})==2;out['new_document_person_and_poi_discovered']=True;out['new_document_seconds']=time.monotonic()-started;out['document_jobs']=jobs;out['control_document']=document;out['control_chunk']=str(c.chunks[0].chunk_id);out['control_sha256']=hashlib.sha256(pdf.read_bytes()).hexdigest();out['control_revision']=revision;out['public_oauth_stage_validate_finalize']=True
 finally:
  if document:
   async with b.data_client._connection(b._headers(p)) as db:
    await db.execute('update rkb_documents set active_revision=0 where id=%s and owner_user_id=rkb_current_actor_id() and source_sha256=%s and id in(select document_id from rkb_ingestion_jobs where source_file_id=%s)',(UUID(document),hashlib.sha256(pdf.read_bytes()).hexdigest(),args.file_id));out['synthetic_control_archived']=True
  for t in tokens:
   access=await provider.load_access_token(t.access_token)
   if access:await provider.revoke_token(access)
  await b.aclose();(ROOT/'control-acceptance.json').write_text(json.dumps(out,ensure_ascii=False,indent=2,default=str));(ROOT/'control-acceptance.json').chmod(0o600)
 print(json.dumps({k:v for k,v in out.items() if k not in ('unseeded_readback','control_document','control_chunk','control_sha256')},default=str))
asyncio.run(main())
