"""Operator synthetic public-OAuth automatic indexing acceptance.

--prepare uploads an owned synthetic PDF, starts through public HTTPS attachment
intake and saves source renders. Review them before --execute, which stages,
validates, finalizes, observes/restarts the automatic owner and archives ONLY this
hash/file-id-bound control. No backfill command or direct vector write is used.
--interrupt-bge uses the existing real queue's liveness fence on one owned
claimed control document job; explicitly accelerated fault, not natural outage.
Private IDs, renders and receipts stay in a managed retained directory. Signed
URLs and temporary OAuth tokens stay only in memory; token families are revoked.
"""
import argparse,asyncio,base64,hashlib,json,os,subprocess,textwrap,time
from pathlib import Path
from uuid import UUID,uuid4
import httpx
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.contracts import Principal
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE


def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str));path.chmod(0o600)

async def run(args):
    assert (args.work_dir/'.artifact.json').exists(),'managed artifacts directory required'
    assert args.file_id.startswith('rkb-auto-index-control-'),'dedicated synthetic control identity required'
    load_service_env();root=args.work_dir;b=backend_from_env();p=Principal(subject=os.environ['RKB_OWNER_SUBJECT'],client_id=os.environ['RKB_OAUTH_CLIENT_ID'],issuer=os.environ['RKB_AUTH_ISSUER'],access_token='operator-actor-bridge')
    provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL']);tokens=[];out={'manual_backfill_used':False,'external_paid_embedding_calls':0};document=None;fixture=None;queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    def mint(subject):
        t=provider.store.mutate(lambda s:provider._mint_family(s,client_id=p.client_id,scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=subject));tokens.append(t);return t
    owner=mint(p.subject);other=mint(str(uuid4()))
    async def digest(exclude=None):
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            return await(await db.execute('''select
             (select count(*) from rkb_chunks where (%s::uuid is null or document_id<>%s)) existing_chunks,
             (select md5(string_agg(id::text||coalesce(embedding::text,''),',' order by id)) from rkb_chunks where (%s::uuid is null or document_id<>%s)) legacy_digest,
             (select md5(string_agg(e.chunk_id::text||e.embedding::text||e.revision::text||e.text_sha256||e.batch_sha256,',' order by e.chunk_id)) from rkb_chunk_embeddings_e5 e join rkb_chunks c on c.id=e.chunk_id where (%s::uuid is null or c.document_id<>%s)) e5_digest,
             (select md5(string_agg(e.chunk_id::text||e.embedding::text||e.revision::text||e.text_sha256,',' order by e.chunk_id)) from rkb_chunk_embeddings_bge e join rkb_chunks c on c.id=e.chunk_id where (%s::uuid is null or c.document_id<>%s)) bge_digest,
             (select md5(string_agg(c.id::text||c.revision::text||c.text_sha256,',' order by c.id)) from rkb_chunks c join rkb_documents d on d.id=c.document_id where c.revision=d.active_revision and (%s::uuid is null or c.document_id<>%s)) active_source_digest''',tuple([exclude]*10))).fetchone()
    def control_jobs():
        with queue.connect() as db:
            rows=db.execute("select id,state,identity,created,updated,attempt from jobs where actor=? and kind='document'",(p.subject,)).fetchall()
            return [dict(r) for r in rows if json.loads(r['identity']).get('chunk_id') in fixture['chunk_ids']]
    try:
        async with httpx.AsyncClient(timeout=120,trust_env=False) as client:
            seq=0
            async def call(name,values,token=owner,deny=False,raw=False):
                nonlocal seq
                seq+=1;r=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':seq,'method':'tools/call','params':{'name':name,'arguments':values}});r.raise_for_status();d=json.loads(next(x[6:] for x in r.text.splitlines() if x.startswith('data: '))) if r.headers.get('content-type','').startswith('text/event-stream') else r.json()
                error='error' in d or d.get('result',{}).get('isError',False)
                if deny:return error
                if error:raise RuntimeError('public tool failed: '+name)
                if raw:return d['result']
                return d['result'].get('structuredContent') or json.loads(d['result']['content'][0]['text'])
            if args.prepare:
                import pymupdf
                save(root/'baseline.json',{'corpus':await digest(),'actor_readiness':await call('indexing_status',{})})
                pdf=root/'synthetic-index-control.pdf'
                if not pdf.exists():
                    book=pymupdf.open()
                    for i in range(12):
                        page=book.new_page();body=f'AUTOINDEX TRANSPORT CONTROL ONLY. NOT HISTORICAL EVIDENCE.\nPage {i+1}. indexprobe\n\n'+textwrap.fill(('The synthetic librarian carries a purple compass through a training archive. This text tests transport indexing and asserts no historical facts. '*12),width=76)
                        assert page.insert_textbox(pymupdf.Rect(40,40,555,800),body,fontsize=10)>0
                    book.save(pdf);book.close();pdf.chmod(0o600)
                sha=hashlib.sha256(pdf.read_bytes()).hexdigest();key='users/'+p.subject+'/transport-controls/'+args.file_id+'/'+sha+'.pdf'
                await b.object_store.put_file(key,str(pdf),'application/pdf')
                url=await asyncio.to_thread(b.object_store.client.generate_presigned_url,'get_object',Params={'Bucket':b.object_store.bucket,'Key':key},ExpiresIn=900)
                started=await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':args.file_id,'mime_type':'application/pdf','file_name':'synthetic-index-control.pdf'},'metadata':{'title':'Synthetic automatic indexing transport control; not historical evidence','language':'en'}})
                document=UUID(started['document_id']);ingestion=started['ingestion_id'];pages=[];chunks=[];cursor=None
                while True:
                    response=await call('book_pages',{'ingestion_id':ingestion,'batch_size':4,**({'cursor':cursor} if cursor else {})},raw=True);manifest=json.loads(response['content'][0]['text']);images=[c for c in response['content'] if c['type']=='image']
                    for item,img in zip(manifest['pages'],images,strict=True):
                        render=root/f"control-page-{item['physical_page_index']+1}.jpg";render.write_bytes(base64.b64decode(img['data']));render.chmod(0o600)
                        native=item['native_text'];assert 'NOT HISTORICAL EVIDENCE' in native and len(native)>1000
                        pages.append({'page_id':item['page_id'],'physical_page_index':item['physical_page_index'],'source_material':'full_native','regions':[{'region_key':'body','kind':'body','bbox':{'left':0,'top':0,'right':1000,'bottom':1000},'reading_order':0,'source_text':native,'normalized_text':native}]})
                        chunks.append({'chunk_key':f"control-{item['physical_page_index']}",'title':'Synthetic transport control','region_refs':[{'page_id':item['page_id'],'region_key':'body'}]})
                    cursor=manifest.get('next_cursor')
                    if cursor is None:break
                fixture={'file_id':args.file_id,'document_id':str(document),'ingestion_id':ingestion,'source_sha256':sha,'pages':pages,'chunks':chunks};save(root/'control-fixture.json',fixture);out['public_attachment_start']=True;out['prepared_pages']=len(pages)
            else:
                fixture=json.loads((root/'control-fixture.json').read_text());assert fixture['file_id']==args.file_id;document=UUID(fixture['document_id']);assert hashlib.sha256((root/'synthetic-index-control.pdf').read_bytes()).hexdigest()==fixture['source_sha256']
                for page in fixture['pages']:page.update(source_material='visual_reviewed',source_review_note='Model reviewed the operator-created synthetic control render: complete heading and repeated test text, no historical content.')
                ingestion=fixture['ingestion_id'];stage={'command':'stage','ingestion_id':ingestion,'pages':fixture['pages'],'chunks':fixture['chunks']};await call('book_ingest',stage);await call('book_ingest',stage);assert (await call('book_ingest',{'command':'validate','ingestion_id':ingestion}))['state']=='ready'
                initial=await call('indexing_status',{'document_id':str(document)});assert initial['active_chunks']==0
                await call('book_ingest',{'command':'finalize','ingestion_id':ingestion});observations=[];marks={};restart=False;interrupt=False;started=time.monotonic();activation=None;fast_tested=False;lex_tested=False
                old_pid=subprocess.check_output(['systemctl','--user','show','regional-knowledge-indexing','-p','MainPID','--value']).decode().strip()
                while time.monotonic()-started<1200:
                    async with b.data_client._connection(b._headers(p)) as db:
                        d=await(await db.execute('select active_revision,updated_at from rkb_documents where id=%s',(document,))).fetchone()
                        if d['active_revision']>0:
                            activation=d['updated_at'].timestamp();fixture['revision']=d['active_revision'];rows=await(await db.execute('select id from rkb_chunks where document_id=%s and revision=%s',(document,d['active_revision']))).fetchall();fixture['chunk_ids']=[str(r['id']) for r in rows]
                            c=await(await db.execute('select * from rkb_index_counts(%s)',(document,))).fetchone();times=await(await db.execute('''select (select min(updated_at) from rkb_chunk_embeddings_e5 where chunk_id=any(%s::uuid[])) first_e5,(select max(updated_at) from rkb_chunk_embeddings_e5 where chunk_id=any(%s::uuid[])) last_e5,(select max(updated_at) from rkb_chunk_embeddings_bge where chunk_id=any(%s::uuid[])) last_bge''',tuple([[r['id'] for r in rows]]*3))).fetchone()
                        else:c=None
                    if c:
                        snapshot=tuple(c.values())
                        if not observations or observations[-1]['counts']!=list(snapshot):observations.append({'seconds':time.time()-activation,'counts':list(snapshot)})
                        if c['e5_ready']>0:marks.setdefault('first_e5',times['first_e5'].timestamp()-activation)
                        if not restart and 0<c['e5_ready']<c['active_chunks']:
                            # Deliberate indexing-owner restart while missing work
                            # remains. E5 and queue state stay independently alive.
                            subprocess.run(['systemctl','--user','restart','regional-knowledge-indexing'],check=True);restart=True;out['owner_restart_partial_counts']=c
                            lexical=await call('search',{'query':'indexprobe purple compass'});lex_tested=lexical['retrieval_mode']=='lexical_only';out['lexical_during_partial']=lex_tested
                        if c['e5_ready']==c['active_chunks']:marks.setdefault('e5_complete',times['last_e5'].timestamp()-activation)
                        jobs=control_jobs()
                        if jobs:marks.setdefault('bge_job_visible',min(j['created'] for j in jobs)-activation)
                        if c['e5_ready']==c['active_chunks'] and c['bge_ready']<c['active_chunks'] and not fast_tested:
                            fast=await call('search',{'query':'indexprobe purple compass'});assert fast['retrieval_mode']=='fast_e5' and fast['main_state']!='ready';hit=next(r for r in fast['results'] if r['id'] in fixture['chunk_ids']);fetched=await call('fetch',{'id':hit['id']});assert 'NOT HISTORICAL EVIDENCE' in fetched['text'];fast_tested=True;out['fast_search_fetch']=True
                        if args.interrupt_bge and not interrupt:
                            with queue.connect() as db:
                                claimed=db.execute("select id,run_id,identity from jobs where actor=? and kind='document' and state='claimed'",(p.subject,)).fetchall();target=next((j for j in claimed if json.loads(j['identity']).get('chunk_id') in fixture['chunk_ids']),None)
                                if target:
                                    db.execute('update runs set heartbeat=? where id=?',(time.time()-121,target['run_id']));queue._expire(db,time.time());assert db.execute('select state from jobs where id=?',(target['id'],)).fetchone()[0]=='pending';out['interrupted_document_job']=target['id'];interrupt=True
                        if c['bge_ready']==c['active_chunks']:
                            marks.setdefault('bge_complete',times['last_bge'].timestamp()-activation);break
                    await asyncio.sleep(.1)
                assert c and c['active_chunks']==12 and c['e5_ready']==c['bge_ready']==12;assert restart and fast_tested
                if args.interrupt_bge:assert interrupt
                out['timings_seconds']=marks;out['partial_progress']=observations;out['new_e5_vectors']=c['e5_ready'];out['new_bge_vectors']=c['bge_ready'];out['bge_jobs']=len(control_jobs());assert out['bge_jobs']==12
                if interrupt:assert next(j for j in control_jobs() if j['id']==out['interrupted_document_job'])['attempt']>=2;out['claimed_bge_job_recovered']=True
                out['owner_pid_changed']=subprocess.check_output(['systemctl','--user','show','regional-knowledge-indexing','-p','MainPID','--value']).decode().strip()!=old_pid;assert out['owner_pid_changed']
                main=await call('search',{'query':'indexprobe purple compass'});assert main['retrieval_mode']=='bge_lexical' and main['main_state']=='ready';hit=next(r for r in main['results'] if r['id'] in fixture['chunk_ids']);assert 'NOT HISTORICAL EVIDENCE' in (await call('fetch',{'id':hit['id']}))['text'];out['main_search_fetch']=True
                ready=await call('book_ingest',{'command':'status','ingestion_id':ingestion});assert ready['indexing']['e5_missing']==ready['indexing']['bge_missing']==0 and 'automatic_indexing_pending' not in ready['warnings'];out['finalized_status']=ready['indexing']
                before=control_jobs();await call('book_ingest',{'command':'finalize','ingestion_id':ingestion});await call('book_ingest',{'command':'finalize','ingestion_id':ingestion});await asyncio.sleep(6);after=control_jobs();assert before==after;out['replay_duplicate_jobs']=0
                foreign=await call('indexing_status',{},other);assert foreign['active_chunks']==foreign['e5_ready']==foreign['bge_ready']==0;assert await call('indexing_status',{'document_id':str(document)},other,deny=True);out['actor_safe_status']=True
                out['control_document']=str(document);out['control_chunk_ids']=fixture['chunk_ids'];save(root/'control-fixture.json',fixture)
    finally:
        if not args.prepare and document and fixture:
            async with b.data_client._connection(b._headers(p)) as db:
                result=await db.execute('''update rkb_documents set active_revision=0 where id=%s and owner_user_id=rkb_current_actor_id() and source_sha256=%s
                 and id in(select document_id from rkb_ingestion_jobs where source_file_id=%s)''',(document,fixture['source_sha256'],args.file_id));out['control_archived']=result.rowcount==1
            if out.get('new_bge_vectors')==12:
                after=await digest(document);before=json.loads((root/'baseline.json').read_text())['corpus'];assert after==before;out['existing_corpus_vector_digests_unchanged']=True
        for t in tokens:
            access=await provider.load_access_token(t.access_token)
            if access:await provider.revoke_token(access)
        out['temporary_oauth_families_revoked']=all([await provider.load_access_token(t.access_token) is None for t in tokens]);await b.aclose();save(root/('prepare-receipt.json' if args.prepare else 'automatic-indexing-acceptance.json'),out)
    print(json.dumps({k:v for k,v in out.items() if k not in ('control_document','control_chunk_ids','interrupted_document_job')},indent=2,default=str))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('work_dir',type=Path);parser.add_argument('--file-id',required=True);mode=parser.add_mutually_exclusive_group(required=True);mode.add_argument('--prepare',action='store_true');mode.add_argument('--execute',action='store_true');parser.add_argument('--interrupt-bge',action='store_true');asyncio.run(run(parser.parse_args()))
