"""Owned synthetic public-OAuth acceptance. No historical book parsing/backfill.

Prepare reads source images; execute requires the operator to review those images
before supplying bounded source/visual semantics. Private receipts stay managed.
"""
import argparse,asyncio,base64,hashlib,json,os,subprocess,time
from pathlib import Path
from uuid import UUID,uuid4
import httpx
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from regional_knowledge.contracts import Principal
from regional_knowledge.search_material import material
from regional_knowledge.bge_queue import BgeQueue

def save(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str));path.chmod(0o600)

def make_pdf(path):
    import pymupdf as fitz
    book=fitz.open();page=book.new_page()
    page.insert_textbox(fitz.Rect(60,60,535,300),'SYNTHETIC TRANSPORT CONTROL. NOT HISTORICAL EVIDENCE.\n\nDieses selbst erstellte Testheft zeigt einen blauen Leuchtturm und ein rotes Fahrrad. Es prueft private Quellen, Abbildungen und Wiederaufnahme. Keine historischen Aussagen.',fontsize=12)
    page=book.new_page()
    blue=(0.05,0.25,0.9);yellow=(1,0.8,0)
    page.draw_rect(fitz.Rect(250,180,340,440),color=blue,fill=blue)
    page.draw_polyline([fitz.Point(230,180),fitz.Point(295,125),fitz.Point(360,180)],color=blue,closePath=True,fill=blue)
    page.draw_rect(fitz.Rect(267,190,323,225),color=yellow,fill=yellow)
    page.draw_rect(fitz.Rect(275,350,315,440),color=(0,0,0),fill=(0,0,0))
    for y in (460,480,500):page.draw_polyline([fitz.Point(x,y+(10 if i%2 else 0)) for i,x in enumerate(range(100,501,40))],color=blue)
    page.insert_text((65,600),'Abb. 1: Blauer Leuchtturm am Meer.',fontsize=14)
    page=book.new_page()
    for x in (170,420):page.draw_circle(fitz.Point(x,410),80,color=(0.9,0,0),width=8)
    for start,end in [((170,410),(270,285)),((270,285),(330,410)),((330,410),(170,410)),((270,285),(390,285)),((390,285),(330,410)),((390,285),(420,410))]:
        page.draw_line(fitz.Point(*start),fitz.Point(*end),color=(0,0.65,0.15),width=8)
    page.draw_line(fitz.Point(260,270),fitz.Point(300,270),color=(0,0,0),width=8)
    page.draw_line(fitz.Point(390,285),fitz.Point(400,245),color=(0,0,0),width=7)
    page.draw_line(fitz.Point(400,245),fitz.Point(435,245),color=(0,0,0),width=7)
    book.save(path);book.close();path.chmod(0o600)

async def run(args):
    root=args.work_dir;assert (root/'.artifact.json').exists()
    load_service_env();b=backend_from_env();owner_id=os.environ['RKB_OWNER_SUBJECT']
    principal=Principal(subject=owner_id,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],issuer=os.environ['RKB_AUTH_ISSUER'],access_token='internal-actor-bridge')
    provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL']);tokens=[]
    def mint(subject):
        token=provider.store.mutate(lambda s:provider._mint_family(s,client_id=principal.client_id,scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=subject));tokens.append(token);return token
    owner=mint(owner_id);fixture_path=root/'visual-fixture.json';fixture=json.loads(fixture_path.read_text()) if fixture_path.exists() else None
    other_owner=fixture['other_owner'] if fixture else str(uuid4())
    if not fixture and (root/'synthetic-visual-control.pdf').exists():
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            old=await(await db.execute("select owner_user_id from rkb_ingestion_jobs where source_file_id='rkb-robust-control-20261004-other-owner'")).fetchall()
            assert len(old)<=1
            if old:other_owner=str(old[0]['owner_user_id'])
    foreign=mint(other_owner);out={'manual_backfill_used':False,'external_paid_embedding_calls':0,'server_ocr_vlm':False,'public_media_publication':False}
    vibe_stopped=False
    try:
        async with httpx.AsyncClient(timeout=90,trust_env=False) as client:
            sequence=0
            async def call(name,arguments,token=owner,raw=False,deny=False):
                nonlocal sequence
                sequence+=1
                response=await client.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':sequence,'method':'tools/call','params':{'name':name,'arguments':arguments}});response.raise_for_status()
                data=json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith('data: '))) if response.headers.get('content-type','').startswith('text/event-stream') else response.json()
                failed='error' in data or data.get('result',{}).get('isError',False)
                if deny:return failed
                if failed:raise RuntimeError('Public tool failed: '+name)
                result=data['result']
                return result if raw else result.get('structuredContent') or json.loads(result['content'][0]['text'])
            async def digest(excluded=()):
                async with b.data_client._connection({'x-rkb-service':'1'}) as db:
                    return await(await db.execute('''select
                     (select md5(string_agg(c.id::text||coalesce(c.embedding::text,''),',' order by c.id)) from rkb_chunks c where not(c.document_id=any(%s::uuid[]))) legacy,
                     (select md5(string_agg(e.chunk_id::text||e.embedding::text||e.updated_at::text,',' order by e.chunk_id)) from rkb_chunk_embeddings_e5 e join rkb_chunks c on c.id=e.chunk_id where not(c.document_id=any(%s::uuid[]))) e5,
                     (select md5(string_agg(e.chunk_id::text||e.embedding::text||e.updated_at::text,',' order by e.chunk_id)) from rkb_chunk_embeddings_bge e join rkb_chunks c on c.id=e.chunk_id where not(c.document_id=any(%s::uuid[]))) bge''',(list(excluded),)*3)).fetchone()
            if args.prepare:
                if fixture:raise RuntimeError('Fixture already prepared; resume execute/readback')
                if not (root/'visual-baseline.json').exists():save(root/'visual-baseline.json',{'corpus':await digest(),'readiness':await call('indexing_status',{})})
                pdf=root/'synthetic-visual-control.pdf'
                if not pdf.exists():make_pdf(pdf)
                sha=hashlib.sha256(pdf.read_bytes()).hexdigest()
                key='users/'+owner_id+'/transport-controls/rkb-robust-control-20261004/'+sha+'.pdf'
                await b.object_store.put_file(key,str(pdf),'application/pdf')
                url=await asyncio.to_thread(b.object_store.client.generate_presigned_url,'get_object',Params={'Bucket':b.object_store.bucket,'Key':key},ExpiresIn=600)
                async def start(suffix,token=owner):
                    return await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':'rkb-robust-control-20261004-'+suffix,'mime_type':'application/pdf'},'metadata':{'title':'Synthetic visual transport control; not historical evidence','language':'de'}},token)
                first,concurrent=await asyncio.gather(start('race-a'),start('race-b'))
                assert first['document_id']==concurrent['document_id'] and first['ingestion_id']==concurrent['ingestion_id']
                repeat=await start('different-file-id');assert repeat['document_id']==first['document_id']
                other=await start('other-owner',foreign);assert other['document_id']!=first['document_id']
                response=await call('book_pages',{'ingestion_id':first['ingestion_id'],'batch_size':3},raw=True);manifest=json.loads(response['content'][0]['text']);images=[c for c in response['content'] if c['type']=='image'];assert len(images)==3
                for page,image in zip(manifest['pages'],images,strict=True):
                    path=root/f"visual-page-{page['physical_page_index']+1}.jpg";path.write_bytes(base64.b64decode(image['data']));path.chmod(0o600)
                assert not (manifest['pages'][2]['native_text'] or '').strip()
                fixture={'document_id':first['document_id'],'ingestion_id':first['ingestion_id'],'other_document_id':other['document_id'],'other_owner':other_owner,'source_sha256':sha,'pages':manifest['pages']}
                save(fixture_path,fixture);save(root/'visual-prepare.json',{'public_ingress':True,'concurrent_starts_logical_documents':1,'new_file_id_extra_documents':0,'different_owner_separate_root':True,'image_only_native_text_empty':True})
            else:
                assert fixture,'prepare and visually review the page images first'
                doc=UUID(fixture['document_id']);ingestion=fixture['ingestion_id']
                if args.readback:
                    for cid in fixture['chunk_ids']:
                        evidence=await call('fetch',{'id':cid})
                        for description in evidence['metadata']['illustrations']:
                            assert description['vibepublish_entry_ref'] and description['visual_description_provenance']=='model_observation'
                            image_result=await call('illustration_fetch',{'id':description['uri']},raw=True)
                            image=next(c for c in image_result['content'] if c['type']=='image')
                            assert hashlib.sha256(base64.b64decode(image['data'])).hexdigest()==description['source_crop_sha256']
                    restored=await call('indexing_status',{});assert restored['e5_missing']==restored['bge_missing']==0
                    assert await digest([doc,UUID(fixture['other_document_id'])])==json.loads((root/'visual-baseline.json').read_text())['corpus']
                    save(root/'robust-public-main-readback.json',{'authorized_crops':2,'rich_fetch':True,'actor_readiness':restored,'existing_corpus_vectors_unchanged':True})
                    print('Exact-main public fetch/crop/readiness readback passed')
                    return
                if args.execute:
                    pages=[];chunks=[]
                    for source in fixture['pages']:
                        i=source['physical_page_index'];pid=source['page_id'];box={'left':0,'top':0,'right':1000,'bottom':1000}
                        page={'page_id':pid,'physical_page_index':i,'source_material':'visual_reviewed','source_review_note':'Operator-created source render visually reviewed; complete text/figure accounted for.','regions':[],'illustrations':[]}
                        if i==0:
                            page['regions']=[{'region_key':'body','kind':'body','bbox':box,'reading_order':0,'source_text':source['native_text']}]
                            chunk={'chunk_key':'body','title':'Synthetic text control','region_refs':[{'page_id':pid,'region_key':'body'}]}
                        else:
                            page['regions']=[{'region_key':'figure','kind':'figure','bbox':{'left':100,'top':100,'right':900,'bottom':650},'reading_order':0}]
                            figure={'illustration_key':'figure','source_region_key':'figure','kind':'drawing','visual_description_provenance':'model_observation','visual_description_language':'de','visual_description':'Ein blauer Leuchtturm mit gelbem Licht, schwarzer Tuer und blauen Meereswellen.' if i==1 else 'Ein rotes Fahrrad mit zwei roten Raedern und einem gruenen Rahmen.'}
                            if i==1:
                                page['regions'].append({'region_key':'caption','kind':'caption','bbox':{'left':100,'top':680,'right':900,'bottom':770},'reading_order':1,'source_text':source['native_text'].strip()});figure['caption_region_keys']=['caption']
                            page['illustrations']=[figure]
                            chunk={'chunk_key':f'figure-{i}','title':'Synthetic lighthouse' if i==1 else 'Synthetic bicycle','region_refs':[{'page_id':pid,'region_key':'caption' if i==1 else 'figure'}],'illustration_refs':[{'page_id':pid,'illustration_key':'figure'}]}
                        pages.append(page);chunks.append(chunk)
                    await call('book_ingest',{'command':'stage','ingestion_id':ingestion,'pages':pages,'chunks':chunks})
                    assert (await call('book_ingest',{'command':'validate','ingestion_id':ingestion}))['state']=='ready'
                    subprocess.run(['systemctl','--user','stop','vibepublish-worker'],check=True);vibe_stopped=True
                    await call('book_ingest',{'command':'finalize','ingestion_id':ingestion})
                    for _ in range(90):
                        state=await call('book_ingest',{'command':'status','ingestion_id':ingestion})
                        if state['state']=='finalized' and state['indexing']['e5_ready']==3:break
                        await asyncio.sleep(1)
                    assert state['state']=='finalized' and state['indexing']['e5_ready']==3
                    async with b.data_client._connection(b._headers(principal)) as db:
                        pending=await(await db.execute('select count(*) n from rkb_illustrations i join rkb_pages p on p.id=i.page_id where i.document_id=%s and p.revision=1 and i.vibepublish_entry_ref is null',(doc,))).fetchone();assert pending['n']==2
                    subprocess.run(['systemctl','--user','restart','regional-knowledge-indexing'],check=True)
                    subprocess.run(['systemctl','--user','start','vibepublish-worker'],check=True);vibe_stopped=False
                    out['book_finalized_during_provider_worker_outage']=True;out['index_owner_restarted']=True
                async def ready():
                    for _ in range(600):
                        state=await call('indexing_status',{'document_id':str(doc)})
                        async with b.data_client._connection(b._headers(principal)) as db:
                            figures=await(await db.execute('select i.id,i.vibepublish_entry_ref,i.mirror_operation_id from rkb_illustrations i join rkb_pages p on p.id=i.page_id where i.document_id=%s and p.revision=1',(doc,))).fetchall()
                        if state['e5_ready']==state['bge_ready']==3 and len(figures)==2 and all(r['vibepublish_entry_ref'] for r in figures):return state,figures
                        await asyncio.sleep(1)
                    raise TimeoutError('automatic visual indexing/mirror acceptance deadline')
                state,figures=await ready();out['automatic_vectors']={'e5':3,'bge':3};out['verified_private_mirrors']=len(figures);out['mirror_receipts']=figures
                async with b.data_client._connection(b._headers(principal)) as db:
                    rows=await(await db.execute('select id,title,text_start,text_end,illustration_ids from rkb_chunks where document_id=%s and revision=1',(doc,))).fetchall()
                fixture['chunk_ids']=[str(r['id']) for r in rows];save(fixture_path,fixture)
                bicycle=next(r for r in rows if r['title']=='Synthetic bicycle');lighthouse=next(r for r in rows if r['title']=='Synthetic lighthouse')
                fetched=await call('fetch',{'id':str(bicycle['id'])});assert fetched['text']=='' and fetched['metadata']['illustrations'][0]['visual_description_provenance']=='model_observation'
                descriptor=fetched['metadata']['illustrations'][0];crop=await call('illustration_fetch',{'id':descriptor['uri']},raw=True)
                image=next(c for c in crop['content'] if c['type']=='image');assert hashlib.sha256(base64.b64decode(image['data'])).hexdigest()==descriptor['source_crop_sha256']
                assert await call('illustration_fetch',{'id':descriptor['uri']},foreign,deny=True)
                out.update(image_only_chunk_empty_source=True,model_observation_label=True,authorized_crop_image_content=True,foreign_crop_denied=True)
                async def query(text,target):
                    result=await call('search',{'query':text});job=result.get('main_job_id')
                    for _ in range(120):
                        if result['main_state']=='ready':break
                        await asyncio.sleep(1);result=await call('search',{'query':text,**({'main_job_id':job} if job else {})});job=result.get('main_job_id')
                    assert any(r['id']==str(target) for r in result['results']),('fixture query not found',result['retrieval_mode'])
                    return {'query':text,'retrieval_mode':result['retrieval_mode'],'target_found':True}
                out['queries']=[await query('Blauer Leuchtturm am Meer',lighthouse['id']),await query('Fahrrad mit zwei roten Raedern',bicycle['id']),await query('Красный велосипед с двумя колёсами и зелёной рамой',bicycle['id'])]
                if args.execute:
                    async with b.data_client._connection({'x-rkb-service':'1'}) as db:
                        unchanged=await(await db.execute('select e.chunk_id,e.updated_at e5_at,b.updated_at bge_at from rkb_chunk_embeddings_e5 e join rkb_chunk_embeddings_bge b using(chunk_id) where e.chunk_id=any(%s::uuid[])',([r['id'] for r in rows if r['id']!=bicycle['id']],))).fetchall()
                        observation='Ein rotes Fahrrad mit zwei roten Raedern, einem gruenen Rahmen und schwarzem Lenker. Model observation only; the unsupported date 1777 is not a printed fact.'
                        text,sha=material('',[{'visual_description':observation}])
                        await db.execute('update rkb_illustrations set visual_description=%s where id=%s and document_id=%s',(observation,bicycle['illustration_ids'][0],doc))
                        await db.execute("update rkb_chunks set search_material=%s,search_material_sha256=%s,fts=to_tsvector('simple',%s) where id=%s and document_id=%s",(text,sha,text,bicycle['id'],doc))
                    await ready()
                    async with b.data_client._connection({'x-rkb-service':'1'}) as db:
                        after=await(await db.execute('select e.chunk_id,e.updated_at e5_at,b.updated_at bge_at from rkb_chunk_embeddings_e5 e join rkb_chunk_embeddings_bge b using(chunk_id) where e.chunk_id=any(%s::uuid[])',([r['id'] for r in rows if r['id']!=bicycle['id']],))).fetchall();assert unchanged==after
                    assert (await call('fetch',{'id':str(bicycle['id'])}))['text']==''
                    out.update(changed_material_e5_delta=1,changed_material_bge_delta=1,unchanged_chunks_skipped=2,misleading_description_not_printed_evidence=True)
                    pdf=root/'synthetic-visual-control.pdf';key='users/'+owner_id+'/transport-controls/rkb-robust-control-20261004/'+fixture['source_sha256']+'.pdf'
                    url=await asyncio.to_thread(b.object_store.client.generate_presigned_url,'get_object',Params={'Bucket':b.object_store.bucket,'Key':key},ExpiresIn=300)
                    revision=await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':'rkb-robust-control-20261004-new-revision','mime_type':'application/pdf'},'metadata':{'duplicate_policy':'new_revision','title':'Must not overwrite original metadata'}})
                    assert revision['document_id']==str(doc) and revision['ingestion_id']!=ingestion
                    async with b.data_client._connection(b._headers(principal)) as db:
                        record=await(await db.execute('select staged_revision from rkb_ingestion_jobs where id=%s',(UUID(revision['ingestion_id']),))).fetchone();assert record['staged_revision']==2
                        assert (await(await db.execute('select count(*) n from rkb_documents where owner_user_id=%s and source_sha256=%s',(UUID(owner_id),fixture['source_sha256']))).fetchone())['n']==1
                    out['new_revision_same_document']=True
                queue=BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
                with queue.connect() as db:
                    jobs=[dict(row) for row in db.execute("select id,identity,state from jobs where kind='document' and actor=?",(owner_id,)) if json.loads(row['identity']).get('chunk_id') in {str(r['id']) for r in rows}]
                out['control_document_jobs']=len(jobs);assert len(jobs)==4 and all(r['state']=='done' for r in jobs)
                before_jobs=jobs;await call('book_ingest',{'command':'finalize','ingestion_id':ingestion});await asyncio.sleep(6)
                with queue.connect() as db:
                    after_jobs=[dict(row) for row in db.execute("select id,identity,state from jobs where kind='document' and actor=?",(owner_id,)) if json.loads(row['identity']).get('chunk_id') in {str(r['id']) for r in rows}]
                assert before_jobs==after_jobs;out['replay_duplicate_jobs']=0
                assert await digest([doc,UUID(fixture['other_document_id'])])==json.loads((root/'visual-baseline.json').read_text())['corpus'];out['existing_corpus_vectors_unchanged']=True
                if args.execute:
                    async with b.data_client._connection({'x-rkb-service':'1'}) as db:
                        await db.execute('update rkb_documents set active_revision=0 where id=any(%s::uuid[]) and source_sha256=%s',([doc,UUID(fixture['other_document_id'])],fixture['source_sha256']))
                    out['synthetic_controls_archived']=True
                save(root/'robust-production-acceptance.json',out)
    finally:
        if vibe_stopped:subprocess.run(['systemctl','--user','start','vibepublish-worker'],check=True)
        for token in tokens:
            access=await provider.load_access_token(token.access_token)
            if access:await provider.revoke_token(access)
        await b.aclose()
    print(json.dumps({k:v for k,v in out.items() if k!='mirror_receipts'},ensure_ascii=False,indent=2,default=str))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('work_dir',type=Path)
    group=parser.add_mutually_exclusive_group(required=True);group.add_argument('--prepare',action='store_true');group.add_argument('--execute',action='store_true');group.add_argument('--readback',action='store_true')
    asyncio.run(run(parser.parse_args()))
