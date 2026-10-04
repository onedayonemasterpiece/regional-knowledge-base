"""Small owned public-OAuth PDF/DjVu/archive/cache acceptance; no real-book parsing."""
import argparse,asyncio,base64,hashlib,json,os,subprocess,time
from pathlib import Path
from uuid import UUID
import httpx
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from regional_knowledge.contracts import Principal
from regional_knowledge.source_adapter import DjVuProcessor

def save(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str));path.chmod(0o600)

async def run(args):
    root=args.work_dir;assert (root/'.artifact.json').exists();load_service_env();backend=backend_from_env()
    owner=os.environ['RKB_OWNER_SUBJECT'];client_id=os.environ['RKB_OAUTH_CLIENT_ID']
    principal=Principal(subject=owner,client_id=client_id,issuer=os.environ['RKB_AUTH_ISSUER'],access_token='internal-actor-bridge')
    provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL'])
    token=provider.store.mutate(lambda s:provider._mint_family(s,client_id=client_id,scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=owner))
    fixture_path=root/'archive-control.json';fixture=json.loads(fixture_path.read_text()) if fixture_path.exists() else {}
    try:
        async with httpx.AsyncClient(timeout=120,trust_env=False) as http:
            seq=0
            async def call(name,arguments,raw=False):
                nonlocal seq
                seq+=1;r=await http.post(os.environ['RKB_RESOURCE_URL'],headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':seq,'method':'tools/call','params':{'name':name,'arguments':arguments}});r.raise_for_status()
                body=json.loads(next(line[6:] for line in r.text.splitlines() if line.startswith('data: '))) if r.headers.get('content-type','').startswith('text/event-stream') else r.json()
                if 'error' in body or body.get('result',{}).get('isError'):raise RuntimeError('Public tool failed: '+name)
                result=body['result'];return result if raw else result.get('structuredContent') or json.loads(result['content'][0]['text'])
            if args.prepare:
                import pymupdf as fitz
                from PIL import Image
                if fixture:raise RuntimeError('Control already prepared; resume execute')
                baseline=await call('indexing_status',{});save(root/'archive-baseline.json',baseline)
                pdf=root/'archive-control.pdf';book=fitz.open();page=book.new_page(width=600,height=800)
                page.insert_text((60,70),'SYNTHETIC ARCHIVE CONTROL. NOT HISTORICAL EVIDENCE.',fontsize=12)
                page.draw_circle(fitz.Point(300,330),100,color=(0,0,1),fill=(0,0,1));page.insert_text((65,550),'Abb. 1: Blauer Kreis.',fontsize=14)
                book.save(pdf);book.close()
                djvu=root/'archive-control.djvu';decoder=DjVuProcessor();singles=[]
                for index,color in enumerate(('blue','red')):
                    ppm=root/f'djvu-source-{index}.ppm';Image.new('RGB',(320,400),color).save(ppm)
                    single=root/f'djvu-source-{index}.djvu';decoder.command('c44',ppm,single);singles.append(single)
                decoder.command('djvm','-c',djvu,*singles)
                subprocess.run(['systemctl','--user','stop','vibepublish-worker'],check=True)
                # Deliberate fault remains until execute; public gateway is still available.
                for format,path,mime in [('pdf',pdf,'application/pdf'),('djvu',djvu,'image/vnd.djvu')]:
                    data=path.read_bytes();sha=hashlib.sha256(data).hexdigest();key=f'users/{owner}/transport-controls/archive-20261004/{sha}.{format}'
                    await backend.object_store.put_file(key,str(path),mime)
                    url=await asyncio.to_thread(backend.object_store.client.generate_presigned_url,'get_object',Params={'Bucket':backend.object_store.bucket,'Key':key},ExpiresIn=600)
                    result=await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':'rkb-archive-control-20261004-'+format,'mime_type':mime,'file_name':path.name},'metadata':{'title':'Synthetic '+format+' archive control; not historical evidence','language':'de'}})
                    replay=await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':'rkb-archive-control-20261004-'+format+'-replay','mime_type':mime}})
                    assert result['document_id']==replay['document_id']
                    pages=await call('book_pages',{'ingestion_id':result['ingestion_id'],'batch_size':2},raw=True)
                    manifest=json.loads(pages['content'][0]['text']);images=[v for v in pages['content'] if v['type']=='image']
                    assert len(images)==(1 if format=='pdf' else 2)
                    for i,image in enumerate(images):(root/f'{format}-page-{i}.jpg').write_bytes(base64.b64decode(image['data']))
                    fixture[format]={'document_id':result['document_id'],'ingestion_id':result['ingestion_id'],'sha256':sha,'pages':manifest['pages'],'fixture_key':key}
                save(fixture_path,fixture)
                # Allow the existing maintenance owner to admit source operations.
                await asyncio.sleep(10)
                async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                    row=await(await db.execute('select d.source_archive_status,o.deleted_at from rkb_documents d join rkb_objects o on o.document_id=d.id and o.kind=\'source_pdf\' where d.id=%s',(UUID(fixture['pdf']['document_id']),))).fetchone()
                    assert row['source_archive_status']=='pending' and row['deleted_at'] is None
                save(root/'archive-outage.json',{'provider_worker_stopped':True,'source_staging_retained':True,'source_archive_pending':True,'different_attachment_extra_roots':0,'pdf_visible_pages':1,'djvu_visible_pages':2,'server_ocr_vlm':False})
                print('PDF/DjVu public ingress/renders prepared; inspect images before execute')
            elif args.execute:
                assert fixture;control=fixture['pdf'];source=control['pages'][0];pid=source['page_id']
                page={'page_id':pid,'physical_page_index':0,'source_material':'visual_reviewed','source_review_note':'Owned synthetic source visually reviewed: exact heading, blue circle, printed caption.',
                    'regions':[{'region_key':'heading','kind':'body','bbox':{'left':50,'top':50,'right':950,'bottom':120},'reading_order':0,'source_text':'SYNTHETIC ARCHIVE CONTROL. NOT HISTORICAL EVIDENCE.'},
                               {'region_key':'figure','kind':'figure','bbox':{'left':300,'top':250,'right':700,'bottom':550},'reading_order':1},
                               {'region_key':'caption','kind':'caption','bbox':{'left':80,'top':650,'right':900,'bottom':730},'reading_order':2,'source_text':'Abb. 1: Blauer Kreis.'}],
                    'illustrations':[{'illustration_key':'circle','source_region_key':'figure','caption_region_keys':['caption'],'kind':'drawing','visual_description':'Ein ausgefuellter blauer Kreis.','visual_description_provenance':'model_observation','visual_description_language':'de'}]}
                await call('book_ingest',{'command':'stage','ingestion_id':control['ingestion_id'],'pages':[page],'chunks':[{'chunk_key':'control','title':'Synthetic blue circle archive evidence','region_refs':[{'page_id':pid,'region_key':'heading'},{'page_id':pid,'region_key':'caption'}],'illustration_refs':[{'page_id':pid,'illustration_key':'circle'}]}]})
                async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                    first=await(await db.execute('select staged_graph_object_id from rkb_ingestion_jobs where id=%s',(UUID(control['ingestion_id']),))).fetchone()
                page['source_review_note']+=' Entire single page accounted for.'
                await call('book_ingest',{'command':'stage','ingestion_id':control['ingestion_id'],'pages':[page],'chunks':[{'chunk_key':'control','title':'Synthetic blue circle archive evidence','region_refs':[{'page_id':pid,'region_key':'heading'},{'page_id':pid,'region_key':'caption'}],'illustration_refs':[{'page_id':pid,'illustration_key':'circle'}]}]})
                assert (await call('book_ingest',{'command':'validate','ingestion_id':control['ingestion_id']}))['state']=='ready'
                await call('book_ingest',{'command':'finalize','ingestion_id':control['ingestion_id']})
                for _ in range(90):
                    status=await call('book_ingest',{'command':'status','ingestion_id':control['ingestion_id']})
                    if status['state']=='finalized' and status['indexing']['e5_ready']==1:break
                    await asyncio.sleep(1)
                assert status['state']=='finalized' and status['source_archive_status']=='pending'
                async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                    objects=await(await db.execute('select kind,count(*) n from rkb_objects where document_id=%s group by kind',(UUID(control['document_id']),))).fetchall();assert not any(r['kind'] in ('page_render','text_projection') for r in objects)
                subprocess.run(['systemctl','--user','restart','regional-knowledge-indexing'],check=True)
                subprocess.run(['systemctl','--user','start','vibepublish-worker'],check=True)
                for _ in range(600):
                    async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                        docs=await(await db.execute('select id,source_archive_status,source_archive_ref from rkb_documents where id=any(%s::uuid[])',([control['document_id'],fixture['djvu']['document_id']],))).fetchall()
                        figure=await(await db.execute('select id,vibepublish_entry_ref,crop_object_id from rkb_illustrations where document_id=%s',(UUID(control['document_id']),))).fetchone()
                    ready=await call('indexing_status',{'document_id':control['document_id']})
                    if all(r['source_archive_status']=='verified' for r in docs) and figure and figure['vibepublish_entry_ref'] and ready['bge_ready']==1:break
                    await asyncio.sleep(1)
                assert all(r['source_archive_status']=='verified' for r in docs) and figure['vibepublish_entry_ref'] and ready['bge_ready']==1
                async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                    chunks=await(await db.execute('select id,source_text,text_object_id from rkb_chunks where document_id=%s',(UUID(control['document_id']),))).fetchall();assert len(chunks)==1 and chunks[0]['text_object_id'] is None
                fetched=await call('fetch',{'id':str(chunks[0]['id'])});assert 'Abb. 1: Blauer Kreis.' in fetched['text']
                crop=await call('illustration_fetch',{'id':str(figure['id'])},raw=True);assert any(c['type']=='image' for c in crop['content'])
                fixture.update(chunk_id=str(chunks[0]['id']),illustration_id=str(figure['id']),superseded_graph_id=str(first['staged_graph_object_id']));save(fixture_path,fixture)
                save(root/'archive-control-acceptance.json',{'source_archives':docs,'illustration':figure,'provider_outage_finalize_ok':True,'index_owner_restart_ok':True,'pdf_djvu_replay_extra_documents':0,'pdf_no_permanent_text_or_page_objects':True,'pdf_e5_ready':1,'pdf_bge_ready':1,'visible_djvu_pages':2,'server_ocr_vlm':False})
                print('PDF/DjVu source archives, KB illustration, replay/outage/restart acceptance passed')
            else:
                assert fixture
                control=fixture['pdf'];fetched=await call('fetch',{'id':fixture['chunk_id']});assert 'Abb. 1: Blauer Kreis.' in fetched['text']
                image=await call('illustration_fetch',{'id':fixture['illustration_id']},raw=True);assert any(c['type']=='image' for c in image['content'])
                pages=await call('book_pages',{'ingestion_id':control['ingestion_id'],'batch_size':1},raw=True);assert any(c['type']=='image' for c in pages['content'])
                save(root/'archive-public-after-gc.json',{'postgres_fetch':True,'illustration_after_crop_gc':True,'source_after_s3_gc':True,'indexing':await call('indexing_status',{})})
                print('Public fetch/crop/source readback passed after GC')
    finally:
        access=await provider.load_access_token(token.access_token)
        if access:await provider.revoke_token(access)
        await backend.aclose()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work-dir',required=True,type=Path);g=p.add_mutually_exclusive_group(required=True);g.add_argument('--prepare',action='store_true');g.add_argument('--execute',action='store_true');g.add_argument('--readback',action='store_true');asyncio.run(run(p.parse_args()))
