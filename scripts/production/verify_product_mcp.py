"""Owned small source controls through real OAuth/MCP, with durable receipts.

No credentials, signed URLs or book text are written to public source control.
Generated controls assert no historical facts. Existing encoders are reused.
"""
import argparse,asyncio,base64,hashlib,io,json,os,time
from pathlib import Path
from uuid import UUID,uuid5
import httpx
import pymupdf as fitz
from PIL import Image
from operator_env import load_service_env
from regional_knowledge.oauth_provider import oauth_provider_from_env,KNOWLEDGE_SCOPE
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.indexing import IndexReconciler
from regional_knowledge.bge_queue import BgeQueue


def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,default=str,indent=2));path.chmod(0o600)

async def run(args):
    load_service_env();os.environ['RKB_SQLITE_CORPUS_PATH']='/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3'
    root=args.evidence;root.mkdir(mode=0o700,parents=True,exist_ok=True);b=backend_from_env()
    provider=oauth_provider_from_env(issuer=os.environ['RKB_AUTH_ISSUER'],resource=os.environ['RKB_RESOURCE_URL'])
    token=provider.store.mutate(lambda state:provider._mint_family(state,client_id=os.environ['RKB_OAUTH_CLIENT_ID'],scopes=[KNOWLEDGE_SCOPE],resource=os.environ['RKB_RESOURCE_URL'],subject=os.environ['RKB_OWNER_SUBJECT']))
    receipt_path=root/'product.json';report=json.loads(receipt_path.read_text()) if receipt_path.exists() else {'materials':[]}
    endpoint=args.endpoint or os.environ['RKB_RESOURCE_URL'];seq=0
    async with httpx.AsyncClient(timeout=120,trust_env=False) as client:
        async def rpc(method,params):
            nonlocal seq
            seq+=1;response=await client.post(endpoint,headers={'Authorization':'Bearer '+token.access_token,'Accept':'application/json,text/event-stream','MCP-Protocol-Version':'2025-11-25'},json={'jsonrpc':'2.0','id':seq,'method':method,'params':params});response.raise_for_status()
            data=response.json() if 'application/json' in response.headers['content-type'] else json.loads(next(x[6:] for x in response.text.splitlines() if x.startswith('data: ')))
            if 'error' in data or data.get('result',{}).get('isError'):
                save(root/'tool-error.json',data);raise RuntimeError('MCP operation failed: '+method)
            return data['result']
        async def call(name,values,*,raw=False):
            result=await rpc('tools/call',{'name':name,'arguments':values})
            return result if raw else result.get('structuredContent') or json.loads(result['content'][0]['text'])
        try:
            tools=await rpc('tools/list',{});save(root/'mcp-schemas.json',tools)
            assert {'catalog','source_proof','book_ingest','search','fetch'}<=set(t['name'] for t in tools['tools'])
            report['endpoint']=endpoint;report['schemas']=len(tools['tools']);save(receipt_path,report)
            if args.verify_only:
                for material in report['materials']:
                    cat=await call('catalog',{'command':'get','document_id':material['document_id']});assert cat['catalog']['kind']==material['kind']
                    if material.get('ingestion_id'):
                        status=await call('book_ingest',{'command':'status','ingestion_id':material['ingestion_id']});assert status['state']=='finalized'
                    for ident in material.get('chunk_ids',[]):assert (await call('fetch',{'id':ident}))['text']
                report['postrollout_verified']=True;save(receipt_path,report);print(json.dumps({'postrollout_verified':True,'materials':len(report['materials']),'schemas':report['schemas']}));return
            for kind in ('book','journal_issue','article'):
                material=next((m for m in report['materials'] if m['kind']==kind and not m.get('component')),None)
                if not material:
                    material={'kind':kind,'file_id':'rkb-sqlite-v2-control-'+root.name+'-'+kind};report['materials'].append(material);save(receipt_path,report)
                pdf=root/(kind+'.pdf')
                if not pdf.exists():
                    with fitz.open() as source:
                        if kind=='journal_issue':
                            page=source.new_page(width=700,height=500);page.insert_text((40,60),'RKB synthetic issue cover. Number 7. Not historical evidence.',fontsize=15)
                            page=source.new_page(width=700,height=500)
                            for point,text in [((40,80),'Article Alpha. Alice and Boris.'),((40,120),'Archive record 1945.'),((380,80),'Article Beta. Carol and David.'),((380,120),'Unrelated column 1234.')]:page.insert_text(point,text,fontsize=14)
                        elif kind=='book':
                            page=source.new_page(width=700,height=500);page.insert_text((40,60),'RKB synthetic title page. Alice and Boris.',fontsize=15)
                            page=source.new_page(width=700,height=500);page.insert_text((40,80),'The purple compass follows the library corridor.',fontsize=15)
                        else:
                            with fitz.open() as native:
                                page=native.new_page(width=700,height=350);page.insert_text((40,80),'Archive record 1945.',fontsize=16);page.insert_text((380,80),'Unrelated column 1234.',fontsize=16)
                                pix=page.get_pixmap(matrix=fitz.Matrix(2,2));page=source.new_page(width=700,height=350);page.insert_image(page.rect,stream=pix.tobytes('png'))
                        source.save(pdf)
                    pdf.chmod(0o600)
                if not material.get('ingestion_id'):
                    sha=hashlib.sha256(pdf.read_bytes()).hexdigest();key='users/'+os.environ['RKB_OWNER_SUBJECT']+'/transport-controls/'+material['file_id']+'/'+sha+'.pdf'
                    await b.object_store.put_file(key,str(pdf),'application/pdf')
                    url=await asyncio.to_thread(b.object_store.client.generate_presigned_url,'get_object',Params={'Bucket':b.object_store.bucket,'Key':key},ExpiresIn=900)
                    metadata={'title':'Synthetic '+kind+' control; no historical claims','authors':['Alice','Boris'],'language':'en','catalog':{'kind':kind,'form':'research_article' if kind=='article' else 'reference','purpose':'educational','cover_page':0,'cover_kind':'issue_cover' if kind=='journal_issue' else 'title_page',**({'issue':'7','volume':'2','identifiers':[{'kind':'ISSN','value':'unknown','status':'unverified'}]} if kind=='journal_issue' else {})}}
                    started=await call('book_ingest',{'command':'start','file':{'download_url':url,'file_id':material['file_id'],'mime_type':'application/pdf','file_name':pdf.name},'metadata':metadata})
                    material.update(document_id=started['document_id'],ingestion_id=started['ingestion_id'],source_sha256=sha,transport_key=key);save(receipt_path,report)
                status=await call('book_ingest',{'command':'status','ingestion_id':material['ingestion_id']})
                if status['state']!='finalized' and status.get('next_cursor')!='vectors':
                    manifest_result=await call('book_pages',{'ingestion_id':material['ingestion_id'],'batch_size':4},raw=True);save(root/(kind+'-pages.json'),{'content':[c for c in manifest_result['content'] if c['type']=='text']})
                    manifest=json.loads(next(c['text'] for c in manifest_result['content'] if c['type']=='text'));pages=[];chunks=[]
                    for i,c in enumerate(x for x in manifest_result['content'] if x['type']=='image'):(root/(kind+'-mcp-page-'+str(i)+'.png')).write_bytes(base64.b64decode(c['data']))
                    for page in manifest['pages']:
                        idx=page['physical_page_index'];pid=page['page_id'];regions=[]
                        if kind=='article':
                            regions=[{'region_key':'body','kind':'body','bbox':{'left':40,'top':170,'right':310,'bottom':260},'reading_order':0,'source_text':'Archive record 1945.','normalized_text':'Archive record 1945.'},{'region_key':'other','kind':'body','bbox':{'left':520,'top':170,'right':980,'bottom':260},'reading_order':1,'source_text':'Unrelated column 1234.','normalized_text':'Unrelated column 1234.'}]
                        elif kind=='journal_issue' and idx==1:
                            for j,(name,text,box) in enumerate([('a-title','Article Alpha. Alice and Boris.',(40,100,480,190)),('a-body','Archive record 1945.',(40,200,480,270)),('b-title','Article Beta. Carol and David.',(520,100,990,190)),('b-body','Unrelated column 1234.',(520,200,990,270))]):regions.append({'region_key':name,'kind':'body','bbox':dict(zip(('left','top','right','bottom'),box)),'reading_order':j,'source_text':text,'normalized_text':text})
                        else:regions=[{'region_key':'body','kind':'body','bbox':{'left':0,'top':0,'right':1000,'bottom':1000},'reading_order':0,'source_text':page['native_text'],'normalized_text':page['native_text']}]
                        pages.append({'page_id':pid,'physical_page_index':idx,'printed_page_number':str(idx+10),'source_material':'visual_reviewed','source_review_note':'Codex inspected the complete synthetic source render; all printed text and both columns are represented; there are no figures.','regions':regions})
                        if kind=='journal_issue' and idx==1:
                            for letter in ('a','b'):chunks.append({'chunk_key':'article-'+letter,'article_id':'article-'+letter,'title':'Synthetic article '+letter,'region_refs':[{'page_id':pid,'region_key':letter+'-title'},{'page_id':pid,'region_key':letter+'-body'}]})
                        elif kind=='article':
                            for r in regions:chunks.append({'chunk_key':r['region_key'],'title':'Synthetic scan '+r['region_key'],'region_refs':[{'page_id':pid,'region_key':r['region_key']}]})
                        else:chunks.append({'chunk_key':'page-'+str(idx),'title':'Synthetic '+kind,'region_refs':[{'page_id':pid,'region_key':'body'}]})
                    if kind=='book':chunks=[{'chunk_key':'multi-page','title':'Synthetic multi-page book','region_refs':[{'page_id':p['page_id'],'region_key':'body'} for p in pages]}]
                    await call('book_ingest',{'command':'stage','ingestion_id':material['ingestion_id'],'pages':pages,'chunks':chunks})
                    validation=await call('book_ingest',{'command':'validate','ingestion_id':material['ingestion_id']});save(root/(kind+'-validation.json'),validation)
                    assert validation['state']=='ready'
                    await call('book_ingest',{'command':'finalize','ingestion_id':material['ingestion_id']})
                    # One wait for durable materialization, not repeated effectful finalize.
                    await asyncio.sleep(2)
                # Reuse the exact existing encoder workers/queue. No new model.
                worker=IndexReconciler(b,BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']));actor=worker.actor(os.environ['RKB_OWNER_SUBJECT'])
                for _ in range(30):
                    await worker.e5(actor,UUID(material['document_id']));await worker.bge(actor,UUID(material['document_id']));await b.activate_pending()
                    status=await call('book_ingest',{'command':'status','ingestion_id':material['ingestion_id']})
                    if status['state']=='finalized':break
                    await asyncio.sleep(3)
                assert status['state']=='finalized'
                material['chunk_ids']=[c['id'] for c in b.corpus.rows('rkb_chunks',{'document_id':material['document_id'],'revision':1})];save(receipt_path,report)
                if material.get('transport_key'):await b.object_store.delete(material['transport_key']);material.pop('transport_key');save(receipt_path,report)
                cat=await call('catalog',{'command':'get','document_id':material['document_id']});assert cat['catalog']['kind']==kind and cat['authors']==['Alice','Boris']
                for ident in material['chunk_ids']:assert (await call('fetch',{'id':ident}))['text']
            issue=next(m for m in report['materials'] if m['kind']=='journal_issue')
            component=next((m for m in report['materials'] if m.get('component')),None)
            if not component:
                chunk=next(c for c in b.corpus.rows('rkb_chunks',{'document_id':issue['document_id'],'revision':1}) if c['metadata'].get('article_id')=='article-a')
                created=await call('book_ingest',{'command':'start','metadata':{'title':'Synthetic article Alpha','authors':['Alice','Boris'],'catalog':{'kind':'article','parent_id':issue['document_id'],'physical_page_start':1,'physical_page_end':1,'region_ids':chunk['region_ids']}}})
                component={'kind':'article','component':True,'document_id':created['document_id'],'chunk_ids':[chunk['id']]};report['materials'].append(component);save(receipt_path,report)
            cat=await call('catalog',{'command':'get','document_id':component['document_id']});assert cat['source_document_id']==issue['document_id'] and cat['chunk_ids']==component['chunk_ids']
            report['shared_original_semantic_ranges']=True;save(receipt_path,report)
            for material in report['materials']:
                if material.get('component'):continue
                result=await call('catalog',{'command':'cover','document_id':material['document_id']},raw=True)
                for c in result['content']:
                    if c['type']=='image':(root/(material['kind']+'-cover.webp')).write_bytes(base64.b64decode(c['data']))
            report['actual_covers']=True;save(receipt_path,report)
            print(json.dumps({'materials':len(report['materials']),'schemas':report['schemas'],'shared_original_semantic_ranges':True,'actual_covers':True}))
        finally:
            access=await provider.load_access_token(token.access_token)
            if access:await provider.revoke_token(access)
            await b.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--evidence',type=Path,required=True);parser.add_argument('--endpoint');parser.add_argument('--verify-only',action='store_true');asyncio.run(run(parser.parse_args()))
