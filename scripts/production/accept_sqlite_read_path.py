"""Real read-path parity/retrieval/proof evidence, aggregate-only public output.

Detailed IDs/images are private retained artifacts; no corpus strings are printed.
This acceptance is explicitly for the migration bridge, not remote retirement.
"""
import argparse,asyncio,hashlib,io,json,os,shlex,time
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import _embedder_from_env,_object_store_from_env
from regional_knowledge.contracts import Principal
from regional_knowledge.quote_proof import FlashLiteLocator,paint,native_page,source_proof
from PIL import Image

async def run(out):
    load_service_env();out.mkdir(parents=True,exist_ok=True)
    for raw in Path('/home/dev/.env').read_text().splitlines():
        try:parts=shlex.split(raw)
        except ValueError:continue
        if len(parts)==1 and parts[0].startswith('GOOGLE_API_KEY='):os.environ['RKB_GEMINI_API_KEY']=parts[0].split('=',1)[1]
    os.environ['RKB_SCAN_PROOF_ENABLED']='1'
    kwargs={'embedder':_embedder_from_env(),'object_store':_object_store_from_env(),'pool_max_size':2}
    old=PostgresBackend(os.environ['KB_SUPABASE_SESSION_CONNECTION'],**kwargs)
    new=SQLiteBackend(os.environ['KB_SUPABASE_SESSION_CONNECTION'],corpus_path='/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3',**{**kwargs,'embedder':_embedder_from_env()})
    report={'bridge_only':True,'checks':{},'queries':[],'proofs':[]}
    try:
        active=[d for d in new.corpus.rows('rkb_documents') if d['active_revision']>0]
        owner=active[0]['owner_user_id'];actor=Principal(subject=owner,client_id='acceptance',issuer='application-actor-bridge',access_token='internal')
        chunks=[c for d in active if d['owner_user_id']==owner for c in new.corpus.rows('rkb_chunks',{'document_id':d['id'],'revision':d['active_revision']})]
        exact=0
        selected=[]
        for d in active:
            selected.extend([c for c in chunks if c['document_id']==d['id']][:3])
        for chunk in selected:
            a=await old.fetch(chunk['id'],actor);b=await new.fetch(chunk['id'],actor)
            if a.model_dump(mode='json')!=b.model_dump(mode='json'):raise ValueError('fetch citation parity failed')
            exact+=1
        report['checks']['fetch_exact']=exact
        report['checks']['fetch_checked']=len(selected)
        report['checks']['all_text_hashes_verified']=all(hashlib.sha256(c['source_text'].encode()).hexdigest()==c['text_sha256'] for c in new.corpus.rows('rkb_chunks'))
        report['checks']['catalog_items']=len((await new.catalog(actor,limit=100))['items'])
        for query in ['Кёнигсберг','1945','Альбрехт','Луиза','Fritz Gause','Замок','Albertina']:
            a=await old.search(query,actor,_fast_only=True);b=await new.search(query,actor,_fast_only=True)
            x={r.id for r in a.results};y={r.id for r in b.results}
            report['queries'].append({'query':query,'before':len(x),'after':len(y),'overlap':len(x&y),'jaccard':len(x&y)/max(1,len(x|y)),'mode':b.retrieval_mode})
        # Native and image-only physical pages are our own permitted fixture,
        # observed independently by a real Gemini reader (no mock transport).
        import fitz
        quote='Koenigsberg archive records dated 1945.'
        path=out/'independent-native.pdf'
        with fitz.open() as pdf:
            page=pdf.new_page(width=600,height=300);page.insert_text((40,80),quote,fontsize=18)
            page.insert_text((40,160),'Unrelated lower line.',fontsize=18);pdf.save(path)
        hit,status=native_page(path,0,quote);assert status=='ok'
        native=paint(hit[0],hit[1]);(out/'native-highlight.webp').write_bytes(native)
        with fitz.open(path) as pdf:scan=Image.open(io.BytesIO(pdf[0].get_pixmap(matrix=fitz.Matrix(2,2)).tobytes('png'))).convert('RGB')
        started=time.monotonic()
        try:
            polygons,visible,calls=await FlashLiteLocator().locate(scan,quote)
            (out/'scan-highlight.webp').write_bytes(paint(scan,polygons))
            scan_result={'fixture':'independent_scan','status':'ok','method':'model_localized','calls':calls,'stripes':len(polygons),'seconds':time.monotonic()-started}
        except Exception as error:
            scan_result={'fixture':'independent_scan','status':'highlight_unavailable','error_type':type(error).__name__,'seconds':time.monotonic()-started}
        report['proofs'].append({'fixture':'independent_native','status':'ok','method':'native_text','stripes':len(hit[1])})
        report['proofs'].append(scan_result)
        # One real archived source, with a strictly mapped quote and source hash.
        candidate=None
        for c in chunks:
            d=new.corpus.one('rkb_documents',c['document_id'])
            fragments=new.corpus.fragments(c['id'])
            if d.get('source_archive_status')=='verified' and d.get('source_format')=='pdf' and fragments:
                f=fragments[0];text=c['source_text'][f['text_start']:f['text_end']]
                q=text.split('\n')[0][:120].strip()
                if len(q)>20 and c['source_text'].count(q)==1:candidate=(c,q,f);break
        if candidate:
            c,q,f=candidate
            result,data=await source_proof(new,actor,c['id'],q,f['physical_page_index'])
            report['proofs'].append({k:v for k,v in result.items() if k not in ('visible_text','polygons','chunk_id','source_sha256')})
            if data:
                (out/'corpus-highlight.webp').write_bytes(data)
                warm,wdata=await source_proof(new,actor,c['id'],q,f['physical_page_index'])
                report['proofs'].append({'warm_status':warm['status'],'cache_hit':warm['cache_hit'],'seconds':warm['seconds'],'same_image':data==wdata})
        report['checks']['supabase_details_deleted']=False
    finally:
        (out/'acceptance.json').write_text(json.dumps(report,indent=2,ensure_ascii=False));await old.aclose();await new.aclose()
    print(json.dumps(report,indent=2,ensure_ascii=False))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);asyncio.run(run(p.parse_args().evidence))
