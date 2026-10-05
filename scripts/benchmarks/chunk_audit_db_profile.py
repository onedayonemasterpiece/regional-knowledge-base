"""Read-only deployed dense/FTS path profile with source-owner admission.

No index/schema/model/ACL changes. No credential values or vectors are printed.
Owned stored vectors are retained privately to test production/lab batch parity.
"""
from __future__ import annotations
import argparse,asyncio,json,statistics,sys,time
from pathlib import Path
from uuid import UUID
from chunk_size_audit import load,dump,sha
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'production'))
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.e5_contract import SPACE


def summary(xs):
    if not xs:return {}
    ys=sorted(xs);return {'n':len(ys),'min':ys[0],'median':statistics.median(ys),'max':ys[-1]}

def compact_plan(node):
    keys=('Node Type','Relation Name','Index Name','Actual Rows','Actual Loops','Actual Total Time','Shared Hit Blocks','Shared Read Blocks','Sort Method','Rows Removed by Filter')
    return {k:node[k] for k in keys if k in node}|{'children':[compact_plan(p) for p in node.get('Plans',[])]}

async def run(a):
    lab=Path(a.lab);m=load(lab/'manifest.json');actor=m['actor'];load_service_env();backend=backend_from_env()
    if not hasattr(backend,'corpus') or not backend.vector_client:raise RuntimeError('deployed SQLite/vector backend required')
    result={'captured_at':time.time(),'active':[],'timings':{},'errors':[]}
    try:
        allowed={}
        for d in m['documents']:
            doc=backend.corpus.authorize(actor,d['id'],owner=True)
            if doc['source_sha256']!=d['source_sha256']:raise ValueError('source changed')
            if doc['active_revision']>0:allowed[d['id']]=doc['active_revision']
            result['active'].append({'document':d['id'],'sampled_revision':d['sampled_revision'],'active_revision':doc['active_revision']})
        ts=[]
        for _ in range(3):
            t=time.monotonic();metadata=backend.corpus.candidate_metadata(allowed);ts.append(time.monotonic()-t)
        selected=[x['chunk_id'] for x in metadata];byid={x['chunk_id']:x for x in metadata}
        result['timings']['all_candidate_metadata_seconds']=summary(ts)
        result['all_candidate_count']=len(selected);result['all_candidate_uuid_json_bytes']=len(json.dumps(selected).encode())
        result['lexical']=[]
        cases=[c for c in load(lab/'gold.json')['cases'] if c['split']=='dev' and c['doc'] in allowed][:8]
        controls=[c['query'] for c in cases]+['Кёнигсберг университет','Альбертина']
        for qi,query in enumerate(controls):
            times=[];r=[]
            for _ in range(3):
                t=time.monotonic();r=backend.corpus.lexical(actor,query,allowed=allowed,depth=100);times.append(time.monotonic()-t)
            result['lexical'].append({'query_ordinal':qi,'natural':qi<len(cases),'hits':len(r),'seconds':summary(times)})
        # Profile the existing adapter separately; do not conflate its setup cost with FTS.
        t=time.monotonic()
        async with backend.data_client._connection({'x-rkb-actor':actor}) as db:
            rows=await(await db.execute('select id,active_revision from rkb_documents')).fetchall()
        result['timings']['adapter_visible_documents_seconds']=time.monotonic()-t
        result['adapter_visible_documents_count']=len(rows)
        headers={'x-rkb-actor':actor,'x-rkb-vector-documents':json.dumps(list(allowed))}
        installed={}
        async with backend.vector_client._connection(headers) as db:
            t=time.monotonic()
            rows=await(await db.execute('''select a.chunk_id,a.document_id,a.revision,a.text_sha256,a.search_material_sha256,e.embedding::text as vector,e.batch_sha256
              from rkb_vector_items a join rkb_chunk_embeddings_e5 e on e.chunk_id=a.chunk_id
              where a.chunk_id=any(%s::uuid[]) and e.embedding_space=%s
                and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256''',(selected,SPACE))).fetchall()
            result['timings']['owned_installed_vector_read_seconds']=time.monotonic()-t
            for row in rows:
                r=dict(row);ident=str(r['chunk_id']);known=byid.get(ident)
                if not known or any(r[k]!=known[k] for k in ('revision','text_sha256','search_material_sha256')):raise ValueError('vector provenance mismatch')
                installed[ident]={'vector':json.loads(r['vector']),'batch_sha256':r['batch_sha256']}
            indexes=await(await db.execute("select tablename,indexname,indexdef from pg_indexes where schemaname='public' and tablename in ('rkb_vector_items','rkb_chunk_embeddings_e5','rkb_chunk_embeddings_bge')")).fetchall()
            result['indexes']=[dict(r) for r in indexes]
        dump(lab/'installed-e5-private.json',{'space':SPACE,'active':allowed,'vectors':installed})
        result['installed_e5_rows']=len(installed);result['queries']=[]
        for c in cases[:6]:
            t=time.monotonic();vector=await backend.embedder.embed(c['query']);encode=time.monotonic()-t
            literal='['+','.join(format(x,'.9g') for x in vector)+']';times=[];rows=[]
            for _ in range(2):
                t=time.monotonic();rows=await backend.vector_client.candidates(actor,list(allowed),selected,literal,SPACE,None,None,100);times.append(time.monotonic()-t)
            result['queries'].append({'id':c['id'],'query_encoding_seconds':encode,'pure_vector_rpc_seconds':summary(times),'returned':len(rows),'lexical_called_in_dense_path':False})
        if cases:
            async with backend.vector_client._connection(headers) as db:
                plan=await(await db.execute('''explain (analyze, buffers, format json)
                  select a.chunk_id,row_number() over(order by e.embedding <=> %s::vector(384),a.chunk_id)
                  from rkb_chunk_embeddings_e5 e join rkb_vector_items a on a.chunk_id=e.chunk_id
                  where a.chunk_id=any(%s::uuid[]) and e.embedding_space=%s and e.revision=a.revision and e.text_sha256=a.text_sha256 and e.search_material_sha256=a.search_material_sha256
                  order by e.embedding <=> %s::vector(384),a.chunk_id limit 100''',(literal,selected,SPACE,literal))).fetchone()
                obj=list(plan.values())[0]
                if isinstance(obj,str):obj=json.loads(obj)
                result['vector_plan']={'planning_ms':obj[0].get('Planning Time'),'execution_ms':obj[0].get('Execution Time'),'plan':compact_plan(obj[0]['Plan'])}
        dump(lab/'runtime-read.json',result)
        print(json.dumps(result,ensure_ascii=False,default=str,indent=2))
    except Exception as exc:
        result['errors'].append(type(exc).__name__);dump(lab/'runtime-read.json',result);print(json.dumps(result,default=str,indent=2));raise
    finally:await backend.aclose()

def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);a=p.parse_args();asyncio.run(run(a))
if __name__=='__main__':main()
