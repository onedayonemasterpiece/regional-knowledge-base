"""Install idempotent vector-only scope/RPC and exact minimal anchors."""
import asyncio,json,os
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.vector_plane import RemoteVectorClient
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from psycopg.types.json import Jsonb

async def main():
    load_service_env();corpus=SQLiteCorpus('/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3')
    client=RemoteVectorClient(os.environ['KB_SUPABASE_SESSION_CONNECTION'],min_size=0,max_size=2)
    docs={d['id']:d for d in corpus.rows('rkb_documents')};items=[]
    for c in corpus.rows('rkb_chunks'):
        d=docs[c['document_id']]
        items.append({k:c[k] for k in ('document_id','revision','text_sha256','search_material_sha256')}|{'chunk_id':c['id'],'source_sha256':d['source_sha256'],'owner_user_id':d['owner_user_id']})
    try:
        async with client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('alter table rkb_vector_items add column if not exists source_sha256 text;alter table rkb_vector_items add column if not exists owner_user_id uuid')
            for offset in range(0,len(items),256):
                await db.execute('''insert into rkb_vector_items(chunk_id,document_id,revision,text_sha256,search_material_sha256,source_sha256,owner_user_id)
                  select * from jsonb_to_recordset(%s::jsonb) as x(chunk_id uuid,document_id uuid,revision bigint,text_sha256 text,search_material_sha256 text,source_sha256 text,owner_user_id uuid)
                  on conflict(chunk_id) do update set source_sha256=excluded.source_sha256,owner_user_id=excluded.owner_user_id
                  where rkb_vector_items.document_id=excluded.document_id and rkb_vector_items.revision=excluded.revision and rkb_vector_items.text_sha256=excluded.text_sha256 and rkb_vector_items.search_material_sha256=excluded.search_material_sha256''',(Jsonb(items[offset:offset+256]),))
            await db.execute(Path('sql/021_vector_only_plane.sql').read_text().strip().removeprefix('begin;').removesuffix('commit;'))
            await db.execute(Path('sql/022_vector_candidate_hotpath.sql').read_text().strip().removeprefix('begin;').removesuffix('commit;'))
            anchors=await(await db.execute('select count(*) n,count(*) filter(where source_sha256 is null or owner_user_id is null) unbound from rkb_vector_items')).fetchone()
            if anchors['n']!=len(items) or anchors['unbound']:raise ValueError('anchor identity coverage failed')
            rpc=await(await db.execute("select to_regprocedure('public.rkb_vector_candidates_v4(text,text,text,text,integer)')::text name,to_regprocedure('public.rkb_vector_revision_scope(uuid,bigint)')::text scope")).fetchone()
            if not rpc or not rpc['name'] or not rpc['scope']:raise ValueError('compact vector candidate scope/RPC missing')
        print(json.dumps(anchors))
    finally:await client.aclose()
if __name__=='__main__':asyncio.run(main())