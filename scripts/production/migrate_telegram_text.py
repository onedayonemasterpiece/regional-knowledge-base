"""Exact legacy text backfill; preserves vectors and refuses incomplete evidence."""
import asyncio,hashlib,json,sys
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from apply_indexing_migration import QUERY

async def migrate(output):
    load_service_env();backend=backend_from_env()
    try:
        async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
            before=await(await db.execute(QUERY)).fetchone()
            sql=Path('sql/017_telegram_archive_text.sql').read_text().strip().removeprefix('begin;').removesuffix('commit;')
            await db.execute(sql)
            chunks=await(await db.execute('select c.id,c.text_start,c.text_end,c.text_sha256,o.object_key,o.sha256 from rkb_chunks c join rkb_objects o on o.id=c.text_object_id where c.source_text is null order by o.id,c.id')).fetchall()
            graphs=await(await db.execute("select distinct o.id,o.object_key,o.sha256 from rkb_objects o join rkb_ingestion_jobs j on j.staged_graph_object_id=o.id where o.kind='document_graph'")).fetchall()
        verified=0;source_bytes=0;cache_key=None;data=None
        for row in chunks:
            if cache_key!=row['object_key']:
                data=await backend.object_store.get_bytes(row['object_key'])
                if hashlib.sha256(data).hexdigest()!=row['sha256']:raise ValueError('legacy_projection_integrity')
                cache_key=row['object_key']
            raw=data[row['text_start']:row['text_end']]
            if hashlib.sha256(raw).hexdigest()!=row['text_sha256']:raise ValueError('legacy_chunk_integrity')
            text=raw.decode('utf-8');source_bytes+=len(raw)
            async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                await db.execute('update rkb_chunks set source_text=%s where id=%s and text_sha256=%s and source_text is null',(text,row['id'],row['text_sha256']))
            verified+=1
        regions=0
        for row in graphs:
            raw=await backend.object_store.get_bytes(row['object_key'])
            if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('legacy_graph_integrity')
            graph=json.loads(raw)
            async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
                for page in graph.get('pages',[]):
                    for region in page.get('regions',[]):
                        text=region.get('source_text','');sha=hashlib.sha256(text.encode()).hexdigest()
                        result=await db.execute('update rkb_regions set source_text=%s where id=%s and (text_sha256=%s or text_sha256 is null) and source_text is null',(text,region['region_id'],sha));regions+=result.rowcount
        # Historical one-region chunks can recover an exact region by its digest.
        async with backend.data_client._connection({'x-rkb-service':'1'}) as db:
            result=await db.execute('''update rkb_regions r set source_text=c.source_text from rkb_chunks c where r.id=any(c.region_ids) and r.source_text is null and r.text_sha256=c.text_sha256 and c.source_text is not null''');regions+=result.rowcount
            missing=await(await db.execute('select count(*) n from rkb_chunks where source_text is null')).fetchone()
            assert missing['n']==0
            mismatch=await(await db.execute("select count(*) n from rkb_chunks where encode(sha256(convert_to(source_text,'UTF8')),'hex')<>text_sha256")).fetchone();assert mismatch['n']==0
            after=await(await db.execute(QUERY)).fetchone();assert before==after
            sizes=await(await db.execute("select pg_database_size(current_database()) database_bytes,pg_total_relation_size('rkb_chunks') chunks_relation_bytes,(select sum(octet_length(source_text))::bigint from rkb_chunks) source_text_bytes,(select count(*) from rkb_chunks) chunks")).fetchone()
        output.write_text(json.dumps({'chunks_backfilled':verified,'source_bytes':source_bytes,'regions_backfilled':regions,'all_chunk_hashes_verified':True,'before':before,'after':after,'sizes_after':sizes},indent=2))
        print('All legacy chunk hashes verified; corpus/vector digests unchanged')
    finally:await backend.aclose()

if __name__=='__main__':asyncio.run(migrate(Path(sys.argv[1])))
