"""Read an active private corpus into managed storage without mutation or embedding calls."""
import argparse
import asyncio
import hashlib
import json
import os
import shlex
import time
from pathlib import Path
from uuid import UUID
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import backend_from_env

DOCUMENT_ID = UUID('7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9')


def load_service_env():
    # The secret source is read in memory; never copied into benchmark evidence.
    for raw in Path('/home/dev/.local/state/regional-knowledge-base/service.env').read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        parsed = shlex.split(line, posix=True)
        if len(parsed) == 1 and '=' in parsed[0]:
            key, value = parsed[0].split('=', 1)
            os.environ[key] = value


async def main(output):
    load_service_env()
    backend = backend_from_env()
    assert isinstance(backend, PostgresBackend)
    try:
        async with backend.data_client._connection({'x-rkb-service':'1'}) as conn:
            await conn.execute('set transaction read only')
            doc = await (await conn.execute('select id,title,page_count,active_revision from public.rkb_documents where id=%s', (DOCUMENT_ID,))).fetchone()
            assert doc is not None, 'document missing'
            revision = doc['active_revision']
            rows = await (await conn.execute('''
                select c.id,c.title,c.text_object_id,c.text_start,c.text_end,c.page_ids,o.object_key
                from public.rkb_chunks c join public.rkb_objects o on o.id=c.text_object_id
                where c.document_id=%s and c.revision=%s order by c.text_start,c.id
            ''', (DOCUMENT_ID,revision))).fetchall()
            pages = await (await conn.execute('select id,physical_page_index,printed_page_number from public.rkb_pages where document_id=%s and revision=%s', (DOCUMENT_ID,revision))).fetchall()
        assert revision == 1 and len(rows) == 712 and len(pages) == 174, 'unexpected live corpus; review fixture before benchmarking'
        page_map = {str(page['id']):dict(page) for page in pages}
        output.parent.mkdir(parents=True,exist_ok=True)
        # Private by creation, before the first byte is written.
        fd=os.open(output,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        cache = {}
        with os.fdopen(fd,'w',encoding='utf-8') as fh:
            for row in rows:
                key=str(row['object_key'])
                if key not in cache: cache[key]=await backend.object_store.get_bytes(key)
                text=cache[key][int(row['text_start']):int(row['text_end'])].decode('utf-8')
                fh.write(json.dumps({'id':str(row['id']),'title':str(row['title']),'text':text,'pages':[page_map[str(pid)] for pid in row['page_ids']]},ensure_ascii=False,default=str)+'\n')
        sha=hashlib.sha256(output.read_bytes()).hexdigest()
        output.with_suffix('.metadata.json').write_text(json.dumps({'document':dict(doc),'chunks':len(rows),'pages':len(pages),'captured_at_unix':time.time(),'sha256':sha},default=str,indent=2))
        print(json.dumps({'chunks':len(rows),'pages':len(pages),'revision':revision,'sha256':sha,'output':str(output)}))
    finally:
        await backend.aclose()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output',type=Path)
    asyncio.run(main(parser.parse_args().output))
