import hashlib
from pathlib import Path
from uuid import UUID,uuid4
import pytest,psycopg
from test_graph_postgres import graph_db,fixture
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.graph_service import GraphService
from regional_knowledge.storage_gc import collect

class Cache:
    def __init__(self):self.deleted=[]
    async def get_range(self,*args):raise AssertionError('Postgres evidence must not read S3')
    async def get_bytes(self,*args):raise AssertionError('Postgres regions must not read S3')
    async def delete(self,key):self.deleted.append(key)

@pytest.mark.asyncio
async def test_postgres_evidence_acl_and_guarded_gc_preserve_pending_source(graph_db):
    doc,owner,foreign,e,bundle,texts=fixture(graph_db);text=texts[e['chunk_id']]
    with psycopg.connect(graph_db,autocommit=True) as db:
        db.execute("update rkb_chunks set source_text=%s where id=%s",(text,UUID(e['chunk_id'])))
        db.execute("update rkb_regions set source_text=%s where id=%s",(text,UUID(e['region_id'])))
        projection=db.execute('select text_object_id from rkb_chunks where id=%s',(UUID(e['chunk_id']),)).fetchone()[0]
        db.execute("update rkb_objects set created_at=now()-interval '2 hours' where id=%s",(projection,))
        source=uuid4()
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes,created_at) values(%s,%s,'source_pdf','pending-source',%s,'application/pdf',10,now()-interval '2 hours')",(source,doc,'a'*64))
    cache=Cache();b=PostgresBackend(graph_db,public_base_url='https://example.test',object_store=cache)
    try:
        assert (await b.fetch(e['chunk_id'],owner)).text==text
        with pytest.raises(LookupError):await b.fetch(e['chunk_id'],foreign)
        graph=GraphService(b)
        assert await graph.region_text(owner,doc,1,e['chunk_id'],e['region_id'])==text
        dry=await collect(b);assert dry['candidate_objects']==1 and not cache.deleted
        applied=await collect(b,apply=True);assert applied['deleted_objects']==1
        assert 'pending-source' not in cache.deleted
        assert (await b.fetch(e['chunk_id'],owner)).text==text
        with psycopg.connect(graph_db) as db:
            assert db.execute('select text_object_id from rkb_chunks where id=%s',(UUID(e['chunk_id']),)).fetchone()[0] is None
            assert db.execute('select deleted_at from rkb_objects where id=%s',(source,)).fetchone()[0] is None
        assert (await collect(b))['candidate_objects']==0
    finally:await b.aclose()
