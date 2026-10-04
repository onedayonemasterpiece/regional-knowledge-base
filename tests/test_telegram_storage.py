import hashlib
from pathlib import Path
from uuid import UUID,uuid4
import pytest,psycopg
from test_graph_postgres import graph_db,fixture
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
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
    cache=Cache();b=PostgresBackend(graph_db,public_base_url='https://example.test',object_store=cache,embedder=LexicalOnlyEmbedder())
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


@pytest.mark.asyncio
async def test_source_archive_lost_response_exact_replay_and_historical_roots(graph_db):
    from types import SimpleNamespace
    from regional_knowledge.source_archive import SourceArchive
    doc,owner,foreign,_,_,_=fixture(graph_db)
    data=b'%PDF-1.7\nowned archive fixture';sha=hashlib.sha256(data).hexdigest()
    duplicate=uuid4();source=uuid4()
    with psycopg.connect(graph_db,autocommit=True) as db:
        db.execute('update rkb_documents set source_sha256=%s,source_identity_primary=false where id=%s',(sha,doc))
        db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,source_identity_primary) values(%s,%s,'Historical separate root',%s,false)",(duplicate,UUID(owner.subject),sha))
        canonical=min(doc,duplicate)
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'source_pdf','owned-source',%s,'application/pdf',%s)",(source,canonical,sha,len(data)))
    class Store:
        async def get_bytes(self,key):assert key=='owned-source';return data
    class Client:
        def __init__(self):
            self.owner=owner.subject;self.issuer='https://vibe.test';self.grant={'destination_alias':'kb','source_thread_ref':'https://t.me/c/4368830579/2'}
            self.puts={};self.lost=True;self.origin=None
        async def request(self,method,url,**kw):
            assert kw['content']==data
            return SimpleNamespace(json=lambda:{'asset_id':'asset_source'})
        async def call(self,name,args):
            cmd=args['command']
            if cmd['kind']=='put':
                assert len(args['request_key'])<=128
                self.puts[args['request_key']]=cmd;self.origin=cmd['origin']
                if self.lost:self.lost=False;raise TimeoutError('lost receipt after durable admission')
                return {'operation_id':'op_source'}
            if cmd['kind']=='search':return {'media_store_items':[self.entry()]}
            return {'operation_id':'op_read'}
        def entry(self):return {'entry_ref':'entry_source','origin':self.origin,'thread_ref':self.grant['source_thread_ref']}
        async def receipt(self,op):
            return {'state':'verified','operation_complete':True,'media_store_items':[self.entry()],
                    'items':[{'media_evidence':[{'media_kind':'document','sha256':sha}]}]}
    b=PostgresBackend(graph_db,object_store=Store(),embedder=LexicalOnlyEmbedder());client=Client()
    try:
        archive=SourceArchive(b,client)
        assert await archive.tick()==0
        with psycopg.connect(graph_db) as db:
            assert db.execute('select source_archive_status from rkb_documents where id=%s',(canonical,)).fetchone()[0]=='pending'
        assert await archive.tick()==1
        assert await archive.tick()==0 and len(client.puts)==1
        with psycopg.connect(graph_db) as db:
            rows=db.execute('select id,source_archive_ref,source_archive_origin_ref,source_archive_status from rkb_documents where id=any(%s)',([doc,duplicate],)).fetchall()
            assert len(rows)==2
            assert all(r[1]=='entry_source' and r[2]=='knowledge://documents/'+str(canonical)+'/source' and r[3]=='verified' for r in rows)
    finally:await b.aclose()
