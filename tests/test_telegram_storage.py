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
async def test_source_cache_reimport_reserves_again_after_gc(graph_db,monkeypatch):
    from types import SimpleNamespace
    from uuid import uuid5
    from regional_knowledge.storage_gc import reserve_source
    doc,owner,_,_,_,_=fixture(graph_db);sha='a'*64
    b=PostgresBackend(graph_db,object_store=Cache(),embedder=LexicalOnlyEmbedder())
    downloaded=SimpleNamespace(sha256=sha,size_bytes=20)
    monkeypatch.setenv('RKB_STAGING_MAX_BYTES','10000000')
    try:
        await reserve_source(b,owner,str(doc),'reimport-source',downloaded,'application/pdf')
        ident=uuid5(doc,'source:'+sha)
        with psycopg.connect(graph_db) as db:
            db.execute("update rkb_documents set source_archive_status='verified',source_archive_ref='entry_verified' where id=%s",(doc,))
            db.execute("update rkb_objects set created_at=now()-interval '2 hours' where id=%s",(ident,))
        assert (await collect(b,apply=True))['deleted_objects']==1
        with psycopg.connect(graph_db) as db:
            used=db.execute('select coalesce(sum(size_bytes),0) from rkb_objects where deleted_at is null').fetchone()[0]
        monkeypatch.setenv('RKB_STAGING_MAX_BYTES',str(used))
        with pytest.raises(RuntimeError,match='capacity_exceeded'):
            await reserve_source(b,owner,str(doc),'reimport-source',downloaded,'application/pdf')
        monkeypatch.setenv('RKB_STAGING_MAX_BYTES','10000000')
        await reserve_source(b,owner,str(doc),'reimport-source',downloaded,'application/pdf')
        with psycopg.connect(graph_db) as db:
            row=db.execute("select deleted_at,created_at>now()-interval '1 minute' from rkb_objects where id=%s",(ident,)).fetchone()
            assert row==(None,True)
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
            self.puts={};self.lost=True;self.origin=None;self.blocked=True;self.retries=[]
        async def request(self,method,url,**kw):
            assert kw['content']==data
            return SimpleNamespace(json=lambda:{'asset_id':'asset_source'})
        async def call(self,name,args):
            if name=='vibepublish_publication_update':
                assert args['publication_id']=='pub_source' and args['change']['kind']=='retry_failed'
                self.retries.append(args);self.blocked=False;return {'operation_id':'op_source'}
            cmd=args['command']
            if cmd['kind']=='put':
                assert len(args['request_key'])<=128
                previous=self.puts.get(args['request_key'])
                if previous is not None and previous!=cmd:
                    raise RuntimeError('idempotency_conflict')
                self.puts.setdefault(args['request_key'],cmd);self.origin=cmd['origin']
                if self.lost:self.lost=False;raise TimeoutError('lost receipt after durable admission')
                return {'operation_id':'op_source'}
            if cmd['kind']=='search':
                values=list(self.puts.values())
                return {'media_store_items':[self.entry()] if any(cmd['text'] in value['content']['text'] for value in values) else []}
            return {'operation_id':'op_read'}
        def entry(self):return {'entry_ref':'entry_source','origin':self.origin,'thread_ref':self.grant['source_thread_ref']}
        async def receipt(self,op):
            if op=='op_source' and self.blocked:
                return {'state':'blocked','operation_complete':True,'resource_id':'pub_source','revision':1}
            return {'state':'verified','operation_complete':True,'media_store_items':[self.entry()],
                    'items':[{'media_evidence':[{'media_kind':'document','sha256':sha}]}]}
    b=PostgresBackend(graph_db,object_store=Store(),embedder=LexicalOnlyEmbedder());client=Client()
    try:
        archive=SourceArchive(b,client)
        assert await archive.tick()==0
        with psycopg.connect(graph_db) as db:
            assert db.execute('select source_archive_status from rkb_documents where id=%s',(canonical,)).fetchone()[0]=='pending'
            db.execute("update rkb_documents set source_archive_attempt_at=now()-interval '1 minute' where id=%s",(canonical,))
        assert await archive.tick()==0 and len(client.retries)==1
        with psycopg.connect(graph_db) as db:
            db.execute("update rkb_documents set source_archive_attempt_at=now()-interval '1 minute' where id=%s",(canonical,))
        assert await archive.tick()==1
        assert await archive.tick()==0 and len(client.puts)==1
        put=next(iter(client.puts.values()))
        with psycopg.connect(graph_db) as db:
            display_title=db.execute("select title from rkb_documents where id=%s",(canonical,)).fetchone()[0]
        assert put["content"]["text"].startswith(display_title)
        assert 'knowledge://documents/'+str(canonical)+'/source' in put["content"]["text"]
        assert put["media"][0]["alt_text"].endswith(".pdf")
        assert put["media"][0]["alt_text"] != "source.pdf"
        with psycopg.connect(graph_db) as db:
            rows=db.execute('select id,source_archive_ref,source_archive_origin_ref,source_archive_status from rkb_documents where id=any(%s)',([doc,duplicate],)).fetchall()
            assert len(rows)==2
            assert all(r[1]=='entry_source' and r[2]=='knowledge://documents/'+str(canonical)+'/source' and r[3]=='verified' for r in rows)
    finally:await b.aclose()


def test_source_display_metadata_uses_human_title_when_filename_missing():
    from regional_knowledge.source_archive import _source_display_metadata
    uri="knowledge://documents/00000000-0000-0000-0000-000000000001/source"
    filename, caption = _source_display_metadata({
        "title": "Кёнигсберг в Пруссии. История одного европейского города",
        "authors": ["Фриц Гаузе"],
        "publication_year": 1994,
        "source_filename": None,
        "source_format": "pdf",
        "mime_type": "application/pdf",
    },uri)
    assert filename.endswith(".pdf") and filename != "source.pdf"
    assert "Кёнигсберг в Пруссии" in filename
    assert "Фриц Гаузе" in caption
    assert "1994" in caption
    assert filename in caption
    assert uri in caption
    long_filename, long_caption = _source_display_metadata({
        "title": "Очень длинное название " * 120,
        "authors": ["Автор " * 100],
        "publication_year": 1994,
        "source_filename": None,
        "source_format": "pdf",
        "mime_type": "application/pdf",
    },uri)
    assert len(long_caption) <= 1000
    assert long_caption.endswith("Источник: "+uri)


@pytest.mark.asyncio
async def test_source_archive_cross_version_uncertain_replay_keeps_legacy_payload(graph_db):
    from types import SimpleNamespace
    from regional_knowledge.source_archive import SourceArchive
    doc,owner,_,_,_,_=fixture(graph_db)
    data=b'%PDF-1.7\nlegacy uncertain send';sha=hashlib.sha256(data).hexdigest();source=uuid4()
    with psycopg.connect(graph_db,autocommit=True) as db:
        db.execute("update rkb_documents set source_sha256=%s,source_archive_status='pending',source_archive_attempt_at=now()-interval '1 minute',source_archive_error='TimeoutError',source_archive_operation_id=null,source_archive_caption=null,source_archive_filename=null where id=%s",(sha,doc))
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'source_pdf','legacy-source',%s,'application/pdf',%s)",(source,doc,sha,len(data)))
    class Store:
        async def get_bytes(self,key): assert key=='legacy-source'; return data
    class Client:
        def __init__(self):
            self.owner=owner.subject;self.issuer='https://vibe.test';self.grant={'destination_alias':'kb','source_thread_ref':'https://t.me/c/4368830579/2'};self.command=None
        async def request(self,method,url,**kw):
            assert kw['content']==data;return SimpleNamespace(json=lambda:{'asset_id':'legacy_asset'})
        async def call(self,name,args):
            cmd=args['command']
            if cmd['kind']=='put':
                self.command=cmd
                assert cmd['content']['text']=='Regional Knowledge source knowledge://documents/'+str(doc)+'/source'
                assert cmd['media'][0]['alt_text']=='source.pdf'
                return {'operation_id':'op'}
            if cmd['kind']=='search':
                return {'media_store_items':[{'entry_ref':'entry','origin':self.command['origin'],'thread_ref':self.grant['source_thread_ref']}]}
            return {'operation_id':'read'}
        async def receipt(self,op):
            if op=='op': return {'operation_complete':True,'state':'verified'}
            return {'operation_complete':True,'state':'verified','media_store_items':[{'entry_ref':'entry','origin':self.command['origin'],'thread_ref':self.grant['source_thread_ref']}],'items':[{'media_evidence':[{'media_kind':'document','sha256':sha}]}]}
    b=PostgresBackend(graph_db,object_store=Store(),embedder=LexicalOnlyEmbedder());client=Client()
    try:
        assert await SourceArchive(b,client).tick()==1
        with psycopg.connect(graph_db) as db:
            frozen=db.execute('select source_archive_caption,source_archive_filename,source_archive_status from rkb_documents where id=%s',(doc,)).fetchone()
        assert frozen==('Regional Knowledge source knowledge://documents/'+str(doc)+'/source','source.pdf','verified')
    finally:await b.aclose()


def test_migration_019_replays_and_backfills_legacy_rows(graph_db):
    owner,doc,page,region=[uuid4() for _ in range(4)];sha='c'*64
    with psycopg.connect(graph_db,autocommit=True) as db:
        db.execute('insert into rkb_users(id) values(%s)',(owner,))
        db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Migration fixture',%s,1,1)",(doc,owner,'d'*64))
        db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
        db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order) values(%s,%s,'figure','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0)",(region,page))
        iid=uuid4()
        db.execute("insert into rkb_illustrations(id,document_id,page_id,source_region_id,kind,source_crop_sha256,display_crop_sha256) values(%s,%s,%s,%s,'drawing',%s,null)",(iid,doc,page,region,sha))
        migration=(Path(__file__).parents[1]/'sql/019_illustration_presentation.sql').read_text()
        db.execute(migration,prepare=False);db.execute(migration,prepare=False)
        row=db.execute('select display_rotation_degrees,display_crop_sha256,provider_crop_sha256 from rkb_illustrations where id=%s',(iid,)).fetchone()
        cols={value[0] for value in db.execute("select column_name from information_schema.columns where table_schema='public' and table_name='rkb_documents' and column_name in ('source_archive_caption','source_archive_filename')").fetchall()}
    assert row==(0,sha,None)
    assert cols=={'source_archive_caption','source_archive_filename'}


@pytest.mark.asyncio
async def test_source_archive_cross_version_djvu_filename_ambiguity_is_recovered(graph_db):
    from types import SimpleNamespace
    from regional_knowledge.source_archive import SourceArchive
    doc,owner,_,_,_,_=fixture(graph_db)
    data=b'AT&TFORMsynthetic-djvu-legacy';sha=hashlib.sha256(data).hexdigest();source=uuid4()
    with psycopg.connect(graph_db,autocommit=True) as db:
        db.execute("update rkb_documents set source_sha256=%s,source_format='djvu',source_archive_status='pending',source_archive_attempt_at=now()-interval '1 minute',source_archive_error='TimeoutError',source_archive_operation_id=null,source_archive_caption=null,source_archive_filename=null where id=%s",(sha,doc))
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'source_pdf','legacy-djvu',%s,'image/vnd.djvu',%s)",(source,doc,sha,len(data)))
    class Store:
        async def get_bytes(self,key): assert key=='legacy-djvu'; return data
    class Client:
        def __init__(self):
            self.owner=owner.subject;self.issuer='https://vibe.test';self.grant={'destination_alias':'kb','source_thread_ref':'https://t.me/c/4368830579/2'};self.command=None;self.attempts=[]
        async def request(self,method,url,**kw):
            assert kw['content']==data;return SimpleNamespace(json=lambda:{'asset_id':'legacy_asset'})
        async def call(self,name,args):
            cmd=args['command']
            if cmd['kind']=='put':
                filename=cmd['media'][0]['alt_text'];self.attempts.append(filename)
                if filename=='source.djvu':
                    raise RuntimeError('VibePublish tool error: idempotency_conflict')
                assert filename=='source.pdf'
                self.command=cmd;return {'operation_id':'op'}
            if cmd['kind']=='search':
                return {'media_store_items':[{'entry_ref':'entry','origin':self.command['origin'],'thread_ref':self.grant['source_thread_ref']}]}
            return {'operation_id':'read'}
        async def receipt(self,op):
            if op=='op': return {'operation_complete':True,'state':'verified'}
            return {'operation_complete':True,'state':'verified','media_store_items':[{'entry_ref':'entry','origin':self.command['origin'],'thread_ref':self.grant['source_thread_ref']}],'items':[{'media_evidence':[{'media_kind':'document','sha256':sha}]}]}
    b=PostgresBackend(graph_db,object_store=Store(),embedder=LexicalOnlyEmbedder());client=Client()
    try:
        assert await SourceArchive(b,client).tick()==1
        assert client.attempts==['source.djvu','source.pdf']
        with psycopg.connect(graph_db) as db:
            frozen=db.execute('select source_archive_filename,source_archive_status from rkb_documents where id=%s',(doc,)).fetchone()
        assert frozen==('source.pdf','verified')
    finally:await b.aclose()
