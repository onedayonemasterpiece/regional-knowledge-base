"""Actual PostgreSQL/RLS graph tests; CI provisions the isolated pgvector service."""
import asyncio,hashlib,json,os,subprocess,sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4,UUID
import pytest,psycopg
from psycopg.types.json import Jsonb
from regional_knowledge.postgres_backend import PostgresDataClient
from regional_knowledge.contracts import Principal,FetchOutput
from regional_knowledge.entity_graph import GraphBundle,GraphAlias
from regional_knowledge.graph_service import GraphService
from regional_knowledge.graph_discovery import GraphDiscoveryWorker

DSN='postgresql://postgres:graph_fixture_only@127.0.0.1:54329/rkb_graph_test'
@pytest.fixture(scope='module')
def graph_db(tmp_path_factory):
    if not os.getenv('RKB_GRAPH_TEST_DSN'):pytest.skip('isolated PostgreSQL not selected')
    assert os.environ['RKB_GRAPH_TEST_DSN']==DSN
    proof=tmp_path_factory.mktemp('graph-proof')/'proof.json'
    subprocess.run([sys.executable,'scripts/production/verify_graph_migration.py',str(proof)],check=True,capture_output=True)
    return DSN

class Backend:
    def __init__(self,dsn,texts):self.data_client=PostgresDataClient(dsn,min_size=1,max_size=2);self.texts=texts
    def _headers(self,p):return {'x-rkb-actor':p.subject}
    async def fetch(self,id,p):
        async with self.data_client._connection(self._headers(p)) as db:
            c=await(await db.execute('select id from rkb_chunks where id=%s',(UUID(id),))).fetchone()
            if not c:raise LookupError('denied evidence')
        return FetchOutput(id=id,title='Synthetic',text=self.texts[id],url='knowledge://chunks/'+id)

class OfflineResolver:
    def resolve(self,locator):raise RuntimeError('boundary unavailable')

def fixture(dsn):
    owner,other,doc,obj,page,region,chunk=[uuid4() for _ in range(7)];text='Ada and Ben attended a meeting.'
    with psycopg.connect(dsn,autocommit=True) as db:
        db.execute('insert into rkb_users(id) values(%s),(%s)',(owner,other))
        db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Synthetic',%s,1,1)",(doc,owner,'a'*64))
        db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'text_projection',%s,%s,'text/plain',32)",(obj,doc,str(obj),'b'*64))
        db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
        hash_=hashlib.sha256(text.encode()).hexdigest();db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order,text_sha256) values(%s,%s,'body','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0,%s)",(region,page,hash_))
        db.execute("insert into rkb_chunks(id,document_id,text_object_id,title,text_start,text_end,text_sha256,fts,page_ids,region_ids,revision) values(%s,%s,%s,'Synthetic',0,32,%s,to_tsvector('simple',%s),%s,%s,1)",(chunk,doc,obj,hash_,text,[page],[region]))
    actor=lambda id:Principal(subject=str(id),client_id='test',issuer='test',access_token='not-a-token')
    evidence={'chunk_id':str(chunk),'page_id':str(page),'region_id':str(region),'exact_quote':'Ada and Ben attended a meeting.'}
    node=lambda key,kind,label,**kw:{'key':key,'kind':kind,'canonical_label':label,'exact_source_spelling':'Ada','evidence':evidence,**kw}
    bundle={'entities':[node('ada','person','Ada'),node('meeting','event','Meeting'),node('thread','historical_thread','Meetings')],'relations':[{'source_key':'ada','target_key':'meeting','kind':'participated_in','evidence':[evidence]},{'source_key':'meeting','target_key':'thread','kind':'member_of','evidence':[evidence]}]}
    return doc,actor(owner),actor(other),evidence,bundle,{str(chunk):text}

@pytest.mark.asyncio
async def test_graph_stage_read_idempotency_acl_and_exact_evidence(graph_db):
    doc,owner,other,e,bundle,texts=fixture(graph_db);backend=Backend(graph_db,texts);g=GraphService(backend,OfflineResolver())
    try:
        first=await g.stage(owner,doc,1,bundle);second=await g.stage(owner,doc,1,bundle);assert first==second
        read=await g.read(owner,first['entities']['ada'],1);assert len(read['neighbors'])==1 and read['max_hops']==1
        assert read['neighbors'][0]['evidence'][0]['region_id']==e['region_id']
        with pytest.raises(LookupError):await g.read(other,first['entities']['ada'])
        with pytest.raises(PermissionError):await g.stage(other,doc,1,bundle)
        alias={'value':'Ada variant','evidence':e,'alias_type':'spelling_variant'}
        assert await g.add_alias(owner,first['entities']['ada'],alias)==await g.add_alias(owner,first['entities']['ada'],alias)
        async with g.connection(owner) as db:
            assert (await(await db.execute('select count(*) n from rkb_entity_relations where document_id=%s',(doc,))).fetchone())['n']==2
            assert (await(await db.execute('select count(*) n from rkb_entity_aliases where entity_id=%s',(UUID(first['entities']['ada']),))).fetchone())['n']==1
        wrong=GraphBundle.model_validate(bundle);wrong.entities[0].evidence.exact_quote='invented'
        with pytest.raises(ValueError):await g.stage(owner,doc,1,wrong)
        with psycopg.connect(graph_db,autocommit=True) as db:db.execute('insert into rkb_document_grants(document_id,grantee_user_id,role) values(%s,%s,\'viewer\')',(doc,UUID(other.subject)))
        await g.read(other,first['entities']['ada'])
        with psycopg.connect(graph_db,autocommit=True) as db:db.execute('delete from rkb_document_grants where document_id=%s',(doc,))
        with pytest.raises(LookupError):await g.read(other,first['entities']['ada'])
    finally:await backend.data_client.aclose()

@pytest.mark.asyncio
async def test_poi_outage_and_same_name_do_not_merge(graph_db):
    doc,owner,other,e,bundle,texts=fixture(graph_db);b=Backend(graph_db,texts);g=GraphService(b,OfflineResolver())
    bundle['entities'] += [{'key':'ada-other','kind':'person','canonical_label':'Ada','exact_source_spelling':'Ada','evidence':e},{'key':'poi','kind':'poi_ref','canonical_label':'Place','exact_source_spelling':'Ada','evidence':e,'poi_locator':{'names':['Place']}}]
    try:
        result=await g.stage(owner,doc,1,bundle);assert result['entities']['ada']!=result['entities']['ada-other'];assert result['unresolved_pois']==1
        poi=await g.read(owner,result['entities']['poi']);assert poi['entity']['external_ref'] is None and poi['entity']['state']=='unresolved'
    finally:await b.data_client.aclose()

@pytest.mark.asyncio
async def test_job_claim_fencing_and_revision_trigger(graph_db):
    doc,owner,other,e,bundle,texts=fixture(graph_db);b=Backend(graph_db,texts);g=GraphService(b);worker=GraphDiscoveryWorker(b)
    try:
        result=await g.stage(owner,doc,1,bundle)
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:await db.execute("update rkb_graph_discovery_jobs set state='done'");await db.execute('update rkb_documents set active_revision=2 where id=%s',(doc,))
        first=await worker.claim();assert first['document_id']==doc
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:await db.execute("update rkb_graph_discovery_jobs set lease_until=now()-interval '1 second' where id=%s",(first['id'],))
        second=await worker.claim();assert first['claim']!=second['claim'];await worker.finish(first)
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:assert (await(await db.execute('select state from rkb_graph_discovery_jobs where id=%s',(first['id'],))).fetchone())['state']=='running'
        await worker.finish(second)
        with pytest.raises(LookupError):await g.read(owner,result['entities']['ada'])
    finally:await b.data_client.aclose()

@pytest.mark.asyncio
async def test_chatgpt_staging_finalize_with_poi_outage_and_replay(graph_db,tmp_path,monkeypatch):
    from uuid import uuid5
    from regional_knowledge.postgres_backend import PostgresBackend
    from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
    from regional_knowledge.file_ingress import DownloadedFile
    from regional_knowledge.contracts import ChatFile,StagePageInput,StageChunkInput
    from regional_knowledge.stage_graph import StagedGraph,compile_model_stage
    from regional_knowledge.stage_service import finalize_ingestion
    monkeypatch.delenv('RKB_STREET_STORY_POI_DB',raising=False);monkeypatch.setenv('RKB_WORK_DIR',str(tmp_path))
    fitz=pytest.importorskip('fitz');pdf=fitz.open();p=pdf.new_page();p.insert_text((50,70),'Ada and Ben attended a meeting.');data=pdf.tobytes();pdf.close()
    class Store:
        def __init__(self):self.data={}
        async def put_bytes(self,k,v,m):self.data[k]=bytes(v)
        async def put_file(self,k,path,m):self.data[k]=Path(path).read_bytes()
        async def get_bytes(self,k):return self.data[k]
        async def get_range(self,k,start,end):return self.data[k][start:end]
        async def download_file(self,k,path):Path(path).write_bytes(self.data[k])
    class Downloader:
        async def download(self,url,directory):
            p=Path(directory)/'fixture.pdf';p.write_bytes(data);return DownloadedFile(path=p,sha256=hashlib.sha256(data).hexdigest(),size_bytes=len(data))
    b=PostgresBackend(graph_db,embedder=LexicalOnlyEmbedder(),object_store=Store());b.file_downloader=Downloader();actor=Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='not-token')
    try:
        start=await b.book_ingest(command='start',principal=actor,file=ChatFile(download_url='https://example.com/graph-fixture.pdf',file_id=str(uuid4())),ingestion_id=None,cursor=None,payload={'title':'Synthetic graph pipeline control'})
        doc=UUID(start.document_id);pid=uuid5(doc,'page:1:0');text='Ada and Ben attended a meeting.'
        page=StagePageInput(page_id=str(pid),physical_page_index=0,source_material='visual_reviewed',source_review_note='Synthetic PDF fixture checked',regions=[{'region_key':'body','kind':'body','bbox':{'left':0,'top':0,'right':1000,'bottom':1000},'reading_order':0,'source_text':text,'normalized_text':text}])
        chunk=StageChunkInput(chunk_key='body',title='Synthetic',region_refs=[{'page_id':str(pid),'region_key':'body'}]);compiled=compile_model_stage(StagedGraph(revision=1),document_id=str(doc),revision=1,pages=[page],chunks=[chunk])
        e={'chunk_id':str(compiled.chunks[0].chunk_id),'page_id':str(pid),'region_id':str(compiled.pages[0].regions[0].region_id),'exact_quote':text}
        bundle={'entities':[{'key':'ada','kind':'person','canonical_label':'Ada','exact_source_spelling':'Ada','evidence':e},{'key':'poi','kind':'poi_ref','canonical_label':'Unresolved place','exact_source_spelling':'meeting','poi_locator':{'names':['Unresolved place']},'evidence':e}],'relations':[]}
        payload={'pages':[page.model_dump(mode='json')],'chunks':[chunk.model_dump(mode='json')],'entity_candidates':bundle}
        kwargs={'principal':actor,'file':None,'ingestion_id':start.ingestion_id,'cursor':None}
        await b.book_ingest(command='stage',payload=payload,**kwargs);await b.book_ingest(command='stage',payload=payload,**kwargs)
        checked=await b.book_ingest(command='validate',payload=None,**kwargs);assert checked.state=='ready'
        result=await finalize_ingestion(b,principal=actor,ingestion_id=start.ingestion_id);assert result.state=='finalized'
        assert (await finalize_ingestion(b,principal=actor,ingestion_id=start.ingestion_id)).state=='finalized'
        async with b.data_client._connection(b._headers(actor)) as db:
            assert (await(await db.execute('select count(*) n from rkb_entities where document_id=%s',(doc,))).fetchone())['n']==2
            assert (await(await db.execute("select state from rkb_entities where document_id=%s and kind='poi_ref'",(doc,))).fetchone())['state']=='unresolved'
            assert (await(await db.execute("select count(*) n from rkb_graph_discovery_jobs where document_id=%s and job_key like 'document:%%'",(doc,))).fetchone())['n']==1
    finally:await b.aclose()

@pytest.mark.asyncio
async def test_new_revision_does_not_hide_active_graph_before_activation(graph_db):
    doc,owner,other,e,bundle,texts=fixture(graph_db);b=Backend(graph_db,texts);g=GraphService(b)
    try:
        first=await g.stage(owner,doc,1,bundle);nid=first['entities']['ada'];await g.read(owner,nid)
        page,region,chunk=[uuid4() for _ in range(3)];text=texts[e['chunk_id']];hash_=hashlib.sha256(text.encode()).hexdigest()
        with psycopg.connect(graph_db,autocommit=True) as db:
            obj=db.execute('select text_object_id from rkb_chunks where id=%s',(UUID(e['chunk_id']),)).fetchone()[0]
            db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,2)',(page,doc))
            db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order,text_sha256) values(%s,%s,'body','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0,%s)",(region,page,hash_))
            db.execute("insert into rkb_chunks(id,document_id,text_object_id,title,text_start,text_end,text_sha256,fts,page_ids,region_ids,revision) values(%s,%s,%s,'Synthetic new revision',0,32,%s,to_tsvector('simple',%s),%s,%s,2)",(chunk,doc,obj,hash_,text,[page],[region]))
        newe={**e,'chunk_id':str(chunk),'page_id':str(page),'region_id':str(region)};newbundle=json.loads(json.dumps(bundle))
        for n in newbundle['entities']:n['evidence']=newe
        for r in newbundle['relations']:r['evidence']=[newe]
        with pytest.raises(ValueError):await g.stage(owner,doc,2,newbundle)
        for _ in range(2):await g.stage(owner,doc,2,newbundle,staged_texts={str(chunk):text})
        still=await g.read(owner,nid);assert still['mentions'][0]['evidence']['chunk_id']==e['chunk_id']
        with psycopg.connect(graph_db,autocommit=True) as db:db.execute('update rkb_documents set active_revision=2 where id=%s',(doc,))
        new=await g.read(owner,nid);assert new['mentions'][0]['evidence']['chunk_id']==str(chunk)
    finally:await b.data_client.aclose()
