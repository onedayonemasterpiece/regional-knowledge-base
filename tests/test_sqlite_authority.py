import hashlib
from uuid import uuid4
import pytest
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.e5_contract import SPACE as ES
from regional_knowledge.bge_contract import SPACE as BS,REVISION

@pytest.fixture
def backend(tmp_path):return SQLiteBackend(corpus_path=tmp_path/'corpus.db',embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())

@pytest.mark.asyncio
async def test_local_authority_and_atomic_activation(backend):
    b=backend;actor=str(uuid4());doc=str(uuid4());job=str(uuid4());h={'x-rkb-actor':actor};url=b.config.url+'/rest/v1/'
    p={'p_ingestion_id':job,'p_document_id':doc,'p_title':'Owned','p_authors':['A','B'],'p_source_sha256':'a'*64,'p_source_file_id':'test','p_page_count':1,'p_duplicate_policy':'reuse'}
    first=await b.local_rpc('rkb_start_ingestion',p,h);assert (await b.local_rpc('rkb_start_ingestion',p,h)).json()==first.json()
    await b.client.patch(url+'rkb_ingestion_jobs',headers=h,params={'id':'eq.'+job},json={'state':'ready'})
    page=str(uuid4());region=str(uuid4());chunk=str(uuid4());text='Exact local text';sha=hashlib.sha256(text.encode()).hexdigest()
    await b.client.post(url+'rkb_pages',headers=h,json=[{'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
    await b.client.post(url+'rkb_regions',headers=h,json=[{'id':region,'page_id':page,'kind':'body','reading_order':0,'source_text':text,'text_sha256':sha}])
    await b.local_rpc('rkb_insert_chunks',{'p_document_id':doc,'p_ingestion_id':job,'p_revision':1,'p_chunks':[{'id':chunk,'source_text':text,'search_material':text,'text_sha256':sha,'search_material_sha256':sha,'region_ids':[region],'footnote_region_ids':[],'page_ids':[page],'illustration_ids':[],'title':'Exact','text_start':0,'text_end':len(text)}]},h)
    ap={'p_document_id':doc,'p_ingestion_id':job,'p_revision':1}
    assert (await b.local_rpc('rkb_activate_revision',ap,h)).json()[0]['pending_vectors']
    assert b.corpus.one('rkb_documents',doc)['active_revision']==0
    for table,space in [('rkb_chunk_embeddings_e5',ES),('rkb_chunk_embeddings_bge',BS)]:
        b.corpus.put(table,[{**defaults(table),'chunk_id':chunk,'embedding_space':space,'revision':1,'text_sha256':sha,'search_material_sha256':sha,'model_revision':REVISION}])
    await b.activate_pending();assert b.corpus.one('rkb_documents',doc)['active_revision']==1
    assert (await b.local_rankings('rkb_fast_e5_search',{'query_text':'Exact'},h)).json()[0]['retrieval_mode']=='lexical_only'
    with pytest.raises(PermissionError):await b.client.patch(url+'rkb_regions',headers=h,params={'id':'eq.'+region},json={'source_text':'changed'})
    other={'x-rkb-actor':str(uuid4())}
    assert (await b.client.get(url+'rkb_chunks',headers=other)).json()==[]
    async with b.data_client._connection(h) as db:
        assert (await(await db.execute('select * from rkb_index_counts(%s)',(doc,))).fetchone())=={'active_chunks':1,'e5_ready':1,'bge_ready':1}
        await db.execute("update rkb_documents set title=%s where id=%s",('Changed',doc))
    assert b.corpus.one('rkb_documents',doc)['title']=='Changed'
    await b.aclose()

@pytest.mark.asyncio
async def test_exact_boolean_public_policy(backend):
    b=backend;owner=str(uuid4());actor=str(uuid4());doc=str(uuid4())
    d={**defaults('rkb_documents'),'id':doc,'owner_user_id':owner,'source_visibility':'private','content_visibility':'public','rights_status':'permission_granted','rights_policy_version':'v1','rights_evidence':{'public_distribution':'true'}}
    b.corpus.put('rkb_documents',[d])
    async with b.data_client._connection({'x-rkb-actor':actor}) as db:
        assert not (await(await db.execute('select id from rkb_documents')).fetchall())
    b.corpus.put('rkb_documents',[{**d,'rights_evidence':{'public_distribution':True}}])
    async with b.data_client._connection({'x-rkb-actor':actor}) as db:
        assert len(await(await db.execute('select id from rkb_documents')).fetchall())==1

@pytest.mark.asyncio
async def test_indexed_reads_and_wal_reader_with_worker(backend):
    import asyncio
    b=backend;actor=str(uuid4());doc=str(uuid4());chunk=str(uuid4());text='Rare lexical citation';sha=hashlib.sha256(text.encode()).hexdigest();h={'x-rkb-actor':actor}
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':actor}])
    b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':actor,'title':'Book','source_sha256':'a'*64,'active_revision':1}])
    b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'title':'Evidence','source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha}])
    async with b.data_client._connection(h) as reader:
        assert not reader.db.in_transaction
        plan=await(await reader.execute('explain query plan select * from rkb_chunks where id=%s',(chunk,))).fetchall()
        assert any('table_name=? AND row_key=?' in r['detail'] for r in plan)
        async with b.data_client._connection(h,write=True) as writer:
            await writer.execute('update rkb_documents set title=%s where id=%s',('Updated',doc))
            # WAL readers remain usable while a worker holds an uncommitted write.
            found=await asyncio.wait_for(b.client.get(b.config.url+'/rest/v1/rkb_chunks',headers=h,params={'id':'eq.'+chunk}),1)
            assert found.json()[0]['source_text']==text
        assert (await(await reader.execute('select title from rkb_documents where id=%s',(doc,))).fetchone())['title']=='Updated'
    original=b.corpus.rows
    def no_full_chunks(table,*args,**kwargs):
        if table=='rkb_chunks':raise AssertionError('entire corpus text hydration')
        return original(table,*args,**kwargs)
    b.corpus.rows=no_full_chunks
    assert b.corpus.candidate_metadata({doc:1})[0]['chunk_id']==chunk
    # Indexed SQL metadata checks do not call rows(rkb_chunks), even readiness.
    async with b.data_client._connection(h) as db:assert (await(await db.execute('select * from rkb_index_counts(%s)',(doc,))).fetchone())['active_chunks']==1

@pytest.mark.asyncio
async def test_vector_outage_is_truthfully_lexical_only(backend,monkeypatch):
    from regional_knowledge.contracts import Principal
    b=backend;actor=str(uuid4());doc=str(uuid4());chunk=str(uuid4());text='Rare lexical citation';sha=hashlib.sha256(text.encode()).hexdigest()
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':actor}]);b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':actor,'title':'Book','source_sha256':'a'*64,'active_revision':1}])
    b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':1,'source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha,'title':'Evidence'}])
    class Encoder:
        embedding_space='test'
        async def embed(self,q):return [0.1]*768
    class Offline:
        async def candidates(self,*a):raise OSError('offline')
    b.embedder=Encoder();b.vector_client=Offline();monkeypatch.delenv('RKB_AUTO_INDEX_ENABLED',raising=False);monkeypatch.delenv('RKB_BGE_ENABLED',raising=False)
    p=Principal(subject=actor,client_id='test',issuer='test',access_token='test')
    out=await b.search('Rare',p)
    assert out.retrieval_mode=='lexical_only' and out.mode=='lexical_degraded' and out.results[0].id==chunk
    assert (await b.fetch(chunk,p)).text==text
    assert (await b.catalog(p))['items'][0]['id']==doc

@pytest.mark.asyncio
async def test_replacement_manifest_and_local_poi_outbox(backend):
    import copy
    b=backend;actor,doc,job,page,region,chunk=[str(uuid4()) for _ in range(6)];h={'x-rkb-actor':actor};text='Replacement accepted evidence';sha=hashlib.sha256(text.encode()).hexdigest()
    b.corpus.put('rkb_users',[{**defaults('rkb_users'),'id':actor}]);b.corpus.put('rkb_documents',[{**defaults('rkb_documents'),'id':doc,'owner_user_id':actor,'title':'Existing','source_sha256':'a'*64,'page_count':1,'active_revision':1}]);b.corpus.put('rkb_ingestion_jobs',[{**defaults('rkb_ingestion_jobs'),'id':job,'document_id':doc,'owner_user_id':actor,'source_sha256':'a'*64,'source_file_id':'replacement','staged_revision':2,'state':'ready'}]);b.corpus.put('rkb_pages',[{**defaults('rkb_pages'),'id':page,'document_id':doc,'revision':2,'physical_page_index':0}]);b.corpus.put('rkb_regions',[{**defaults('rkb_regions'),'id':region,'page_id':page,'kind':'body','source_text':text,'text_sha256':sha}]);b.corpus.put('rkb_chunks',[{**defaults('rkb_chunks'),'id':chunk,'document_id':doc,'revision':2,'region_ids':[region],'page_ids':[page],'source_text':text,'search_material':text,'text_sha256':sha,'search_material_sha256':sha}])
    event={'event_id':str(uuid4()),'contract_version':'poi.fact_evidence.v1','producer':'regional_knowledge','idempotency_key':'own-event','source':{'document_ref':'knowledge://documents/'+doc,'revision':2},'scope':{'owner_sub':actor,'visibility':'private'},'claim':{'candidate_id':chunk},'evidence':{'evidence_ref':'knowledge://evidence/'+chunk,'page_ids':[page],'region_ids':[region],'provenance_precision_score':90,'source_family_id':'unresolved:test'}}
    p={'p_document_id':doc,'p_ingestion_id':job,'p_revision':2,'p_poi_events':[event]};bad=copy.deepcopy(p);bad['p_poi_events'][0]['scope']['owner_sub']=str(uuid4())
    with pytest.raises(ValueError,match='owner'):await b.local_rpc('rkb_activate_revision',bad,h)
    assert (await b.local_rpc('rkb_activate_revision',p,h)).json()[0]['pending_vectors']
    assert b.corpus.one('rkb_documents',doc)['active_revision']==1 and not b.corpus.rows('rkb_integration_outbox')
    for table,space in [('rkb_chunk_embeddings_e5',ES),('rkb_chunk_embeddings_bge',BS)]:b.corpus.put(table,[{**defaults(table),'chunk_id':chunk,'embedding_space':space,'revision':2,'text_sha256':sha,'search_material_sha256':sha,'model_revision':'wrong' if table.endswith('bge') else REVISION}])
    await b.activate_pending();assert b.corpus.one('rkb_documents',doc)['active_revision']==1
    entry=b.corpus.one('rkb_chunk_embeddings_bge',chunk);b.corpus.put('rkb_chunk_embeddings_bge',[{**entry,'model_revision':REVISION}]);await b.activate_pending()
    assert b.corpus.one('rkb_documents',doc)['active_revision']==2
    outbox=b.corpus.rows('rkb_integration_outbox');assert len(outbox)==1 and outbox[0]['state']=='pending_authorization' and outbox[0]['payload']['evidence']['source_family_id']=='unknown'
    await b.local_rpc('rkb_activate_revision',p,h);assert len(b.corpus.rows('rkb_integration_outbox'))==1
