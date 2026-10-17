"""Compatible metadata correction through the existing ingestion tool."""
from uuid import uuid4
import pytest
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.contracts import Principal, StartMetadataInput
from regional_knowledge.sqlite_data import defaults
from test_catalog_correction import source, correction

@pytest.mark.asyncio
async def test_finalized_stage_catalog_alias_preserves_identity(source):
    corpus,actor,doc,_,_=source
    backend=SQLiteBackend(corpus_path=corpus.path,embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    job=str(uuid4())
    corpus.put('rkb_ingestion_jobs',[{**defaults('rkb_ingestion_jobs'),'id':job,'document_id':doc['id'],'owner_user_id':actor,'state':'finalized','staged_revision':1}])
    principal=Principal(subject=actor,client_id='test',issuer='test',access_token='synthetic')
    kwargs=dict(command='stage',principal=principal,file=None,ingestion_id=job,cursor=None,document_id=doc['id'])
    payload={'catalog_update':{**correction(),'publication_year':None}}
    before=corpus.rows('rkb_chunks')
    result=await backend.book_ingest(**kwargs,payload=payload)
    assert result.state=='finalized' and result.document_id==doc['id']
    assert corpus.one('rkb_documents',doc['id'])['publication_year'] is None
    assert corpus.rows('rkb_chunks')==before
    assert (await backend.book_ingest(**kwargs,payload=payload)).state=='finalized'
    with pytest.raises(ValueError,match='revision changed'):
        await backend.book_ingest(**kwargs,payload={'catalog_update':{**correction(),'expected_revision':2}})
    with pytest.raises(ValueError,match='metadata_only'):
        await backend.book_ingest(**kwargs,payload={**payload,'pages':[]})
    with pytest.raises(ValueError,match='source_mismatch'):
        await backend.book_ingest(**{**kwargs,'document_id':str(uuid4())},payload=payload)
    other=Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='synthetic')
    with pytest.raises((LookupError,PermissionError,ValueError)):
        await backend.book_ingest(**{**kwargs,'principal':other},payload=payload)
    row=corpus.one('rkb_ingestion_jobs',job);row['state']='ready';corpus.put('rkb_ingestion_jobs',[row])
    with pytest.raises(ValueError,match='finalized_owned'):
        await backend.book_ingest(**kwargs,payload=payload)
    await backend.aclose()

@pytest.mark.asyncio
async def test_mcp_adapter_preserves_null_and_rejects_mixed_payload(monkeypatch):
    from types import SimpleNamespace
    from regional_knowledge import server as module
    from regional_knowledge.contracts import BookIngestOutput
    actor=Principal(subject=str(uuid4()),client_id='test',issuer='test',access_token='synthetic')
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    monkeypatch.setattr(module,'_principal',lambda:actor)
    calls=[]
    async def ingest(**kwargs):
        calls.append(kwargs)
        return BookIngestOutput(state='finalized',message='Corrected')
    server=module.build_server(backend=SimpleNamespace(book_ingest=ingest))
    args={'command':'stage','ingestion_id':str(uuid4()),'metadata':{'catalog_update':{'expected_revision':1,'publication_year':None}}}
    await server._tool_manager.call_tool('book_ingest',args,None)
    assert calls[0]['payload']=={'catalog_update':{'expected_revision':1,'publication_year':None}}
    for extra in ({'pages':[]},{'chunks':[]},{'story_candidates':[]},{'command':'start'}):
        with pytest.raises(Exception,match='metadata_only'):
            await server._tool_manager.call_tool('book_ingest',{**args,**extra},None)
    assert len(calls)==1


def test_nested_correction_preserves_explicit_null_and_rejects_acl():
    model=StartMetadataInput.model_validate({'catalog_update':{'expected_revision':1,'publication_year':None}})
    assert model.catalog_update.model_dump(exclude_unset=True)=={'expected_revision':1,'publication_year':None}
    with pytest.raises(ValueError):
        StartMetadataInput.model_validate({'catalog_update':{'expected_revision':1,'owner_user_id':str(uuid4())}})
