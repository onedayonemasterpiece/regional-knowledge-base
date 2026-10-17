"""Catalog component search uses its exact semantic range, not the whole issue."""
import json
from uuid import uuid4
import pytest
from test_service_retrieval import fixture
from regional_knowledge.retrieval_scope import resolve_scope
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.contracts import Principal


def component(corpus,doc,chunks,*,revision=1,searchable=True):
    ident=str(uuid4())
    row={'id':ident,'parent_id':doc,'active_revision':revision,
         'catalog':{'kind':'article','searchable':searchable}}
    with corpus.connect() as db:
        db.execute('INSERT INTO catalog_components VALUES(?,?,?,?)',
                   (ident,doc,revision,json.dumps(row)))
        db.executemany('INSERT INTO component_chunks VALUES(?,?)',
                       [(chunk,ident) for chunk in chunks])
    return ident


def test_article_filter_before_lexical_limit_and_vector_hydration(tmp_path):
    corpus,owner,doc,add=fixture(tmp_path)
    outside=[add(['body'],n) for n in range(8)]
    inside=add(['body'],20)
    article=component(corpus,doc,[inside])
    allowed,scope=resolve_scope(corpus,[article],{doc:1})
    assert allowed=={doc:1}
    assert [r['chunk_id'] for r in corpus.lexical(owner,'needle',depth=1,
        allowed=allowed,scope=scope)]==[inside]
    assert [r['chunk_id'] for r in corpus.candidate_metadata_ids(
        outside+[inside],allowed,scope=scope)]==[inside]
    _,union=resolve_scope(corpus,[doc,article],{doc:1})
    assert {r['chunk_id'] for r in corpus.lexical(owner,'needle',
        allowed=allowed,scope=union)}==set(outside+[inside])


@pytest.mark.parametrize('case',['private','stale','hidden','missing'])
def test_article_scope_rejects_unreadable_or_unselected_component(tmp_path,case):
    corpus,owner,doc,add=fixture(tmp_path)
    chunk=add(['body'],0)
    article=component(corpus,doc,[chunk],revision=2 if case=='stale' else 1,
                      searchable=case!='hidden')
    with pytest.raises(PermissionError):
        resolve_scope(corpus,[str(uuid4()) if case=='missing' else article],
                      {} if case=='private' else {doc:1})


@pytest.mark.asyncio
async def test_all_local_search_branches_and_continuation_respect_article(tmp_path,monkeypatch):
    corpus,owner,doc,add=fixture(tmp_path)
    before=add(['body'],0);first=add(['body'],10)
    second=add(['body'],20);after=add(['body'],30)
    article=component(corpus,doc,[first,second])
    corpus.build_fragments(doc,1)
    backend=SQLiteBackend(corpus_path=corpus.path,embedder=LexicalOnlyEmbedder(),
                          object_store=UnavailableObjectStore())
    class Vectors:
        async def candidates(self,actor,allowed,*args):
            assert actor==owner and allowed=={doc:1}
            return [{**row,'branch':'e5','rank':n+1} for n,row in
                    enumerate(corpus.candidate_metadata_ids([before,first,second,after],allowed))]
        async def aclose(self): pass
    backend.vector_client=Vectors()
    payload={'query_text':'needle','document_ids':[article],
             'e5_vector':[1],'query_embedding':[1],'aliases':[{'name':'needle','kind':'current'}]}
    try:
        for name in ['rkb_multilingual_rankings','rkb_hybrid_search','rkb_fast_e5_search']:
            rows=(await backend.local_rankings(name,payload,{'x-rkb-actor':owner})).json()
            assert {r['chunk_id'] for r in rows}=={first,second}
        neighbors=corpus.neighbors(owner,[first])[0]
        assert neighbors['next_id']==second
        assert not any(value in [before,after] for key,value in neighbors.items() if key.endswith('_id'))
        monkeypatch.delenv('RKB_BGE_ENABLED',raising=False)
        principal=Principal(subject=owner,client_id='test',issuer='test',access_token='test')
        result=await backend.search('needle',principal,document_ids=[article])
        assert {r.id for r in result.results}=={first,second}
    finally:
        await backend.aclose()
