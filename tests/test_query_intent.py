"""Literal identifiers must be source-anchored, without a fake dense fallback."""
import hashlib
from uuid import uuid4
import pytest
from regional_knowledge.query_intent import is_opaque_identifier
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.contracts import Principal

@pytest.mark.parametrize("query",["xyzzy12345","ABC987654","Catalog-ID_123456"])
def test_opaque_identifiers(query):
    assert is_opaque_identifier(query)

@pytest.mark.parametrize("query",[
    "Геркус Монте","Twanste","Оттокар","1255","1233 Kulmer Handfeste",
    "Herkus Monte 1255","Albrecht1945","Moral insanity",
    "xyzzy", "Königsberg","ABC1234"
])
def test_historical_queries_remain_semantic(query):
    assert not is_opaque_identifier(query)

@pytest.mark.asyncio
async def test_negative_identifier_abstains_but_existing_code_returns_source(tmp_path,monkeypatch):
    monkeypatch.delenv("RKB_BGE_ENABLED",raising=False)
    monkeypatch.delenv("RKB_BGE_QUERY_LOCAL_ENABLED",raising=False)
    backend=SQLiteBackend(corpus_path=tmp_path/"corpus.sqlite",
        embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    actor=str(uuid4());doc=str(uuid4());chunk=str(uuid4())
    source="Archive contains identifier ABC987654 and a relevant notation."
    sha=hashlib.sha256(source.encode()).hexdigest()
    try:
        backend.corpus.put("rkb_users",[{**defaults("rkb_users"),"id":actor}])
        backend.corpus.put("rkb_documents",[{
            **defaults("rkb_documents"),"id":doc,"owner_user_id":actor,
            "source_sha256":"a"*64,"active_revision":1,"title":"Archive book",
        }])
        backend.corpus.put("rkb_chunks",[{
            **defaults("rkb_chunks"),"id":chunk,"document_id":doc,
            "revision":1,"source_text":source,"search_material":source,
            "title":"Actual evidence","text_sha256":sha,
            "search_material_sha256":sha,
        }])
        backend.corpus.build_fragments(doc,1)
        principal=Principal(subject=actor,client_id="test",issuer="test",access_token="test")
        absent=await backend.search("xyzzy12345",principal)
        assert absent.main_state=="ready"
        assert absent.retrieval_policy=="exact_identifier"
        assert absent.results==[]
        available=await backend.search("ABC987654",principal)
        assert available.main_state=="ready"
        assert available.retrieval_policy=="exact_identifier"
        assert [r.id for r in available.results]==[chunk]
        assert available.results[0].ranking_signals[0]["branch"]=="lexical"
        evidence=await backend.search_evidence("ABC987654",principal)
        assert evidence.retrieval_policy=="exact_identifier"
        assert evidence.evidence and evidence.evidence[0].id==chunk
    finally:
        await backend.aclose()
