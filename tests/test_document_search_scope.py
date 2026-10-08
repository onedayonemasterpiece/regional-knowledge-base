"""Authorised explicit document scoping must never widen corpus access."""
import hashlib
from uuid import uuid4

import pytest

from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.contracts import Principal


@pytest.mark.asyncio
async def test_document_scope_is_exact_acl_bound_and_cannot_include_controls(tmp_path,monkeypatch):
    monkeypatch.delenv("RKB_BGE_QUERY_LOCAL_ENABLED",raising=False)
    backend=SQLiteBackend(corpus_path=tmp_path/"corpus.db",
        embedder=LexicalOnlyEmbedder(),object_store=UnavailableObjectStore())
    owner,other=str(uuid4()),str(uuid4())
    first,second,foreign,hidden=[str(uuid4()) for _ in range(4)]
    refs={}
    try:
        backend.corpus.put("rkb_users",[
            {**defaults("rkb_users"),"id":owner},
            {**defaults("rkb_users"),"id":other},
        ])
        for doc,actor,searchable in ((first,owner,True),(second,owner,True),
                                     (foreign,other,True),(hidden,owner,False)):
            key=str(uuid4());refs[doc]=key
            phrase="identical searchable medieval history test"
            sha=hashlib.sha256(phrase.encode()).hexdigest()
            backend.corpus.put("rkb_documents",[{
                **defaults("rkb_documents"),"id":doc,"owner_user_id":actor,
                "title":"Book "+doc,"active_revision":1,
                "source_sha256":"a"*64,"catalog":{"kind":"book","searchable":searchable},
            }])
            backend.corpus.put("rkb_chunks",[{
                **defaults("rkb_chunks"),"id":key,"document_id":doc,"revision":1,
                "title":"Evidence","source_text":phrase,"search_material":phrase,
                "text_sha256":sha,"search_material_sha256":sha,
            }])
            backend.corpus.build_fragments(doc,1)

        actor=Principal(subject=owner,client_id="test",issuer="test",access_token="test")
        hdr={"x-rkb-actor":owner}
        payload={"query_text":"medieval history","bge_vector":None,
                 "e5_vector":None,"aliases":[],"depth":20}
        unrestricted=(await backend.local_rankings("rkb_multilingual_rankings",payload,hdr)).json()
        assert {r["chunk_id"] for r in unrestricted}=={refs[first],refs[second]}

        scoped=(await backend.local_rankings("rkb_multilingual_rankings",
            {**payload,"document_ids":[first]},hdr)).json()
        assert {r["chunk_id"] for r in scoped}=={refs[first]}

        # The same constraints apply to ordinary lexical fallback.
        fallback=(await backend.local_rankings("rkb_hybrid_search",
            {**payload,"document_ids":[second],"match_count":8},hdr)).json()
        assert {r["chunk_id"] for r in fallback}=={refs[second]}
        with pytest.raises(PermissionError):
            await backend.local_rankings("rkb_multilingual_rankings",
                {**payload,"document_ids":[foreign]},hdr)
        with pytest.raises(PermissionError):
            await backend.local_rankings("rkb_multilingual_rankings",
                {**payload,"document_ids":[hidden]},hdr)
        with pytest.raises(PermissionError):
            await backend.local_rankings("rkb_multilingual_rankings",
                {**payload,"document_ids":[first,foreign]},hdr)
        for invalid in ([],[first,first],["not-uuid"],[first]*21,"first"):
            with pytest.raises(ValueError):
                await backend.local_rankings("rkb_multilingual_rankings",
                    {**payload,"document_ids":invalid},hdr)
        monkeypatch.delenv("RKB_BGE_ENABLED",raising=False)
        response=await backend.search("medieval history",actor,document_ids=[first])
        assert {r.id for r in response.results}=={refs[first]}
        with pytest.raises(PermissionError):
            await backend.search("medieval history",actor,document_ids=[foreign])
    finally:
        await backend.aclose()
