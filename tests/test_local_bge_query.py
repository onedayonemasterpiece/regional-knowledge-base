import asyncio
from types import SimpleNamespace

import pytest

from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.bge_query_service import QueryQueue
from regional_knowledge.contracts import Principal
from regional_knowledge import multilingual_retrieval as retrieval


@pytest.mark.asyncio
async def test_query_queue_returns_bounded_local_timing():
    queue=QueryQueue(lambda text:[1.0]+[0.0]*1023,capacity=2)
    result=await queue.submit("query")
    assert result["space"]==BGE_SPACE
    assert len(result["vectors"])==1
    assert result["encoder_revision"].startswith("onnx-community/bge-m3-ONNX:int8:")
    assert result["queue_wait_seconds"]>=0
    assert result["encoder_seconds"]>=0
    queue.worker.cancel()
    await asyncio.gather(queue.worker,return_exceptions=True)
    queue.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_main_search_prefers_local_bge_without_durable_query_job(monkeypatch):
    actor=Principal(subject="actor",client_id="test",issuer="test",access_token="token")
    monkeypatch.setenv("RKB_BGE_WARM_MODE","bge")
    monkeypatch.setattr(retrieval,"BgeQueue",lambda *_:(_ for _ in ()).throw(AssertionError("durable queue must not be used")))

    class Local:
        async def embed(self,text):
            assert text=="Kulmer Handfeste"
            retrieval.query_timings.set({
                "bge_queue_seconds":0.01,
                "bge_encoder_seconds":0.06,
                "bge_query_local":1.0,
            })
            return [1.0]+[0.0]*1023

    class Response:
        def raise_for_status(self):pass
        def json(self):
            return [{
                "chunk_id":"hit","branch":"bge","rank":1,"revision":1,
                "text_sha256":"a"*64,"search_material_sha256":"b"*64,
            }]

    class Client:
        async def post(self,url,headers=None,json=None):
            assert json["bge_vector"].startswith("[1")
            assert json["bge_space"]==BGE_SPACE
            return Response()

    class Corpus:
        def one(self,table,ident):
            assert table=="rkb_chunks" and ident=="hit"
            return {"id":"hit","title":"Evidence"}

    class Backend:
        bge_query_embedder=Local()
        client=Client()
        corpus=Corpus()
        embedder=SimpleNamespace()
        config=SimpleNamespace(url="sqlite://regional-knowledge.internal")
        def _headers(self,principal):return {"x-rkb-actor":principal.subject}
        def _evidence_url(self,ident):return "knowledge://evidence/"+ident
        async def search(self,*args,**kwargs):
            raise AssertionError("local BGE ready path must not fall back")

    result=await retrieval.main_search(Backend(),"Kulmer Handfeste",actor)
    assert result.main_state=="ready"
    assert result.retrieval_mode=="bge"
    assert result.main_job_id is None
    assert result.results[0].id=="hit"
    assert result.timings["bge_query_local"]==1.0


@pytest.mark.asyncio
async def test_main_search_uses_remote_queue_when_local_bge_fails(monkeypatch,tmp_path):
    actor=Principal(subject="actor",client_id="test",issuer="test",access_token="token")
    monkeypatch.setenv("RKB_BGE_WARM_MODE","bge")
    monkeypatch.setenv("RKB_BGE_QUEUE_PATH",str(tmp_path/"queue.sqlite"))

    class Local:
        async def embed(self,text):raise RuntimeError("local unavailable")

    class Backend:
        bge_query_embedder=Local()
        async def search(self,query,principal,**kwargs):
            from regional_knowledge.contracts import SearchOutput
            assert kwargs["_fast_only"] is True
            return SearchOutput(results=[],retrieval_mode="lexical_only")

    result=await retrieval.main_search(Backend(),"new query",actor)
    assert result.main_state in ("starting","pending","unavailable")
    assert result.main_job_id is not None
