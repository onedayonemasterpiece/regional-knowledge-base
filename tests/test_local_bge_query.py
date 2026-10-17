"""Regression: BGE query inference runs on Kaggle, never the host."""
from types import SimpleNamespace

import pytest

from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.contracts import Principal
from regional_knowledge import multilingual_retrieval as retrieval


@pytest.mark.asyncio
async def test_main_search_uses_kaggle_queue_even_with_legacy_local_encoder(monkeypatch):
    actor = Principal(subject="actor", client_id="test", issuer="test",
                      access_token="synthetic")
    monkeypatch.setenv("RKB_BGE_WARM_MODE", "bge")
    monkeypatch.setenv("RKB_BGE_QUEUE_PATH", "unused")
    calls = []

    class Queue:
        def status(self):
            return {"state": "ready"}

        def enqueue(self, actor_id, key, texts, *, identity):
            assert actor_id == actor.subject
            assert texts == ["Kulmer Handfeste"]
            assert identity["query_sha256"]
            calls.append(key)
            return "kaggle-job"

        def result(self, actor_id, job_id):
            assert actor_id == actor.subject and job_id == "kaggle-job"
            return {
                "state": "done",
                "identity": {},
                "result": {"space": BGE_SPACE,
                           "vectors": [[1.0] + [0.0]*1023],
                           "encoder_seconds": 0.04},
            }

    monkeypatch.setattr(retrieval, "BgeQueue", lambda *_args: Queue())

    class ForbiddenLocal:
        async def embed(self, _text):
            raise AssertionError("Local BGE inference is forbidden")

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"chunk_id": "hit", "branch": "bge", "rank": 1,
                     "revision": 1, "text_sha256": "a"*64,
                     "search_material_sha256": "b"*64}]

    class Client:
        async def post(self, url, headers=None, json=None):
            assert json["bge_vector"].startswith("[1")
            assert json["bge_space"] == BGE_SPACE
            return Response()

    class Corpus:
        def one(self, table, ident):
            assert table == "rkb_chunks" and ident == "hit"
            return {"id": "hit", "title": "Evidence"}

    class Backend:
        bge_query_embedder = ForbiddenLocal()
        client = Client()
        corpus = Corpus()
        embedder = SimpleNamespace()
        config = SimpleNamespace(url="sqlite://regional-knowledge.internal")

        def _headers(self, principal):
            return {"x-rkb-actor": principal.subject}

        def _evidence_url(self, ident):
            return "knowledge://evidence/" + ident

        async def search(self, *args, **kwargs):
            raise AssertionError("ready Kaggle BGE must not fall back")

    result = await retrieval.main_search(Backend(), "Kulmer Handfeste", actor)
    assert result.main_state == "ready"
    assert result.retrieval_mode == "bge"
    assert result.main_job_id == "kaggle-job"
    assert result.results[0].id == "hit"
    assert result.timings["bge_query_local"] == 0.0
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_kaggle_cold_start_returns_pending_not_local_inference(monkeypatch, tmp_path):
    actor = Principal(subject="actor", client_id="test", issuer="test",
                      access_token="synthetic")
    monkeypatch.setenv("RKB_BGE_WARM_MODE", "bge")
    monkeypatch.setenv("RKB_BGE_QUEUE_PATH", str(tmp_path/"queue.sqlite"))

    class ForbiddenLocal:
        async def embed(self, _text):
            raise AssertionError("Local BGE inference is forbidden")

    class Backend:
        bge_query_embedder = ForbiddenLocal()

        async def search(self, query, principal, **kwargs):
            from regional_knowledge.contracts import SearchOutput
            assert kwargs["_fast_only"] is True
            return SearchOutput(results=[], retrieval_mode="lexical_only")

    response = await retrieval.main_search(Backend(), "new question", actor)
    assert response.main_state in {"starting", "pending", "unavailable"}
    assert response.main_job_id is not None
