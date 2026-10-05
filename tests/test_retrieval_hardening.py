import hashlib
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.contracts import Principal
from regional_knowledge.e5_contract import MAX_TOKENS, TARGET_PASSAGE_TOKENS, SPACE as E5_SPACE
from regional_knowledge.local_e5 import LocalE5Embedder
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage
from regional_knowledge.stage_service import _validate_with_token_budget
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder

from test_stage_graph import DOCUMENT_ID, REVISION, chunk_input, page_input


def _backend_with_chunk(tmp_path, text="Kulmer Handfeste wurde hier erörtert."):
    backend = SQLiteBackend(
        corpus_path=tmp_path / "corpus.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    actor, document, chunk = (str(uuid4()) for _ in range(3))
    digest = hashlib.sha256(text.encode()).hexdigest()
    backend.corpus.put("rkb_users", [{**defaults("rkb_users"), "id": actor}])
    backend.corpus.put("rkb_documents", [{
        **defaults("rkb_documents"),
        "id": document,
        "owner_user_id": actor,
        "title": "Synthetic source",
        "source_sha256": "a" * 64,
        "active_revision": 1,
    }])
    backend.corpus.put("rkb_chunks", [{
        **defaults("rkb_chunks"),
        "id": chunk,
        "document_id": document,
        "revision": 1,
        "title": "Evidence",
        "source_text": text,
        "search_material": text,
        "text_sha256": digest,
        "search_material_sha256": digest,
    }])
    return backend, actor, document, chunk, digest


@pytest.mark.asyncio
async def test_natural_lexical_branch_is_bounded_or_and_alias_phrase_stays_exact(tmp_path):
    backend, actor, document, chunk, _ = _backend_with_chunk(tmp_path)
    allowed = {document: 1}
    natural = backend.corpus.lexical(
        actor,
        "Warum wurde die Kulmer Handfeste hier erwähnt und wann geschah dies?",
        allowed=allowed,
        timeout_ms=100,
    )
    assert natural and natural[0]["chunk_id"] == chunk
    exact = backend.corpus.lexical(
        actor, "Kulmer Handfeste", allowed=allowed, phrase=True, timeout_ms=100
    )
    reversed_phrase = backend.corpus.lexical(
        actor, "Handfeste Kulmer", allowed=allowed, phrase=True, timeout_ms=100
    )
    assert exact and exact[0]["chunk_id"] == chunk
    assert reversed_phrase == []
    await backend.aclose()


@pytest.mark.asyncio
async def test_vector_lookup_uses_document_scope_and_validates_only_returned_candidates(tmp_path):
    backend, actor, document, chunk, digest = _backend_with_chunk(tmp_path)
    calls = []

    class VectorClient:
        async def candidates(self, actor_id, revisions, e5, es, bge, bs, depth):
            calls.append({
                "actor": actor_id,
                "revisions": revisions,
                "e5": e5,
                "bge": bge,
                "depth": depth,
            })
            return [{
                "chunk_id": chunk,
                "branch": "bge",
                "rank": 1,
                "revision": 1,
                "text_sha256": digest,
                "search_material_sha256": digest,
            }]

        async def aclose(self):
            pass

    backend.vector_client = VectorClient()
    backend.corpus.candidate_metadata = lambda allowed: (_ for _ in ()).throw(
        AssertionError("full active chunk enumeration must not run")
    )
    response = await backend.local_rankings(
        "rkb_multilingual_rankings",
        {
            "query_text": "semantic question with no lexical overlap",
            "include_lexical": False,
            "bge_vector": "[1]",
            "bge_space": BGE_SPACE,
            "depth": 8,
        },
        {"x-rkb-actor": actor},
    )
    rows = response.json()
    assert calls == [{
        "actor": actor,
        "revisions": {document: 1},
        "e5": None,
        "bge": "[1]",
        "depth": 8,
    }]
    assert len(rows) == 1 and rows[0]["chunk_id"] == chunk and rows[0]["branch"] == "bge"
    await backend.aclose()


@pytest.mark.asyncio
async def test_token_count_client_requires_both_pinned_spaces():
    embedder = LocalE5Embedder()

    async def handler(request):
        assert request.url.path == "/token-count"
        return httpx.Response(200, json={
            "e5_space": E5_SPACE,
            "bge_space": BGE_SPACE,
            "target_passage_tokens": TARGET_PASSAGE_TOKENS,
            "e5_max_tokens": MAX_TOKENS,
            "bge_max_tokens": 512,
            "e5_tokens": [255, 257],
            "bge_tokens": [250, 260],
        })

    await embedder.client.aclose()
    embedder.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://127.0.0.1:8767",
    )
    assert await embedder.passage_token_counts(["first", "second"]) == [
        {"e5": 255, "bge": 250},
        {"e5": 257, "bge": 260},
    ]
    await embedder.aclose()


@pytest.mark.asyncio
async def test_ingestion_token_budget_warns_at_target_and_blocks_real_truncation():
    graph = merge_stage(
        StagedGraph(revision=REVISION),
        compile_model_stage(
            StagedGraph(revision=REVISION),
            document_id=DOCUMENT_ID,
            revision=REVISION,
            pages=[page_input(0)],
            chunks=[chunk_input(0)],
        ),
    )
    embedder = LocalE5Embedder()

    class Service:
        pass

    service = Service()
    service.embedder = embedder

    async def over_target(_):
        return [{"e5": TARGET_PASSAGE_TOKENS + 1, "bge": TARGET_PASSAGE_TOKENS}]

    embedder.passage_token_counts = over_target
    warning = await _validate_with_token_budget(service, graph, expected_page_count=1)
    assert warning.ready
    assert any(
        item.startswith("retrieval_quality:passages_over_256_token_target:")
        for item in warning.warnings
    )

    async def truncated(_):
        return [{"e5": MAX_TOKENS + 1, "bge": 400}]

    embedder.passage_token_counts = truncated
    blocked = await _validate_with_token_budget(service, graph, expected_page_count=1)
    assert not blocked.ready
    assert any(
        item.startswith("retrieval_quality:encoder_token_limit_exceeded:e5:")
        for item in blocked.errors
    )
    await embedder.aclose()

@pytest.mark.asyncio
async def test_main_retrieval_defaults_to_bge_without_e5_or_lexical(monkeypatch):
    from types import SimpleNamespace
    import regional_knowledge.multilingual_retrieval as retrieval

    query = "cross-language fact"
    query_hash = hashlib.sha256(query.encode()).hexdigest()
    actor = Principal(
        subject="owner",
        client_id="test",
        issuer="test",
        access_token="",
    )

    class FakeQueue:
        def status(self):
            return {"state": "ready"}

        def result(self, actor_id, job_id):
            assert actor_id == actor.subject and job_id == "ready-job"
            return {
                "state": "done",
                "identity": {"query_sha256": query_hash},
                "result": {
                    "space": BGE_SPACE,
                    "vectors": [[1.0] + [0.0] * 1023],
                    "queue_seconds": 0.0,
                    "encoder_seconds": 0.01,
                },
            }

    monkeypatch.setattr(retrieval, "BgeQueue", lambda path: FakeQueue())
    monkeypatch.setenv("RKB_BGE_QUEUE_PATH", "unused")
    monkeypatch.delenv("RKB_BGE_WARM_MODE", raising=False)
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{
                "chunk_id": "hit",
                "branch": "bge",
                "rank": 1,
                "revision": 1,
                "text_sha256": "a" * 64,
                "search_material_sha256": "b" * 64,
            }]

    class Client:
        async def post(self, url, headers=None, json=None):
            calls.append(json)
            return Response()

    class Corpus:
        def one(self, table, ident):
            assert table == "rkb_chunks" and ident == "hit"
            return {"id": "hit", "title": "Evidence"}

    class NoE5:
        async def embed(self, value):
            raise AssertionError("default BGE mode must not encode E5")

    class Backend:
        client = Client()
        corpus = Corpus()
        embedder = NoE5()
        config = SimpleNamespace(url="sqlite://regional-knowledge.internal")

        def _headers(self, principal):
            return {"x-rkb-actor": principal.subject}

        def _evidence_url(self, ident):
            return "knowledge://evidence/" + ident

        async def search(self, *args, **kwargs):
            raise AssertionError("ready BGE result must not fall back")

    out = await retrieval.main_search(
        Backend(), query, actor, main_job_id="ready-job"
    )
    assert out.retrieval_mode == "bge" and out.main_state == "ready"
    assert out.results[0].id == "hit"
    assert len(calls) == 1
    assert calls[0]["include_lexical"] is False
    assert calls[0]["e5_vector"] is None


@pytest.mark.asyncio
async def test_local_service_keeps_inference_queue_separate_from_token_counter():
    from regional_knowledge.e5_service import LocalService, SerialEncoder

    def encode(texts, role):
        return [[1.0] + [0.0] * 383 for _ in texts]

    def count(texts, role):
        return {
            "e5_space": E5_SPACE,
            "bge_space": BGE_SPACE,
            "target_passage_tokens": TARGET_PASSAGE_TOKENS,
            "e5_max_tokens": MAX_TOKENS,
            "bge_max_tokens": 512,
            "e5_tokens": [10 for _ in texts],
            "bge_tokens": [11 for _ in texts],
        }

    queue = SerialEncoder(encode)
    service = LocalService(queue, token_counts=count, token_budget_ready=True)
    status = service.status()
    assert status["ready"] and status["token_budget_ready"]
    assert service.token_counts(["passage"], "passage")["bge_tokens"] == [11]
    await queue.close()

@pytest.mark.asyncio
async def test_production_ingestion_fails_closed_when_exact_token_counter_is_unavailable(monkeypatch):
    graph = merge_stage(
        StagedGraph(revision=REVISION),
        compile_model_stage(
            StagedGraph(revision=REVISION),
            document_id=DOCUMENT_ID,
            revision=REVISION,
            pages=[page_input(0)],
            chunks=[chunk_input(0)],
        ),
    )

    class Service:
        embedder = LexicalOnlyEmbedder()

    monkeypatch.setenv("RKB_AUTO_INDEX_ENABLED", "1")
    result = await _validate_with_token_budget(Service(), graph, expected_page_count=1)
    assert not result.ready
    assert "token_budget_unavailable:local_e5_required" in result.errors

def test_vector_sql_compact_revision_scope_contract():
    sql = Path("sql/021_vector_only_plane.sql").read_text()
    assert "rkb_vector_candidates_v3" in sql
    assert "rkb_vector_revision_scope" in sql
    v3 = sql.split("create or replace function public.rkb_vector_candidates_v3", 1)[1]
    v3 = v3.split("commit;", 1)[0]
    assert "active_chunks" not in v3
    assert "public.rkb_vector_revision_scope(a.document_id,a.revision)" in v3
    assert "language plpgsql stable security invoker set search_path=public as $rkb_v3$" in v3
    assert "end $rkb_v3$;" in v3
