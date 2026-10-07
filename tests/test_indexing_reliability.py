import hashlib
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from regional_knowledge.bge_contract import SPACE as BGE_SPACE
from regional_knowledge.indexing import IndexReconciler, STALL_SECONDS, write_health


class _Corpus:
    def __init__(self, chunk):
        self.chunk = chunk
        self.authorized = []

    def authorize(self, actor, document_id, *, owner=False):
        self.authorized.append((actor, document_id, owner))
        return {"id": document_id}

    def one(self, table, ident):
        assert table == "rkb_chunks"
        assert ident == self.chunk["id"]
        return dict(self.chunk)


class _Backend:
    def __init__(self, corpus):
        self.corpus = corpus
        self.embedder = None

    async def fetch(self, *_args, **_kwargs):
        raise AssertionError("public evidence hydration must not be used for local indexing")


@pytest.mark.asyncio
async def test_local_index_source_reads_exact_authorized_chunk_without_public_fetch(monkeypatch):
    monkeypatch.delenv("RKB_REQUIRED_VECTOR_SPACES", raising=False)
    monkeypatch.delenv("RKB_INDEX_E5_DIAGNOSTIC", raising=False)
    source = "Exact Brünneck indexing passage."
    digest = hashlib.sha256(source.encode()).hexdigest()
    chunk = {
        "id": "chunk-1",
        "document_id": "doc-1",
        "revision": 2,
        "source_text": source,
        "search_material": source,
        "text_sha256": digest,
        "search_material_sha256": digest,
    }
    corpus = _Corpus(chunk)
    worker = IndexReconciler(_Backend(corpus), object())
    actor = SimpleNamespace(subject="actor-1")
    row = {key: chunk[key] for key in (
        "id", "document_id", "revision", "text_sha256", "search_material_sha256"
    )}

    assert await worker.source(actor, row) == source
    assert corpus.authorized == [("actor-1", "doc-1", True)]


@pytest.mark.asyncio
async def test_ready_bge_results_are_installed_in_one_local_batch(monkeypatch):
    monkeypatch.delenv("RKB_REQUIRED_VECTOR_SPACES", raising=False)
    monkeypatch.delenv("RKB_INDEX_E5_DIAGNOSTIC", raising=False)
    backend = _Backend(_Corpus({"id": "unused"}))
    worker = IndexReconciler(backend, object())
    worker.install_local = AsyncMock(return_value=2)
    vector = [1.0] + [0.0] * 1023
    rows = [
        {"id": "a", "document_id": "doc", "revision": 2, "text_sha256": "a" * 64, "search_material_sha256": "a" * 64},
        {"id": "b", "document_id": "doc", "revision": 2, "text_sha256": "b" * 64, "search_material_sha256": "b" * 64},
    ]
    ready = [(row, {"space": BGE_SPACE, "vectors": [vector]}) for row in rows]

    assert await worker.install_bge_batch(SimpleNamespace(subject="actor"), ready) == 2
    worker.install_local.assert_awaited_once()
    args = worker.install_local.await_args.args
    assert args[1] == rows
    assert len(args[2]) == 2
    assert args[3] == BGE_SPACE


def test_indexing_health_marks_persistent_no_progress_as_stalled(tmp_path, monkeypatch):
    path = tmp_path / "indexing-health.json"
    monkeypatch.setenv("RKB_INDEXING_HEALTH_PATH", str(path))
    old = time.time() - STALL_SECONDS - 2
    path.write_text(json.dumps({
        "updated_at": old,
        "state": "running",
        "pending_documents": 1,
        "last_progress_at": old,
        "stalled_seconds": 0,
    }))

    write_health({
        "e5_written": 0,
        "bge_submitted": 0,
        "bge_written": 0,
        "errors": [],
        "pending_documents": 1,
        "phase_seconds": {"publication": 0.1},
    })

    value = json.loads(path.read_text())
    assert value["state"] == "degraded"
    assert value["error_type"] == "indexing_stalled"
    assert value["stalled_seconds"] >= STALL_SECONDS
    assert value["phase_seconds"]["publication"] == 0.1
