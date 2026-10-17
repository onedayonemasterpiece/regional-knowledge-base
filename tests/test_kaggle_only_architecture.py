"""The RKB production backend must never create a BGE encoder on the host."""
from __future__ import annotations

import pytest

from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore


@pytest.mark.asyncio
async def test_stale_local_bge_flag_cannot_enable_local_inference(monkeypatch, tmp_path):
    monkeypatch.setenv("RKB_BGE_QUERY_LOCAL_ENABLED", "1")
    from regional_knowledge import local_bge

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Local ONNX BGE must never be instantiated")

    monkeypatch.setattr(local_bge.LocalBGEQueryEmbedder, "__init__", forbidden)
    backend = SQLiteBackend(
        corpus_path=tmp_path/"kaggle-only.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    try:
        assert backend.bge_query_embedder is None
    finally:
        await backend.aclose()
