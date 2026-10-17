"""Repository-level guard: BGE only on Kaggle; E5 local and pinned."""
from __future__ import annotations

from pathlib import Path

import pytest

from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.object_store import UnavailableObjectStore

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.asyncio
async def test_stale_local_bge_flag_cannot_enable_inference(monkeypatch,tmp_path):
    monkeypatch.setenv("RKB_BGE_QUERY_LOCAL_ENABLED","1")
    monkeypatch.setenv("RKB_BGE_QUERY_MODEL_DIR","/nonexistent/forbidden-model")
    backend=SQLiteBackend(
        corpus_path=tmp_path/"kaggle-only.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    try:
        assert backend.bge_query_embedder is None
    finally:
        await backend.aclose()


def test_bge_local_inference_removed_and_cannot_be_installed_from_repo():
    forbidden=(
        "src/regional_knowledge/bge_query_service.py",
        "src/regional_knowledge/local_bge.py",
        "ops/regional-knowledge-bge-query.service",
    )
    assert all(not (ROOT/path).exists() for path in forbidden)
    for prefix in ("src/regional_knowledge","ops","scripts/production"):
        for suffix in ("*.py","*.service","*.conf"):
            for path in (ROOT/prefix).glob(suffix):
                text=path.read_text(encoding="utf-8")
                assert "PinnedBGEQueryEncoder" not in text
                assert "LocalBGEQueryEmbedder" not in text
                assert "RKB_BGE_QUERY_MODEL_DIR" not in text
    # The BGE tokenizer is required only for ingestion token-budget safety.
    assert "BGE_TOKENIZER_SHA256" in (
        ROOT/"src/regional_knowledge/e5_service.py").read_text()
    # The Kaggle vector queue and E5 sidecar remain intact.
    assert (ROOT/"src/regional_knowledge/bge_queue.py").is_file()
    assert (ROOT/"ops/regional-knowledge-e5.service").is_file()


def test_agents_document_hard_architectural_invariant():
    instructions=(ROOT/"AGENTS.md").read_text()
    assert "Kaggle-only BGE" in instructions
    assert "never install or start" in instructions
