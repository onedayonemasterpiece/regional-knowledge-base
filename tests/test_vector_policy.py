import pytest

from regional_knowledge.bge_contract import REVISION
from regional_knowledge.sqlite_revision import ready
from regional_knowledge.vector_policy import (
    index_vector_spaces,
    missing_required,
    required_vector_spaces,
)


def test_bge_is_default_publication_gate(monkeypatch):
    monkeypatch.delenv("RKB_REQUIRED_VECTOR_SPACES", raising=False)
    monkeypatch.delenv("RKB_INDEX_E5_DIAGNOSTIC", raising=False)
    assert required_vector_spaces() == ("bge",)
    assert index_vector_spaces() == ("bge",)
    assert missing_required(e5_missing=100, bge_missing=0) is False
    assert missing_required(e5_missing=0, bge_missing=1) is True


def test_e5_can_be_indexed_diagnostically_without_becoming_required(monkeypatch):
    monkeypatch.delenv("RKB_REQUIRED_VECTOR_SPACES", raising=False)
    monkeypatch.setenv("RKB_INDEX_E5_DIAGNOSTIC", "1")
    assert required_vector_spaces() == ("bge",)
    assert index_vector_spaces() == ("bge", "e5")
    assert missing_required(e5_missing=3, bge_missing=0) is False


def test_explicit_strict_dual_space_gate_remains_available(monkeypatch):
    monkeypatch.setenv("RKB_REQUIRED_VECTOR_SPACES", "e5,bge")
    monkeypatch.delenv("RKB_INDEX_E5_DIAGNOSTIC", raising=False)
    assert required_vector_spaces() == ("e5", "bge")
    assert index_vector_spaces() == ("bge", "e5")
    assert missing_required(e5_missing=1, bge_missing=0) is True


@pytest.mark.parametrize("value", ["", "e5", "bge,unknown"])
def test_invalid_required_space_policy_fails_closed(monkeypatch, value):
    monkeypatch.setenv("RKB_REQUIRED_VECTOR_SPACES", value)
    with pytest.raises(RuntimeError):
        required_vector_spaces()


def test_sqlite_publication_accepts_bge_without_e5_by_default(monkeypatch):
    monkeypatch.delenv("RKB_REQUIRED_VECTOR_SPACES", raising=False)
    chunk = {
        "id": "chunk",
        "revision": 2,
        "text_sha256": "a" * 64,
        "search_material_sha256": "b" * 64,
    }

    class Context:
        def one(self, table, ident):
            assert ident == "chunk"
            if table == "rkb_chunk_embeddings_e5":
                return None
            return {
                "chunk_id": ident,
                "embedding_space": "bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1",
                "model_revision": REVISION,
                "revision": 2,
                "text_sha256": "a" * 64,
                "search_material_sha256": "b" * 64,
            }

    assert ready(Context(), chunk) is True
    monkeypatch.setenv("RKB_REQUIRED_VECTOR_SPACES", "e5,bge")
    assert ready(Context(), chunk) is False
