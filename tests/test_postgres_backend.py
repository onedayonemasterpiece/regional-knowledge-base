from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.postgres_backend import PostgresBackend, PostgresDataClient
from regional_knowledge.supabase_backend import (
    LexicalOnlyEmbedder,
    OpenAICompatibleEmbedder,
    _embedder_from_env,
    backend_from_env,
)


@pytest.mark.asyncio
async def test_direct_backend_discards_mcp_bearer() -> None:
    backend = PostgresBackend(
        "postgresql://user:password@127.0.0.1:6543/postgres",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    principal = Principal(
        subject=str(uuid4()),
        client_id="chatgpt",
        issuer="https://knowledge.example.test",
        access_token="must-never-reach-supabase",
    )
    headers = backend._headers(principal)
    assert headers == {"x-rkb-actor": principal.subject}
    assert "Authorization" not in headers
    assert principal.access_token not in str(headers)
    await backend.aclose()


def test_backend_from_env_prefers_session_pooler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "KB_SUPABASE_SESSION_CONNECTION",
        "postgresql://user:password@127.0.0.1:6543/postgres",
    )
    monkeypatch.delenv("RKB_ALLOW_LEGACY_SUPABASE_USER_JWT", raising=False)
    for name in (
        "RKB_S3_ENDPOINT",
        "RKB_S3_REGION",
        "RKB_S3_BUCKET",
        "RKB_S3_ACCESS_KEY_ID",
        "RKB_S3_SECRET_ACCESS_KEY",
        "RKB_EMBEDDING_ENDPOINT",
        "RKB_EMBEDDING_API_KEY",
        "RKB_EMBEDDING_MODEL",
        "RKB_EMBEDDING_SPACE",
        "OPENAI_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    backend = backend_from_env()
    assert isinstance(backend, PostgresBackend)


def test_direct_backend_requires_uuid_subject() -> None:
    backend = PostgresBackend(
        "postgresql://user:password@127.0.0.1:6543/postgres",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    principal = Principal(
        subject="not-a-uuid",
        client_id="chatgpt",
        issuer="https://knowledge.example.test",
        access_token="secret",
    )
    with pytest.raises(ValueError):
        backend._headers(principal)


@pytest.mark.asyncio
async def test_rpc_dispatch_is_lazy_for_start_ingestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = PostgresDataClient(
        "postgresql://user:password@127.0.0.1:6543/postgres"
    )

    class Cursor:
        async def fetchall(self):
            return [{"ingestion_id": uuid4(), "document_id": uuid4()}]

    class Connection:
        async def execute(self, statement, values):
            assert "rkb_start_ingestion" in statement
            assert len(values) == 9
            return Cursor()

    @asynccontextmanager
    async def fake_connection(headers):
        assert headers == {"x-rkb-service": "1"}
        yield Connection()

    monkeypatch.setattr(client, "_connection", fake_connection)
    response = await client._rpc(
        "rkb_start_ingestion",
        {
            "p_ingestion_id": str(uuid4()),
            "p_document_id": str(uuid4()),
            "p_title": "Test",
            "p_authors": [],
            "p_publication_year": 1893,
            "p_language": "de",
            "p_source_sha256": "a" * 64,
            "p_source_file_id": "file-1",
            "p_page_count": 40,
        },
        {"x-rkb-service": "1"},
    )
    assert response.json()


def test_shared_openai_key_is_not_embedding_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "RKB_EMBEDDING_ENDPOINT",
        "RKB_EMBEDDING_API_KEY",
        "RKB_EMBEDDING_MODEL",
        "RKB_EMBEDDING_SPACE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    embedder = _embedder_from_env()
    assert isinstance(embedder, LexicalOnlyEmbedder)


def test_explicit_embedding_quartet_selects_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "RKB_EMBEDDING_ENDPOINT",
        "http://127.0.0.1:8199/v1/embeddings",
    )
    monkeypatch.setenv("RKB_EMBEDDING_API_KEY", "local")
    monkeypatch.setenv("RKB_EMBEDDING_MODEL", "multilingual-mpnet-base-v2")
    monkeypatch.setenv(
        "RKB_EMBEDDING_SPACE",
        "local:multilingual-mpnet-base-v2:v1",
    )
    embedder = _embedder_from_env()
    assert isinstance(embedder, OpenAICompatibleEmbedder)
    assert embedder.embedding_space == "local:multilingual-mpnet-base-v2:v1"


def test_embedding_config_without_space_degrades_safely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "RKB_EMBEDDING_ENDPOINT",
        "http://127.0.0.1:8199/v1/embeddings",
    )
    monkeypatch.setenv("RKB_EMBEDDING_API_KEY", "local")
    monkeypatch.setenv("RKB_EMBEDDING_MODEL", "multilingual-mpnet-base-v2")
    monkeypatch.delenv("RKB_EMBEDDING_SPACE", raising=False)
    embedder = _embedder_from_env()
    assert isinstance(embedder, LexicalOnlyEmbedder)
