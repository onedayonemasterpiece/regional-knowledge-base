import json

import httpx
import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.supabase_backend import (
    LexicalOnlyEmbedder,
    SupabaseConfig,
    SupabaseRestBackend,
)


def principal():
    return Principal(
        subject="11111111-1111-1111-1111-111111111111",
        client_id="chatgpt",
        issuer="https://issuer.example/auth/v1",
        access_token="user-jwt",
    )


@pytest.mark.asyncio
async def test_search_preserves_user_jwt_and_degrades_to_lexical():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["authorization"] = request.headers.get("authorization")
        seen["apikey"] = request.headers.get("apikey")
        seen["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            json=[
                {
                    "chunk_id": "22222222-2222-2222-2222-222222222222",
                    "document_id": "33333333-3333-3333-3333-333333333333",
                    "title": "Книга",
                    "snippet": "Фрагмент",
                    "page_ids": [],
                    "illustration_ids": [],
                    "score": 0.02,
                }
            ],
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="public-anon-key",
            public_base_url="https://knowledge.example",
        ),
        embedder=LexicalOnlyEmbedder(),
        client=client,
    )
    result = await backend.search("Кёнигсберг", principal())
    await client.aclose()

    assert seen["authorization"] == "Bearer user-jwt"
    assert seen["apikey"] == "public-anon-key"
    assert seen["payload"]["query_embedding"] is None
    assert result.mode == "lexical_degraded"
    assert result.results[0].url.endswith(
        "/evidence/22222222-2222-2222-2222-222222222222"
    )


@pytest.mark.asyncio
async def test_fetch_uses_rls_user_path():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer user-jwt"
        assert request.url.path == "/rest/v1/rkb_chunks"
        return httpx.Response(
            200,
            json=[
                {
                    "id": "22222222-2222-2222-2222-222222222222",
                    "document_id": "33333333-3333-3333-3333-333333333333",
                    "title": "Источник",
                    "source_text": "Точный фрагмент источника",
                    "metadata": {"printed_pages": ["15"]},
                    "page_ids": ["44444444-4444-4444-4444-444444444444"],
                    "illustration_ids": ["55555555-5555-5555-5555-555555555555"],
                    "footnote_region_ids": [],
                }
            ],
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="public-anon-key",
            public_base_url="https://knowledge.example",
        ),
        client=client,
    )
    result = await backend.fetch(
        "22222222-2222-2222-2222-222222222222", principal()
    )
    await client.aclose()

    assert result.text == "Точный фрагмент источника"
    assert result.metadata["printed_pages"] == ["15"]
    assert result.metadata["illustrations"][0]["illustration_id"].startswith("5555")
