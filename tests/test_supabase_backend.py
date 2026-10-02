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
async def test_fetch_authorizes_with_user_rls_then_reads_exact_object_range():
    import hashlib

    text = "Точный фрагмент источника".encode("utf-8")

    class Store:
        async def get_range(self, key, start, end):
            assert key == "tenants/t/documents/d/text/rev-1.txt"
            assert (start, end) == (100, 100 + len(text))
            return text

        async def get_bytes(self, key):
            raise AssertionError("fetch should use a byte range")

        async def put_bytes(self, key, data, content_type):
            raise AssertionError("fetch is read-only")

    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.headers["authorization"]))
        if request.url.path == "/rest/v1/rkb_chunks":
            assert request.headers["authorization"] == "Bearer user-jwt"
            return httpx.Response(
                200,
                json=[{
                    "id": "22222222-2222-2222-2222-222222222222",
                    "document_id": "33333333-3333-3333-3333-333333333333",
                    "title": "Источник",
                    "metadata": {"printed_pages": ["15"]},
                    "page_ids": ["44444444-4444-4444-4444-444444444444"],
                    "illustration_ids": ["55555555-5555-5555-5555-555555555555"],
                    "footnote_region_ids": [],
                    "text_object_id": "66666666-6666-6666-6666-666666666666",
                    "text_start": 100,
                    "text_end": 100 + len(text),
                    "text_sha256": hashlib.sha256(text).hexdigest(),
                }],
            )
        if request.url.path == "/rest/v1/rkb_objects":
            assert request.headers["authorization"] == "Bearer server-role"
            assert (
                request.url.params["id"]
                == "eq.66666666-6666-6666-6666-666666666666"
            )
            assert (
                request.url.params["document_id"]
                == "eq.33333333-3333-3333-3333-333333333333"
            )
            return httpx.Response(
                200,
                json=[{
                    "id": "66666666-6666-6666-6666-666666666666",
                    "object_key": "tenants/t/documents/d/text/rev-1.txt",
                    "sha256": "0" * 64,
                    "mime_type": "text/plain",
                }],
            )
        raise AssertionError(request.url)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="public-anon-key",
            public_base_url="https://knowledge.example",
            service_role_key="server-role",
        ),
        client=client,
        object_store=Store(),
    )
    result = await backend.fetch(
        "22222222-2222-2222-2222-222222222222", principal()
    )
    await client.aclose()

    assert result.text == "Точный фрагмент источника"
    assert result.metadata["printed_pages"] == ["15"]
    assert result.metadata["illustrations"][0]["illustration_id"].startswith("5555")
    assert calls == [
        ("/rest/v1/rkb_chunks", "Bearer user-jwt"),
        ("/rest/v1/rkb_objects", "Bearer server-role"),
    ]


@pytest.mark.asyncio
async def test_live_evidence_search_fetches_only_small_selected_set():
    class Backend(SupabaseRestBackend):
        def __init__(self):
            pass

        async def search(self, query, principal):
            from regional_knowledge.contracts import SearchOutput, SearchResult
            return SearchOutput(
                results=[
                    SearchResult(id=str(i), title=f"T{i}", url=f"https://e/{i}")
                    for i in range(8)
                ],
                mode="hybrid",
            )

        async def fetch(self, item_id, principal):
            from regional_knowledge.contracts import FetchOutput
            return FetchOutput(
                id=item_id,
                title=f"T{item_id}",
                text=f"E{item_id}",
                url=f"https://e/{item_id}",
            )

    result = await Backend().search_evidence("x", principal(), max_evidence=3)
    assert result.mode == "hybrid"
    assert [item.id for item in result.evidence] == ["0", "1", "2"]
