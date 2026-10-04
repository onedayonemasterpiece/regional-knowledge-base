import json

import httpx
import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.supabase_backend import (
    LexicalOnlyEmbedder,
    SupabaseConfig,
    SupabaseRestBackend,
)


def test_evidence_url_uses_canonical_uri_without_public_base():
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="public-anon-key",
            public_base_url=None,
        )
    )
    assert (
        backend._evidence_url("abc/123")
        == "knowledge://evidence/abc%2F123"
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
    assert seen["payload"]["query_embedding_space"] is None
    assert result.mode == "lexical_degraded"
    assert result.results[0].url.endswith(
        "/evidence/22222222-2222-2222-2222-222222222222"
    )


@pytest.mark.asyncio
async def test_search_sends_embedding_space_with_vector():
    seen = {}

    class FixedEmbedder:
        embedding_space = "local:multilingual-mpnet-base-v2:v1"

        async def embed(self, text):
            assert text
            return [0.001] * 768

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["payload"] = json.loads(request.content)
        return httpx.Response(200, json=[])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    backend = SupabaseRestBackend(
        SupabaseConfig(url="https://db.example", anon_key="public-anon-key"),
        embedder=FixedEmbedder(),
        client=client,
    )
    result = await backend.search("семь мостов", principal())
    await client.aclose()

    assert seen["payload"]["query_embedding"] is not None
    assert (
        seen["payload"]["query_embedding_space"]
        == "local:multilingual-mpnet-base-v2:v1"
    )
    assert result.mode == "hybrid"


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
        if request.url.path == '/rest/v1/rkb_illustrations':
            assert request.headers['authorization']=='Bearer user-jwt'
            return httpx.Response(200,json=[{'id':'55555555-5555-5555-5555-555555555555','document_id':'33333333-3333-3333-3333-333333333333','page_id':'44444444-4444-4444-4444-444444444444','source_region_id':'77777777-7777-7777-7777-777777777777','kind':'drawing','caption_text':'Printed caption','visual_description':'A model description','visual_description_provenance':'model_observation'}])
        if request.url.path == '/rest/v1/rkb_pages':
            return httpx.Response(200,json=[{'id':'44444444-4444-4444-4444-444444444444','document_id':'33333333-3333-3333-3333-333333333333','physical_page_index':0,'revision':1}])
        if request.url.path == '/rest/v1/rkb_regions':
            return httpx.Response(200,json=[{'id':'77777777-7777-7777-7777-777777777777','page_id':'44444444-4444-4444-4444-444444444444','kind':'figure','bbox':{'left':0,'top':0,'right':500,'bottom':500}}])
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
        ('/rest/v1/rkb_illustrations', 'Bearer user-jwt'),
        ('/rest/v1/rkb_pages', 'Bearer user-jwt'),
        ('/rest/v1/rkb_regions', 'Bearer user-jwt'),
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


def test_new_supabase_secret_key_is_not_sent_as_bearer():
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="sb_publishable_test",
            public_base_url="https://knowledge.example",
            service_role_key="sb_secret_test",
        ),
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(500)
            )
        ),
    )
    headers = backend._service_headers()
    assert headers["apikey"] == "sb_secret_test"
    assert "Authorization" not in headers


def test_legacy_service_role_keeps_bearer_header():
    backend = SupabaseRestBackend(
        SupabaseConfig(
            url="https://db.example",
            anon_key="legacy-anon",
            public_base_url="https://knowledge.example",
            service_role_key="legacy-service-role",
        ),
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(500)
            )
        ),
    )
    headers = backend._service_headers()
    assert headers["apikey"] == "legacy-service-role"
    assert headers["Authorization"] == "Bearer legacy-service-role"


def test_backend_from_env_refuses_hidden_supabase_user_jwt_coupling(monkeypatch):
    from regional_knowledge.backend import UnavailableBackend
    from regional_knowledge.supabase_backend import backend_from_env

    monkeypatch.delenv(
        "RKB_ALLOW_LEGACY_SUPABASE_USER_JWT",
        raising=False,
    )
    monkeypatch.setenv("KB_SUPABASE_URL", "https://kb.example")
    monkeypatch.setenv("KB_SUPABASE_PUBLISHABLE_KEY", "sb_publishable_kb")
    monkeypatch.setenv("KB_SUPABASE_SECRET_KEY", "sb_secret_kb")

    backend = backend_from_env()
    assert isinstance(backend, UnavailableBackend)


def test_legacy_backend_requires_explicit_opt_in(monkeypatch):
    from regional_knowledge.supabase_backend import backend_from_env

    for name in (
        "SUPABASE_URL",
        "SUPABASE_PUBLISHABLE_KEY",
        "SUPABASE_ANON_KEY",
        "SUPABASE_SECRET_KEY",
        "SUPABASE_SERVICE_ROLE_KEY",
        "KB_SUPABASE_ANON_KEY",
        "KB_SUPABASE_SERVICE_ROLE_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RKB_ALLOW_LEGACY_SUPABASE_USER_JWT", "1")
    monkeypatch.setenv("KB_SUPABASE_URL", "https://kb.example")
    monkeypatch.setenv("KB_SUPABASE_PUBLISHABLE_KEY", "sb_publishable_kb")
    monkeypatch.setenv("KB_SUPABASE_SECRET_KEY", "sb_secret_kb")
    monkeypatch.setenv("RKB_PUBLIC_BASE_URL", "https://knowledge.example")

    backend = backend_from_env()
    assert isinstance(backend, SupabaseRestBackend)
    assert backend.config.url == "https://kb.example"
    assert backend.config.anon_key == "sb_publishable_kb"
    assert backend.config.service_role_key == "sb_secret_kb"
