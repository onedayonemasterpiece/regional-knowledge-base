from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote

import httpx

from .backend import KnowledgeBackend, RenderedPageBatch, UnavailableBackend
from .contracts import (
    BookIngestOutput,
    ChatFile,
    DocumentAccessOutput,
    FetchOutput,
    Principal,
    RightsStatus,
    SearchOutput,
    SearchResult,
    Visibility,
)
from .rights import assert_visibility_allowed


class Embedder(Protocol):
    async def embed(self, text: str) -> list[float] | None: ...


class LexicalOnlyEmbedder:
    async def embed(self, text: str) -> list[float] | None:
        return None


class OpenAICompatibleEmbedder:
    """Small external embedding client; never runs embedding compute on this server."""

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        model: str,
        dimensions: int = 768,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.dimensions = dimensions
        self.timeout_seconds = timeout_seconds

    async def embed(self, text: str) -> list[float] | None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "input": text,
                    "dimensions": self.dimensions,
                },
            )
            response.raise_for_status()
            payload = response.json()
        vector = payload["data"][0]["embedding"]
        if not isinstance(vector, list) or len(vector) != self.dimensions:
            raise RuntimeError("embedding provider returned an unexpected dimension")
        return [float(value) for value in vector]


@dataclass(frozen=True, slots=True)
class SupabaseConfig:
    url: str
    anon_key: str
    public_base_url: str


def _halfvec_literal(values: list[float] | None) -> str | None:
    if values is None:
        return None
    if len(values) != 768:
        raise ValueError("query embedding must have exactly 768 dimensions")
    return "[" + ",".join(format(float(value), ".9g") for value in values) + "]"


class SupabaseRestBackend(KnowledgeBackend):
    """User-JWT PostgREST backend. RLS stays in the request path."""

    def __init__(
        self,
        config: SupabaseConfig,
        *,
        embedder: Embedder | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.embedder = embedder or LexicalOnlyEmbedder()
        self.client = client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = client is None

    def _headers(self, principal: Principal) -> dict[str, str]:
        return {
            "apikey": self.config.anon_key,
            "Authorization": f"Bearer {principal.access_token}",
            "Content-Type": "application/json",
        }

    def _evidence_url(self, item_id: str) -> str:
        return f"{self.config.public_base_url.rstrip('/')}/evidence/{quote(item_id, safe='')}"

    async def search(self, query: str, principal: Principal) -> SearchOutput:
        query = query.strip()
        if not query:
            return SearchOutput(results=[], mode="lexical_degraded")

        vector: list[float] | None = None
        try:
            vector = await self.embedder.embed(query)
        except (httpx.HTTPError, TimeoutError, RuntimeError, ValueError):
            # Search availability is more important than vector-only perfection:
            # the SQL RPC accepts NULL and performs RLS-protected lexical retrieval.
            vector = None

        response = await self.client.post(
            f"{self.config.url.rstrip('/')}/rest/v1/rpc/rkb_hybrid_search",
            headers=self._headers(principal),
            json={
                "query_text": query,
                "query_embedding": _halfvec_literal(vector),
                "match_count": 8,
            },
        )
        response.raise_for_status()
        rows = response.json()
        return SearchOutput(
            results=[
                SearchResult(
                    id=str(row["chunk_id"]),
                    title=str(row["title"]),
                    url=self._evidence_url(str(row["chunk_id"])),
                )
                for row in rows
            ],
            mode="hybrid" if vector is not None else "lexical_degraded",
        )

    async def fetch(self, item_id: str, principal: Principal) -> FetchOutput:
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_chunks",
            headers=self._headers(principal),
            params={
                "id": f"eq.{item_id}",
                "select": (
                    "id,document_id,title,source_text,metadata,page_ids,"
                    "illustration_ids,footnote_region_ids"
                ),
                "limit": "1",
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError("evidence_not_found")
        row = rows[0]
        metadata = dict(row.get("metadata") or {})
        metadata.update(
            {
                "document_id": str(row["document_id"]),
                "pages": [str(value) for value in row.get("page_ids") or []],
                "illustrations": [
                    {"illustration_id": str(value)}
                    for value in row.get("illustration_ids") or []
                ],
                "footnotes": [
                    {"region_id": str(value)}
                    for value in row.get("footnote_region_ids") or []
                ],
            }
        )
        return FetchOutput(
            id=str(row["id"]),
            title=str(row["title"]),
            text=str(row["source_text"]),
            url=self._evidence_url(str(row["id"])),
            metadata=metadata,
        )

    async def document_access(
        self,
        *,
        document_id: str,
        principal: Principal,
        visibility: Visibility | None,
        grantee_user_id: str | None,
    ) -> DocumentAccessOutput:
        base = f"{self.config.url.rstrip('/')}/rest/v1/rkb_documents"
        response = await self.client.get(
            base,
            headers=self._headers(principal),
            params={
                "id": f"eq.{document_id}",
                "select": "id,owner_user_id,content_visibility,rights_status",
                "limit": "1",
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError("document_not_found")
        row = rows[0]
        current_visibility = Visibility(row["content_visibility"])
        rights_status = RightsStatus(row["rights_status"])
        changed = False

        if visibility is not None and visibility is not current_visibility:
            assert_visibility_allowed(visibility, rights_status)
            update = await self.client.patch(
                base,
                headers={**self._headers(principal), "Prefer": "return=representation"},
                params={"id": f"eq.{document_id}"},
                json={"content_visibility": visibility.value},
            )
            update.raise_for_status()
            current_visibility = visibility
            changed = True

        if grantee_user_id:
            grant = await self.client.post(
                f"{self.config.url.rstrip('/')}/rest/v1/rkb_document_grants",
                headers={
                    **self._headers(principal),
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json={
                    "document_id": document_id,
                    "grantee_user_id": grantee_user_id,
                    "role": "viewer",
                },
            )
            grant.raise_for_status()
            changed = True

        return DocumentAccessOutput(
            document_id=document_id,
            visibility=current_visibility,
            rights_status=rights_status,
            changed=changed,
            message="Access state updated" if changed else "Access state unchanged",
        )

    async def book_ingest(self, **_: object) -> BookIngestOutput:
        raise RuntimeError("ingestion adapter is not configured yet")

    async def book_pages(self, **_: object) -> RenderedPageBatch:
        raise RuntimeError("ingestion adapter is not configured yet")

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()


def backend_from_env() -> KnowledgeBackend:
    url = os.getenv("SUPABASE_URL", "").strip()
    anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    public_base = os.getenv("RKB_PUBLIC_BASE_URL", "").strip()
    if not (url and anon_key and public_base):
        return UnavailableBackend()

    endpoint = os.getenv("RKB_EMBEDDING_ENDPOINT", "").strip()
    api_key = os.getenv("RKB_EMBEDDING_API_KEY", "").strip()
    model = os.getenv("RKB_EMBEDDING_MODEL", "").strip()
    embedder: Embedder
    if endpoint and api_key and model:
        embedder = OpenAICompatibleEmbedder(
            endpoint=endpoint,
            api_key=api_key,
            model=model,
        )
    else:
        embedder = LexicalOnlyEmbedder()

    return SupabaseRestBackend(
        SupabaseConfig(url=url, anon_key=anon_key, public_base_url=public_base),
        embedder=embedder,
    )
