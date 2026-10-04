from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .contracts import (
    BookIngestOutput,
    ChatFile,
    DocumentAccessOutput,
    EvidenceSearchOutput,
    FetchOutput,
    Principal,
    SearchOutput,
    Visibility,
)


@dataclass(frozen=True, slots=True)
class RenderedPage:
    page_id: str
    physical_page_index: int
    printed_page_number: str | None
    mime_type: str
    data: bytes
    native_text: str | None = None
    native_blocks: tuple[dict[str, Any], ...] = ()
    native_text_info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RenderedPageBatch:
    pages: tuple[RenderedPage, ...]
    next_cursor: str | None = None


class KnowledgeBackend(Protocol):
    async def search(self, query: str, principal: Principal) -> SearchOutput: ...

    async def fetch(self, item_id: str, principal: Principal) -> FetchOutput: ...

    async def search_evidence(
        self,
        query: str,
        principal: Principal,
        *,
        max_evidence: int = 3,
    ) -> EvidenceSearchOutput: ...

    async def book_pages(
        self,
        *,
        ingestion_id: str,
        principal: Principal,
        cursor: str | None,
        batch_size: int,
    ) -> RenderedPageBatch: ...

    async def book_ingest(
        self,
        *,
        command: str,
        principal: Principal,
        file: ChatFile | None,
        ingestion_id: str | None,
        cursor: str | None,
        payload: dict[str, Any] | None,
    ) -> BookIngestOutput: ...

    async def document_access(
        self,
        *,
        document_id: str,
        principal: Principal,
        visibility: Visibility | None,
        grantee_user_id: str | None,
    ) -> DocumentAccessOutput: ...


class UnavailableBackend:
    """Fail closed until Supabase/object-store adapters are configured."""

    async def search(self, query: str, principal: Principal) -> SearchOutput:
        raise RuntimeError("knowledge backend is not configured")

    async def fetch(self, item_id: str, principal: Principal) -> FetchOutput:
        raise RuntimeError("knowledge backend is not configured")

    async def search_evidence(self, *args: Any, **kwargs: Any) -> EvidenceSearchOutput:
        raise RuntimeError("knowledge backend is not configured")

    async def book_pages(self, **_: Any) -> RenderedPageBatch:
        raise RuntimeError("ingestion backend is not configured")

    async def book_ingest(self, **_: Any) -> BookIngestOutput:
        raise RuntimeError("ingestion backend is not configured")

    async def document_access(self, **_: Any) -> DocumentAccessOutput:
        raise RuntimeError("access backend is not configured")