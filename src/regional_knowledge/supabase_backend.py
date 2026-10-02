from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote
from uuid import UUID, uuid4, uuid5

import httpx

from .backend import KnowledgeBackend, RenderedPage, RenderedPageBatch, UnavailableBackend
from .contracts import (
    BookIngestOutput,
    ChatFile,
    DocumentAccessOutput,
    EvidenceSearchOutput,
    FetchOutput,
    Principal,
    RightsStatus,
    SearchOutput,
    SearchResult,
    Visibility,
)
from .file_ingress import ChatFileDownloader, sha256_file
from .ingestion import PdfProcessor, PyMuPdfProcessor
from .object_store import ObjectStore, S3Config, S3ObjectStore, UnavailableObjectStore
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
    service_role_key: str | None = None


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
        object_store: ObjectStore | None = None,
        file_downloader: ChatFileDownloader | None = None,
        pdf_processor: PdfProcessor | None = None,
    ) -> None:
        self.config = config
        self.embedder = embedder or LexicalOnlyEmbedder()
        self.object_store = object_store or UnavailableObjectStore()
        self.file_downloader = file_downloader or ChatFileDownloader()
        self.pdf_processor = pdf_processor or PyMuPdfProcessor()
        self.client = client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = client is None

    def _headers(self, principal: Principal) -> dict[str, str]:
        return {
            "apikey": self.config.anon_key,
            "Authorization": f"Bearer {principal.access_token}",
            "Content-Type": "application/json",
        }

    def _service_headers(self) -> dict[str, str]:
        if not self.config.service_role_key:
            raise RuntimeError(
                "Supabase service role is required for server-only object locator lookup"
            )
        return {
            "apikey": self.config.service_role_key,
            "Authorization": f"Bearer {self.config.service_role_key}",
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
        # Resolve the chunk under the caller's JWT first. This RLS query is the
        # authorization boundary; service credentials never select user-visible rows.
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_chunks",
            headers=self._headers(principal),
            params={
                "id": f"eq.{item_id}",
                "select": (
                    "id,document_id,title,metadata,page_ids,illustration_ids,"
                    "footnote_region_ids,text_object_id,text_start,text_end,text_sha256"
                ),
                "limit": "1",
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError("evidence_not_found")
        row = rows[0]

        # Only after user-RLS authorization may the server resolve the exact
        # private object locator. Bind object + document IDs to prevent an
        # arbitrary service-role object lookup.
        object_response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_objects",
            headers=self._service_headers(),
            params={
                "id": f"eq.{row['text_object_id']}",
                "document_id": f"eq.{row['document_id']}",
                "select": "id,object_key,sha256,mime_type",
                "limit": "1",
            },
        )
        object_response.raise_for_status()
        objects = object_response.json()
        if not objects:
            raise RuntimeError("authorized text object locator is missing")

        raw = await self.object_store.get_range(
            str(objects[0]["object_key"]),
            int(row["text_start"]),
            int(row["text_end"]),
        )
        if hashlib.sha256(raw).hexdigest() != row["text_sha256"]:
            raise RuntimeError("chunk text integrity check failed")
        text = raw.decode("utf-8")

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
            text=text,
            url=self._evidence_url(str(row["id"])),
            metadata=metadata,
        )

    async def search_evidence(
        self,
        query: str,
        principal: Principal,
        *,
        max_evidence: int = 3,
    ) -> EvidenceSearchOutput:
        limit = max(1, min(int(max_evidence), 5))
        found = await self.search(query, principal)
        selected = found.results[:limit]
        if not selected:
            return EvidenceSearchOutput(evidence=[], mode=found.mode)
        evidence = await asyncio.gather(
            *(self.fetch(item.id, principal) for item in selected)
        )
        return EvidenceSearchOutput(evidence=list(evidence), mode=found.mode)

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
                "select": (
                    "id,owner_user_id,content_visibility,rights_status,"
                    "rights_evidence,rights_policy_version"
                ),
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
            assert_visibility_allowed(
                visibility,
                rights_status,
                evidence=dict(row.get("rights_evidence") or {}),
                policy_version=row.get("rights_policy_version"),
            )
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

    async def _ingestion_row(
        self,
        *,
        principal: Principal,
        ingestion_id: str | None = None,
        source_file_id: str | None = None,
    ) -> dict[str, Any] | None:
        params: dict[str, str] = {
            "select": (
                "id,document_id,source_file_id,source_object_id,source_sha256,"
                "staged_graph_object_id,state,cursor,staged_revision,warnings,"
                "error_code,created_at"
            ),
            "order": "created_at.desc",
            "limit": "1",
        }
        if ingestion_id is not None:
            params["id"] = f"eq.{ingestion_id}"
        elif source_file_id is not None:
            params["source_file_id"] = f"eq.{source_file_id}"
        else:
            raise ValueError("ingestion lookup needs an id or file id")
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_ingestion_jobs",
            headers=self._headers(principal),
            params=params,
        )
        response.raise_for_status()
        rows = response.json()
        return dict(rows[0]) if rows else None

    @staticmethod
    def _ingestion_output(row: dict[str, Any], message: str) -> BookIngestOutput:
        return BookIngestOutput(
            ingestion_id=str(row["id"]),
            document_id=str(row["document_id"]) if row.get("document_id") else None,
            state=str(row["state"]),
            message=message,
            next_cursor=row.get("cursor"),
            warnings=[str(value) for value in (row.get("warnings") or [])],
        )

    async def _patch_ingestion(
        self,
        ingestion_id: str,
        principal: Principal,
        values: dict[str, Any],
        *,
        representation: bool = False,
    ) -> list[dict[str, Any]]:
        headers = self._headers(principal)
        if representation:
            headers = {**headers, "Prefer": "return=representation"}
        response = await self.client.patch(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_ingestion_jobs",
            headers=headers,
            params={"id": f"eq.{ingestion_id}"},
            json=values,
        )
        response.raise_for_status()
        if not representation or not response.content:
            return []
        return [dict(row) for row in response.json()]

    async def _mark_ingestion_failed(
        self,
        ingestion_id: str,
        principal: Principal,
        error_code: str,
    ) -> None:
        try:
            await self._patch_ingestion(
                ingestion_id,
                principal,
                {"state": "failed", "error_code": error_code[:120]},
            )
        except httpx.HTTPError:
            return

    async def _server_object(
        self,
        *,
        document_id: str,
        object_key: str | None = None,
        object_id: str | None = None,
        kind: str | None = None,
    ) -> dict[str, Any] | None:
        params = {
            "document_id": f"eq.{document_id}",
            "select": "id,object_key,sha256,mime_type",
            "limit": "1",
        }
        if object_key is not None:
            params["object_key"] = f"eq.{object_key}"
        if object_id is not None:
            params["id"] = f"eq.{object_id}"
        if kind is not None:
            params["kind"] = f"eq.{kind}"
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_objects",
            headers=self._service_headers(),
            params=params,
        )
        response.raise_for_status()
        rows = response.json()
        return dict(rows[0]) if rows else None

    @staticmethod
    def _start_metadata(
        file: ChatFile,
        payload: dict[str, Any] | None,
        pdf_title: str | None,
    ) -> tuple[str, list[str], int | None, str | None]:
        metadata = payload or {}
        raw_title = metadata.get("title") or pdf_title or file.file_name or "Untitled source"
        title = str(raw_title).strip()[:500] or "Untitled source"
        if title.lower().endswith(".pdf"):
            title = Path(title).stem or title
        raw_authors = metadata.get("authors", [])
        authors = (
            [str(value).strip()[:300] for value in raw_authors if str(value).strip()]
            if isinstance(raw_authors, list)
            else []
        )[:50]
        year = metadata.get("publication_year")
        publication_year = year if isinstance(year, int) and 1 <= year <= 3000 else None
        raw_language = metadata.get("language")
        language = str(raw_language).strip()[:80] if raw_language else None
        return title, authors, publication_year, language

    async def book_ingest(
        self,
        *,
        command: str,
        principal: Principal,
        file: ChatFile | None,
        ingestion_id: str | None,
        cursor: str | None,
        payload: dict[str, Any] | None,
    ) -> BookIngestOutput:
        if command == "status":
            if not ingestion_id:
                raise ValueError("ingestion_id is required for status")
            row = await self._ingestion_row(
                principal=principal,
                ingestion_id=ingestion_id,
            )
            if not row:
                raise LookupError("ingestion_not_found")
            return self._ingestion_output(row, "Ingestion status")

        if command in {"stage", "validate", "finalize"}:
            if not ingestion_id:
                raise ValueError(f"ingestion_id is required for {command}")
            from .stage_service import (
                finalize_ingestion,
                stage_ingestion,
                validate_ingestion,
            )

            if command == "stage":
                return await stage_ingestion(
                    self,
                    principal=principal,
                    ingestion_id=ingestion_id,
                    cursor=cursor,
                    payload=payload,
                )
            if command == "validate":
                return await validate_ingestion(
                    self,
                    principal=principal,
                    ingestion_id=ingestion_id,
                )
            return await finalize_ingestion(
                self,
                principal=principal,
                ingestion_id=ingestion_id,
            )

        if command != "start":
            raise ValueError("unsupported ingestion command")
        if file is None:
            raise ValueError("attached PDF file is required for start")
        if file.mime_type and file.mime_type.lower() not in {
            "application/pdf",
            "application/x-pdf",
            "application/octet-stream",
        }:
            raise ValueError("attached file must be a PDF")

        previous = await self._ingestion_row(
            principal=principal,
            source_file_id=file.file_id,
        )
        if previous and previous.get("state") != "failed":
            return self._ingestion_output(
                previous,
                "Existing ingestion for this attached file",
            )

        work_root = os.getenv("RKB_WORK_DIR") or None
        with tempfile.TemporaryDirectory(prefix="rkb-ingest-", dir=work_root) as temp_dir:
            downloaded = await self.file_downloader.download(
                str(file.download_url),
                Path(temp_dir),
            )
            source_sha256 = downloaded.sha256
            pdf_info = await self.pdf_processor.inspect_file(downloaded.path)
            title, authors, publication_year, language = self._start_metadata(
                file, payload, pdf_info.title
            )

            if (
                previous
                and previous.get("state") == "failed"
                and previous.get("document_id")
                and previous.get("source_sha256") == source_sha256
            ):
                document_id = str(previous["document_id"])
                new_ingestion_id = str(previous["id"])
                await self._patch_ingestion(
                    new_ingestion_id,
                    principal,
                    {"state": "processing", "error_code": None},
                )
            else:
                document_id = str(uuid4())
                new_ingestion_id = str(uuid4())
                start = await self.client.post(
                    f"{self.config.url.rstrip('/')}/rest/v1/rpc/rkb_start_ingestion",
                    headers=self._headers(principal),
                    json={
                        "p_ingestion_id": new_ingestion_id,
                        "p_document_id": document_id,
                        "p_title": title,
                        "p_authors": authors,
                        "p_publication_year": publication_year,
                        "p_language": language,
                        "p_source_sha256": source_sha256,
                        "p_source_file_id": file.file_id,
                        "p_page_count": pdf_info.page_count,
                    },
                )
                start.raise_for_status()

            object_key = (
                f"users/{principal.subject}/documents/{document_id}/"
                f"source/{source_sha256}.pdf"
            )
            try:
                await self.object_store.put_file(
                    object_key,
                    str(downloaded.path),
                    "application/pdf",
                )
                existing_object = await self._server_object(
                    document_id=document_id,
                    object_key=object_key,
                    kind="source_pdf",
                )
                if existing_object:
                    if existing_object.get("sha256") != source_sha256:
                        raise RuntimeError("existing source object hash mismatch")
                    object_id = str(existing_object["id"])
                else:
                    object_id = str(uuid4())
                    object_write = await self.client.post(
                        f"{self.config.url.rstrip('/')}/rest/v1/rkb_objects",
                        headers={**self._service_headers(), "Prefer": "return=minimal"},
                        json={
                            "id": object_id,
                            "document_id": document_id,
                            "kind": "source_pdf",
                            "object_key": object_key,
                            "sha256": source_sha256,
                            "mime_type": "application/pdf",
                            "size_bytes": downloaded.size_bytes,
                            "access_class": "private",
                        },
                    )
                    object_write.raise_for_status()

                rows = await self._patch_ingestion(
                    new_ingestion_id,
                    principal,
                    {
                        "source_object_id": object_id,
                        "state": "staged",
                        "cursor": "0",
                        "error_code": None,
                    },
                    representation=True,
                )
                row = rows[0] if rows else {
                    "id": new_ingestion_id,
                    "document_id": document_id,
                    "state": "staged",
                    "cursor": "0",
                    "warnings": [],
                }
                return self._ingestion_output(
                    row,
                    "Source PDF stored; use book_pages to parse small page batches",
                )
            except Exception as exc:
                await self._mark_ingestion_failed(
                    new_ingestion_id,
                    principal,
                    type(exc).__name__,
                )
                raise

    async def book_pages(
        self,
        *,
        ingestion_id: str,
        principal: Principal,
        cursor: str | None,
        batch_size: int,
    ) -> RenderedPageBatch:
        row = await self._ingestion_row(
            principal=principal,
            ingestion_id=ingestion_id,
        )
        if not row:
            raise LookupError("ingestion_not_found")
        if row["state"] == "failed":
            raise RuntimeError("ingestion_failed")
        if not row.get("document_id") or not row.get("source_object_id"):
            raise RuntimeError("ingestion_source_not_ready")

        source_object = await self._server_object(
            document_id=str(row["document_id"]),
            object_id=str(row["source_object_id"]),
            kind="source_pdf",
        )
        if not source_object:
            raise RuntimeError("ingestion source object is missing")

        try:
            start = int(cursor if cursor is not None else "0")
        except (TypeError, ValueError) as exc:
            raise ValueError("cursor must be a zero-based page index") from exc
        if start < 0:
            raise ValueError("cursor must be a zero-based page index")

        work_root = os.getenv("RKB_WORK_DIR") or None
        with tempfile.TemporaryDirectory(prefix="rkb-pages-", dir=work_root) as temp_dir:
            source_path = Path(temp_dir) / "source.pdf"
            await self.object_store.download_file(
                str(source_object["object_key"]),
                str(source_path),
            )
            actual_sha256, _ = await asyncio.to_thread(sha256_file, source_path)
            if actual_sha256 != row["source_sha256"]:
                raise RuntimeError("source PDF integrity check failed")

            total, rendered = await self.pdf_processor.render_file(
                source_path,
                start=start,
                count=max(1, min(int(batch_size), 8)),
            )

        revision = int(row.get("staged_revision") or 1)
        namespace = UUID(str(row["document_id"]))
        pages = tuple(
            RenderedPage(
                page_id=str(
                    uuid5(namespace, f"page:{revision}:{page.physical_page_index}")
                ),
                physical_page_index=page.physical_page_index,
                printed_page_number=None,
                mime_type=page.mime_type,
                data=page.data,
                native_text=page.native_text,
                native_blocks=page.native_blocks,
            )
            for page in rendered
        )
        next_index = start + len(pages)
        return RenderedPageBatch(
            pages=pages,
            next_cursor=str(next_index) if next_index < total else None,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()


def backend_from_env() -> KnowledgeBackend:
    url = os.getenv("SUPABASE_URL", "").strip()
    anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    public_base = os.getenv("RKB_PUBLIC_BASE_URL", "").strip()
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip() or None
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

    object_store: ObjectStore = UnavailableObjectStore()
    s3 = S3Config(
        endpoint_url=os.getenv("RKB_S3_ENDPOINT", "").strip(),
        region_name=os.getenv("RKB_S3_REGION", "").strip(),
        bucket=os.getenv("RKB_S3_BUCKET", "").strip(),
        access_key_id=os.getenv("RKB_S3_ACCESS_KEY_ID", "").strip(),
        secret_access_key=os.getenv("RKB_S3_SECRET_ACCESS_KEY", "").strip(),
    )
    if all(
        (
            s3.endpoint_url,
            s3.region_name,
            s3.bucket,
            s3.access_key_id,
            s3.secret_access_key,
        )
    ):
        object_store = S3ObjectStore(s3)

    return SupabaseRestBackend(
        SupabaseConfig(
            url=url,
            anon_key=anon_key,
            public_base_url=public_base,
            service_role_key=service_role_key,
        ),
        embedder=embedder,
        object_store=object_store,
    )
