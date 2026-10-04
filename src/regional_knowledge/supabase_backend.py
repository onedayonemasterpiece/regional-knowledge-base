from __future__ import annotations

import asyncio
import hashlib
import os
import time
import logging
import json
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
from .local_e5 import LocalE5Embedder, encoding_timings
from .e5_contract import SPACE, validate_vector
logger = logging.getLogger(__name__)


class Embedder(Protocol):
    embedding_space: str | None

    async def embed(self, text: str) -> list[float] | None: ...


class LexicalOnlyEmbedder:
    embedding_space: str | None = None

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
        embedding_space: str,
        dimensions: int = 768,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.embedding_space = embedding_space
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
    public_base_url: str | None = None
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
        from .source_adapter import SourceProcessor
        self.pdf_processor = pdf_processor or SourceProcessor()
        self.client = client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = client is None
        self._finalize_lock = asyncio.Lock()
        self._finalize_tasks: dict[str, asyncio.Task[None]] = {}

    def _headers(self, principal: Principal) -> dict[str, str]:
        return {
            "apikey": self.config.anon_key,
            "Authorization": f"Bearer {principal.access_token}",
            "Content-Type": "application/json",
        }

    def _service_headers(self) -> dict[str, str]:
        if not self.config.service_role_key:
            raise RuntimeError(
                "Supabase secret/service key is required for server-only object locator lookup"
            )
        key = self.config.service_role_key
        headers = {
            "apikey": key,
            "Content-Type": "application/json",
        }
        # New sb_secret_* API keys are not JWTs and must not be sent as
        # Authorization: Bearer. Legacy service_role JWTs still need the
        # Authorization header for backward compatibility.
        if not key.startswith("sb_secret_"):
            headers["Authorization"] = f"Bearer {key}"
        return headers

    def _evidence_url(self, item_id: str) -> str:
        encoded = quote(item_id, safe="")
        if self.config.public_base_url:
            return f"{self.config.public_base_url.rstrip('/')}/evidence/{encoded}"
        return f"knowledge://evidence/{encoded}"

    async def search(self, query: str, principal: Principal, *, match_count: int = 8, main_job_id: str | None = None, aliases: list | None = None, _fast_only: bool = False) -> SearchOutput:
        started = time.monotonic()
        query = query.strip()
        if not query:
            return SearchOutput(results=[], mode="lexical_degraded")
        if not _fast_only and os.getenv('RKB_BGE_ENABLED')=='1':
            from .multilingual_retrieval import main_search
            from .index_readiness import enabled,counts,status
            if enabled():
                coverage=await counts(self,principal)
                if coverage['bge_ready']<coverage['active_chunks']:
                    fast=await self.search(query,principal,match_count=match_count,_fast_only=True)
                    return fast.model_copy(update={'main_state':'pending'})
            result=await main_search(self,query,principal,match_count=match_count,main_job_id=main_job_id,aliases=aliases)
            if enabled():
                readiness=await status(self,principal)
                if readiness.bge_missing and result.main_state=='ready':
                    fast=await self.search(query,principal,match_count=match_count,_fast_only=True)
                    return fast.model_copy(update={'main_state':'pending'})
                result=result.model_copy(update={'indexing':readiness})
            return result

        vector: list[float] | None = None
        from .index_readiness import enabled,counts,status
        coverage=await counts(self,principal) if enabled() else None
        if coverage is None or coverage['e5_ready']==coverage['active_chunks']:
            try:
                vector = await self.embedder.embed(query)
            except (httpx.HTTPError, TimeoutError, RuntimeError, ValueError):
                # Search availability is more important than vector-only perfection:
                # the SQL RPC accepts NULL and performs RLS-protected lexical retrieval.
                vector = None

        is_e5 = isinstance(self.embedder, LocalE5Embedder)
        if is_e5 and vector is not None:
            vector = validate_vector(vector)
        database_start = time.monotonic()
        rpc = "rkb_fast_e5_search" if is_e5 else "rkb_hybrid_search"
        response = await self.client.post(
            f"{self.config.url.rstrip('/')}/rest/v1/rpc/{rpc}",
            headers=self._headers(principal),
            json={
                "query_text": query,
                "query_embedding": ("[" + ",".join(format(v,".9g") for v in vector) + "]") if is_e5 and vector is not None else _halfvec_literal(vector),
                "query_embedding_space": (
                    self.embedder.embedding_space if vector is not None else None
                ),
                "match_count": max(1,min(match_count,20)),
            },
        )
        response.raise_for_status()
        rows = response.json()
        timings = {**(encoding_timings.get() if is_e5 else {}), "database_seconds": time.monotonic()-database_start, "search_seconds": time.monotonic()-started}
        retrieval_mode = rows[0].get("retrieval_mode", "fast_e5" if is_e5 and vector is not None else "lexical_only") if rows else ("fast_e5" if is_e5 and vector is not None else "lexical_only")
        readiness=await status(self,principal) if enabled() else None
        if readiness and readiness.e5_missing:
            retrieval_mode='lexical_only';vector=None
        logger.info(json.dumps({"event":"retrieval_served","retrieval_mode":retrieval_mode,"results":len(rows),"embedding_space":self.embedder.embedding_space if vector is not None else None,**timings}))
        return SearchOutput(
            retrieval_mode=retrieval_mode, timings=timings,indexing=readiness,
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
                    "footnote_region_ids,text_object_id,text_start,text_end,text_sha256,source_text"
                ),
                "limit": "1",
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError("evidence_not_found")
        row = rows[0]

        if row.get('source_text') is not None:
            raw=row['source_text'].encode('utf-8')
        else:
            # Only after user-RLS authorization may the server resolve the exact
            # private object locator. Bind object + document IDs to prevent an
            # arbitrary service-role object lookup.
            object_response = await self.client.get(
                f"{self.config.url.rstrip('/')}/rest/v1/rkb_objects",
                headers=self._service_headers(),
                params={
                    "id": f"eq.{row['text_object_id']}",
                    "document_id": f"eq.{row['document_id']}",
                    "select": "id,object_key,sha256,mime_type,deleted_at",
                    "limit": "1",
                },
            )
            object_response.raise_for_status()
            objects = object_response.json()
            if not objects:
                raise RuntimeError("authorized text object locator is missing")

            raw = b'' if row['text_start'] == row['text_end'] else await self.object_store.get_range(
                str(objects[0]["object_key"]),
                int(row["text_start"]),
                int(row["text_end"]),
            )
        if hashlib.sha256(raw).hexdigest() != row["text_sha256"]:
            raise RuntimeError("chunk text integrity check failed")
        text = raw.decode("utf-8")

        metadata = dict(row.get("metadata") or {})
        from .illustrations import descriptor
        figures = [await descriptor(self, principal, str(value), row['document_id']) for value in row.get('illustration_ids') or []]
        metadata.update(
            {
                "document_id": str(row["document_id"]),
                "pages": [str(value) for value in row.get("page_ids") or []],
                "illustrations": figures,
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
        main_job_id: str | None = None,
    ) -> EvidenceSearchOutput:
        started = time.monotonic()
        limit = max(1, min(int(max_evidence), 5))
        found = await self.search(query, principal, **({'main_job_id': main_job_id} if main_job_id else {}))
        selected = found.results[:limit]
        if not selected:
            return EvidenceSearchOutput(evidence=[], mode=found.mode, retrieval_mode=found.retrieval_mode, main_state=found.main_state, main_job_id=found.main_job_id, indexing=found.indexing, timings={**found.timings,"hydration_seconds":0,"total_seconds":time.monotonic()-started})
        hydration_start = time.monotonic()
        evidence = await asyncio.gather(
            *(self.fetch(item.id, principal) for item in selected)
        )
        return EvidenceSearchOutput(evidence=list(evidence), mode=found.mode, retrieval_mode=found.retrieval_mode, main_state=found.main_state, main_job_id=found.main_job_id, indexing=found.indexing, timings={**found.timings,"hydration_seconds":time.monotonic()-hydration_start,"total_seconds":time.monotonic()-started})

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

    async def _finalize_in_background(
        self,
        principal: Principal,
        ingestion_id: str,
    ) -> None:
        from .stage_service import finalize_ingestion

        try:
            await finalize_ingestion(
                self,
                principal=principal,
                ingestion_id=ingestion_id,
            )
            logger.info(json.dumps({'event':'ingestion_finalize_complete','ingestion_id':ingestion_id}))
        except asyncio.CancelledError:
            # Keep state=processing/cursor=finalize so a later finalize call can
            # resume after a normal service restart.
            raise
        except Exception as exc:
            logger.error(json.dumps({'event':'ingestion_finalize_failed','ingestion_id':ingestion_id,'error_type':type(exc).__name__,'sqlstate':getattr(exc,'sqlstate',None)}))
            # Finalize is retryable: it resets the inactive revision before
            # materializing rows, while object writes are content-addressed.
            # Return the ingestion to ready instead of leaving a dead
            # processing job after a dependency failure.
            try:
                row = await self._ingestion_row(
                    principal=principal,
                    ingestion_id=ingestion_id,
                )
                warnings = [
                    str(value) for value in ((row or {}).get("warnings") or [])
                ]
                warning = f"finalize_failed:{type(exc).__name__}"
                if warning not in warnings:
                    warnings.append(warning)
                await self._patch_ingestion(
                    ingestion_id,
                    principal,
                    {
                        "state": "ready",
                        "cursor": "finalize",
                        "warnings": warnings,
                        "error_code": type(exc).__name__[:120],
                    },
                )
            except Exception:
                # If the data plane is unavailable as well, persisted
                # state=processing/cursor=finalize is still resumable later.
                return

    def _schedule_finalize(
        self,
        principal: Principal,
        ingestion_id: str,
    ) -> None:
        current = self._finalize_tasks.get(ingestion_id)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._finalize_in_background(principal, ingestion_id),
            name=f"rkb-finalize-{ingestion_id}",
        )
        self._finalize_tasks[ingestion_id] = task

        def forget(done: asyncio.Task[None]) -> None:
            if self._finalize_tasks.get(ingestion_id) is done:
                self._finalize_tasks.pop(ingestion_id, None)

        task.add_done_callback(forget)

    async def _start_or_resume_finalize(
        self,
        principal: Principal,
        ingestion_id: str,
    ) -> BookIngestOutput:
        async with self._finalize_lock:
            row = await self._ingestion_row(
                principal=principal,
                ingestion_id=ingestion_id,
            )
            if not row:
                raise LookupError("ingestion_not_found")
            if row["state"] == "finalized":
                return self._ingestion_output(row, "Ingestion already finalized")
            if row["state"] == "ready":
                rows = await self._patch_ingestion(
                    ingestion_id,
                    principal,
                    {
                        "state": "processing",
                        "cursor": "finalize",
                        "error_code": None,
                    },
                    representation=True,
                )
                row = (
                    rows[0]
                    if rows
                    else {**row, "state": "processing", "cursor": "finalize"}
                )
            elif not (
                row["state"] == "processing" and row.get("cursor") == "finalize"
            ):
                raise RuntimeError("ingestion must validate as ready before finalize")

            self._schedule_finalize(principal, ingestion_id)
            return self._ingestion_output(
                row,
                (
                    "Finalization accepted and is running server-side. "
                    "Do not keep this ChatGPT turn open polling; return control "
                    "to the user and check status in a later turn."
                ),
            )

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
            "select": "id,object_key,sha256,mime_type,deleted_at",
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

    async def _ingestion_with_indexing(self,output,principal):
        from .index_readiness import enabled,status
        if output.document_id and hasattr(self,'data_client'):
            async with self.data_client._connection(self._headers(principal)) as db:
                source=await(await db.execute('select source_archive_status from rkb_documents where id=%s', (UUID(output.document_id),))).fetchone()
                if source:output=output.model_copy(update={'source_archive_status':source['source_archive_status']})
        if not enabled() or output.state!='finalized' or not output.document_id:return output
        readiness=await status(self,principal,output.document_id)
        warnings=[w for w in output.warnings if w not in ('fast_e5_backfill_required','automatic_indexing_pending')]
        if readiness.e5_missing or readiness.bge_missing:warnings.append('automatic_indexing_pending')
        return output.model_copy(update={'indexing':readiness,'warnings':warnings})

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
            message = "Ingestion status"
            if row["state"] == "processing" and row.get("cursor") == "finalize":
                task = self._finalize_tasks.get(ingestion_id)
                if task is not None and not task.done():
                    message = "Finalization is running server-side; check status later."
                else:
                    message = (
                        "Finalization is paused after an interruption or service "
                        "restart; call finalize once to resume."
                    )
            return await self._ingestion_with_indexing(self._ingestion_output(row, message),principal)

        if command in {"stage", "validate", "finalize"}:
            if not ingestion_id:
                raise ValueError(f"ingestion_id is required for {command}")
            if command == "finalize":
                result=await self._start_or_resume_finalize(
                    principal,
                    ingestion_id,
                )
                return await self._ingestion_with_indexing(result,principal)
            from .stage_service import stage_ingestion, validate_ingestion

            if command == "stage":
                return await stage_ingestion(
                    self,
                    principal=principal,
                    ingestion_id=ingestion_id,
                    cursor=cursor,
                    payload=payload,
                )
            return await validate_ingestion(
                self,
                principal=principal,
                ingestion_id=ingestion_id,
            )

        if command != "start":
            raise ValueError("unsupported ingestion command")
        if file is None:
            raise ValueError("attached source file is required for start")
        if file.mime_type and file.mime_type.lower() not in {
            "application/pdf",
            "application/x-pdf",
            "image/vnd.djvu", "image/x-djvu", "application/x-djvu",
            "application/octet-stream",
        }:
            raise ValueError("attached file must be PDF or DjVu")

        previous = await self._ingestion_row(
            principal=principal,
            source_file_id=file.file_id,
        )
        policy = (payload or {}).get('duplicate_policy', 'reuse')
        if policy not in ('reuse', 'new_revision'):
            raise ValueError('invalid duplicate_policy')
        if previous and previous.get("state") != "failed" and policy == 'reuse':
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
            from .source_adapter import source_format
            format_name=source_format(downloaded.path)
            source_mime="application/pdf" if format_name=="pdf" else "image/vnd.djvu"
            source_sha256 = downloaded.sha256
            pdf_info = await self.pdf_processor.inspect_file(downloaded.path)
            title, authors, publication_year, language = self._start_metadata(
                file, payload, pdf_info.title
            )

            if (
                previous
                and policy == 'reuse'
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
                        "p_duplicate_policy": policy,
                    },
                )
                start.raise_for_status()
                identities = start.json()
                if identities:
                    new_ingestion_id = str(identities[0]['ingestion_id'])
                    document_id = str(identities[0]['document_id'])
                    existing = await self._ingestion_row(principal=principal, ingestion_id=new_ingestion_id)
                    if existing and existing.get('source_object_id') and existing['state'] != 'failed':
                        return await self._ingestion_with_indexing(self._ingestion_output(existing, 'Existing exact-source document/revision reused'), principal)

            filename=Path(file.file_name or ('source.'+format_name)).name[:160]
            description=await self.client.patch(self.config.url.rstrip('/')+'/rest/v1/rkb_documents',
                headers={**self._service_headers(), 'Prefer':'return=minimal'},
                params={'id':'eq.'+document_id},json={'source_format':format_name,'source_filename':filename})
            description.raise_for_status()

            object_key = (
                f"users/{principal.subject}/documents/{document_id}/"
                f"source/{source_sha256}.{format_name}"
            )
            try:
                from .storage_gc import reserve_source
                await reserve_source(self,principal,document_id,object_key,downloaded,source_mime)
                await self.object_store.put_file(
                    object_key,
                    str(downloaded.path),
                    source_mime,
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
                    object_id = str(uuid5(UUID(document_id), 'source:' + source_sha256))
                    object_write = await self.client.post(
                        f"{self.config.url.rstrip('/')}/rest/v1/rkb_objects",
                        headers={**self._service_headers(), "Prefer": "resolution=ignore-duplicates,return=minimal"},
                        json={
                            "id": object_id,
                            "document_id": document_id,
                            "kind": "source_pdf",
                            "object_key": object_key,
                            "sha256": source_sha256,
                            "mime_type": source_mime,
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

        text_part = None
        try:
            if cursor and cursor.startswith("text:"):
                _, page_index, block_index, offset = cursor.split(":")
                start = int(page_index)
                text_part = (int(block_index), int(offset))
                batch_size = 1
            else:
                start = int(cursor if cursor is not None else "0")
        except (TypeError, ValueError) as exc:
            raise ValueError("cursor must be a zero-based page index") from exc
        if start < 0:
            raise ValueError("cursor must be a zero-based page index")

        work_root = os.getenv("RKB_WORK_DIR") or None
        with tempfile.TemporaryDirectory(prefix="rkb-pages-", dir=work_root) as temp_dir:
            source_path = Path(temp_dir) / "source.pdf"
            from .source_archive import download_source
            await download_source(self,principal,str(row['document_id']),source_object,source_path)
            actual_sha256, _ = await asyncio.to_thread(sha256_file, source_path)
            if actual_sha256 != row["source_sha256"]:
                raise RuntimeError("source PDF integrity check failed")

            total, rendered = await self.pdf_processor.render_file(
                source_path,
                start=start,
                count=max(1, min(int(batch_size), 8)),
                **({"text_part": text_part} if text_part is not None else {}),
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
                native_text_info=page.native_text_info,
            )
            for page in rendered
        )
        next_index = start + len(pages)
        return RenderedPageBatch(
            pages=pages,
            next_cursor=str(next_index) if next_index < total else None,
        )

    async def aclose(self) -> None:
        tasks = [task for task in self._finalize_tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._finalize_tasks.clear()
        if isinstance(self.embedder, LocalE5Embedder):
            await self.embedder.aclose()
        if self._owns_client:
            await self.client.aclose()


def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _embedder_from_env() -> Embedder:
    fast = os.getenv("RKB_FAST_E5_ENABLED", "0")
    if os.getenv('RKB_AUTO_INDEX_ENABLED')=='1' and fast!='1':raise RuntimeError('automatic indexing requires local E5; external provider fallback forbidden')
    if fast == "1":
        return LocalE5Embedder(os.getenv("RKB_FAST_E5_ENDPOINT", "http://127.0.0.1:8767"))
    if fast not in {"0", ""}:
        raise RuntimeError("RKB_FAST_E5_ENABLED must be0/1")
    endpoint = os.getenv("RKB_EMBEDDING_ENDPOINT", "").strip()
    api_key = os.getenv("RKB_EMBEDDING_API_KEY", "").strip()
    model = os.getenv("RKB_EMBEDDING_MODEL", "").strip()
    embedding_space = os.getenv("RKB_EMBEDDING_SPACE", "").strip()
    if endpoint and api_key and model and embedding_space:
        return OpenAICompatibleEmbedder(
            endpoint=endpoint,
            api_key=api_key,
            model=model,
            embedding_space=embedding_space,
        )

    # Embeddings are opt-in for this service. Never inherit a shared provider
    # credential such as OPENAI_API_KEY: doing so can silently turn search or
    # ingestion into a billable external operation. Operators must configure
    # the dedicated RKB_EMBEDDING_* quartet explicitly; otherwise the service
    # degrades safely to lexical retrieval. The explicit space identifier also
    # prevents comparing vectors produced by different models.
    return LexicalOnlyEmbedder()


def _object_store_from_env() -> ObjectStore:
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
    return object_store


def backend_from_env() -> KnowledgeBackend:
    public_base = os.getenv("RKB_PUBLIC_BASE_URL", "").strip() or None
    embedder = _embedder_from_env()
    object_store = _object_store_from_env()

    # Production path: Session Pooler + transaction-local application actor.
    # This path never forwards an MCP bearer to Supabase/PostgREST.
    session_dsn = _first_env("KB_SUPABASE_SESSION_CONNECTION")
    if session_dsn:
        from .postgres_backend import PostgresBackend

        try:
            pool_max = max(1, min(int(os.getenv("RKB_DB_POOL_MAX", "6")), 32))
        except ValueError as exc:
            raise RuntimeError("RKB_DB_POOL_MAX must be an integer") from exc
        return PostgresBackend(
            session_dsn,
            embedder=embedder,
            object_store=object_store,
            pool_min_size=1,
            pool_max_size=pool_max,
            public_base_url=public_base,
        )

    # Transitional test-only PostgREST adapter. It forwards the caller bearer
    # token and is therefore never selected while the production Session Pooler
    # DSN is configured.
    if os.getenv("RKB_ALLOW_LEGACY_SUPABASE_USER_JWT") != "1":
        return UnavailableBackend()

    url = _first_env("KB_SUPABASE_URL", "SUPABASE_URL")
    anon_key = _first_env(
        "KB_SUPABASE_PUBLISHABLE_KEY",
        "KB_SUPABASE_ANON_KEY",
        "SUPABASE_PUBLISHABLE_KEY",
        "SUPABASE_ANON_KEY",
    )
    service_role_key = _first_env(
        "KB_SUPABASE_SECRET_KEY",
        "KB_SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_SECRET_KEY",
        "SUPABASE_SERVICE_ROLE_KEY",
    ) or None
    if not (url and anon_key):
        return UnavailableBackend()

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
