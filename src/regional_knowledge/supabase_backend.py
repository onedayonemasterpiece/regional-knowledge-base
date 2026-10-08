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
    BookFindOutput,
    BookFindResult,
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

    async def book_find(
        self,
        query: str,
        principal: Principal,
        *,
        limit: int = 8,
    ) -> BookFindOutput:
        query = query.strip()
        limit = max(1, min(int(limit), 8))
        if not query:
            return BookFindOutput(query=query, results=[])

        if hasattr(self, "data_client"):
            async with self.data_client._connection(self._headers(principal)) as db:
                rows = await (
                    await db.execute(
                        """
                        select id,title,authors,publication_year,active_revision,
                               source_format,source_archive_status
                          from rkb_documents
                         where strpos(lower(title), lower(%s)) > 0
                            or strpos(lower(coalesce(authors::text, '')), lower(%s)) > 0
                         order by
                           case
                             when lower(title)=lower(%s) then 0
                             when strpos(lower(title), lower(%s))=1 then 1
                             when strpos(lower(title), lower(%s))>0 then 2
                             else 3
                           end,
                           char_length(title), lower(title), id
                         limit %s
                        """,
                        (query, query, query, query, query, limit),
                    )
                ).fetchall()
        else:
            needle = query.casefold()

            def score(row: dict[str, Any]) -> tuple[int, int, str, str]:
                title = str(row.get("title") or "")
                authors = " ".join(str(value) for value in (row.get("authors") or []))
                folded_title = title.casefold()
                if folded_title == needle:
                    rank = 0
                elif folded_title.startswith(needle):
                    rank = 1
                elif needle in folded_title:
                    rank = 2
                elif needle in authors.casefold():
                    rank = 3
                else:
                    rank = 9
                return rank, len(title), folded_title, str(row.get("id") or "")

            rows = []
            offset = 0
            # Transitional REST deployments must not silently miss books beyond
            # the first catalog page. RLS applies to every page; retain only the
            # best bounded matches rather than materializing the whole catalog.
            while True:
                response = await self.client.get(
                    f"{self.config.url.rstrip('/')}/rest/v1/rkb_documents",
                    headers=self._headers(principal),
                    params={
                        "select": (
                            "id,title,authors,publication_year,active_revision,"
                            "source_format,source_archive_status"
                        ),
                        "order": "title.asc,id.asc",
                        "limit": "512",
                        "offset": str(offset),
                    },
                )
                response.raise_for_status()
                page = response.json()
                rows.extend(dict(row) for row in page if score(row)[0] < 9)
                rows.sort(key=score)
                rows = rows[:limit]
                if len(page) < 512:
                    break
                offset += len(page)

        return BookFindOutput(
            query=query,
            results=[
                BookFindResult(
                    document_id=str(row["id"]),
                    title=str(row.get("title") or ""),
                    authors=[str(value) for value in (row.get("authors") or [])],
                    publication_year=row.get("publication_year"),
                    active_revision=int(row.get("active_revision") or 0),
                    source_format=(
                        str(row["source_format"]) if row.get("source_format") else None
                    ),
                    source_archive_status=row.get("source_archive_status"),
                )
                for row in rows
            ],
        )

    async def _expand_continuation_results(
        self,
        output: SearchOutput,
        principal: Principal,
        *,
        query: str,
        match_count: int,
    ) -> SearchOutput:
        """Use at most one result slot for a high-confidence prose continuation."""
        limit=max(1,min(int(match_count),20))
        if limit < 2 or not output.results:
            return output
        corpus=getattr(self,"corpus",None)
        connection_factory=getattr(self.client,"_connection",None)
        if connection_factory is None and corpus is None:
            # Legacy PostgREST is transitional/test-only. Production direct
            # Postgres owns the RLS-safe ordered-neighbor lookup.
            return output
        # Only top two anchors can legally trigger continuation; do not hydrate
        # the remaining ranked hits merely to discard their neighbors.
        selected=[UUID(item.id) for item in output.results[:min(2,limit)]]
        statement="""with selected_docs as (
          select distinct c.document_id,c.revision
          from public.rkb_chunks c
          join public.rkb_documents d on d.id=c.document_id
          where c.id=any(%s::uuid[]) and c.revision=d.active_revision
        ), active as (
          select c.id,c.document_id,c.revision,c.title,c.source_text,
                 c.illustration_ids,
                 first_region.physical_page_index as start_page,
                 first_region.reading_order as start_order,
                 last_region.physical_page_index as end_page,
                 last_region.reading_order as end_order
          from public.rkb_chunks c
          join selected_docs s on s.document_id=c.document_id and s.revision=c.revision
          join public.rkb_documents d on d.id=c.document_id
          join lateral (
            select p.physical_page_index,r.reading_order
            from unnest(c.region_ids) rid
            join public.rkb_regions r on r.id=rid
            join public.rkb_pages p on p.id=r.page_id
            order by p.physical_page_index,r.reading_order,r.id
            limit 1
          ) first_region on true
          join lateral (
            select p.physical_page_index,r.reading_order
            from unnest(c.region_ids) rid
            join public.rkb_regions r on r.id=rid
            join public.rkb_pages p on p.id=r.page_id
            order by p.physical_page_index desc,r.reading_order desc,r.id desc
            limit 1
          ) last_region on true
          where c.revision=d.active_revision and cardinality(c.region_ids)>0
        )
        select s.*,
          prev.id prev_id,prev.title prev_title,prev.source_text prev_text,
          prev.illustration_ids prev_illustrations,
          nxt.id next_id,nxt.title next_title,nxt.source_text next_text,
          nxt.illustration_ids next_illustrations
        from active s
        left join lateral (
          select p.*
          from active p
          where p.document_id=s.document_id and p.revision=s.revision
            and (p.end_page,p.end_order) < (s.start_page,s.start_order)
          order by p.end_page desc,p.end_order desc,
                   p.start_page desc,p.start_order desc,p.id
          limit 1
        ) prev on true
        left join lateral (
          select n.*
          from active n
          where n.document_id=s.document_id and n.revision=s.revision
            and (n.start_page,n.start_order) > (s.end_page,s.end_order)
          order by n.start_page,n.start_order,n.end_page,n.end_order,n.id
          limit 1
        ) nxt on true
        where s.id=any(%s::uuid[])"""
        try:
            if corpus is not None:
                rows=await asyncio.to_thread(corpus.neighbors,principal.subject,selected)
            else:
                async with connection_factory(self._headers(principal)) as connection:
                    rows=await(await connection.execute(statement,(selected,selected))).fetchall()
        except Exception as error:
            logger.warning(json.dumps({
                "event":"continuation_neighbor_lookup_degraded",
                "error_type":type(error).__name__,
            }))
            return output

        from .continuation_context import (
            STRONG_THRESHOLD,
            continuation_signal,
            query_allows_continuation_expansion,
            strong_continuation_boundary,
        )
        context={str(row["id"]):dict(row) for row in rows}
        working=list(output.results[:limit])
        inserted=0
        # One inherited-context slot is enough to close a split phrase while
        # preserving low-ranked independent evidence. Only the top two source
        # hits are eligible; production stress showed broader expansion can
        # evict a correct rank-7 answer.
        neighbor_budget=1
        source_budget=min(2,limit)
        protected_ids={item.id for item in output.results[:source_budget]}

        def annotated(item: SearchResult, signal: dict[str, Any]) -> SearchResult:
            if any(
                value.get("branch")=="continuation_neighbor"
                and value.get("source_chunk_id")==signal["source_chunk_id"]
                and value.get("direction")==signal["direction"]
                for value in item.ranking_signals
            ):
                return item
            return item.model_copy(update={
                "ranking_signals":[*item.ranking_signals,signal]
            })

        def candidate_for(source: SearchResult):
            row=context.get(source.id)
            if row is None:
                return None
            left_visual=bool(row.get("illustration_ids"))
            choices=[]
            if row.get("next_id"):
                strength=strong_continuation_boundary(
                    row.get("source_text"),row.get("next_text"),
                    left_has_illustrations=left_visual,
                    right_has_illustrations=bool(row.get("next_illustrations")),
                )
                if (
                    strength>=STRONG_THRESHOLD
                    and query_allows_continuation_expansion(
                        query,row.get("source_text"),row.get("next_text"),
                        neighbor_side="right",
                    )
                ):
                    choices.append((
                        strength,1,"next",str(row["next_id"]),
                        str(row.get("next_title") or ""),
                    ))
            if row.get("prev_id"):
                strength=strong_continuation_boundary(
                    row.get("prev_text"),row.get("source_text"),
                    left_has_illustrations=bool(row.get("prev_illustrations")),
                    right_has_illustrations=left_visual,
                )
                if (
                    strength>=STRONG_THRESHOLD
                    and query_allows_continuation_expansion(
                        query,row.get("prev_text"),row.get("source_text"),
                        neighbor_side="left",
                    )
                ):
                    choices.append((
                        strength,0,"previous",str(row["prev_id"]),
                        str(row.get("prev_title") or ""),
                    ))
            return max(choices) if choices else None

        ranked_sources=list(output.results[:source_budget])
        candidates={
            source.id:candidate_for(source)
            for source in ranked_sources
        }

        # Protect complete eligible pairs already present in the result window
        # before inserting anything for another source. Otherwise a rank-1
        # insertion could evict rank-8 evidence needed by rank 2.
        present_ids={item.id for item in working}
        for source in ranked_sources:
            candidate=candidates.get(source.id)
            if candidate is None:
                continue
            neighbor_id=candidate[3]
            if neighbor_id in present_ids:
                protected_ids.update({source.id,neighbor_id})

        for source in ranked_sources:
            candidate=candidates.get(source.id)
            if candidate is None:
                continue
            strength,_,direction,neighbor_id,neighbor_title=candidate
            signal=continuation_signal(
                direction=direction,source_chunk_id=source.id,strength=strength
            )
            source_index=next(
                (i for i,item in enumerate(working) if item.id==source.id),None
            )
            if source_index is None:
                continue
            existing_index=next(
                (i for i,item in enumerate(working) if item.id==neighbor_id),None
            )
            if existing_index is not None:
                # Evidence is already in the requested result window. Preserve
                # its original rank and attach only an inspectable diagnostic.
                working[existing_index]=annotated(working[existing_index],signal)
                continue
            if inserted>=neighbor_budget:
                continue
            neighbor=SearchResult(
                id=neighbor_id,
                title=neighbor_title,
                url=self._evidence_url(neighbor_id),
                ranking_signals=[signal],
            )
            protected_ids.update({source.id,neighbor_id})
            inserted+=1
            if len(working)>=limit:
                # Preserve the ordinary ranking of retained hits: replace only
                # the lowest-ranked unprotected slot, then append inherited
                # context at the bottom of the requested window.
                drop=next(
                    (
                        index for index in range(len(working)-1,-1,-1)
                        if working[index].id not in protected_ids
                    ),
                    None,
                )
                if drop is None:
                    continue
                working.pop(drop)
            working.append(neighbor)

        return output.model_copy(update={"results":working[:limit]})

    async def search(self, query: str, principal: Principal, *, match_count: int = 8, main_job_id: str | None = None, aliases: list | None = None, document_ids: list[str] | None = None, _fast_only: bool = False) -> SearchOutput:
        started = time.monotonic()
        query = query.strip()
        if document_ids is not None:
            if not hasattr(self,'corpus'):
                raise NotImplementedError('document-scoped retrieval requires local ACL authority')
            if not isinstance(document_ids,list) or not 1<=len(document_ids)<=20:
                raise ValueError('document scope requires 1-20 document IDs')
            try:document_ids=[str(UUID(str(value))) for value in document_ids]
            except (TypeError,ValueError) as exc:raise ValueError('invalid document scope') from exc
            if len(set(document_ids))!=len(document_ids):raise ValueError('duplicate document scope')
        if not query:
            return SearchOutput(results=[], mode="lexical_degraded")
        if not _fast_only and hasattr(self,'corpus'):
            from .query_intent import is_opaque_identifier,exact_identifier_search
            if is_opaque_identifier(query):
                return await exact_identifier_search(self,query,principal,match_count=match_count,document_ids=document_ids)
        if aliases is None and not _fast_only and hasattr(self,'automatic_aliases'):
            try:aliases=await self.automatic_aliases(query,principal)
            except Exception as error:
                logger.warning(json.dumps({'event':'automatic_alias_expansion_failed','error_type':type(error).__name__}))
                aliases=[]
        if not _fast_only and os.getenv('RKB_BGE_ENABLED')=='1':
            from .multilingual_retrieval import main_search
            from .index_readiness import enabled,status
            readiness=await status(self,principal) if enabled() else None
            if readiness and readiness.bge_missing:
                fast=await self.search(query,principal,match_count=match_count,_fast_only=True,document_ids=document_ids)
                return fast.model_copy(update={'main_state':'pending','latency_ms':round(1000*(time.monotonic()-started),1)})
            result=await main_search(self,query,principal,match_count=match_count,main_job_id=main_job_id,aliases=aliases,**({'document_ids':document_ids} if document_ids is not None else {}))
            if readiness:
                result=result.model_copy(update={'indexing':readiness})
            if result.main_state=='ready':
                result=await self._expand_continuation_results(
                    result,principal,query=query,match_count=match_count
                )
            return result.model_copy(update={'latency_ms':round(1000*(time.monotonic()-started),1)})

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
                "document_ids": document_ids,
            },
        )
        response.raise_for_status()
        rows = response.json()
        timings = {**(encoding_timings.get() if is_e5 else {}), "database_seconds": time.monotonic()-database_start, "search_seconds": time.monotonic()-started}
        retrieval_mode = rows[0].get("retrieval_mode", "fast_e5" if is_e5 and vector is not None else "lexical_only") if rows else ("fast_e5" if is_e5 and vector is not None else "lexical_only")
        retrieval_mode=getattr(response,"retrieval_mode",None) or retrieval_mode
        readiness=await status(self,principal) if enabled() else None
        if readiness and readiness.e5_missing:
            retrieval_mode='lexical_only';vector=None
        logger.info(json.dumps({"event":"retrieval_served","retrieval_mode":retrieval_mode,"results":len(rows),"embedding_space":self.embedder.embedding_space if vector is not None else None,**timings}))
        output=SearchOutput(
            retrieval_mode=retrieval_mode, timings=timings,indexing=readiness,
            results=[
                SearchResult(
                    id=str(row["chunk_id"]),
                    title=str(row["title"]),
                    url=self._evidence_url(str(row["chunk_id"])),
                )
                for row in rows
            ],
            mode="hybrid" if (retrieval_mode!="lexical_only" if hasattr(self,"corpus") else vector is not None) else "lexical_degraded",
        )
        expanded=await self._expand_continuation_results(
            output,principal,query=query,match_count=match_count
        )
        return expanded.model_copy(update={'latency_ms':round(1000*(time.monotonic()-started),1)})

    async def fetch(self, item_id: str, principal: Principal) -> FetchOutput:
        # Resolve the chunk under the caller's JWT first. This RLS query is the
        # authorization boundary; service credentials never select user-visible rows.
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_chunks",
            headers=self._headers(principal),
            params={
                "id": f"eq.{item_id}",
                "select": (
                    "id,document_id,revision,title,metadata,page_ids,region_ids,illustration_ids,"
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
        page_ids=[str(value) for value in row.get("page_ids") or []]
        region_ids=[str(value) for value in row.get("region_ids") or []]
        async def related_rows(table: str, ids: list[str], select: str) -> dict[str, dict[str, Any]]:
            # Keep this compatible with the direct PostgreSQL REST adapter, which
            # deliberately implements only exact filters. A semantic chunk normally
            # spans only a small number of source regions/pages.
            output: dict[str, dict[str, Any]] = {}
            for ident in ids:
                related_response=await self.client.get(
                    f"{self.config.url.rstrip('/')}/rest/v1/{table}",
                    headers=self._headers(principal),
                    params={"id":"eq."+ident,"select":select,"limit":"1"},
                )
                related_response.raise_for_status()
                values=related_response.json()
                if values:
                    output[str(values[0]["id"])]=values[0]
            return output

        page_meta=await related_rows("rkb_pages",page_ids,"id,physical_page_index,revision")
        region_meta=await related_rows("rkb_regions",region_ids,"id,page_id,kind,reading_order")
        from .illustrations import descriptor
        figures = [await descriptor(self, principal, str(value), row['document_id']) for value in row.get('illustration_ids') or []]
        revision=row.get("revision")
        if revision is None and page_meta:
            revision=next(iter(page_meta.values())).get("revision")
        metadata.update(
            {
                "document_id": str(row["document_id"]),
                **({"revision": int(revision)} if revision is not None else {}),
                "pages": page_ids,
                "source_pages": [
                    {
                        "page_id": page_id,
                        "physical_page_index": page_meta[page_id]["physical_page_index"],
                    }
                    for page_id in page_ids if page_id in page_meta
                ],
                "regions": [
                    {
                        "region_id": region_id,
                        "page_id": str(region_meta[region_id]["page_id"]),
                        "kind": region_meta[region_id]["kind"],
                        "reading_order": region_meta[region_id]["reading_order"],
                    }
                    for region_id in region_ids if region_id in region_meta
                ],
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
        document_ids: list[str] | None = None,
    ) -> EvidenceSearchOutput:
        started = time.monotonic()
        limit = max(1, min(int(max_evidence), 5))
        found = await self.search(query, principal, **({'main_job_id': main_job_id} if main_job_id else {}), **({'document_ids':document_ids} if document_ids is not None else {}))
        selected = found.results[:limit]
        if not selected:
            return EvidenceSearchOutput(evidence=[], mode=found.mode, retrieval_mode=found.retrieval_mode, retrieval_policy=found.retrieval_policy, main_state=found.main_state, main_job_id=found.main_job_id, indexing=found.indexing, latency_ms=round(1000*(time.monotonic()-started),1), timings={**found.timings,"hydration_seconds":0,"total_seconds":time.monotonic()-started})
        hydration_start = time.monotonic()
        evidence = await asyncio.gather(
            *(self.fetch(item.id, principal) for item in selected)
        )
        return EvidenceSearchOutput(evidence=list(evidence), mode=found.mode, retrieval_mode=found.retrieval_mode, retrieval_policy=found.retrieval_policy, main_state=found.main_state, main_job_id=found.main_job_id, indexing=found.indexing, latency_ms=round(1000*(time.monotonic()-started),1), timings={**found.timings,"hydration_seconds":time.monotonic()-hydration_start,"total_seconds":time.monotonic()-started})

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
        state = str(row["state"])
        cursor = row.get("cursor")
        if state == "ready":
            next_action = "finalize"
        elif state == "finalized":
            next_action = "done"
        elif state == "failed":
            next_action = "blocker"
        elif state == "processing" and cursor in ("finalize","vectors"):
            next_action = "wait"
        elif state in {"staged", "processing"} and cursor in (None, ""):
            next_action = "validate"
        else:
            next_action = "continue_pages"
        return BookIngestOutput(
            ingestion_id=str(row["id"]),
            document_id=str(row["document_id"]) if row.get("document_id") else None,
            state=state,
            message=message,
            next_cursor=cursor or None,
            next_action=next_action,
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
        sha256: str | None = None,
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
        if sha256 is not None:
            params["sha256"] = f"eq.{sha256}"
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
        from .vector_policy import missing_required
        if output.document_id and hasattr(self,'data_client'):
            async with self.data_client._connection(self._headers(principal)) as db:
                source=await(await db.execute('select source_archive_status from rkb_documents where id=%s', (UUID(output.document_id),))).fetchone()
                if source:output=output.model_copy(update={'source_archive_status':source['source_archive_status']})
        if not enabled() or output.state!='finalized' or not output.document_id:return output
        readiness=await status(self,principal,output.document_id)
        warnings=[w for w in output.warnings if w not in ('fast_e5_backfill_required','automatic_indexing_pending')]
        if missing_required(e5_missing=readiness.e5_missing,bge_missing=readiness.bge_missing):
            warnings.append('automatic_indexing_pending')
            next_action='wait'
        else:
            next_action=output.next_action
        return output.model_copy(update={'indexing':readiness,'warnings':warnings,'next_action':next_action})

    async def _reprocess_existing_source(
        self,
        *,
        principal: Principal,
        document_id: str,
        force_new_revision: bool = False,
        derived_only: bool = False,
    ) -> BookIngestOutput:
        response = await self.client.get(
            f"{self.config.url.rstrip('/')}/rest/v1/rkb_documents",
            headers=self._headers(principal),
            params={
                "id": f"eq.{document_id}",
                "select": (
                    "id,owner_user_id,title,authors,publication_year,language,"
                    "source_sha256,page_count,active_revision,source_format,"
                    "source_filename,source_archive_status,source_archive_ref"
                ),
                "limit": "1",
            },
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            raise LookupError("book_not_found_or_not_accessible")
        document = dict(rows[0])
        if str(document.get("owner_user_id")) != principal.subject:
            raise PermissionError("book_reprocess_owner_required")
        if (
            document.get("source_archive_status") != "verified"
            or not document.get("source_archive_ref")
        ):
            raise RuntimeError(
                "source archive is not verified yet; wait for archival recovery and retry. "
                "Ask the user to attach the original PDF/DjVu again only if the archive cannot be recovered"
            )
        source_sha256 = str(document.get("source_sha256") or "")
        if not source_sha256:
            raise RuntimeError("archived source metadata is incomplete")

        source_object = await self._server_object(
            document_id=document_id,
            kind="source_pdf",
            sha256=source_sha256,
        )
        if not source_object:
            raise RuntimeError("archived source object metadata is missing")

        active_revision = int(document.get("active_revision") or 0)
        if force_new_revision and not hasattr(self, "corpus"):
            raise RuntimeError("forced archived reprocess requires local revision authority")
        if derived_only:
            if force_new_revision or not hasattr(self, "corpus"):
                raise RuntimeError("derived rechunk requires the local source authority")
            source_file_id = f"accepted-rechunk:{document_id}:after:{active_revision}"
        else:
            source_file_id = (
                f"archive-reprocess:{document_id}:force-after:{active_revision}"
                if force_new_revision
                else f"archive-reprocess:{document_id}:after:{active_revision}"
            )

        previous = await self._ingestion_row(
            principal=principal,
            source_file_id=source_file_id,
        )
        if previous and previous.get("state") != "failed" and previous.get("source_object_id"):
            return await self._ingestion_with_indexing(
                self._ingestion_output(
                    previous,
                    "Existing archived-source reprocess resumed",
                ),
                principal,
            )

        stored_page_count = int(document.get("page_count") or 0)
        if derived_only:
            if stored_page_count < 1 or active_revision < 1:
                raise RuntimeError("derived rechunk requires an active complete source")
            jobs = self.corpus.rows("rkb_ingestion_jobs")
            if not any(
                item.get("document_id") == document_id
                and item.get("owner_user_id") == principal.subject
                and item.get("source_sha256") == source_sha256
                and item.get("state") == "finalized"
                and int(item.get("staged_revision") or 0) == active_revision
                for item in jobs
            ):
                raise RuntimeError("accepted finalized source revision missing")
            if any(
                item.get("document_id") == document_id
                and item.get("state") not in ("finalized", "failed", "cancelled", "superseded")
                for item in jobs
            ):
                raise RuntimeError("another revision is already pending")
            old_pages = [
                page for page in self.corpus.rows("rkb_pages")
                if page.get("document_id") == document_id
                and int(page.get("revision") or 0) == active_revision
            ]
            if len(old_pages) != stored_page_count:
                raise RuntimeError("accepted source page count is incomplete")
            new_page_count = stored_page_count
        else:
            # Real re-import still downloads original archived bytes and verifies
            # the exact PDF/DjVu SHA and physical page count.
            work_root = os.getenv("RKB_WORK_DIR") or None
            format_name = str(document.get("source_format") or "pdf").lower()
            suffix = "djvu" if format_name == "djvu" else "pdf"
            with tempfile.TemporaryDirectory(prefix="rkb-reprocess-", dir=work_root) as temp_dir:
                source_path = Path(temp_dir) / f"source.{suffix}"
                from .source_archive import download_source
    
                await download_source(
                    self,
                    principal,
                    document_id,
                    source_object,
                    source_path,
                    require_archive=True,
                )
                actual_sha256, _ = await asyncio.to_thread(sha256_file, source_path)
                if actual_sha256 != source_sha256:
                    raise RuntimeError("archived source integrity mismatch")
                source_info = await self.pdf_processor.inspect_file(source_path)
    
            new_page_count = source_info.page_count
            if stored_page_count and new_page_count != stored_page_count:
                raise RuntimeError("archived source page count mismatch")

        new_ingestion_id = str(uuid4())
        start = await self.client.post(
            f"{self.config.url.rstrip('/')}/rest/v1/rpc/rkb_start_ingestion",
            headers=self._headers(principal),
            json={
                "p_ingestion_id": new_ingestion_id,
                "p_document_id": document_id,
                "p_title": str(document.get("title") or "Untitled source"),
                "p_authors": document.get("authors") or [],
                "p_publication_year": document.get("publication_year"),
                "p_language": document.get("language"),
                "p_source_sha256": source_sha256,
                "p_source_file_id": source_file_id,
                "p_page_count": new_page_count,
                "p_duplicate_policy": "new_revision",
                **({"p_force_new_revision": True} if force_new_revision else {}),
            },
        )
        start.raise_for_status()
        identities = start.json()
        if not identities:
            raise RuntimeError("reprocess_start_failed")
        new_ingestion_id = str(identities[0]["ingestion_id"])
        resolved_document_id = str(identities[0]["document_id"])
        if resolved_document_id != document_id:
            raise RuntimeError("reprocess_document_identity_mismatch")

        existing = await self._ingestion_row(
            principal=principal,
            ingestion_id=new_ingestion_id,
        )
        if existing and existing.get("state") != "failed" and existing.get("source_object_id"):
            return await self._ingestion_with_indexing(
                self._ingestion_output(
                    existing,
                    "Existing archived-source reprocess resumed",
                ),
                principal,
            )

        rows = await self._patch_ingestion(
            new_ingestion_id,
            principal,
            {
                "source_object_id": str(source_object["id"]),
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
        return await self._ingestion_with_indexing(
            self._ingestion_output(
                row,
                ("Accepted source selected for derived-only re-chunk; stage exact source proof"
                 if derived_only else
                 "Verified archived source opened; continue with book_pages and stage"),
            ),
            principal,
        )

    async def book_ingest(
        self,
        *,
        command: str,
        principal: Principal,
        file: ChatFile | None,
        ingestion_id: str | None,
        cursor: str | None,
        payload: dict[str, Any] | None,
        document_id: str | None = None,
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
            output = self._ingestion_output(row, message)
            if row["state"] == "processing" and row.get("cursor") == "finalize":
                task = self._finalize_tasks.get(ingestion_id)
                output = output.model_copy(
                    update={
                        "next_action": (
                            "wait"
                            if task is not None and not task.done()
                            else "resume_finalize"
                        )
                    }
                )
            return await self._ingestion_with_indexing(output,principal)

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

        if command in ("reprocess", "rechunk"):
            if file is not None:
                raise ValueError("existing-source operation must not attach a new file")
            if not document_id:
                raise ValueError("document_id is required for existing-source operation")
            if command == "rechunk":
                if (payload or {}).get("duplicate_policy") not in (None, "reuse"):
                    raise ValueError("derived-only rechunk cannot force an unfinished revision")
                return await self._reprocess_existing_source(
                    principal=principal, document_id=document_id, derived_only=True,
                )
            force_new_revision = bool(
                (payload or {}).get("duplicate_policy") == "new_revision"
            )
            return await self._reprocess_existing_source(
                principal=principal,
                document_id=document_id,
                force_new_revision=force_new_revision,
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
    if session_dsn or os.getenv("RKB_SQLITE_CORPUS_PATH"):
        from .postgres_backend import PostgresBackend
        from .sqlite_backend import SQLiteBackend

        try:
            pool_max = max(1, min(int(os.getenv("RKB_DB_POOL_MAX", "4")), 32))
        except ValueError as exc:
            raise RuntimeError("RKB_DB_POOL_MAX must be an integer") from exc
        backend_class=SQLiteBackend if os.getenv("RKB_SQLITE_CORPUS_PATH") else PostgresBackend
        corpus_options={"corpus_path":os.environ["RKB_SQLITE_CORPUS_PATH"]} if backend_class is SQLiteBackend else {}
        return backend_class(
            session_dsn,
            embedder=embedder,
            object_store=object_store,
            pool_min_size=1,
            pool_max_size=pool_max,
            public_base_url=public_base,
            **corpus_options,
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
