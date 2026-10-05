from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4, uuid5

import httpx

from .entity_graph import GraphBundle,validate_staged_bundle,digest
from .graph_service import GraphService

from .contracts import (
    BookIngestOutput,
    Principal,
    StageChunkInput,
    StagePageInput,
    StagePoiFactInput,
    StagePoiMediaLinkInput,
)
from .file_ingress import sha256_file
from .stage_graph import (
    GraphValidation,
    StagedGraph,
    compile_model_stage,
    merge_stage,
    validate_graph,
)
from .e5_contract import MAX_TOKENS as E5_MAX_TOKENS, TARGET_PASSAGE_TOKENS
from .bge_contract import MAX_TOKENS as BGE_MAX_TOKENS


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _vector_literal(values: list[float] | None) -> str | None:
    if values is None:
        return None
    if len(values) != 768:
        raise ValueError("chunk embedding must have exactly 768 dimensions")
    return "[" + ",".join(format(float(value), ".9g") for value in values) + "]"


async def _validate_with_token_budget(
    service,
    graph: StagedGraph,
    *,
    expected_page_count: int,
) -> GraphValidation:
    result = validate_graph(graph, expected_page_count=expected_page_count)
    from .local_e5 import LocalE5Embedder
    if not isinstance(getattr(service, "embedder", None), LocalE5Embedder):
        if os.getenv("RKB_AUTO_INDEX_ENABLED") == "1":
            return GraphValidation(
                errors=[*result.errors, "token_budget_unavailable:local_e5_required"],
                warnings=result.warnings,
            )
        return result

    from .search_material import graph_material
    materials: list[str] = []
    for chunk in graph.chunks:
        try:
            text, _ = graph_material(graph, chunk)
        except (KeyError, ValueError):
            continue
        materials.append(text)
    if not materials:
        return result

    try:
        counts = await service.embedder.passage_token_counts(materials)
    except (httpx.HTTPError, TimeoutError, RuntimeError, ValueError) as exc:
        return GraphValidation(
            errors=[*result.errors, f"token_budget_unavailable:{type(exc).__name__}"],
            warnings=result.warnings,
        )

    e5_counts = [row["e5"] for row in counts]
    bge_counts = [row["bge"] for row in counts]
    errors = list(result.errors)
    warnings = list(result.warnings)
    e5_over = sum(value > E5_MAX_TOKENS for value in e5_counts)
    bge_over = sum(value > BGE_MAX_TOKENS for value in bge_counts)
    if e5_over:
        errors.append(
            f"retrieval_quality:encoder_token_limit_exceeded:e5:"
            f"{e5_over}/{len(counts)}:max={max(e5_counts)}"
        )
    if bge_over:
        errors.append(
            f"retrieval_quality:encoder_token_limit_exceeded:bge:"
            f"{bge_over}/{len(counts)}:max={max(bge_counts)}"
        )
    over_target = sum(
        max(e5, bge) > TARGET_PASSAGE_TOKENS
        for e5, bge in zip(e5_counts, bge_counts, strict=True)
    )
    if over_target:
        warnings.append(
            f"retrieval_quality:passages_over_{TARGET_PASSAGE_TOKENS}_token_target:"
            f"{over_target}/{len(counts)}:max_e5={max(e5_counts)}:"
            f"max_bge={max(bge_counts)}"
        )
    return GraphValidation(
        errors=list(dict.fromkeys(errors)),
        warnings=list(dict.fromkeys(warnings)),
    )


async def _ensure_object(
    service,
    *,
    document_id: str,
    kind: str,
    object_key: str,
    data: bytes,
    mime_type: str,
) -> str:
    digest = hashlib.sha256(data).hexdigest()
    existing = await service._server_object(
        document_id=document_id,
        object_key=object_key,
        kind=kind,
    )
    if existing:
        if existing.get("sha256") != digest:
            raise RuntimeError("existing object hash mismatch")
        return str(existing["id"])

    await service.object_store.put_bytes(object_key, data, mime_type)
    object_id = str(uuid4())
    response = await service.client.post(
        f"{service.config.url.rstrip('/')}/rest/v1/rkb_objects",
        headers={**service._service_headers(), "Prefer": "return=minimal"},
        json={
            "id": object_id,
            "document_id": document_id,
            "kind": kind,
            "object_key": object_key,
            "sha256": digest,
            "mime_type": mime_type,
            "size_bytes": len(data),
            "access_class": "private",
        },
    )
    response.raise_for_status()
    return object_id


async def _document_row(
    service,
    principal: Principal,
    document_id: str,
) -> dict[str, Any]:
    response = await service.client.get(
        f"{service.config.url.rstrip('/')}/rest/v1/rkb_documents",
        headers=service._headers(principal),
        params={
            "id": f"eq.{document_id}",
            "select": (
                "id,page_count,active_revision,title,authors,publication_year,"
                "content_visibility,workspace_id"
            ),
            "limit": "1",
        },
    )
    response.raise_for_status()
    rows = response.json()
    if not rows:
        raise LookupError("document_not_found")
    return dict(rows[0])


async def _load_graph(service, row: dict[str, Any]) -> StagedGraph:
    revision = int(row.get("staged_revision") or 1)
    object_id = row.get("staged_graph_object_id")
    if not object_id:
        return StagedGraph(revision=revision)

    object_row = await service._server_object(
        document_id=str(row["document_id"]),
        object_id=str(object_id),
        kind="document_graph",
    )
    if not object_row:
        raise RuntimeError("staged graph object is missing")
    data = await service.object_store.get_bytes(str(object_row["object_key"]))
    if hashlib.sha256(data).hexdigest() != object_row["sha256"]:
        raise RuntimeError("staged graph integrity check failed")
    graph = StagedGraph.model_validate_json(data)
    if graph.revision != revision:
        raise RuntimeError("staged graph revision mismatch")
    return graph


async def _store_graph(
    service,
    principal: Principal,
    row: dict[str, Any],
    graph: StagedGraph,
    *,
    cursor: str | None,
) -> dict[str, Any]:
    document_id = str(row["document_id"])
    data = _canonical_json_bytes(graph.model_dump(mode="json"))
    digest = hashlib.sha256(data).hexdigest()
    key = (
        f"users/{principal.subject}/documents/{document_id}/"
        f"graph/staged-r{graph.revision}-{digest}.json"
    )
    object_id = await _ensure_object(
        service,
        document_id=document_id,
        kind="document_graph",
        object_key=key,
        data=data,
        mime_type="application/json",
    )
    values: dict[str, Any] = {
        "staged_graph_object_id": object_id,
        "state": "processing",
        "warnings": [],
        "error_code": None,
    }
    if cursor is not None:
        values["cursor"] = cursor
    rows = await service._patch_ingestion(
        str(row["id"]),
        principal,
        values,
        representation=True,
    )
    return rows[0] if rows else {**row, **values}


async def stage_ingestion(
    service,
    *,
    principal: Principal,
    ingestion_id: str,
    cursor: str | None,
    payload: dict[str, Any] | None,
) -> BookIngestOutput:
    row = await service._ingestion_row(
        principal=principal,
        ingestion_id=ingestion_id,
    )
    if not row:
        raise LookupError("ingestion_not_found")
    if row["state"] == "finalized":
        raise RuntimeError("ingestion_already_finalized")
    if not row.get("document_id") or not row.get("source_object_id"):
        raise RuntimeError("ingestion_source_not_ready")
    if payload is None:
        raise ValueError("stage pages/chunks are required")

    entity_candidates = GraphBundle.model_validate(payload["entity_candidates"]) if payload.get("entity_candidates") is not None else None
    raw_pages = payload.get("pages", [])
    raw_chunks = payload.get("chunks", [])
    raw_poi_facts = payload.get("poi_facts", [])
    raw_poi_media_links = payload.get("poi_media_links", [])
    if (
        not isinstance(raw_pages, list)
        or not isinstance(raw_chunks, list)
        or not isinstance(raw_poi_facts, list)
        or not isinstance(raw_poi_media_links, list)
    ):
        raise ValueError(
            "stage pages/chunks/poi_facts/poi_media_links must be arrays"
        )
    pages = [StagePageInput.model_validate(value) for value in raw_pages]
    chunks = [StageChunkInput.model_validate(value) for value in raw_chunks]
    poi_facts = [
        StagePoiFactInput.model_validate(value)
        for value in raw_poi_facts
    ]
    poi_media_links = [
        StagePoiMediaLinkInput.model_validate(value)
        for value in raw_poi_media_links
    ]
    if len(pages) > 8:
        raise ValueError("stage accepts at most 8 pages per call")
    if len(poi_facts) > 100:
        raise ValueError("stage accepts at most 100 POI facts per call")
    if len(poi_media_links) > 100:
        raise ValueError("stage accepts at most 100 POI media links per call")
    if not pages and not chunks and not poi_facts and not poi_media_links and not entity_candidates:
        raise ValueError(
            "stage requires a page, chunk, POI fact or POI media link"
        )

    graph = await _load_graph(service, row)
    revision = int(row.get("staged_revision") or 1)
    compiled = compile_model_stage(
        graph,
        document_id=str(row["document_id"]),
        revision=revision,
        pages=pages,
        chunks=chunks,
        poi_facts=poi_facts,
        poi_media_links=poi_media_links,
    )
    merged = merge_stage(graph, compiled)
    if entity_candidates is not None:
        validate_staged_bundle(entity_candidates,merged)
        if digest(entity_candidates.model_dump(mode="json")) not in {digest(b.model_dump(mode="json")) for b in merged.entity_candidates}:
            merged = StagedGraph.model_validate({**merged.model_dump(mode="json"),"entity_candidates":[*[b.model_dump(mode="json") for b in merged.entity_candidates],entity_candidates.model_dump(mode="json")]})
    document = await _document_row(service, principal, str(row["document_id"]))
    page_count = int(document.get("page_count") or 0)
    reviewed = {page.physical_page_index for page in merged.pages}
    missing = next((index for index in range(page_count) if index not in reviewed), None)
    # Persist the next model action, including across status/restart. Complete
    # page transport is not semantic validation; validate remains a separate step.
    progress_cursor = "" if page_count and missing is None else cursor
    if progress_cursor is None and missing is not None:
        progress_cursor = str(missing)
    updated = await _store_graph(
        service,
        principal,
        row,
        merged,
        cursor=progress_cursor,
    )
    return service._ingestion_output(
        updated,
        (
            f"Staged {len(pages)} pages, {len(chunks)} chunks, "
            f"{len(poi_facts)} POI facts and {len(poi_media_links)} POI media "
            "links; continue with book_pages/stage or validate when complete"
        ),
    )


async def validate_ingestion(
    service,
    *,
    principal: Principal,
    ingestion_id: str,
) -> BookIngestOutput:
    row = await service._ingestion_row(
        principal=principal,
        ingestion_id=ingestion_id,
    )
    if not row:
        raise LookupError("ingestion_not_found")
    if not row.get("document_id"):
        raise RuntimeError("ingestion_document_not_ready")

    graph = await _load_graph(service, row)
    document = await _document_row(service, principal, str(row["document_id"]))
    page_count = int(document.get("page_count") or 0)
    result = await _validate_with_token_budget(
        service,
        graph,
        expected_page_count=page_count,
    )
    warnings = [
        *[f"error:{item}" for item in result.errors],
        *result.warnings,
    ]
    state = "ready" if result.ready else "needs_review"
    rows = await service._patch_ingestion(
        ingestion_id,
        principal,
        {
            "state": state,
            "warnings": warnings,
            "error_code": None if result.ready else "graph_validation",
        },
        representation=True,
    )
    updated = rows[0] if rows else {**row, "state": state, "warnings": warnings}
    return service._ingestion_output(
        updated,
        "Graph is ready for finalize" if result.ready else "Graph needs review",
    )


async def _post_rows(
    service,
    principal: Principal,
    table: str,
    rows: list[dict[str, Any]],
) -> None:
    for offset in range(0, len(rows), 200):
        batch = rows[offset : offset + 200]
        if not batch:
            continue
        response = await service.client.post(
            f"{service.config.url.rstrip('/')}/rest/v1/{table}",
            headers={**service._headers(principal), "Prefer": "return=minimal"},
            json=batch,
        )
        response.raise_for_status()


async def _reset_revision(
    service,
    principal: Principal,
    *,
    document_id: str,
    revision: int,
) -> None:
    for table in ("rkb_chunks", "rkb_pages"):
        response = await service.client.delete(
            f"{service.config.url.rstrip('/')}/rest/v1/{table}",
            headers=service._headers(principal),
            params={
                "document_id": f"eq.{document_id}",
                "revision": f"eq.{revision}",
            },
        )
        response.raise_for_status()


def _rotate_png_clockwise(data: bytes, degrees: int) -> bytes:
    if degrees not in (0, 90, 180, 270):
        raise ValueError("display rotation must be a cardinal clockwise turn")
    if degrees == 0:
        return data
    import io
    from PIL import Image
    with Image.open(io.BytesIO(data)) as image:
        rotated = image.convert("RGB").rotate(-degrees, expand=True)
        output = io.BytesIO()
        rotated.save(output, format="PNG")
        return output.getvalue()


def _crop_sync(path: Path, page_index: int, bbox: dict[str, int]) -> bytes:
    import fitz

    with fitz.open(path) as document:
        page = document.load_page(page_index)
        rect = page.rect
        clip = fitz.Rect(
            rect.x0 + rect.width * bbox["left"] / 1000.0,
            rect.y0 + rect.height * bbox["top"] / 1000.0,
            rect.x0 + rect.width * bbox["right"] / 1000.0,
            rect.y0 + rect.height * bbox["bottom"] / 1000.0,
        )
        if clip.width <= 0 or clip.height <= 0:
            raise ValueError("illustration crop has no area")
        longest = max(float(clip.width), float(clip.height), 1.0)
        scale = min(3.0, max(1.0, 2000.0 / longest))
        pixmap = page.get_pixmap(
            matrix=fitz.Matrix(scale, scale),
            clip=clip,
            alpha=False,
        )
        return pixmap.tobytes("png")


async def _embeddings(
    service,
    texts: list[str],
) -> tuple[list[list[float] | None], list[str]]:
    from .e5_contract import SPACE
    if getattr(service.embedder, "embedding_space", "") == SPACE:
        return [None] * len(texts), ["automatic_indexing_pending" if os.getenv('RKB_AUTO_INDEX_ENABLED')=='1' else "fast_e5_backfill_required"]
    semaphore = asyncio.Semaphore(4)
    warnings: list[str] = []

    async def one(index: int, text: str) -> list[float] | None:
        async with semaphore:
            try:
                return await service.embedder.embed(text)
            except (httpx.HTTPError, TimeoutError, RuntimeError, ValueError):
                warnings.append(f"embedding_degraded:{index}")
                return None

    values = await asyncio.gather(
        *(one(index, text) for index, text in enumerate(texts))
    )
    return list(values), warnings


_PUBLICATION_METHOD_SCORES = {
    "source_edition": 95,
    "scholarly_monograph": 85,
    "institutional_catalogue": 80,
    "general_history": 65,
    "memoir": 55,
    "unknown": None,
}
_YEAR_RE = re.compile(r"\b(?:1[0-9]{3}|20[0-9]{2}|[5-9][0-9]{2})\b")


def _poi_semantic_key(kind: str, text: str) -> str:
    years = list(dict.fromkeys(_YEAR_RE.findall(text)))
    if kind != "other" and years:
        return f"{kind}:" + "-".join(years)
    digest = hashlib.sha256(text.casefold().encode("utf-8")).hexdigest()[:20]
    return f"{kind}:producer:{digest}"


async def _author_authority(
    service,
    principal: Principal,
    *,
    contributor_names: list[str],
    subject: str,
) -> tuple[int | None, list[str], str | None]:
    names = list(
        dict.fromkeys(
            value.strip()
            for value in contributor_names
            if value.strip()
        )
    )
    if not names:
        return None, [], None

    response = await service.client.post(
        f"{service.config.url.rstrip('/')}/rest/v1/rpc/"
        "rkb_author_authority_for_names",
        headers=service._headers(principal),
        json={
            "p_names": names,
            "p_subject": subject,
            "p_geography": "kaliningrad_oblast",
        },
    )
    response.raise_for_status()
    rows = response.json()
    by_input = {
        str(row["input_name"]).casefold(): row
        for row in rows
        if row.get("input_name")
    }
    if any(name.casefold() not in by_input for name in names):
        return None, [
            f"knowledge://authors/{row['author_id']}"
            for row in rows
            if row.get("author_id")
        ], None

    scores = [int(by_input[name.casefold()]["score"]) for name in names]
    policy_versions = {
        str(by_input[name.casefold()]["policy_version"])
        for name in names
    }
    if len(policy_versions) != 1:
        return None, [
            f"knowledge://authors/{by_input[name.casefold()]['author_id']}"
            for name in names
        ], None

    return (
        round(sum(scores) / len(scores)),
        [
            f"knowledge://authors/{by_input[name.casefold()]['author_id']}"
            for name in names
        ],
        next(iter(policy_versions)),
    )


async def _build_poi_events(
    service,
    *,
    principal: Principal,
    graph: StagedGraph,
    document: dict[str, Any],
    document_id: str,
    revision: int,
) -> list[dict[str, Any]]:
    authors = [
        str(value).strip()
        for value in (document.get("authors") or [])
        if str(value).strip()
    ]
    events: list[dict[str, Any]] = []
    for fact in graph.poi_facts:
        contributors = fact.contributor_names or authors
        author_score, author_refs, authority_policy = await _author_authority(
            service,
            principal,
            contributor_names=contributors,
            subject=fact.kind,
        )
        method_score = _PUBLICATION_METHOD_SCORES.get(
            fact.publication_method
        )
        provenance_score = 100
        verification_score = None
        if author_score is not None and method_score is not None:
            verification_score = round(
                0.55 * author_score
                + 0.25 * method_score
                + 0.20 * provenance_score
            )

        candidate_id = str(fact.candidate_id)
        scope_visibility = str(
            document.get("content_visibility") or "private"
        )
        event_id = str(uuid5(fact.candidate_id, "poi.fact_evidence.v1"))
        events.append(
            {
                "contract_version": "poi.fact_evidence.v1",
                "event_id": event_id,
                "idempotency_key": (
                    f"knowledge:{document_id}:{revision}:{candidate_id}"
                ),
                "producer": "regional_knowledge",
                "scope": {
                    "visibility": scope_visibility,
                    "owner_sub": principal.subject,
                    "workspace_id": document.get("workspace_id"),
                },
                "source": {
                    "document_ref": f"knowledge://documents/{document_id}",
                    "revision": revision,
                    "title": str(document.get("title") or "Untitled source"),
                    "publication_year": document.get("publication_year"),
                },
                "poi_locator": fact.poi_locator.model_dump(
                    mode="json",
                    exclude_none=True,
                ),
                "claim": {
                    "candidate_id": candidate_id,
                    "semantic_key": _poi_semantic_key(
                        fact.kind,
                        fact.text,
                    ),
                    "kind": fact.kind,
                    "text": fact.text,
                    "time_scope": fact.time_scope,
                },
                "evidence": {
                    "evidence_ref": f"knowledge://evidence/{candidate_id}",
                    "page_ids": [str(value) for value in fact.page_ids],
                    "region_ids": [str(value) for value in fact.region_ids],
                    "source_family_id": (
                        f"unresolved:knowledge:{document_id}"
                    ),
                    "author_profile_refs": author_refs,
                    "author_subject_authority": author_score,
                    "publication_method_score": method_score,
                    "provenance_precision_score": provenance_score,
                    "evidence_verification_score": verification_score,
                    "score_policy_version": (
                        "book-evidence-v1"
                        if authority_policy is None
                        else f"book-evidence-v1+{authority_policy}"
                    ),
                },
            }
        )
    return events


def _build_poi_media_events(
    *,
    principal: Principal,
    graph: StagedGraph,
    document: dict[str, Any],
    document_id: str,
    revision: int,
    illustration_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    illustration_row_by_id = {
        str(row["id"]): row
        for row in illustration_rows
    }
    illustration_by_id = {
        str(item.illustration_id): item
        for item in graph.illustrations
    }
    region_by_id = {
        str(region.region_id): region
        for page in graph.pages
        for region in page.regions
    }

    events: list[dict[str, Any]] = []
    for link in graph.poi_media_links:
        illustration_id = str(link.illustration_id)
        illustration = illustration_by_id.get(illustration_id)
        row = illustration_row_by_id.get(illustration_id)
        if illustration is None or row is None:
            raise RuntimeError("POI media link illustration is not materialized")

        caption_parts = [
            region_by_id[str(region_id)].source_text.strip()
            for region_id in illustration.caption_region_ids
            if str(region_id) in region_by_id
            and region_by_id[str(region_id)].source_text.strip()
        ]
        caption = "\n".join(caption_parts)[:2000] or None
        link_id = str(link.link_id)
        event_id = str(uuid5(link.link_id, "poi.media_evidence.v1"))
        evidence_region_ids = [
            str(illustration.source_region_id),
            *[str(value) for value in illustration.caption_region_ids],
        ]
        visibility = str(row.get("visibility") or "private")
        events.append(
            {
                "contract_version": "poi.media_evidence.v1",
                "event_id": event_id,
                "idempotency_key": (
                    f"knowledge-media:{document_id}:{revision}:{link_id}"
                ),
                "producer": "regional_knowledge",
                "scope": {
                    "visibility": visibility,
                    "owner_sub": principal.subject,
                    "workspace_id": (
                        document.get("workspace_id")
                        if visibility == "workspace"
                        else None
                    ),
                },
                "source": {
                    "document_ref": f"knowledge://documents/{document_id}",
                    "revision": revision,
                    "title": str(document.get("title") or "Untitled source"),
                    "publication_year": document.get("publication_year"),
                },
                "poi_locator": link.poi_locator.model_dump(
                    mode="json",
                    exclude_none=True,
                ),
                "media": {
                    "link_id": link_id,
                    "illustration_id": illustration_id,
                    "illustration_ref": (
                        f"knowledge://illustrations/{illustration_id}"
                    ),
                    "relation": link.relation,
                    "time_scope": link.time_scope,
                    "kind": illustration.kind,
                    "caption": caption,
                    "page_id": str(illustration.page_id),
                    "source_region_id": str(illustration.source_region_id),
                    "caption_region_ids": [
                        str(value)
                        for value in illustration.caption_region_ids
                    ],
                    "source_crop_sha256": row["source_crop_sha256"],
                    "rights_status": row.get("rights_status") or "unknown",
                    "visibility": visibility,
                    "vibepublish_entry_ref": row.get(
                        "vibepublish_entry_ref"
                    ),
                },
                "evidence": {
                    "page_ids": [str(illustration.page_id)],
                    "region_ids": list(dict.fromkeys(evidence_region_ids)),
                    "source_family_id": (
                        f"unresolved:knowledge:{document_id}"
                    ),
                },
            }
        )
    return events


async def finalize_ingestion(
    service,
    *,
    principal: Principal,
    ingestion_id: str,
) -> BookIngestOutput:
    row = await service._ingestion_row(
        principal=principal,
        ingestion_id=ingestion_id,
    )
    if not row:
        raise LookupError("ingestion_not_found")
    if row["state"] == "finalized":
        return service._ingestion_output(row, "Ingestion already finalized")
    if row["state"] == "processing" and row.get("cursor") == "finalize":
        pass
    elif row["state"] != "ready":
        raise RuntimeError("ingestion must validate as ready before finalize")
    if not row.get("document_id") or not row.get("source_object_id"):
        raise RuntimeError("ingestion_source_not_ready")

    document_id = str(row["document_id"])
    revision = int(row.get("staged_revision") or 1)
    graph = await _load_graph(service, row)
    document = await _document_row(service, principal, document_id)
    validation = await _validate_with_token_budget(
        service,
        graph,
        expected_page_count=int(document.get("page_count") or 0),
    )
    if not validation.ready:
        return await validate_ingestion(
            service,
            principal=principal,
            ingestion_id=ingestion_id,
        )

    projection = bytearray()
    chunk_rows: list[dict[str, Any]] = []
    normalized_texts: list[str] = []
    from .search_material import graph_material
    for chunk in graph.chunks:
        raw = chunk.text.encode("utf-8")
        start = len(projection)
        projection.extend(raw)
        end = len(projection)
        projection.extend(b"\n\n")
        normalized, material_sha = graph_material(graph, chunk)
        normalized_texts.append(normalized)
        chunk_rows.append(
            {
                "id": str(chunk.chunk_id),
                "region_ids": [str(value) for value in chunk.region_ids],
                "page_ids": [str(value) for value in chunk.page_ids],
                "illustration_ids": [
                    str(value) for value in chunk.illustration_ids
                ],
                "footnote_region_ids": [
                    str(value) for value in chunk.footnote_region_ids
                ],
                "text_start": start,
                "text_end": end,
                "text_sha256": hashlib.sha256(raw).hexdigest(),
                "source_text":chunk.text,
                "search_material": normalized,
                "search_material_sha256": material_sha,
                "title": chunk.title,
                "normalized_text": normalized,
                "metadata": {"chunk_key": chunk.chunk_key,**({"article_id":chunk.article_id} if chunk.article_id else {})},
            }
        )

    text_object_id = None

    embeddings, embedding_warnings = await _embeddings(
        service,
        normalized_texts,
    )
    embedding_space = getattr(service.embedder, "embedding_space", None)
    for item, embedding in zip(chunk_rows, embeddings, strict=True):
        item["embedding"] = _vector_literal(embedding)
        if embedding is not None:
            if not embedding_space:
                raise RuntimeError(
                    "embedding provider returned a vector without an embedding space"
                )
            item["metadata"]["embedding_space"] = embedding_space

    source_object = await service._server_object(
        document_id=document_id,
        object_id=str(row["source_object_id"]),
        kind="source_pdf",
    )
    if not source_object:
        raise RuntimeError("source PDF object is missing")

    page_render_object_ids: dict[int, str] = {}
    illustration_rows: list[dict[str, Any]] = []
    work_root = os.getenv("RKB_WORK_DIR") or None
    with tempfile.TemporaryDirectory(
        prefix="rkb-finalize-",
        dir=work_root,
    ) as temp_dir:
        source_path = Path(temp_dir) / "source.pdf"
        from .source_archive import download_source
        await download_source(service,principal,document_id,source_object,source_path)
        actual_sha, _ = await asyncio.to_thread(sha256_file, source_path)
        if actual_sha != row["source_sha256"]:
            raise RuntimeError(
                "source PDF integrity check failed during finalize"
            )

        if graph.illustrations:
            page_by_id = {str(page.page_id): page for page in graph.pages}
            for item in graph.illustrations:
                page = page_by_id[str(item.page_id)]
                source_crop = await asyncio.to_thread(
                    __import__("regional_knowledge.source_adapter",fromlist=["crop_sync"]).crop_sync,
                    source_path,
                    page.physical_page_index,
                    item.bbox.model_dump(),
                )
                source_crop_sha = hashlib.sha256(source_crop).hexdigest()
                display_crop = await asyncio.to_thread(
                    _rotate_png_clockwise,
                    source_crop,
                    item.display_rotation_degrees,
                )
                display_crop_sha = hashlib.sha256(display_crop).hexdigest()
                crop_key = (
                    f"users/{principal.subject}/documents/{document_id}/"
                    f"illustrations/r{revision}/"
                    f"{item.illustration_id}-{display_crop_sha}.png"
                )
                crop_object_id = await _ensure_object(
                    service,
                    document_id=document_id,
                    kind="illustration_crop",
                    object_key=crop_key,
                    data=display_crop,
                    mime_type="image/png",
                )
                illustration_rows.append(
                    {
                        "id": str(item.illustration_id),
                        "document_id": document_id,
                        "page_id": str(item.page_id),
                        "source_region_id": str(item.source_region_id),
                        "crop_object_id": crop_object_id,
                        "kind": item.kind,
                        "visual_description": item.visual_description,
                        "visual_description_provenance": item.visual_description_provenance,
                        "visual_description_language": item.visual_description_language,
                        "display_rotation_degrees": item.display_rotation_degrees,
                        "display_crop_sha256": display_crop_sha,
                        "caption_text": '\n'.join(region.source_text for page in graph.pages for rid in item.caption_region_ids for region in page.regions if region.region_id == rid),
                        "caption_region_ids": [
                            str(value) for value in item.caption_region_ids
                        ],
                        "nearby_region_ids": [
                            str(value) for value in item.nearby_region_ids
                        ],
                        "visibility": "private",
                        "rights_status": "unknown",
                        "rights_evidence": {},
                        "source_crop_sha256": source_crop_sha,
                    }
                )

    await _reset_revision(
        service,
        principal,
        document_id=document_id,
        revision=revision,
    )

    page_rows: list[dict[str, Any]] = []
    region_rows: list[dict[str, Any]] = []
    for page in graph.pages:
        page_rows.append(
            {
                "id": str(page.page_id),
                "document_id": document_id,
                "physical_page_index": page.physical_page_index,
                "printed_page_number": page.printed_page_number,
                "width": page.width,
                "height": page.height,
                "layout_kind": page.layout_kind,
                "page_object_id": None,
                "revision": revision,
            }
        )
        for region in page.regions:
            text = region.source_text.encode("utf-8")
            region_rows.append(
                {
                    "id": str(region.region_id),
                    "page_id": str(page.page_id),
                    "kind": region.kind.value,
                    "bbox": region.bbox.model_dump(),
                    "column_id": region.column_id,
                    "reading_order": region.reading_order,
                    "text_sha256": hashlib.sha256(text).hexdigest()
                    if text
                    else None,
                    "source_text":region.source_text,
                    "confidence": region.confidence,
                    "needs_review": region.needs_review,
                }
            )

    relation_rows = [
        {
            "source_region_id": str(item.source_region_id),
            "target_region_id": str(item.target_region_id),
            "kind": item.kind.value,
        }
        for item in graph.relations
    ]

    await _post_rows(service, principal, "rkb_pages", page_rows)
    await _post_rows(service, principal, "rkb_regions", region_rows)
    await _post_rows(
        service,
        principal,
        "rkb_region_relations",
        relation_rows,
    )
    await _post_rows(
        service,
        principal,
        "rkb_illustrations",
        illustration_rows,
    )

    for offset in range(0, len(chunk_rows), 100):
        response = await service.client.post(
            f"{service.config.url.rstrip('/')}/rest/v1/rpc/rkb_insert_chunks",
            headers=service._headers(principal),
            json={
                "p_document_id": document_id,
                "p_ingestion_id": ingestion_id,
                "p_revision": revision,
                "p_text_object_id": text_object_id,
                "p_chunks": chunk_rows[offset : offset + 100],
            },
        )
        response.raise_for_status()

    if embedding_warnings:
        await service._patch_ingestion(
            ingestion_id,
            principal,
            {"warnings": [*validation.warnings, *embedding_warnings]},
        )

    poi_events = await _build_poi_events(
        service,
        principal=principal,
        graph=graph,
        document=document,
        document_id=document_id,
        revision=revision,
    )
    poi_events.extend(
        _build_poi_media_events(
            principal=principal,
            graph=graph,
            document=document,
            document_id=document_id,
            revision=revision,
            illustration_rows=illustration_rows,
        )
    )

    if graph.entity_candidates:
        semantic = GraphService(service)
        for bundle in graph.entity_candidates:
            await semantic.stage(principal,document_id,revision,bundle,staged_texts={str(c.chunk_id):c.text for c in graph.chunks})

    activation = await service.client.post(
        f"{service.config.url.rstrip('/')}/rest/v1/rpc/rkb_activate_revision",
        headers=service._headers(principal),
        json={
            "p_document_id": document_id,
            "p_ingestion_id": ingestion_id,
            "p_revision": revision,
            "p_poi_events": poi_events,
        },
    )
    activation.raise_for_status()

    pending=bool(activation.json() and activation.json()[0].get("pending_vectors")) if hasattr(service,"corpus") else False
    return BookIngestOutput(
        next_action="wait" if pending else "done",
        ingestion_id=ingestion_id,
        document_id=document_id,
        state="processing" if pending else "finalized",
        message=f"Revision {revision} waiting for vector publication" if pending else f"Revision {revision} activated with {len(chunk_rows)} chunks",
        warnings=[*validation.warnings, *embedding_warnings],
    )