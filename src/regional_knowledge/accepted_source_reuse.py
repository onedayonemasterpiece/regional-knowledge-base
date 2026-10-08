"""Proof-gated reuse of *unchanged* accepted book material for re-chunking.

This is not a new visual review. Only a finalized, owner-matched, archived
active revision can authorize deterministic reuse. Source/page/region/media
identities are compared by source position, not revision-specific UUIDs.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict


def _bbox(value):
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("missing source geometry")
    return tuple(int(value[k]) for k in ("left", "top", "right", "bottom"))


def _row_value(value):
    return str(value.value if hasattr(value, "value") else value)


def _regions(regions, *, staged=False):
    result = []
    references = {}
    for region in regions:
        if staged:
            source = region.source_text
            if region.normalized_text != source:
                raise ValueError("unverified normalized source text")
            ident = str(region.region_id)
            order = int(region.reading_order)
            kind = _row_value(region.kind)
            bbox = region.bbox
            column = region.column_id
            needs_review = region.needs_review
        else:
            source = str(region.get("source_text") or "")
            ident = str(region["id"])
            order = int(region["reading_order"])
            kind = str(region["kind"])
            bbox = region["bbox"]
            column = region.get("column_id")
            needs_review = region.get("needs_review")
            declared_sha = region.get("text_sha256")
            if not (kind == "figure" and source == ""):
                # Image-only figure regions may carry a binary-crop digest in
                # text_sha256. Their source proof is the archived PDF SHA,
                # unchanged figure bbox and checked illustration references.
                if not declared_sha or declared_sha != hashlib.sha256(source.encode()).hexdigest():
                    raise ValueError("accepted source region SHA mismatch")
        if order in (value[1] for value in references.values()):
            raise ValueError("duplicate source reading order")
        if ident in references:
            raise ValueError("duplicate source region identity")
        references[ident] = (None, order)
        result.append((
            order, kind, _bbox(bbox), str(column or ""),
            hashlib.sha256(source.encode()).hexdigest(), bool(needs_review),
        ))
    return tuple(sorted(result)), references


def _signature(pages, regions, illustrations, relations, *, staged=False):
    """Canonical source-position signature. Never hashes new derived chunks."""
    page_signatures = []
    positions = {}
    bounds = {}
    if staged:
        ordered_pages = list(pages)
    else:
        ordered_pages = list(pages.values())
    if len({int(p.physical_page_index if staged else p["physical_page_index"])
            for p in ordered_pages}) != len(ordered_pages):
        raise ValueError("duplicate page indexes")
    for page in ordered_pages:
        index = int(page.physical_page_index if staged else page["physical_page_index"])
        page_id = str(page.page_id if staged else page["id"])
        region_list = page.regions if staged else regions.get(page_id, [])
        reg_sig, refs = _regions(region_list, staged=staged)
        for ident, (_, order) in refs.items():
            if ident in positions:
                raise ValueError("cross-page repeated region")
            positions[ident] = (index, order)
        for region in region_list:
            ident = str(region.region_id if staged else region["id"])
            bounds[ident] = _bbox(region.bbox if staged else region["bbox"])
        metadata = (
            index,
            str((page.printed_page_number if staged else page.get("printed_page_number")) or ""),
            int(page.width if staged else page["width"]),
            int(page.height if staged else page["height"]),
            str((page.layout_kind if staged else page.get("layout_kind")) or ""),
        )
        # Exclusion reasons are not materialized in accepted page rows. Do not
        # infer them from silence; a new proposal with exclusions needs review.
        if staged and page.excluded_figure_regions:
            raise ValueError("unverifiable source figure exclusions")
        page_signatures.append((metadata, reg_sig))
    image_signatures = []
    for item in illustrations:
        if staged:
            source = str(item.source_region_id)
            captions = [str(x) for x in item.caption_region_ids]
            nearby = [str(x) for x in item.nearby_region_ids]
            kind = item.kind
            description = item.visual_description
            lang = item.visual_description_language
            rot = item.display_rotation_degrees
            page_id = str(item.page_id)
        else:
            source = str(item["source_region_id"])
            captions = [str(x) for x in item.get("caption_region_ids") or []]
            nearby = [str(x) for x in item.get("nearby_region_ids") or []]
            kind = item["kind"]
            description = item.get("visual_description")
            lang = item.get("visual_description_language")
            rot = item.get("display_rotation_degrees") or 0
            page_id = str(item["page_id"])
        if source not in positions or any(key not in positions for key in (*captions, *nearby)):
            raise ValueError("source illustration references unknown region")
        if staged and _bbox(item.bbox) != bounds[source]:
            raise ValueError("illustration crop differs from accepted region")
        page_idx = positions[source][0]
        if staged:
            expected_id = next(
                (str(page.page_id) for page in pages if page.physical_page_index == page_idx), None
            )
        else:
            expected_id = next(
                (str(page["id"]) for page in pages.values()
                 if int(page["physical_page_index"]) == page_idx), None
            )
        if expected_id != page_id:
            raise ValueError("illustration references another page")
        image_signatures.append((
            positions[source], _row_value(kind), str(description or ""),
            str(lang or ""), int(rot),
            tuple(positions[key] for key in captions),
            tuple(positions[key] for key in nearby),
        ))
    relation_signatures = []
    for item in relations:
        if staged:
            left = str(item.source_region_id)
            right = str(item.target_region_id)
            kind = _row_value(item.kind)
        else:
            left = str(item["source_region_id"])
            right = str(item["target_region_id"])
            kind = _row_value(item["kind"])
        if left not in positions or right not in positions:
            raise ValueError("source relation crosses inaccessible corpus")
        relation_signatures.append((kind, positions[left], positions[right]))
    return (
        tuple(sorted(page_signatures)),
        tuple(sorted(image_signatures)),
        tuple(sorted(relation_signatures)),
    )


def certified_accepted_source_reuse(corpus, ingestion, graph) -> tuple[bool, int | None]:
    """Return an authorization-bound source proof, not a visual-review assertion.

    Fail closed for a modified book, replaced OCR, missing region/figure, wrong
    actor, incomplete source graph or missing previously finalized revision.
    """
    if not graph.pages or any(page.source_material != "accepted_reuse" for page in graph.pages):
        return False, None
    try:
        doc_id = str(ingestion["document_id"])
        owner = str(ingestion["owner_user_id"])
        sha = str(ingestion["source_sha256"])
        doc = corpus.one("rkb_documents", doc_id)
        if not doc or doc.get("owner_user_id") != owner or doc.get("source_sha256") != sha:
            return False, None
        if (doc.get("source_archive_status") != "verified" or
            not doc.get("source_archive_ref") or len(sha) != 64):
            return False, None
        revision = int(doc["active_revision"])
        if revision < 1 or int(ingestion["staged_revision"]) <= revision:
            return False, None
        jobs = corpus.rows("rkb_ingestion_jobs")
        if not any(
            job.get("document_id") == doc_id and
            job.get("owner_user_id") == owner and
            job.get("source_sha256") == sha and
            job.get("state") == "finalized" and
            int(job.get("staged_revision") or 0) == revision
            for job in jobs
        ):
            return False, None
        old_pages = {
            str(page["id"]): page for page in corpus.rows("rkb_pages")
            if page.get("document_id") == doc_id and
            int(page.get("revision") or 0) == revision
        }
        if not old_pages or len(old_pages) != int(doc["page_count"]):
            return False, None
        if len(graph.pages) != len(old_pages):
            return False, None
        old_regions = defaultdict(list)
        for reg in corpus.rows("rkb_regions"):
            if str(reg.get("page_id") or "") in old_pages:
                old_regions[str(reg["page_id"])].append(reg)
        old_ids = {str(reg["id"]) for records in old_regions.values() for reg in records}
        images = [
            item for item in corpus.rows("rkb_illustrations")
            if str(item.get("page_id") or "") in old_pages
        ]
        relations = [
            item for item in corpus.rows("rkb_region_relations")
            if str(item.get("source_region_id") or "") in old_ids or
               str(item.get("target_region_id") or "") in old_ids
        ]
        old = _signature(old_pages, old_regions, images, relations)
        new = _signature(graph.pages, {}, graph.illustrations, graph.relations, staged=True)
        return old == new, revision if old == new else None
    except (KeyError, TypeError, ValueError, AttributeError, json.JSONDecodeError):
        return False, None
