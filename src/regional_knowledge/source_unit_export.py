"""Accepted-source PDF assembly inside Kaggle; never use search passage previews."""
from __future__ import annotations

import asyncio
from uuid import UUID, uuid5

from .contracts import Region, StartMetadataInput
from .source_unit_pdf import SourceUnitPdfError, render_source_unit_pdf
from .source_units import resolve_source_units


def complete_unit(pages, *, source_id, page_count, read_complete, article_id):
    """Return *all* exact spans of one completed unit, never a 16-span preview."""
    preview = resolve_source_units(pages, source_id=source_id,
        page_count=page_count, read_complete=read_complete, span_preview_limit=16)
    match = next((u for u in preview if u["article_id"] == article_id), None)
    if match is None:
        raise SourceUnitPdfError("unit_not_found: select an existing article_id from book_find(view=units)")
    if match["state"] != "complete":
        raise SourceUnitPdfError("unit_not_ready: continue book_ingest until the article ends and its pages are accepted")
    return resolve_source_units(pages, source_id=source_id, page_count=page_count,
        read_complete=read_complete, after_ordinal=match["ordinal"]-1, limit=1)[0]


def render_accepted_unit(*, unit, pages, edition):
    """Pages must be the actual accepted stage JSON including complete source_text."""
    lookup = {}
    for page in pages:
        for raw in page["regions"]:
            region = Region(region_id=str(uuid5(UUID(page["page_id"]), "region:" + raw["region_key"])),
                page_id=page["page_id"], kind=raw["kind"], bbox=raw["bbox"],
                reading_order=raw["reading_order"], source_text=raw["source_text"],
                needs_review=raw.get("needs_review", False))
            if region.region_id in lookup:
                raise SourceUnitPdfError("duplicate_accepted_region")
            lookup[region.region_id] = region
    selected = {span["region_id"] for span in unit["source_spans"]}
    selected.add(unit["end_anchor"]["region_id"])
    if selected - lookup.keys():
        raise SourceUnitPdfError("unit_not_ready: accept the missing source regions before export")
    printed = {
        page["physical_page_index"]: (page.get("printed_page_number") or
            "физическая " + str(page["physical_page_index"]+1))
        for page in pages
    }
    start = unit["start_anchor"]["physical_page_index"]
    end = unit["end_anchor"]["physical_page_index"]
    original_pages = tuple(str(printed[index]) for index in range(start, end+1))
    return render_source_unit_pdf(unit=unit,
        regions=[lookup[span] for span in selected],
        accepted_revision=1, edition=StartMetadataInput.model_validate(edition),
        original_pages=original_pages)


async def render_accepted_source_pdf(*, acquire, source_id, article_id):
    """One read-only PG snapshot; PDF bytes are created only on Kaggle."""
    async with acquire() as connection:
        async with connection.transaction():
            async with connection.cursor() as cursor:
                await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                await cursor.execute("""SELECT physical_page_index,page,page_count,read_complete
                    FROM rkb_v4.read_source_unit_pages_v1(%s::uuid)
                    ORDER BY physical_page_index""", (source_id,))
                rows = await cursor.fetchall()
                if not rows:
                    raise SourceUnitPdfError("unit_not_ready: accepted source not available")
                unit = complete_unit([row[1] for row in rows], source_id=source_id,
                    page_count=rows[0][2], read_complete=rows[0][3], article_id=article_id)
                start = unit["start_anchor"]["physical_page_index"]
                end = unit["end_anchor"]["physical_page_index"]
                await cursor.execute("""SELECT page,edition FROM
                    rkb_v4.read_source_unit_export_pages_v1(%s::uuid,%s::integer,%s::integer)""",
                    (source_id,start,end))
                material = await cursor.fetchall()
                if len(material) != end-start+1:
                    raise SourceUnitPdfError("unit_not_ready: not all accepted pages are available")
                pages = [row[0] for row in material]
                if [p["physical_page_index"] for p in pages] != list(range(start,end+1)):
                    raise SourceUnitPdfError("unit_not_ready: accepted pages are not contiguous")
                edition = material[0][1]
    return await asyncio.to_thread(render_accepted_unit,unit=unit,pages=pages,edition=edition)
