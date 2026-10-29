"""Accepted-source PDF assembly inside Kaggle; never use search passage previews."""
from __future__ import annotations

from uuid import UUID, uuid5

from .contracts import Region, StartMetadataInput
from .native_snapshot_transfer import _blocking
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
        raise SourceUnitPdfError("unit_not_ready: статья ещё не завершена; продолжите book_ingest до закрытия границ и приёмки всех страниц")
    return resolve_source_units(pages, source_id=source_id, page_count=page_count,
        read_complete=read_complete, after_ordinal=match["ordinal"]-1, limit=1)[0]


def _unit_pages(unit):
    return list(dict.fromkeys([span["physical_page_index"] for span in unit["source_spans"]]
        + [unit["end_anchor"]["physical_page_index"]]))


def render_accepted_unit(*, unit, pages, edition, annotation_ru=None, translation_ru=None):
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
    original_pages = tuple(str(printed[index]) for index in _unit_pages(unit))
    return render_source_unit_pdf(unit=unit,
        regions=[lookup[span] for span in selected],
        accepted_revision=1, edition=StartMetadataInput.model_validate(edition),
        original_pages=original_pages,annotation_ru=annotation_ru,
        translation_ru=translation_ru)


async def render_accepted_source_pdf(*, acquire, source_id, article_id,
                                     annotation_ru=None, translation_ru=None):
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
                selected = _unit_pages(unit)
                await cursor.execute("""SELECT page,edition FROM
                    rkb_v4.read_source_unit_export_pages_v1(%s::uuid,%s::integer[])""",
                    (source_id,selected))
                material = await cursor.fetchall()
                if len(material) != len(selected):
                    raise SourceUnitPdfError("unit_not_ready: not all accepted pages are available")
                pages = [row[0] for row in material]
                if [p["physical_page_index"] for p in pages] != selected:
                    raise SourceUnitPdfError("unit_not_ready: accepted page order differs")
                edition = material[0][1]
    # Keep the read's resource until actual rendering exits, even after cancel.
    return await _blocking(lambda: render_accepted_unit(unit=unit,pages=pages,edition=edition,
        annotation_ru=annotation_ru,translation_ru=translation_ru))
