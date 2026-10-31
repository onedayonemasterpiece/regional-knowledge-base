"""Accepted-source PDF assembly inside Kaggle; never use search passage previews."""
from __future__ import annotations

from uuid import UUID, uuid5
from io import BytesIO

from PIL import Image

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


def render_accepted_unit(*, unit, pages, edition, annotation_ru=None, translation_ru=None,
                         illustrations=()):
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
        translation_ru=translation_ru,illustrations=illustrations)


def _unit_figures(unit, pages, rows):
    """Accepted figures strictly inside the complete unit's model reading anchors."""
    rank={page['physical_page_index']:i for i,page in enumerate(pages)}
    positions={}
    for page in pages:
        for raw in page['regions']:
            region_id=str(uuid5(UUID(page['page_id']),'region:'+raw['region_key']))
            positions[region_id]=(rank[page['physical_page_index']],raw['reading_order'],raw)
    def anchor(item):
        location=positions.get(item['region_id'])
        if location is None:raise SourceUnitPdfError('exact_source_region_required')
        return (*location[:2],item['offset'])
    start, end=anchor(unit['start_anchor']),anchor(unit['end_anchor'])
    figures=[]
    for data,physical,ingestion in rows:
        location=positions.get(data['source_region_id'])
        if location is None or physical not in rank or location[0]!=rank[physical]:
            raise SourceUnitPdfError('accepted_illustration_region_unavailable')
        if not start <= (*location[:2],0) < end:
            continue
        if location[2]['kind']!='figure' or location[2]['bbox']!=data['bbox']:
            raise SourceUnitPdfError('accepted_illustration_geometry_changed')
        binding=data.get('crop_binding') or {}
        if not binding.get('original_sha256'):
            raise SourceUnitPdfError('pdf_illustration_original_unavailable')
        page=pages[rank[physical]]
        figures.append(dict(source_id=unit['source_id'],ingestion_id=ingestion,
            original_sha256=binding['original_sha256'],
            physical_page_index=physical,reading_order=location[1],
            source_region_id=data['source_region_id'],page_id=data['page_id'],
            bbox=data['bbox'],display_rotation_degrees=data['display_rotation_degrees'],
            caption_text=data['caption_text'],caption_region_ids=data['caption_region_ids'],
            page_label=str(page.get('printed_page_number') or
                'физическая '+str(physical+1))))
    return figures


def _pdf_figure_crop(page_jpeg,figure,budget):
    """Shared illustration_fetch geometry, readable JPEG sizing for an A4 PDF."""
    from .kaggle_illustration_preview import accepted_figure_crop
    crop=accepted_figure_crop(page_jpeg,figure)
    crop.thumbnail((1200,1200),Image.Resampling.LANCZOS)
    while True:
        for quality in (85,75,65,55):
            output=BytesIO()
            crop.save(output,format='JPEG',quality=quality,optimize=True)
            if output.tell() <= budget:return output.getvalue()
        if max(crop.size)<=640:
            raise SourceUnitPdfError('pdf_transport_limit: figure cannot remain legible')
        crop.thumbnail((max(640,crop.width*4//5),max(640,crop.height*4//5)),
            Image.Resampling.LANCZOS)


async def render_accepted_source_pdf(*, acquire, source_id, article_id,
                                     annotation_ru=None, translation_ru=None,
                                     illustration_page=None):
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
                await cursor.execute("""SELECT i.data,p.physical_page_index,s.ingestion_id::text
                    FROM rkb_v4.source_illustrations i
                    JOIN rkb_v4.source_stage_pages p ON p.source_id=i.source_id
                        AND p.page_id=(i.data->>'page_id')::uuid
                    JOIN rkb_v4.source_stage_sessions s ON s.source_id=i.source_id
                    WHERE i.source_id=%s::uuid AND p.physical_page_index=ANY(%s::integer[])
                    ORDER BY array_position(%s::integer[],p.physical_page_index),
                        i.illustration_id LIMIT 2001""", (source_id,selected,selected))
                figure_rows = await cursor.fetchall()
                if len(figure_rows)>2000:
                    raise SourceUnitPdfError('render_limit_exceeded')
                figures = _unit_figures(unit,pages,figure_rows)
    if figures and illustration_page is None:
        raise SourceUnitPdfError('pdf_illustration_original_unavailable')
    from .kaggle_protocol import RESULT_MAX_BYTES
    # The base64 WSS envelope is 256 KiB; reserve equal space for typography
    # and for each accepted picture. Actual complete PDF still has its native bound.
    budget = ((RESULT_MAX_BYTES-4096)*3//4)//(len(figures)+1)
    illustrated = []
    for figure in figures:
        page_jpeg = await illustration_page(figure)
        pixels = await _blocking(_pdf_figure_crop,page_jpeg,figure,budget)
        illustrated.append(dict(figure,jpeg=pixels))
    # Keep the read's resource until actual rendering exits, even after cancel.
    return await _blocking(lambda: render_accepted_unit(unit=unit,pages=pages,edition=edition,
        annotation_ru=annotation_ru,translation_ru=translation_ru,illustrations=illustrated))
