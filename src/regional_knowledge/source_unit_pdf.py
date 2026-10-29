"""Fixed A4 renderer for an already authorized complete accepted source unit.

Pure source helper intended for the existing Kaggle process, outside read hot
paths. The caller MUST resolve full exact accepted spans (not bounded previews
or search passages), verified edition metadata, and recheck actor/source/revision
at delivery. This module cannot authorize a user or prove corpus eligibility.
No transport, persistence, demand, model, or external-resource access lives here.
"""
from __future__ import annotations

import html
import io
import re
import time
from itertools import islice
from uuid import UUID

from .contracts import Region, RegionKind, StartMetadataInput

SERVICE_CAPTION = ('Материал подготовлен сервисом „База знаний региона Калининградская область“ '
    'на основе указанного первоисточника. Текст автора сохранён; вёрстка выполнена сервисом')
MAX_CHARACTERS = 250_000
MAX_BLOCKS = 2000
MAX_PAGES = 128
MAX_BYTES = 20 * 1024 * 1024
MAX_SECONDS = 20
_FONT_SIZE = 11.5
_LINE_HEIGHT = _FONT_SIZE * 1.3
_MARGIN = 56


class SourceUnitPdfError(ValueError):
    """A complete faithful PDF cannot be produced; never return partial bytes."""


def _blocks(unit, regions, accepted_revision):
    if (unit.get('state') != 'complete' or unit.get('source_spans_has_more')
            or not unit.get('source_spans') or not unit.get('start_anchor') or not unit.get('end_anchor')
            or unit.get('source_span_count',len(unit.get('source_spans',[]))) != len(unit.get('source_spans',[]))):
        raise SourceUnitPdfError('unit_not_ready')
    first = unit['source_spans'][0]
    anchor = unit['start_anchor']
    if any(first.get(key) != anchor.get(key) for key in ('page_id','region_id','physical_page_index')) or first.get('start') != anchor.get('offset'):
        raise SourceUnitPdfError('exact_source_start_required')
    if type(accepted_revision) is not int or accepted_revision < 1:
        raise SourceUnitPdfError('accepted_revision_required')
    try: UUID(unit['source_id'])
    except (KeyError, ValueError, TypeError):
        raise SourceUnitPdfError('invalid_source_identity') from None
    if not unit.get('article_id') or not unit.get('title'):
        raise SourceUnitPdfError('unit_identity_required')
    if len(regions) > MAX_BLOCKS or len(unit['source_spans']) > MAX_BLOCKS:
        raise SourceUnitPdfError('render_limit_exceeded')
    lookup = {}
    for region in regions:
        if type(region) is not Region or region.region_id in lookup:
            raise SourceUnitPdfError('exact_source_regions_required')
        lookup[region.region_id] = region
    blocks = []
    prior = None
    passed_pages = set()
    for index,span in enumerate(unit['source_spans']):
        region = lookup.get(span.get('region_id'))
        start, end = span.get('start'), span.get('end')
        page = span.get('physical_page_index')
        if (region is None or region.page_id != span.get('page_id')
                or region.kind != span.get('kind') or type(page) is not int or page < 0
                or type(start) is not int or type(end) is not int
                or not 0 <= start < end <= len(region.source_text)):
            raise SourceUnitPdfError('exact_source_span_required')
        if ((index > 0 and start != 0)
                or (index < len(unit['source_spans'])-1 and end != len(region.source_text))):
            raise SourceUnitPdfError('contiguous_source_range_required')
        if prior is not None:
            if page != prior[0]:
                passed_pages.add(prior[0])
                if page in passed_pages:
                    raise SourceUnitPdfError('source_order_or_overlap_invalid')
            elif (region.reading_order, start) < prior[1:]:
                raise SourceUnitPdfError('source_order_or_overlap_invalid')
        prior = (page, region.reading_order, end)
        if region.needs_review:
            raise SourceUnitPdfError('source_review_required')
        # No footnote callout/cell/list semantics are inferred from plain text.
        # These significant structures need their actual accepted structure.
        if region.kind not in (RegionKind.BODY, RegionKind.HEADING):
            raise SourceUnitPdfError('unsupported_structure:' + region.kind.value)
        text = region.source_text[start:end]
        if not text.strip() or any(ord(c) < 32 and c not in '\n\t\r' for c in text):
            raise SourceUnitPdfError('unsupported_source_text')
        blocks.append((region.kind.value, text))
    last = unit['source_spans'][-1]
    terminal = unit['end_anchor']
    end_region = lookup.get(terminal.get('region_id'))
    end_page, end_offset = terminal.get('physical_page_index'), terminal.get('offset')
    if (end_region is None or end_region.page_id != terminal.get('page_id')
            or type(end_page) is not int or end_page < 0 or type(end_offset) is not int
            or not 0 <= end_offset <= len(end_region.source_text)):
        raise SourceUnitPdfError('exact_source_end_required')
    if end_region.region_id == last['region_id']:
        if (end_page != last['physical_page_index'] or end_offset != last['end']):
            raise SourceUnitPdfError('exact_source_end_required')
    else:
        # A half-open boundary at the next region's offset zero contributes no
        # text there. The caller includes that accepted endpoint Region too,
        # so its page/order can be checked without inventing an empty block.
        last_region = lookup[last['region_id']]
        if (end_offset != 0 or last['end'] != len(last_region.source_text)
                or (end_page == last['physical_page_index']
                    and end_region.reading_order <= last_region.reading_order)):
            raise SourceUnitPdfError('exact_source_end_required')
    if sum(len(text) for _, text in blocks) > MAX_CHARACTERS:
        raise SourceUnitPdfError('render_limit_exceeded')
    return blocks


def _bibliography(unit, edition, original_pages):
    if type(edition) is not StartMetadataInput or not edition.title:
        raise SourceUnitPdfError('verified_edition_required')
    if not original_pages or any(type(p) is not str or not p.strip() for p in original_pages):
        raise SourceUnitPdfError('verified_original_page_labels_required')
    values = ['; '.join(edition.authors), edition.title, unit['title']]
    if edition.publication_year is not None: values.append(str(edition.publication_year))
    catalog = edition.catalog
    if catalog is not None:
        for name in ('subtitle','publisher','date','edition','volume','issue'):
            value = getattr(catalog, name)
            if value: values.append(value)
        for item in catalog.identifiers:
            # Preserve the existing catalog's exact identifier label/value
            # objects; no guessed identifier schema or invented values.
            for label,value in item.items():
                if not label.strip() or not value.strip():
                    raise SourceUnitPdfError('unsupported_bibliographic_identifier')
                values.append(label + ': ' + value)
    values += ['Исходные страницы: ' + ', '.join(original_pages), SERVICE_CAPTION]
    return [value for value in values if value]


def render_source_unit_pdf(*, unit, regions, accepted_revision, edition, original_pages):
    """Return complete PDF bytes or an explicit failure, never a partial file.

    ``unit`` is the full internal completed source_units projection, ``regions``
    are existing Region objects with original source_text. A source span is one
    explicit supplied author block; internal newlines are retained. Callers must
    not infer paragraph/list/note structure or pass the 16-span public preview.
    Bibliography uses existing StartMetadataInput/CatalogMetadata fields already
    verified by the caller. Revision is a required binding, not an auth proof.
    Unsupported significant region kinds fail until their structure is wired.
    """
    import pymupdf
    started = time.monotonic()
    blocks = _blocks(unit, regions, accepted_revision)
    bibliography = _bibliography(unit, edition, original_pages)
    if sum(map(len, bibliography)) + sum(len(t) for _, t in blocks) > MAX_CHARACTERS:
        raise SourceUnitPdfError('render_limit_exceeded')
    paper = pymupdf.paper_rect('a4')
    bottom = paper.height - _MARGIN
    full_lines = int((bottom - _MARGIN) / _LINE_HEIGHT)

    def check():
        if time.monotonic() - started > MAX_SECONDS:
            raise SourceUnitPdfError('render_deadline_exceeded')

    def story(text, kind='body'):
        # Text is never interpreted as author HTML, resource URLs, or CSS.
        escaped = html.escape(text).replace('\r\n','\n').replace('\r','\n').replace('\n','<br>')
        # MuPDF ignores CSS ligature controls. Separate literal ASCII f nodes
        # preserve fi/ff/ffi in ToUnicode without normalizing original ligatures.
        escaped = escaped.replace('f','<span>f</span>')
        indent = '14pt' if kind == 'body' else '0'
        weight = 'bold' if kind == 'heading' else 'normal'
        css = (f'body {{font-family:serif;font-size:{_FONT_SIZE}pt;'
            f'line-height:1.3;margin:0}} p {{margin:0;text-indent:{indent};font-weight:{weight}}}')
        check()
        value = pymupdf.Story(html='<p>'+escaped+'</p>',user_css=css)
        more, filled = value.place(pymupdf.Rect(_MARGIN,_MARGIN,paper.width-_MARGIN,100000))
        if more: raise SourceUnitPdfError('render_limit_exceeded')
        lines = max(1, round(pymupdf.Rect(filled).height / _LINE_HEIGHT))
        value.reset()
        return value, lines

    # Premeasure actual shaped text at the final width. CSS pagination hints
    # are deliberately not trusted: MuPDF currently ignores several of them.
    prepared = [(kind,text,*story(text,kind)) for kind,text in blocks]
    if prepared[-1][0] == 'heading':
        raise SourceUnitPdfError('orphan_heading_without_body')
    stream = io.BytesIO()
    writer = pymupdf.DocumentWriter(stream)
    pages = 0
    device = None
    y = _MARGIN

    def end_page():
        footer,_ = story(f'Страница PDF {pages}','metadata')
        footer.place(pymupdf.Rect(_MARGIN,paper.height-35,paper.width-_MARGIN,paper.height-15))
        footer.draw(device)
        writer.end_page()

    def new_page():
        nonlocal pages, device, y
        check()
        if device is not None: end_page()
        if pages >= MAX_PAGES: raise SourceUnitPdfError('render_page_limit_exceeded')
        device = writer.begin_page(paper)
        pages += 1
        y = _MARGIN

    try:
        new_page()
        # The first page is the bibliographic service passport, separate from
        # the immutable author sequence which starts on the following page.
        for text in bibliography:
            check()
            value, lines = story(text,'metadata')
            if y + lines * _LINE_HEIGHT > bottom:
                raise SourceUnitPdfError('bibliography_page_overflow')
            more, filled = value.place(pymupdf.Rect(_MARGIN,y,paper.width-_MARGIN,bottom))
            if more: raise SourceUnitPdfError('bibliography_page_overflow')
            value.draw(device)
            y = pymupdf.Rect(filled).y1 + _LINE_HEIGHT
        new_page()
        for index, (kind,text,value,remaining) in enumerate(prepared):
            check()
            if kind == 'heading':
                needed = remaining + 0.4
                for next_kind,_,_,next_lines in islice(prepared,index+1,None):
                    needed += next_lines + 0.4 if next_kind == 'heading' else next_lines if next_lines <= 4 else 2
                    if next_kind != 'heading': break
                if needed > full_lines: raise SourceUnitPdfError('heading_group_too_large')
                if y + needed * _LINE_HEIGHT > bottom: new_page()
            while remaining:
                check()
                available = int((bottom-y+0.001) / _LINE_HEIGHT)
                if available < min(2,remaining) or remaining <= 4 and remaining > available:
                    new_page(); continue
                take = min(remaining,available)
                if remaining-take == 1: take -= 1
                if take < 1: new_page(); continue
                more, filled = value.place(pymupdf.Rect(_MARGIN,y,paper.width-_MARGIN,
                    y + take * _LINE_HEIGHT + 0.01))
                consumed = round(pymupdf.Rect(filled).height / _LINE_HEIGHT)
                if consumed != take or bool(more) != (remaining > take):
                    raise SourceUnitPdfError('layout_measurement_mismatch')
                value.draw(device)
                remaining -= consumed
                y = pymupdf.Rect(filled).y1
                if remaining: new_page()
            y += _LINE_HEIGHT * 0.4
        end_page(); device = None
    finally:
        writer.close()
    check()
    raw = stream.getvalue()
    if len(raw) > MAX_BYTES: raise SourceUnitPdfError('render_byte_limit_exceeded')
    with pymupdf.open(stream=raw,filetype='pdf') as document:
        # Actual selectable text and embedded Unicode maps, not a successful
        # library return, are the fidelity boundary. Layout whitespace differs.
        expected = ''.join(bibliography) + ''.join(text for _,text in blocks)
        extracted_parts = []
        for number,page in enumerate(document,start=1):
            check()
            if any(word[0] < _MARGIN-1 or word[2] > paper.width-_MARGIN+1
                    or word[1] < 40 or word[3] > paper.height-15
                    for word in page.get_text('words')):
                raise SourceUnitPdfError('layout_bounds_exceeded')
            text = page.get_text(sort=False)
            footer = f'Страница PDF {number}\n'
            if not text.endswith(footer): raise SourceUnitPdfError('render_page_number_failed')
            extracted_parts.append(text[:-len(footer)])
        extracted = ''.join(extracted_parts)
        flatten = lambda text: re.sub(r'[ \t\r\n]+', '', text)
        if '\ufffd' in extracted or flatten(extracted) != flatten(expected):
            raise SourceUnitPdfError('render_text_fidelity_failed')
        fonts = {row[0] for page in document for row in page.get_fonts(full=True)}
        if not fonts or any('/ToUnicode' not in document.xref_object(xref)
                or not document.extract_font(xref)[3] for xref in fonts):
            raise SourceUnitPdfError('embedded_unicode_font_required')
        document.set_metadata(dict(title=unit['title'],author='; '.join(edition.authors),
            subject=edition.title,creator='Regional Knowledge Base',
            keywords=f"source:{unit['source_id']} unit:{unit['article_id']} accepted_revision:{accepted_revision}"))
        raw = document.tobytes(garbage=3,deflate=True,no_new_id=True)
    check()
    if len(raw) > MAX_BYTES: raise SourceUnitPdfError('render_byte_limit_exceeded')
    return raw
