"""Deterministic source-unit anchors from accepted model-reviewed pages.

Only the existing book_ingest stage model chooses boundaries. No BGE passages,
native OCR, synthetic book text, or reinterpretation of historical commands.
The caller supplies *text-published* page projections in original reading order.
"""
from __future__ import annotations

from uuid import UUID, uuid5


class SourceUnitBoundaryError(ValueError):
    """Conflicting accepted unit boundaries: never infer a repair."""


def resolve_source_units(pages, *, source_id: str, page_count: int, read_complete: bool):
    """Resolve original Unicode-codepoint ranges without copying author text.

    An explicit end boundary closes a unit at an exact half-open coordinate.
    Only verified reading completion AND publication of every physical page
    can close otherwise open units at EOF. Gaps make even an ended unit unready.
    """
    UUID(source_id)
    if type(page_count) is not int or not 1 <= page_count <= 2500:
        raise SourceUnitBoundaryError("verified physical page count required")
    if type(read_complete) is not bool or type(pages) not in (tuple, list):
        raise SourceUnitBoundaryError("accepted source page projection required")
    regions = []
    seen_pages = set()
    events = []
    for page in sorted(pages, key=lambda p: p["physical_page_index"]):
        number = page["physical_page_index"]
        page_id = page["page_id"]
        if (type(number) is not int or not 0 <= number < page_count
                or number in seen_pages or str(UUID(page_id)) != page_id):
            raise SourceUnitBoundaryError("duplicate or invalid published page")
        seen_pages.add(number)
        keyed = {}
        positions = set()
        for raw in page["regions"]:
            key = raw["region_key"]
            order = raw["reading_order"]
            length = raw.get("source_length")
            if length is None and "source_text" in raw:
                length = len(raw["source_text"])
            if (type(key) is not str or type(order) is not int or type(length) is not int
                    or order < 0 or length < 0 or key in keyed or order in positions):
                raise SourceUnitBoundaryError("invalid source region reading order")
            positions.add(order)
            item = dict(page_id=page_id, physical_page_index=number,
                        region_id=str(uuid5(UUID(page_id), "region:" + key)),
                        region_key=key, reading_order=order, kind=raw["kind"],
                        source_length=length)
            keyed[key] = item
            regions.append(item)
        prior = (-1, -1, -1)
        for marker in page.get("source_unit_boundaries") or []:
            key = marker["region_key"]
            item = keyed.get(key)
            offset = marker["offset"]
            action = marker["action"]
            if (item is None or type(offset) is not int
                    or not 0 <= offset <= item["source_length"]
                    or action not in ("start", "end")
                    or (action == "start" and offset == item["source_length"])):
                raise SourceUnitBoundaryError("unit anchor outside original source region")
            position = (number, item["reading_order"], offset)
            within_page = (item["reading_order"], offset, 0 if action == "end" else 1)
            if within_page < prior:
                raise SourceUnitBoundaryError("model unit anchors out of reading order")
            prior = within_page
            events.append((position, dict(item, offset=offset), marker))

    regions.sort(key=lambda r: (r["physical_page_index"], r["reading_order"]))
    # A source with no unit declarations remains an ordinary accepted,
    # searchable source; old replay does not invent section boundaries.
    started, opened, ended, next_boundary_ends = {}, {}, {}, set()
    for position, anchor, marker in events:
        ident = marker["article_id"]  # Reuse the existing StageChunkInput.article_id.
        if marker["action"] == "start":
            if ident in started:
                raise SourceUnitBoundaryError("duplicate source article identity")
            # A model-confirmed *next start* is itself an exact end boundary
            # for the preceding unit of the same kind, even without a
            # redundant explicit end marker. A chapter containing sections
            # stays open when a nested, differently typed section begins.
            for previous in tuple(opened):
                if started[previous][2]["kind"] == marker["kind"]:
                    if position <= opened[previous]:
                        raise SourceUnitBoundaryError("overlapping same-kind starts")
                    ended[previous] = (position, anchor)
                    next_boundary_ends.add(previous)
                    del opened[previous]
            started[ident] = (position, anchor, marker)
            opened[ident] = position
        else:
            if ident not in opened or position <= opened[ident]:
                raise SourceUnitBoundaryError("unmatched or reversed source unit end")
            ended[ident] = (position, anchor)
            del opened[ident]

    has_eof = read_complete and seen_pages == set(range(page_count))
    if has_eof and regions:
        final = next((r for r in reversed(regions) if r["source_length"]), None)
        eof = ((final["physical_page_index"], final["reading_order"], final["source_length"]),
               dict(final, offset=final["source_length"])) if final else None
    else:
        eof = None

    output = []
    for ordinal, (ident, (start_pos, start, declaration)) in enumerate(
            sorted(started.items(), key=lambda pair: (pair[1][0], pair[0])), start=1):
        finish = ended.get(ident)
        close_reason = ("next_boundary" if ident in next_boundary_ends
                        else "boundary" if finish is not None else None)
        if finish is None and eof is not None and eof[0] > start_pos:
            finish = eof
            close_reason = "verified_eof"
        stop_pos, stop = finish if finish is not None else (None, None)
        pages_complete = bool(
            stop_pos is not None and
            set(range(start_pos[0], stop_pos[0] + 1)) <= seen_pages
        )
        spans = []
        if pages_complete:
            for region in regions:
                pos = (region["physical_page_index"], region["reading_order"])
                if pos < start_pos[:2] or pos > stop_pos[:2]:
                    continue
                begin = start_pos[2] if pos == start_pos[:2] else 0
                end = stop_pos[2] if pos == stop_pos[:2] else region["source_length"]
                if begin < end:
                    spans.append({k: region[k] for k in (
                        "page_id", "physical_page_index", "region_id", "region_key", "kind")}
                        | {"start": begin, "end": end})
        # An end that is known but separated by an unread page is not export ready.
        # An open unit never gets an arbitrary last-visible-page endpoint.
        output.append(dict(ordinal=ordinal, source_id=source_id, article_id=ident,
            kind=declaration["kind"], title=declaration["title"],
            start_anchor={k: start[k] for k in (
                "page_id", "physical_page_index", "region_id", "region_key", "offset")},
            end_anchor=({k: stop[k] for k in (
                "page_id", "physical_page_index", "region_id", "region_key", "offset")}
                if pages_complete else None),
            close_reason=close_reason if pages_complete else None,
            boundary_status="closed" if finish is not None else "open",
            state="complete" if pages_complete and spans else "unit_not_ready",
            source_spans=spans if pages_complete else []))
    return output


def bounded_unit_records(units, *, after_ordinal=None, limit=8):
    """Source read result contract: bounded preview; exact spans stay derivable."""
    selected = [unit for unit in units if after_ordinal is None
                or unit["ordinal"] > after_ordinal]
    records = []
    for unit in selected[:limit]:
        item = {k: v for k, v in unit.items() if k != "source_spans"}
        spans = unit["source_spans"]
        item.update(source_spans=spans[:16], source_span_count=len(spans),
                    source_spans_has_more=len(spans) > 16)
        records.append(item)
    return tuple(records), records[-1]["ordinal"] if len(selected) > limit else None
