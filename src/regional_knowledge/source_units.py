"""Deterministic source-unit anchors from accepted model-reviewed pages.

Only the existing book_ingest stage model chooses boundaries. No BGE passages,
native OCR, synthetic book text, or reinterpretation of historical commands.
The caller supplies *text-published* page projections in original reading order.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from uuid import UUID, uuid5


class SourceUnitBoundaryError(ValueError):
    """Invalid accepted page projection or resolver request."""


def ordered_model_pages(pages):
    """Follow the model's predecessor links; old accepted pages use scan order."""
    pages = list(pages)
    def value(page, key):
        return page.get(key) if isinstance(page, dict) else getattr(page, key, None)
    by_index = {value(page, "physical_page_index"): page for page in pages}
    if len(by_index) != len(pages):
        raise SourceUnitBoundaryError("duplicate physical page")
    successor, roots = {}, []
    for index, page in by_index.items():
        previous = value(page, "previous_physical_page_index")
        if previous is None:
            previous = index - 1 if index else None
        if previous == index or previous in successor and previous in by_index:
            raise SourceUnitBoundaryError("ambiguous page reading predecessor")
        if previous in by_index:
            successor[previous] = index
        else:
            roots.append(index)
    ordered, breaks, seen = [], [], set()
    for root in sorted(roots):
        if ordered:
            breaks.append(len(ordered))
        current = root
        while current is not None:
            if current in seen:
                raise SourceUnitBoundaryError("cyclic page reading order")
            seen.add(current)
            ordered.append(by_index[current])
            current = successor.get(current)
    if len(ordered) != len(pages):
        raise SourceUnitBoundaryError("cyclic page reading order")
    return ordered, breaks


def resolve_source_units(
    pages, *, source_id: str, page_count: int, read_complete: bool,
    after_ordinal: int | None = None, limit: int | None = None,
    span_preview_limit: int | None = None,
):
    """Resolve half-open author spans from model-confirmed accepted boundaries.

    Every accepted page/boundary is validated regardless of the requested
    cursor. The ordered region index is built once; only selected units scan
    their *own* nonempty region range. Read pagination never has to construct
    the other units' spans. With no limits, retain the original full result.
    """
    UUID(source_id)
    if type(page_count) is not int or not 1 <= page_count <= 2500:
        raise SourceUnitBoundaryError("verified physical page count required")
    if type(read_complete) is not bool or type(pages) not in (tuple, list):
        raise SourceUnitBoundaryError("accepted source page projection required")
    if after_ordinal is not None and (type(after_ordinal) is not int or after_ordinal < 0):
        raise SourceUnitBoundaryError("nonnegative unit cursor required")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise SourceUnitBoundaryError("positive unit limit required")
    if span_preview_limit is not None and (type(span_preview_limit) is not int or span_preview_limit < 1):
        raise SourceUnitBoundaryError("positive source span preview limit required")

    ordered_pages, breaks = ordered_model_pages(pages)
    reading_rank = {page["physical_page_index"]: n for n, page in enumerate(ordered_pages)}
    regions = []
    seen_pages = set()
    events = []
    invalid = set()
    for page in ordered_pages:
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
        # Source regions need not arrive already sorted inside a model page.
        regions.extend(sorted(keyed.values(), key=lambda r: r["reading_order"]))
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
                invalid.add(marker["article_id"])
                continue
            position = (reading_rank[number], item["reading_order"], offset)
            within_page = (item["reading_order"], offset, 0 if action == "end" else 1)
            if within_page < prior:
                invalid.add(marker["article_id"])
                continue
            prior = within_page
            events.append((position, dict(item, offset=offset), marker))

    # Build the index exactly once, including gaps and zero-length regions.
    region_positions = [(reading_rank[r["physical_page_index"]], r["reading_order"]) for r in regions]
    nonempty_indices = [i for i, r in enumerate(regions) if r["source_length"] > 0]
    missing_pages = sorted(set(range(page_count)) - seen_pages)

    # The stable start ordinal is unchanged: source reading order, then ID
    # for different unit kinds beginning at one exact coordinate.
    started, opened, open_by_kind, ended, next_boundary_ends = {}, {}, {}, {}, set()
    last_start = None
    for position, anchor, marker in events:
        ident = marker["article_id"]  # Reuse the existing StageChunkInput.article_id.
        if marker["action"] == "start":
            last_start = ident
            if ident in started:
                invalid.add(ident)
                continue
            # At most one open unit per kind: a confirmed next same-kind start
            # closes the prior one. Nested units of another kind stay open.
            previous = open_by_kind.get(marker["kind"])
            if previous is not None:
                if position <= opened[previous]:
                    invalid.update((previous, ident))
                ended[previous] = (position, anchor)
                next_boundary_ends.add(previous)
                del opened[previous]
            started[ident] = (position, anchor, marker)
            opened[ident] = position
            open_by_kind[marker["kind"]] = ident
        else:
            if ident not in opened:
                if ident in ended and last_start == ident and position > ended[ident][0]:
                    ended[ident] = (position, anchor)
                else:
                    invalid.add(ident)
                continue
            if position <= opened[ident]:
                invalid.add(ident)
                continue
            ended[ident] = (position, anchor)
            del opened[ident]
            del open_by_kind[started[ident][2]["kind"]]

    has_eof = read_complete and not missing_pages and not breaks
    if has_eof:
        final = next((r for r in reversed(regions) if r["source_length"]), None)
        eof = ((reading_rank[final["physical_page_index"]], final["reading_order"], final["source_length"]),
               dict(final, offset=final["source_length"])) if final else None
    else:
        eof = None

    starts = sorted(started.items(), key=lambda pair: (pair[1][0], pair[0]))
    skipped = after_ordinal or 0
    selected = starts[skipped:skipped + limit] if limit is not None else starts[skipped:]
    output = []
    for ordinal, (ident, (start_pos, start, declaration)) in enumerate(selected, start=skipped + 1):
        finish = ended.get(ident)
        close_reason = ("next_boundary" if ident in next_boundary_ends
                        else "boundary" if finish is not None else None)
        if finish is None and eof is not None and eof[0] > start_pos:
            finish = eof
            close_reason = "verified_eof"
        stop_pos, stop = finish if finish is not None else (None, None)
        pages_complete = False
        if stop_pos is not None and ident not in invalid:
            pages_complete = not any(start_pos[0] < at <= stop_pos[0] for at in breaks)

        spans = []
        span_count = 0
        if pages_complete:
            # Boundaries are exact *regions*, so half-open [start,end] region
            # range lookup is logarithmic; offset=0 excludes the ending region.
            left = bisect_left(region_positions, start_pos[:2])
            right = bisect_right(region_positions, stop_pos[:2])
            first_nonempty = bisect_left(nonempty_indices, left)
            after_nonempty = bisect_left(nonempty_indices, right)
            if (stop_pos[2] == 0 and right > left
                    and regions[right - 1]["source_length"] > 0):
                after_nonempty -= 1
            span_count = after_nonempty - first_nonempty
            take = span_count if span_preview_limit is None else min(span_count, span_preview_limit)
            # Index the selected nonempty regions directly. No regions[:end]
            # copy and no scan of unrelated unit regions (even for pagination).
            for n in range(first_nonempty, first_nonempty + take):
                region = regions[nonempty_indices[n]]
                pos = (reading_rank[region["physical_page_index"]], region["reading_order"])
                begin = start_pos[2] if pos == start_pos[:2] else 0
                end = stop_pos[2] if pos == stop_pos[:2] else region["source_length"]
                if begin < end:
                    spans.append({k: region[k] for k in (
                        "page_id", "physical_page_index", "region_id", "region_key", "kind")}
                        | {"start": begin, "end": end})

        unit = dict(ordinal=ordinal, source_id=source_id, article_id=ident,
            kind=declaration["kind"], title=declaration["title"],
            start_anchor={k: start[k] for k in (
                "page_id", "physical_page_index", "region_id", "region_key", "offset")},
            end_anchor=({k: stop[k] for k in (
                "page_id", "physical_page_index", "region_id", "region_key", "offset")}
                if pages_complete else None),
            close_reason=close_reason if pages_complete else None,
            boundary_status="closed" if finish is not None else "open",
            state="complete" if pages_complete and span_count else "unit_not_ready",
            source_spans=spans if pages_complete else [])
        if span_preview_limit is not None:
            unit["source_span_count"] = span_count
            unit["source_spans_has_more"] = span_count > span_preview_limit
        output.append(unit)
    return output


def bounded_unit_records(units, *, after_ordinal=None, limit=8):
    """Source read result contract: bounded preview; exact spans stay derivable."""
    selected = [unit for unit in units if after_ordinal is None
                or unit["ordinal"] > after_ordinal]
    records = []
    for unit in selected[:limit]:
        item = {k: v for k, v in unit.items() if k != "source_spans"}
        spans = unit["source_spans"]
        count = unit.get("source_span_count", len(spans))
        item.update(source_spans=spans[:16], source_span_count=count,
                    source_spans_has_more=count > 16)
        records.append(item)
    return tuple(records), records[-1]["ordinal"] if len(selected) > limit else None
