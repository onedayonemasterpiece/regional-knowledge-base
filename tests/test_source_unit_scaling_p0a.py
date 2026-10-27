"""P0.A scaling regression: real 20-region/page source projections, no fixtures/books.

The old implementation ran every unit across all source regions before
pagination. This suite exercises physical 100/500/1000-page envelopes using
the same one-article-per-page, limit=8 selection, checking output and growth.
"""
from __future__ import annotations

from statistics import median
from time import perf_counter
from uuid import UUID, uuid5

import pytest

from regional_knowledge.source_units import bounded_unit_records, resolve_source_units

SOURCE = "31111111-2222-4333-8444-555555555555"


def synthetic_pages(page_count: int, regions_per_page: int = 20):
    """Twenty actual distinct source regions per physical page, accepted order."""
    result = []
    for number in range(page_count):
        page_id = str(uuid5(UUID(SOURCE), f"page:1:{number}"))
        result.append(dict(
            page_id=page_id, physical_page_index=number,
            # One observed *start* per article; the next same-kind start
            # closes the previous article without a guessed page range.
            source_unit_boundaries=[dict(
                article_id=f"article-{number:04d}", action="start",
                region_key="region-00", offset=0,
                kind="article", title=f"Article {number:04d}")],
            regions=[dict(
                region_key=f"region-{n:02d}", reading_order=n,
                kind="footnote" if n == 19 else "body",
                source_length=len(f"Real synthetic text p{number} region{n}."))
                for n in range(regions_per_page)],
        ))
    return result


def page_read(pages, *, after_ordinal=None, limit=8):
    # This is the actual SOURCE_READ selection contract, with one lookahead.
    rows = resolve_source_units(pages, source_id=SOURCE,
        page_count=len(pages), read_complete=True,
        after_ordinal=after_ordinal, limit=limit + 1,
        span_preview_limit=16)
    return bounded_unit_records(rows, after_ordinal=after_ordinal, limit=limit)


def test_synthetic_one_article_per_page_exact_bounded_first_middle_last_and_eof():
    pages = synthetic_pages(1000)
    before = [dict(page) for page in pages]
    first, cursor = page_read(pages)
    assert len(first) == 8 and cursor == 8
    assert [x["article_id"] for x in first] == [
        f"article-{n:04d}" for n in range(8)]
    assert all(x["state"] == "complete" for x in first)
    assert all(x["source_span_count"] == 20 and len(x["source_spans"]) == 16
               and x["source_spans_has_more"] for x in first)
    assert first[0]["start_anchor"]["physical_page_index"] == 0
    assert first[0]["end_anchor"]["physical_page_index"] == 1
    assert first[0]["end_anchor"]["offset"] == 0
    assert first[0]["source_spans"][0]["region_key"] == "region-00"
    assert first[0]["source_spans"][-1]["region_key"] == "region-15"
    assert all(s["physical_page_index"] == 0
               for s in first[0]["source_spans"])
    middle, next_cursor = page_read(pages, after_ordinal=500)
    assert len(middle) == 8 and next_cursor == 508
    assert [x["ordinal"] for x in middle] == list(range(501, 509))
    assert middle[0]["article_id"] == "article-0500"
    assert middle[0]["start_anchor"]["physical_page_index"] == 500
    last, end_cursor = page_read(pages, after_ordinal=992)
    assert len(last) == 8 and end_cursor is None
    assert last[-1]["article_id"] == "article-0999"
    assert last[-1]["close_reason"] == "verified_eof"
    assert last[-1]["end_anchor"]["physical_page_index"] == 999
    assert last[-1]["end_anchor"]["offset"] == pages[-1]["regions"][-1]["source_length"]
    assert pages == before  # Accepted projections are never rewritten.


def test_long_single_unit_count_without_materializing_20000_spans_in_preview():
    pages = synthetic_pages(1000)
    for page in pages[1:]:
        page["source_unit_boundaries"] = []
    chosen, cursor = page_read(pages)
    assert cursor is None and len(chosen) == 1
    unit = chosen[0]
    assert unit["article_id"] == "article-0000"
    assert unit["state"] == "complete"
    assert unit["source_span_count"] == 20_000
    assert len(unit["source_spans"]) == 16
    assert unit["source_spans_has_more"]
    assert unit["close_reason"] == "verified_eof"
    # The full legacy resolver still provides all source spans on explicit
    # request, in exactly the same original reading order.
    full = resolve_source_units(pages, source_id=SOURCE,
        page_count=1000, read_complete=True)
    assert len(full[0]["source_spans"]) == 20_000
    assert full[0]["source_spans"][:16] == unit["source_spans"]


def test_zero_length_regions_gaps_and_same_region_half_open_offsets():
    pages = synthetic_pages(3, regions_per_page=3)
    pages[0]["regions"][1]["source_length"] = 0
    pages[1]["source_unit_boundaries"] = [
        dict(article_id="article-0000", action="end", region_key="region-00", offset=0),
        dict(article_id="article-0001", action="start", region_key="region-00", offset=2,
             kind="article", title="Article 0001")]
    # In the source, the first article ends at the next page's first region,
    # leaving its 0-length region out of the count.
    units = resolve_source_units(pages, source_id=SOURCE, page_count=3,
        read_complete=False, span_preview_limit=16)
    assert units[0]["source_span_count"] == 2
    assert [x["region_key"] for x in units[0]["source_spans"]] == ["region-00", "region-02"]
    assert units[1]["start_anchor"]["offset"] == 2
    # An unaccepted page inside known boundaries denies completion.
    hole = resolve_source_units([pages[0], pages[2]], source_id=SOURCE,
        page_count=3, read_complete=True, span_preview_limit=16)
    assert hole[0]["state"] == "unit_not_ready"
    assert hole[0]["end_anchor"] is None


def test_bounded_growth_on_100_versus_1000_accepted_pages():
    """Relative regression, not a live Kaggle or Dev latency SLA.

    Old O(U*R) shows roughly quadratic 100->1000 growth. The indexed
    implementation does one shared O(R) pass and bisected bounded previews.
    Repeated medians reduce CI scheduler noise; 30x is intentionally relaxed
    relative to 10x corpus size and rejects the demonstrated ~100x growth.
    """
    corpus = synthetic_pages(1000)
    def sample(n):
        pages = corpus[:n]
        samples = []
        for _ in range(3):
            start = perf_counter()
            records, cursor = page_read(pages)
            elapsed = perf_counter() - start
            assert len(records) == 8 and cursor == 8
            assert records[0]["source_span_count"] == 20
            samples.append(elapsed)
        return median(samples)
    small = sample(100)
    large = sample(1000)
    assert large < small * 30, (
        f"bounded 8-unit source read scaling regressed: 100p={small:.4f}s, "
        f"1000p={large:.4f}s; this is a synthetic comparative guard, not an SLA"
    )
