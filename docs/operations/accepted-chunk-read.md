# Accepted source chunks by physical page

The existing MCP `fetch` tool accepts a second, read-only form for a chunking reviewer in the **Kaggle native** profile. The ordinary `fetch(id=...)` passage continuation is unchanged.

## Request

Use `fetch(source_id=<canonical source UUID>, page_start=0, page_end=3, limit=8)`, omitting `id`. Page indices are **zero-based physical page indices**, inclusive; a request covers one to four physical pages (0–2499). To advance through a range with more than eight intersecting chunks, pass `after_ordinal` from `next_after_ordinal` to the same tool, source and page range. The maximum page range and item limit apply independently. Unknown ranges produce no chunks, never unaccepted staging or original page images.

## Response

The read returns `status`, `source_id`, `page_start`, `page_end`, `chunks`, `has_more`, `next_after_ordinal` and existing read trace/frontier metadata. Each accepted chunk has its real `chunk_id`, `source_id`, accepted source `ordinal`, full accepted `text`, `pages` and `regions`.

The `pages` array has exact `page_id` and `physical_page_index`. Raw page metadata and image receipts remain private to the writer role. The `regions` array has `region_id`, `page_id`, `start`, `end`, and `role` (`body` or `footnote`). Offsets are **[start, end) Unicode code-point offsets** into the accepted model-transcribed source region, not byte offsets into the original PDF. A chunk spanning the boundary of the requested range includes all its referenced pages and spans, so the reviewer can assess continuity. Chunk ordering uses immutable accepted `passage_search.ordinal`, not alphabetic UUID or page boundaries.

## Authority and implementation

The existing MCP fetch principal, `public_search` capability, controller `SourceReadGate`, current PostgreSQL/worker binding, source withdrawal checks and request-local final output issuance all remain mandatory. A returned chunk and its exact per-source revision dependencies are validated and rechecked before final output; a source UUID alone grants no read rights. Legacy-profile reads do **not** fall back to another backend.

The native `SOURCE_READ` operation `accepted_chunks` selects only accepted, non-withdrawn `passage_search` rows with the active revision. It joins existing `source_passage_provenance`, `source_stage_pages`, `source_regions` and `source_visibility` in a bounded read-only repeatable-read transaction. No new SQL migration, table, service, durable queue, source import, model execution or search-ranking change is introduced. A missing or inconsistent region/page/offset binding fails closed instead of inventing the missing provenance.

Tests: `tests/test_kaggle_source_read.py`, `tests/test_source_read_gate.py` and `tests/test_kaggle_public_backend.py`.

The native catalogue lookup reports `source_format` from the original receipt
inside the accepted manifest for the current source revision. Only the format
leaves PostgreSQL; the receipt stays there. Sources without that manifest report
an unknown format (`null`) rather than an assumed PDF.
