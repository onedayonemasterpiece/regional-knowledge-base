# Source-backed semantic passage draft — safe staged import

The production retrieval stress measured 1,914 active chunks for Gause and Brünneck, with **26.5% longer than 1,100 characters**, against 2,343 shorter passages in the frozen t256 experiment. Current active BGE-only held-out Hit@5/Hit@10 is **78.1%/84.4%** with frozen FP32 queries, below the protected **85%/90%** gates. This feature enables smaller coherent passages without fabricating page/region offsets or replacing active revisions.

## Contract

`StageChunkInput` now accepts **either** legacy full `region_refs` or `span_refs` of individually identified regions. Each span includes:
- the UUID of its source page and the exact region key;
- start/end offsets in original `source_text` (Python Unicode codepoint indices);
- SHA-256 of the complete original source region text, not the excerpt.

The compiler rejects missing regions, stale hashes, reversed/overlapping spans, out-of-range offsets, empty spans, and word-midpoint cuts. `StagedChunk.source_spans` preserves region UUID, offsets and whole-region SHA. The finalized chunk metadata stores these references for audit, while its text is deterministically assembled **only from the specified source excerpts**, not free model prose. The staged graph validator rejects source changes and **missing non-whitespace source coverage** even if a region was partially cited. Legacy full-region chunks remain compatible.

`draft_semantic_passages(pages,count_tokens,target_tokens=256,hard_max_tokens=512)` is pure/read-only. It accepts only previously visually reviewed, non-needs-review source pages, groups adjacent short textual regions on the same page, prefers sentence/punctuation boundaries, refuses to cut words, and uses the supplied **exact model token counter** rather than character limits. It returns `StageChunkInput` proposals with SHA-bound spans. Footnotes remain full-region chunks and fail closed if they exceed the strict budget. Image/figure linkage remains a distinct reviewed task; no automatic silent loss of illustrations is authorized.

## Pilot and activation gates

1. Work on owner-authorized **staged** source pages/revisions. Do **not** reinterpret an older active page as freshly visually reviewed without provenance of the original review. Verify archive source SHA and complete page/region geometry first.
2. Supply both pinned E5 and BGE tokenizer counts; use their maximum including final search-material augmentation. `_validate_with_token_budget` remains the authoritative final E5/BGE 512-token cap. Any 256-target excess is a warning requiring review; there is no silent truncation.
3. Build the staged graph and verify every original textual region has full non-whitespace coverage and each span's region/page/source hash is current. Do not finalize/activate if visual figures, provenance or page review are incomplete.
4. Publish new BGE vectors for a **candidate** staged revision only, then benchmark against the same frozen source-grounded facts and new page/region mappings. Require dense-only BGE Hit@5>=85%, Hit@10>=90%, RU→German Hit@10>=85%; compare false positive/negative and boundary-pair retrieval, as well as per-book source metadata and 1–2 caller p95.
5. Activate the revision **only after** all applicable tests pass, with previous active revision retained for rollback. This interface enhancement alone does not constitute a completed reimport and does not waive the separate >=100,000 active chunk capacity/quota gate.

The original source PDF/DjVu, original page image, archived SHA and geometry remain unchanged. This change introduces no automatic mutation of existing book revisions and no new external provider dependency.
