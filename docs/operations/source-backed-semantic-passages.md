# Source-backed semantic passage draft — safe staged import

## Target accepted-source and semantic contract (2026-10-19)

**Target requirements; historical measurements remain unchanged.** Follow the [single handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Fresh book imports use Kaggle PG/pgvector/BGE. Accepted_reuse/rechunk is a maintenance path for accepted unchanged sources, not fresh cutover or a substitute for missing visual review.

Preserve exact accepted transcription, reading order, geometry, captions, figure links and semantic spans as immutable payloads. SHA/parser version cannot reconstruct them. A compiler may rebuild chunks from exact accepted spans; recovery cannot request new model boundaries and call them the same accepted result.

Keep Unicode codepoint offsets, complete-region hashes, exact page/region IDs, word boundaries, non-overlap and complete non-whitespace source coverage. Assemble source text from cited excerpts. Interpretation, translation and model observations remain separately labelled.

Count the complete final augmented material, including applicable prefixes, with the pinned BGE tokenizer: target about256 tokens, strict maximum512 without truncation. E5 tokenization is no longer a target gate. Preserve real cross-page continuations; page edges/character heuristics are not semantic boundaries.

In the same review retain source-bound facts/dates/measurements without Story cards. Preserve calendar/precision/role/history and decimal/metric/unit/subject/period/method/evidence. A fact referenced by several stories keeps one identity; no silent calendar conversion or unreviewed arithmetic.

Stories may describe episodes/practices/processes/connected explanations; no required drama/quotas. Contested and readiness are independent, private drafts are not shared variants. Concise voice may select relevant detail while preserving material values/attribution/uncertainty; canonical records retain accepted details.

Validate actual source/revision boundaries, figures, BGE quality, facts without stories, date/unit semantics and exact accepted-payload recovery. Source acceptance, index readiness and research coverage are separate. Convenience samples are not original rereads or calibrated automatic quality tests. Preserve protected capacity/quality/latency gates. Re-embedding after loss is degraded recovery, not rapid ready-index restore.

## Retained experiment and maintenance baseline

Following sections preserve prior measurements/span/review-retention contracts. Old two-tokenizer/reuse instructions do not override target BGE-only fresh import. Historical numbers retain their original scope.


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

### Reusing prior visual reviews in a later revision

The server may inherit a prior page-level visual review only from a
validated, same-source staged graph whose exact page content also matches the
new graph: printed page/geometry, OCR/normalized source region texts,
reading order, excluded figures, region relationships, and captions/
illustration references. Newly generated page/region/illustration UUIDs are
not themselves required to match.

A matching book SHA and physical page number without this evidence are
insufficient. Changed or unprovable pages remain unreviewed and block
validation instead of silently gaining `visual_reviewed`. If the historical
review graph was discarded or deleted, its evidence must be recovered or the
page explicitly reviewed again. This safeguard does not automatically
activate any book revision and does not change dense-quality acceptance.

### Retention of accepted review evidence

Source-review graphs are **not disposable staging caches** after successful
finalization. Storage GC retains the current graph object referenced by every
finalized ingestion job, including older finalized revisions needed for
provenance and rollback. Intermediate superseded/orphaned graphs and graphs
from failed jobs remain eligible for normal bounded GC.

This policy prevents future imports from deleting the sole proof of page-level
visual review. It is prospective: graphs that were already physically deleted
cannot be declared recovered or retroactively marked reviewed.

### Exact accepted-source reuse without a new page review

A re-chunk proposal may declare each page as \`source_material: accepted_reuse\`
instead of \`visual_reviewed\` only if it reuses a **previously accepted,
unchanged** source. It does **not** claim a new visual inspection. An
owner-controlled staged ingestion cannot self-certify this status: the server
rechecks the existing finalized active revision, exact archived source SHA-256,
owner identity, all page dimensions and printed numbers, source-region text
hashes, reading order and geometry, figure/caption associations, and
cross-region relationships against the proposal.

This applies to any book, language or page layout supported by the existing
graph model; no author names or evaluation questions participate in the
decision. Reused source changes **only** the derived semantic passages, not
the original OCR, source archive or accepted evidence coordinates. A single
missing, changed or unverifiable part fails closed to \`needs_review\`.
The regular page-level \`visual_reviewed\` process remains required for
new or changed source material.

The existing BGE-only held-out evidence gate, source recovery and vector
readiness checks remain separate requirements: a proven source match is **not**
permission to activate a new vector revision or declare mass ingestion ready.

### Derived-only re-chunk from an accepted source

Use \`book_ingest(command="rechunk", document_id=...)\` instead of \`reprocess\`
only to replace the derived retrieval passages of a previously accepted,
unchanged book. It is owner-scoped, idempotent to the current active revision
and starts a *pending* revision using the recorded complete accepted page
structure. It neither re-downloads the original archive nor creates a fresh
visual/OCR review. No PDF bytes are needed to start a stage.

Every staged page must be \`source_material="accepted_reuse"\`. Existing
server validation and finalization independently verify the entire candidate
against its finalized, archived SHA-bound source: all page geometry, exact
region source text, figures/captions and relations. Drift fails closed.
Only *text-only* verified rechunks may skip opening the PDF at finalization;
illustrated sources still need the verified original for deterministic
re-cropping. Genuine source re-import always uses \`reprocess\` and validates
the original archived PDF/DjVu.

The pending publication uses the existing protected BGE vector readiness
gate; previous active revisions stay searchable until the complete new
revision is accepted. Archive OAuth problems remain visible for operations
that truly need original bytes, not bypassed.
