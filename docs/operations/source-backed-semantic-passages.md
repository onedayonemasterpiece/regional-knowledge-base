# Source-backed semantic passages and accepted reuse

Target2026-10-20. This is maintenance for **newly accepted target sources**, not migration of old books. [Ingestion](../ingestion.md) and [single handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) govern activation and gates. Historical stress measurements remain in [expanded stress report](../reports/expanded-retrieval-stress-20261008.md), not a new PASS.

## Exact source contract

StageChunkInput accepts existing full region_refs or span_refs. Each span has source page/region identity, Unicode codepoint start/end and SHA-256 of the whole original region text. Reject stale/missing refs, reversed/overlapping/out-of-range/empty spans and word-midpoint cuts. Compile chunk text deterministically from those exact excerpts only. Preserve full non-whitespace coverage, reading order, geometry, captions/figures/relations, footnotes and original evidence. Interpretation/translation/model observations are separate fields.

Semantic passages target about256 BGE tokens; strict maximum512 of final augmented material including prefix by pinned BGE tokenizer. Do not silently truncate; character counts do not substitute. Genuine cross-page continuations remain coherent. Existing draft_semantic_passages is a pure bounded proposal helper for visually reviewed pages, not semantic authority or automatic acceptance. Long footnotes/figures require a valid source-aware proposal, not omission. E5 is not a target tokenizer or comparison gate.

## Accepted reuse and revisions

Exact accepted transcription, page structure and model-selected spans are durable payloads. SHA/parser version alone cannot reconstruct them, and replay cannot ask a model for new boundaries while claiming the same accepted operation. Derived vectors/FTS may be rebuilt from this payload, independently from canonical outcome hashes; readiness shows projection lag.

`accepted_reuse` is server-verified, not an owner/self-certified replacement for visual review. Verify same archived SHA/edition, accepted revision and complete original review evidence; dimensions/printed numbering, source text hashes, reading order/geometry, caption/figure links and cross-region relationships match. New internal UUIDs do not alone disqualify equivalent mapped source content. Any changed/missing/unverifiable material remains needs_review.

`book_ingest(rechunk, document_id)` changes derived passages only, starts/resumes an idempotent pending revision of unchanged accepted source, and preserves old active revision until new source/retrieval gates pass. No new visual/OCR review is claimed. Text-only validated reuse may avoid opening PDF at finalization; illustrated recrop still requires verified original. Real changed-source reimport uses reprocess and verified PDF/DjVu. No author-specific shortcuts or evaluation-query logic.

## Evidence retention and ecology

Accepted source-review evidence is not a disposable stage cache. Persist compact normalized immutable review records/graphs referenced by active source/evidence/history and designated rollback baselines, without duplicating full originals in each graph. Old revision needed by a live assertion/variant/review retains its reference. Intermediate superseded/orphaned graphs without consumers are eligible for bounded GC; only designated bounded rollback references extend recovery retention. Do not keep every failed stage/full snapshot forever, and do not delete the sole proof of an active page review.

Already deleted historical review evidence cannot be declared recovered: restore the exact retained record or perform a new visual review. GEO-only cutover does not import old review graphs merely to avoid fresh MCP book reading.

## Activation and checks

Validate source completeness/spans/figures, final token input and proposed projection on exact revision. Measure per-book natural/paraphrased/cross-language retrieval; changed chunking/index/model invokes protected dense/latency/capacity gates (BGE Hit@5≥85%, Hit@10≥90%, RU→DE Hit@10≥85%, at least30 held-out fact families; scale≥100books/100kchunks). One passing draft helper is not activation permission or mass readiness.

The same model review saves facts/dates/observations/entities independently of Story. Sufficient episode saves real default Variant.body with source dependencies; zero narrative leaves all factual knowledge available. A source fidelity test does not establish storytelling quality, and an improved story does not waive retrieval evidence.

### Corrected bibliography in passage fetch

Numbered migration 041 makes passage citation metadata use the latest accepted source catalog correction, matching book_find. Sources without a correction retain manifest bibliography. Original stage/manifest bytes and page, region, passage and original-file provenance remain unchanged; correcting bibliography does not require reimporting or reembedding accepted pages.
