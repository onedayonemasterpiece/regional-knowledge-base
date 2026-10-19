# Accepted import repair

## Target port and preserved repair behavior — 2026-10-19

The [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) governs the PG/BGE target. The SQLite repair and test reports below are historical baseline behavior to preserve through the new corpus adapter; they are not target acceptance.

Accepted-source extraction and fresh import use the same facts-first contract. Source-bound claims, calendar/date precision, observations and entities must be saved independently of an optional Story. Main8799 still routes these through the candidate's Story assertion, so rejecting a weak episode before introducing this path would lose useful facts. Keep exact source revision/evidence, stable finding IDs and continuation checkpoints. Repair updates a bounded metadata result and its application receipt atomically; preserve human changes, private drafts, optimistic revisions and conflict detection. Retry must not generate a new Story/Variant or rerun an already accepted model output.

Semantic source-region coverage and source-format safeguards below remain required. Target BGE validation counts the exact complete input, including real prefixes, against the 512-token bound. No E5 publication gate or backfill is retained. Fresh reimport replaces old searchable books under the [layers-only cutover](../migrations/rkb-layers-only-cutover-20261019.md); minimal legacy anchors and inventoried human work must remain recoverable.

## Preserved implementation and historical record


Retrieval coverage uses the same semantic region kinds in preliminary graph validation and SQLite activation: heading, body, caption, footnote, table and marginalia. Running headers and page numbers remain preserved source regions but do not require standalone search passages. This does not waive page completeness, visual review, source spans, or provenance checks. Existing accepted revisions are not rewritten automatically.

Accepted-source story metadata repair uses the existing `book_ingest` stage operation. It accepts bounded model-authored story candidates linked to the exact active source revision. SQLite adapter UUIDs and JSON string IDs are normalized at the activation boundary. Candidate checkpoint persistence and story application now share one transaction, so an unexpected activation error cannot leave a newly overwritten checkpoint committed on its own. Optional invalid candidate metadata still becomes an explicit needs-review result, not a fabricated accepted claim. Entity graph staging retains its existing separately validated transaction.

For checkpoints left staged by an older release, resend the same corrected metadata-only batch to the same ingestion ID after installing the fix. Do not create a new source/revision or delete story identities. Existing source fingerprints reuse the same stories; repeated delivery is idempotent. Check observations/dates and candidate states through readback, not only a successful tool response. Preserve independent editorial changes.

The synthetic regression suite `tests/test_import_repair_boundaries.py` covers semantic versus layout coverage, UUID inputs, rollback on activation error, and two previously staged corrections resumed without duplicate stories. Existing ingestion, graph, activation, and source-span suites remain required checks. No private source content is used in tests.

The existing catalog tool now accepts update with typed metadata and expected_revision. Only the source owner may correct descriptive title/authors/year/language/catalog fields. The active revision, source SHA, archive state, ACL, chunks, vector identities and story links remain unchanged. Unknown/identity fields are rejected; article semantic boundaries are not silently changed by this operation.

For a PDF containing neighboring materials, describe the physical source honestly as a journal_issue fragment (form=fragment), without inventing a journal identity or claiming a complete issue. Then use existing book_ingest start component registration with catalog.kind=article, parent_id and exact region_ids. This reuses the original and existing chunks; overlapping article ranges or a boundary cutting a chunk are rejected. Register only verified title/byline and preserve uncertainty about filename-derived dates. The component mapping isolates article neighbor expansion without reimport or new vectors.

This patch does not automatically rewrite the trial source, remove existing layout passages, claim archive delivery, change embeddings or enable local BGE. Runtime release and source-specific replay/readback remain separate acceptance steps.
