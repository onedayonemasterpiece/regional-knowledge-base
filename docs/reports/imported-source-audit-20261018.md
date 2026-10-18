# Imported-source audit, 18 October 2026

## Scope and outcome

This is an operational audit, not a claim of mass-ingestion readiness. The private
owner-scoped catalogue contains 31 logical records: 13 real bibliographic records
sharing 12 physical originals, and 18 explicitly synthetic controls. The real
records comprise seven books, two journal issues/fragments, and four articles.
Two physical sources are registered but have no active revision. They must not
be counted as searchable completed imports.

Private source IDs, Telegram references, original text, crops, storage keys and
credentials are deliberately omitted from this public report. The owner receives
the per-source inventory separately.

## Original-byte audit

All 12 real originals were freshly downloaded through authorized archive paths
and matched their registered SHA-256. Eight were verified through the existing
technical principal; four legacy entries required the owner connector.

Object Storage HEAD checks found three originals present. Nine were absent with
the application catalogue recording prior source-byte garbage collection. This
is permitted after archive verification; it is not evidence of data loss.

However, four legacy canonical entries return `media_store_entry_not_found` to
the current technical principal. The owner connector can read the same entries.
This is a principal-scoping continuity defect, not an OAuth failure or missing
Telegram bytes. Two affected originals have already been garbage-collected from
Object Storage, making a reliable archive fallback particularly important.

One historical original has two native Telegram posts containing identical bytes.
Both were read back. No post was deleted and no binding was changed during this
audit; canonical references must be preserved during any approved cleanup.

A separate large-original read exceeded the current 30-second polling window.
The original operation subsequently completed and its bytes matched the source
hash. The operation was resumed/read, not replaced by another upload or get.
Pending provider work must not be reported as missing data.

## Catalogue correction delivered

[PR135](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/135)
fixes stale archive-state projections on component articles. The existing parent
authorization remains mandatory. Current archive state is projected only when
the parent and component share an identical nonempty source hash. Frozen revision,
pages, regions, chunks and stored component JSON remain unchanged.

Twenty-six targeted tests passed, including nine new projection cases. Independent
review passed; [CI](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39117778864)
passed on `74f507c1d8acfcea02f74dbc5636415b6750338b`. The owner explicitly approved
merge and RKB-only rollout. Merge: `f042d8c2bcbfe6ce2d28cea3030644828f4b4ebe`.
The rollout receipt reports public health OK and a consistent backup with integrity
and foreign-key checks passing. Live catalogue reads show all four affected
components verified. No source content or access grant was changed.

Fresh 10:27–10:30 UTC readback again returned `verified` on all four cards.
The API process (PID 127256) owns the loopback TCP port 8000 and runs
`regional_knowledge.server`. Its working directory remains the older `215b3b79`
release, while the allowlisted process `PYTHONPATH` and `RKB_RELEASE_SHA` point
to `f042d8c2`. The configured catalogue module file matches the merged release
(SHA-256 `b86d0e489b6b00a4ebc8855b125c646e765f5d6a807ecaedb9b3540a5df039ea`).
This is process configuration plus disk-hash evidence, not direct inspection
of Python's in-memory `sys.modules`; the four live MCP responses establish the
observed catalogue behavior. The public tunnel-to-listener mapping was not
independently verified: the registered manifest probe was unavailable.

The separate indexing process (PID 107990) was NOT updated by this API-only
rollout. It runs `regional_knowledge.indexing`, has no listening socket, and
uses the older `215b3b79` source path. Its `77d46bb` release environment label
is stale and must not be treated as proof of loaded code. Stored component JSON
still contains the historical snapshot status by design; projection on read
does not rewrite those records. No blind restart or rollback was performed.

## Search and linkage checks

Four older active sources passed focused BGE search-to-fetch probes with the
expected passage ranked first and exact source page/region provenance returned.
This is a smoke test, not a comprehensive relevance assessment. One checked crop
was delivered from Object Storage with matching SHA-256; its model observation
was correctly distinguished from a missing printed caption.

Existing entity and POI evidence was inspected. Unresolved/candidate identities
remain explicitly labelled. A zero entity result or an unfinished newer ingestion
checkpoint must not be confused with loss of the active searchable revision.

The separate five-source acceptance work reports 68 substantive checks, 65 with
sufficient evidence, and 53/58 strict expected-ID matches. Seventeen diagnostics
were excluded from acceptance denominators. Known OCR, stale checkpoint and
retrieval misses remain documented in those source reports. Chunk counts alone
do not measure search quality.

PR121 is already merged. Its test programme and test-results journal are project
documents, not an additional imported historical source or a still-open draft.

## Open actions and authority boundaries

1. Restore exact legacy archive read continuity under the existing dedicated
   service identity. Any persistent access expansion requires specific owner
   approval. Preserve tenant, destination, topic, origin and hash checks; do not
   introduce an owner bearer, browser OAuth, reupload, or blanket principal bypass.
2. Preserve/resume pending archive read operation IDs when provider latency exceeds
   the bounded synchronous wait. Add slow-provider regression coverage.
3. Review the historical duplicate only after authoritative binding inspection;
   do not delete it blindly.
4. Finish the two unactivated imports only after reviewing their existing staged
   work. Do not mass-reimport or replace active revisions with incomplete work.
5. Continue source-specific quality and completeness checks. Synthetic controls
   are listed separately; their existence is not historical-corpus acceptance.

The full audit remains open while the production archive path has these defects.
