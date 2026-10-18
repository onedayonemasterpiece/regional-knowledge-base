# Imported-source audit, 18 October 2026

## Production update, 18:02 UTC

This update supersedes the earlier deployment and pending-authorization statements
below; those sections are retained as dated investigation evidence, not current
runtime status. This remains a partial acceptance report.

- The owner deployed RKB `926b8564d7d444cf144301ed68f88689f2ebda46`
  earlier today, including the SQLite mirror fix, durable archive reads, exact
  text-to-illustration linkage, canonical runbook and disk-admission guard.
- [PR143](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/143)
  fixes recovery of explicitly terminal transient Telegram archive GET failures.
  It preserves the verified PUT and exact archive entry, uses a stable retry key
  derived from the previous GET, and persists a 15-minute cooldown. In-flight,
  unknown-outcome and authorization failures are not eligible.
  Exact head `3f13e5182eaac56ea60bab4f68fbda4517b7326e` passed
  [CI 39150981613](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39150981613)
  and merged as `70b77babb38aff0c77314caf434f13d4f88a9d6a`.
- At 18:00 UTC, one official RKB API + indexer apply from a clean exact-head
  checkout succeeded. The preflight job was `job_40942ed8d47e00329deb3a27`;
  the apply job was `job_172e7852aa7fb7dd0ca7fb6b`, exit 0.
  The official helper verified candidate process working directories,
  PYTHONPATH and release labels. API PID **480103**, indexer PID **480107**;
  public health **OK**.
- The pre-apply SQLite recovery snapshot is **114008064 bytes**, with SHA-256
  `5b853a207129440fc76719854881a602e15de6574358921faca3cb4350276331`;
  integrity and foreign-key checks passed. It is recovery evidence, not proof of
  a restore drill. The helper's automatic rollback restores service configuration,
  not database contents.
- Independent post-apply service reads at 18:01 UTC confirmed VibePublish API PID
  **246618**, worker PID **212399**, and Street Story PID **467693** with unchanged
  start times. This RKB release did not restart those services.
- At 18:00:42 UTC, the indexing journal recorded
  `illustration_mirror_read_recovered` for the one remaining active illustration.
  The new read was initiated by the normal indexer, not a manual PUT or GET.
  This event proves recovery admission, **not** successful provider bytes/hash
  acceptance. Final receipt and SHA verification remain pending here.
- The owner's preceding independent census reported **150/151** active
  illustrations with archive references and provider hashes, including the
  **30 legacy** entries. That count is not this audit's independent byte-level
  acceptance of all entries. The old 30 service-identity read continuity cases
  remain separate from the newly processed mirror queue.
- VibePublish [PR84](https://github.com/onedayonemasterpiece/vibepublish/pull/84)
  remains a separate draft/schema-11 stage. No migration or legacy read grants
  were applied by this release. A fresh consistent backup and restore test,
  coordinated API/worker window, schema-compatible rollback, and exact-entry
  provenance/byte verification remain required before that stage.

The existing operational guide remains
[docs/operations/illustration-pipeline.md](../operations/illustration-pipeline.md).
No second runbook is introduced.

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

## Illustration completeness defect

The active real-source revisions contain 151 illustrations. Thirty already have
archive references. The other 121 are eligible private crops but have no archive
operation or readback IDs and no attempt timestamps. Object Storage HEAD confirms
121/121 present; four representative crop downloads, one from each affected
source, match both the object SHA-256 and displayed-image SHA-256. This is four
actual byte checks, not a claim that all 121 bytes were downloaded.

The indexing worker repeatedly logs `illustration_mirror_pass_retry` with
`OperationalError`. A minimal in-memory reproduction and a regression through
the real SQLite adapter confirm the cause: PostgreSQL-style
`UPDATE rkb_illustrations i SET ...` lacks SQLite's required `AS` keyword. It fails
before storing the first attempt; the error-status update encounters the same
problem. This explains the empty attempt/error metadata, not missing crop bytes.

A one-line `AS i` correction with 13 passing SQLite tests has been prepared and
independently reviewed. The new test fails on the unchanged baseline with
`sqlite3.OperationalError: near "i": syntax error` and passes with the fix.
The PostgreSQL integration fixture was skipped without an isolated DSN; no live
PostgreSQL validation is claimed. Owner, active-user, active-revision and private
visibility predicates are unchanged. This patch is NOT yet published, merged or
deployed. Restarting the corrected worker would allow its existing queue to
publish the 121 private crops, so specific owner authorization remains pending.
No new archive operations or grants were made in this investigation.

## Fresh archive-backed illustration check, 11:13 UTC

A single existing archive-backed Gause illustration was requested through the
normal product `illustration_fetch` path. Its original crop-object pointer was
already absent before this check; no production object was deleted to force the
fallback. The tool returned `isError: true` and exactly
`Error executing tool illustration_fetch`, without image bytes or metadata.
Consequently delivered-crop origin, actual image SHA-256, caption and page-link
presentation were not verified through this call. The cause is currently unknown
and must not be assumed to match the separate legacy-original access problem.
No retry, alternate principal, upload or permission change was attempted.
This archive-backed illustration path is an explicit failed acceptance case,
not a successful visual end-to-end demonstration.

## Visual retrieval and private delivery acceptance

Three fresh specified searches completed the full product chain:
`search → fetch → illustration_fetch`. The requested historical zoo plan,
monkey photograph and cross-potent diagram appeared at BGE ranks 1, 2 and 3
respectively. All three returned real PNG `ImageContent` with
`delivered_crop_origin=object_store`. Independently calculated byte SHA-256
matched the returned delivered/source crop hashes in all three cases. These are
hashes of the original RKB ImageContent bytes, not Slack transport-file hashes. All three
images were visually inspected and matched their requested subjects.

Printed captions were kept separate from model observations: the plan has no
printed caption text in its returned metadata, while the photograph and diagram
have populated caption text. The lack of a caption was not replaced by a model
observation presented as source evidence.

The three specified PNG files and the additional generic-query postcard below
were sent privately to the owner: four images in total. Independent Slack thread
readback confirmed the actual attachments in the requested existing owner DM
thread. This proves attachment delivery, not that the owner opened or viewed
those images. Private files, file IDs, source text and delivery links are omitted
from this public report.

The owner independently verified the three specified Slack PNG attachments and
confirmed the correct RKB images were readable. According to that independent
check, each Slack transport file is 21 bytes shorter because only the ancillary
PNG `pHYs` chunk (physical resolution/DPI metadata) was removed. The owner reported
matching IHDR, dimensions, IDAT length and CRC32, and IEND against the exact RKB
`illustration_fetch` outputs, confirming unchanged pixel content for these three
images. Whole-file SHA-256 therefore differs between the RKB source and Slack
transport; byte-for-byte transport identity is NOT claimed.

This transport comparison is attributed to the owner's independent verification.
The audit agent did not independently compute Slack transport hashes and received
no numeric transport SHA-256 values. The resulting visual smoke acceptance is
3/3 for the specified images. It does not extend the pixel-preservation claim to
the fourth postcard, for which only the separately described RKB byte/visual
checks and Slack thread/file delivery readback are established.

Two additional generic queries tested discovery without diagnostic image IDs:

- A zoo-opening query returned relevant prose at rank 1 without linked images,
  but a historical postcard at rank 5 in the same result set. The postcard was
  successfully fetched, its actual PNG hash matched, and it was visually checked.
  This is a successful agent-led image discovery, not a guarantee that the first
  prose result contains every useful image.
- A bridge-construction query returned relevant prose at rank 3 without images.
  A broader bridge text result at rank 7 contained a figure-caption region but
  returned an empty `illustrations` list. A caption-focused follow-up found the
  separate registered illustration chunk at rank 1 on the same page and caption
  region. Thus the image exists, but the related-image linkage exposed by that
  general text result is incomplete. The underlying code cause was not inspected
  and must not be asserted from these observations alone.

These results accept three concrete Object Storage visual-delivery paths and
one additional generic discovery path. They do NOT accept the archive fallback
that failed above, close the 121-item mirror backlog, or establish complete
related-image recall for arbitrary prose queries. No code, production runtime,
archive permissions or private publication destinations were changed.

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
5. After specific approval, apply the illustration SQL correction and verify the
   existing private mirror queue end to end without duplicate publications.
6. Continue source-specific quality and completeness checks. Synthetic controls
   are listed separately; their existence is not historical-corpus acceptance.

The full audit remains open while the production archive path has these defects.
