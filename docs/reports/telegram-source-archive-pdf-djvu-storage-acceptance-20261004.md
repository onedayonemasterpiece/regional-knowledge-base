# Telegram originals, PDF/DjVu and compact storage acceptance — 2026-10-04

Engineering acceptance for the authorized storage task. No semantic Gause
reimport, OCR, VLM, caption/entity recognizer, model research or new queue/platform
was introduced. Corrected ChatGPT-led Gause reimport readiness: **YES**; the owner
must deliberately choose `new_revision` on the original source. This task did not
perform that reimport.

## Delivered production boundary

- Dedicated Vibe connection `devcoveer2-knowledge-base` resolves only the existing
  `TELEGRAM_KNOWLEDGE_BASE` secret. Ordinary `devcoveer2-telegram` continues using
  its original credential. Connection claim/lane/rolling media budgets are separate.
- Exact original PDF/DjVu DOCUMENTs go to `/2`; illustrations go to `/4` in the
  private forum `4368830579`. Vibe owns durable send/replay/native readback.
- PostgreSQL stores exact chunk and region text, catalog/ACL/revisions, FTS,
  E5/BGE and graph evidence. No new permanent text projection/page-render objects.
- S3 is capacity-reserved staging/cache. One-hour recovery/reader grace; guarded
  existing-owner GC deletes only registered objects with replacement evidence.
  Current ingestion pointers and unverified sources/crops are protected.
- PDF uses PyMuPDF; DjVu uses bounded deterministic DjVuLibre page count/render
  and embedded text hints. `book_pages` remains the public page API. No OCR/model
  runs on the server. Source originals require owner authorization even if parsed
  text is visible. Illustration authorization remains independent.
- Stable source keys use owner/root/SHA; lost admission responses replay the same
  operation. Terminal proven zero-dispatch failures use Vibe `retry_failed` on
  the original immutable intent; uncertain/dispatched effects cannot be retried
  by this gate. Failed source retries are bounded by a 30-second interval.
- A deliberate revision after source GC reserves capacity again and revives the
  same temporary cache identity under a document lock. No capacity bypass or
  untracked source copy is created.

## Actual provider and public-product evidence

Owned controls were visibly labelled synthetic and are now inactive audit roots.
Exactly two source files and one new illustration were used for these controls.
PDF: one visually reviewed page, exact heading/caption, one blue circle; its
finalized chunk obtained E5 and BGE automatically. DjVu: two actual model-visible
pages (blue and red), no embedded text or semantic parsing claimed. Deterministic
DjVu cropping is additionally covered by real-decoder tests.

| Evidence | Actual result |
| --- | --- |
| Ordinary public URL-only Telegram topic read | `op_02a3bd87947c41f5aa47e92a96993c8a`, verified; ledger binding resolves `devcoveer2-telegram` |
| PDF control source | `/2`, verified entry `pub_96d7dd2274d346e3b08afb6d4335b5d8`, exact DOCUMENT/origin/download SHA |
| DjVu control source | `/2`, verified entry `pub_2d81a86ed6de46bfbbbbe2b361c5b63d`, two rendered image contents |
| PDF control illustration | `/4`, verified entry `pub_b8caf4cc704241859a2d783ae14dbd04`, stable illustration origin |
| Provider outage | Worker stopped; source remained pending and staging retained; PDF finalize and E5 succeeded |
| Recovery | Indexing owner restarted; provider worker resumed; both sources and the illustration verified; BGE became ready |
| Exact replay | Eight source publication request keys returned their original operations: **zero extra publications**; alternate attachment IDs added zero roots |
| Historical duplicates | Logical roots remain separate; identical owner/SHA shares one explicit archive origin/ref, never a semantic merge |
| Existing Gause | One source archive, entry `pub_fc7a9ebff7324722a079d80efb857e79`, operation `op_e7d8c6771efb47699cf7e67d3470bd43` |

Gause source root: `7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9`.
Its **12,752,172** original bytes were freshly downloaded from Telegram after S3
source GC and matched SHA-256
`10328207e2664aabef78201fb997e18f3ea8e48f8d7d56a98f3d6832b21ddb65`.
Public `book_pages` rendered the archived original after deletion of its S3 copy.
This was readback, not a semantic reimport. Nine owner roots became verified
(eight distinct source operations); the separate other-owner pending source was
not touched.

After GC, actual public `fetch` returned Gause exact PostgreSQL text with its
original hash; search returned eight results; existing graph entity read succeeded;
`illustration_fetch` returned real ImageContent for a verified illustration whose
S3 crop pointer/bytes had already been removed. No delivered/source image SHA
sameness gate was added. Source-book exact SHA verification remains mandatory.

## Migration and physical storage

Backfilled **all 924 legacy chunks**, active and historical, after whole-projection
and range SHA checks: **1,676,476 UTF-8 bytes**. Backfilled **12,182 regions** from
hash-proved staged graphs or exact matching chunk text; no text was invented.
All stored chunk/hashed-region texts passed the final SHA checks. With the inactive
PDF canary retained, PostgreSQL has 925 durable chunks / 1,676,549 chunk text bytes.

Legacy corpus/vector totals and digests stayed identical before migration and
after GC/control archival: 924 chunks, 763 E5, 763 BGE; chunk legacy digest
`acdb5945a8f9d2b23ad6476b270a1fba`, E5
`dcf73fbcb14bbdb286dc4cccec28d5ec`, BGE
`cd71a78de5581e956dc45275972f9814`.
Active production coverage is restored to **747/747 E5 and 747/747 BGE**, zero
missing, ready. Canary coverage was 1/1 for both spaces before archiving it.

Measured immediately after backfill: database **58,734,259 bytes**, chunk relation
**18,980,864 bytes**. No pre-migration physical database measurement was captured;
a physical-size delta is therefore not claimed. The exact added chunk-text volume
above and region counts are measured, not estimated.

Guarded dry-run preceded deletion. Actual bucket inventory, not merely DB totals:

| Measurement | Objects | Bytes |
| --- | ---: | ---: |
| Before guarded GC | 719 | 355,339,601 |
| Deleted by guarded GC | 703 | 331,150,188 |
| After guarded GC | 16 | 24,189,413 |

The two explicitly owned transport-fixture objects were subsequently removed.
The small new canary source/snapshot/crop caches remain subject to the same
one-hour grace and existing maintenance GC. Remaining old staging includes a
still-active ingestion; its source/current snapshot and the unverified crop stay
protected. No unknown bucket objects or active source data were bulk-deleted.
Production `RKB_STORAGE_GC_ENABLED=1` is enabled only after these readbacks.

## Tests, releases and retained receipts

RKB implementation: `74ba7ae6601e20938f67b263d7bf0c6065c5014d`.
Vibe implementation: `fa709cb88552044cc2731d259432a05546f49a03`; subsequent
acceptance documentation: `149f6c453f089bdb9e47005b7c0d207b032ef135`.
PRs: [RKB #35](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/35)
and [Vibe #34](https://github.com/onedayonemasterpiece/vibepublish/pull/34).
Canonical merge SHAs and exact deployed-source/service readback are retained in
`canonical-runtime-readback.json` after merge; they are also returned to the owner.

- RKB full actual-PostgreSQL suite: **127 tests passed**. Covers owner ACL, exact
  Postgres evidence, lost-response/idempotent archival, safe recovery, separate
  historical roots, pending-source GC protection and post-GC capacity reservation.
  [Python 3.12/3.13 CI](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37216895529)
  passed at the implementation SHA.
- Vibe: 61 focused native/provider/media-store tests passed; full supported-version
  [CI](https://github.com/onedayonemasterpiece/vibepublish/actions/runs/37215614348)
  passed all six Python 3.12/3.13 verify/core-recovery/Telegram-P0 jobs, including
  the mandatory full browser/runtime suite. Earlier full local run passed 1,173
  tests and 219 subtests. A later Python 3.14 run was interrupted after four
  browser deadline failures and 315 passes; all four affected cases passed
  separately (42.56 seconds). This incomplete local run is not claimed as a full pass.
- Rollout incident `inc_14e6b46cfbc3549c18b86e40` was closed after native public
  ordinary/KB connection readback and zero-dispatch recovery verification.

Retained evidence directory:
`/home/dev/artifacts/regional-knowledge-base/20261004T151348Z-telegram-source-archive-20261004`
(mode-restricted receipts, no credentials). Important files:
`text-migration-production.json`, `archive-outage.json`,
`archive-control-acceptance.json`, `archive-source-audit.json`,
`ordinary-connection-readback.json`, `public-post-gc.json`, `corpus-after-gc.json`,
`gc-dry-run.json`, `gc-applied.json`, `s3-before-gc.json`, `s3-after-gc.json`,
`canonical-runtime-readback.json`. Provider originals were not copied into reports.

## Final replay and stable Kaggle versions follow-up

Final public binary-ingress replay exposed a legacy in-flight admission without
`document_receipt`: after verified source purge, the old PDF/DjVu upload keys
returned `422 asset_not_available`. Vibe
[PR #35](https://github.com/onedayonemasterpiece/vibepublish/pull/35) now reconstructs
only the scalar receipt from the complete immutable owner-scoped ingress intent.
Actual PDF and DjVu HTTP replay returned the original asset identities after
purge; subsequent archive put replays added **zero publications**. All eight
source operations were verified/replayed; Gause's fresh provider original still
matched its exact source SHA. No image equality gate was introduced.

The former BGE controller derived the notebook slug from the run UUID, creating
separate version-1 notebooks. RKB
[PR #36](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/36)
uses one private CPU notebook, `zigomaro/rkb-bge-m3-cpu`. Run UUID and provider
version remain separate durable identities. Recovery reads the exact run identity
from provider source without executing it; an old latest version cannot verify
an ambiguous newer save. Existing pre-upgrade provider refs remain valid.

Production acceptance used the existing planned-rotation path with an explicitly
accelerated 10h45m age, not a new launch path. Two successive real workers became
ready as **versions 1 and 2 of the same notebook**. Each completed its own
synthetic query with a 1024-dimensional vector, attributed to that exact run.
Final queue: ready, depth 0, no warming successor. Original model, CPU runtime,
vector space and corpus were unchanged. Safe receipts are retained in
`stable-kaggle-acceptance.json`; private generated credential-bearing source
remains solely in the protected runtime launch directory.

RKB full PostgreSQL suite: **130 tests passed**; supported Python 3.12/3.13
[implementation CI](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37218402728)
passed. Vibe's legacy replay change passed 30 focused media/HTTP tests; its full
supported-version CI and exact final canonical runtime are checked before closing
this work.

Automatic grace-based GC subsequently removed the owned control caches. Actual
final bucket inventory: **8 objects, 24,164,398 bytes**, compared with the original
719 objects / 355,339,601 bytes. The remaining active ingestion graph/source,
unverified crop and another owner's pending source are protected. Final receipts
are `s3-final-inventory.json`, `archive-source-audit.json` and
`canonical-runtime-readback.json` in the retained task directory.
