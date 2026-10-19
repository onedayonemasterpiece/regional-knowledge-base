# RKB illustration archive — live product acceptance, 19 October 2026

**Scope:** already imported source illustrations, their searchable evidence and private Telegram archival restoration. This is a *live* check of deployed RKB and VibePublish against the authorized production corpus; it is not permission to redistribute private sources. No source reimport, new Telegram publication, generation or ACL expansion was undertaken for this acceptance.

## Runtime and authority verified

| Production component | Observed immutable source |
| --- | --- |
| RKB MCP API and indexing owner | `481241fa36db30e3f0e6c4c4b88fb3ce84964676` (includes #136–#144) |
| VibePublish API and Telegram worker | `d1f880b24788f609398a660ba53dd8c4731b7aec` (includes Street Story full-frame #80, media recovery #83, soft worker dependency #85, scoped-entry #84) |
| VibePublish live SQLite authority | `PRAGMA user_version=11`; both processes active on the **same** source release |
| RKB retrieval | BGE worker/query/owner ready; active chunks 2959, BGE ready 2959; Kaggle-only inference boundary retained |
| Existing Street Story | Service active; 119 retained visual candidates with format `original` in VibePublish ledger on this check; no new generation/publication to test RKB |

This table describes the observed runtime source and persisted metadata, not a separate Street Story visual-quality acceptance or a production mass-ingestion throughput certificate.

## Entire active-revision archive reconciled

Read-only joins of the RKB `rkb_illustrations`, pages and active documents with VibePublish `publications`, `principals`, native `facts.snapshot` and revision origin showed:

| Independent invariant | Actual result |
| --- | ---: |
| Active-revision RKB illustration records | 151 |
| Valid archive publication references | 151 / 151 |
| Native forum destination **topic 4** | 151 / 151 |
| Native `origin.system` and exact `origin.ref=knowledge://illustrations/<illustration_id>` | 151 / 151 |
| RKB `provider_crop_sha256` equals native `observed_media[0].sha256` | 151 / 151 |
| Duplicate active publication refs / missing native publications | 0 / 0 |
| Newly archived technical-principal images | 121 |
| Existing owner-owned legacy images | 30 |
| Exact legacy read grants, scoped to those image entries | 30 / 30 |
| Exact legacy full-source read grants, topic **2** | 4 / 4 |

There are **215** total historical illustration rows, including inactive revisions. The sole row without an archive ref is **inactive**; all **151 active** images are archived. Corpus document archive statuses are 27 `verified`, one `pending` synthetic control with no active revision. Neither inactive fixture is a pending production source.

These 151 matches are *database/native-receipt integrity checks*, not a claim that every image was downloaded afresh during this one acceptance. New files use the narrow service identity and the older entries retain their original ownership; exact-entry read grants do not expose whole owner libraries, generic search/list or public links.

## Real MCP search, image and restoration checks

- Natural-language prompts requesting a historical zoo plan, an old animal-enclosure photo and related period imagery returned BGE-ranked evidence. A text-only query about old gate architecture identified an illustrated chunk and `fetch` projected the image **only** by exact shared printed caption-region (`association_basis=exact_shared_region`). The resulting image must not be mislabelled as a drawing of a neighboring bridge.
- The standard actor-scoped sequence `Kalinigrad_base.search → fetch → illustration_fetch` yielded real `ImageContent` PNG, not web images, direct Object Storage shortcuts or generated facsimiles.
- At least **three active legacy images** and **two newly mirrored images** were downloaded again using the exact same `illustration_fetch` MCP, `delivered_crop_origin=telegram_archive`, image MIME PNG, provider SHA equal to delivered SHA. An additional linked-gate image returned the same successful archive origin and provider-byte proof. The actor lacked no additional owner credential.
- The earlier rejected image example belonged to an **inactive synthetic source** with `active_revision=0`, not one of the 30 real active legacy images; its refusal is expected and does not evidence a broken live fallback.
- The owner-private Slack acceptance thread previously received **four real image attachments** from the RKB tool flow. Slack may rewrite auxiliary PNG metadata, so entire Slack attachment SHA is not used as proof of equal source bytes.

**Important search behavior:** an initial `search` can return `main_state=pending` and `mode=lexical_degraded` with zero provisional hits while BGE completes. Reusing the exact `main_job_id` with the same query later returned `main_state=ready`, `retrieval_mode=bge` and ranked results. An agent must continue that job before reporting a missing illustration; see [the canonical runbook](../operations/illustration-pipeline.md).

## Boundaries and remaining independent coverage

- Legacy PDF/DjVu: exact topic-2 read grants are present (4/4); the existing source-archive statuses include 27 verified entries and the earlier end-to-end original-PDF readback with byte SHA. This acceptance did **not** independently re-download all four old original PDFs; do not describe those four as newly byte-tested here.
- The **151/151** native-receipt audit does not mean 151/151 image downloads were replayed in this check. Provider-readback byte equality was demonstrated by multiple active legacy and newly mirrored examples. Wrong-actor, wrong-topic, malformed-image and revoked-grant fail-closed cases are covered by merged repository regressions, not live attempts against other users' media.
- Current rights remain private/unknown unless separately cleared. Nothing in archive verification authorizes a public post, an external URL or a model to distribute copyrighted scans.
- The retained `original` visual candidate count and an active Street Story service show cross-product persistence, not new proof of all Original pixels; protect Street Story's independent source-frame/compositor and publishing tests.
- Capacity/latency of **mass ingestion** and all-book semantic coverage remain separate protected requirements. Do not infer readiness just from vector counts or this archival acceptance.

## Minimal repeatable operator checklist

1. Confirm exact running API/indexer/worker SHA and RKB active-revision counts. Confirm free disk exceeds the configured 2-GiB new-mirror admission reserve.
2. Join active image refs to immutable Vibe native topic4/origin/`observed_media` SHA. Require all 151/151 with no duplicates, then verify topic2 legacy grants separately. Do not leak entry IDs, source bytes, tokens or owner metadata in public reports.
3. Run three representative semantic queries; continue the **same** BGE job when `main_state` is pending. `fetch` must include source-accurate image descriptors; `illustration_fetch` must return PNG/JPEG ImageContent with `delivered_crop_origin` and matching provider SHA.
4. Include both a naturally GC'd **new** archive image and an active **old** exact-grant archive image, and check an unrelated same-page / unauthorized case. Do not re-upload the same bytes or temporarily delete source crops for a test.
5. Preserve durable PUT/GET operation IDs across restarts and never interpret a missing receipt through a different principal as permission to replay publication. Confirm Street Story services and existing media remain intact.

**Outcome of the scoped illustration pipeline:** the current active catalog, private Telegram topic4 native bindings, lookup-to-image flow and both old/new restoration paths have been verified on the live deployment. The exclusions above are explicit and do not invalidate this scoped product acceptance.
