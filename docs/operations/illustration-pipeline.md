# RKB illustration pipeline — operator and agent contract

Canonical product path: **source/page/figure → indexed evidence → canonical crop → private Telegram mirror → verified restoration**. This document applies equally to new and already imported sources. Preserve the existing SQLite corpus, single indexing owner, VibePublish worker and current BGE/Kaggle boundary. No new service, vector index, scheduler or source import is required.

## Three separately testable capabilities

| Capability | Authority and entry point | Evidence of success |
| --- | --- | --- |
| Find the right image | `Kalinigrad_base.search(query)` → `fetch(evidence_id)` → `metadata.illustrations[]` | Actor-visible chunk and **correct source-region** illustration ID, page and figure provenance. Ranking/availability reported; no invented image association. |
| Get a real image | `Kalinigrad_base.illustration_fetch(id)` | Authenticated MCP `ImageContent` PNG/JPEG **bytes** plus `delivered_crop_origin`, `delivered_crop_sha256`, private/rights labels. Printed caption and model observation never conflated. |
| Archive and restore | Existing RKB indexing owner's `IllustrationMirror` → local VibePublish media-store service principal → Telegram **private forum topic 4** | Stable operation ID, native receipt, exact topic/origin, media `provider_crop_sha256`, `vibepublish_entry_ref`; `illustration_fetch` can restore when local crop is already absent. Original PDF/DjVu uses **topic 2**, not topic 4. |

**One logical illustration identity:** `illustration_id` belongs to a selected document revision, real page and `source_region_id` (figure). Printed `caption_region_ids` and model-labelled `visual_description` are stored separately. A canonical crop has `source_crop_sha256` and optional oriented/display `display_crop_sha256`; provider Telegram rendition may have a distinct `provider_crop_sha256`. Never use one hash as evidence of another rendition's bytes.

**Search is over evidence chunks, not a separate image gallery.** At staging, `search_material` includes accepted printed captions and explicitly labelled visual descriptions without overwriting source text. E5/BGE revision hashes fence stale embeddings. `search` may rank an illustrated chunk rather than put an image into the result JSON. The agent calls `fetch`, follows returned exact illustration descriptors, and calls `illustration_fetch` for the image. Fetch can project an omitted legacy illustration ID only if a user-authorized figure and the chunk share the **same accepted caption/figure region** on the same document/page; the projection is marked `association_basis=exact_shared_region`. Mere co-occurrence on a page, nearby place name or inferred visual similarity is **not** proof of linkage. Never identify a Green Gate figure as a Wooden Bridge.

## Transport and durable boundaries

- RKB and VibePublish share DevCoveer: existing authenticated **loopback HTTP**, independent least-privileged technical service principal, narrow Telegram KB destination and exact topic grants. See [canonical interservice policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md). Do not use a human OAuth/browser owner token, unscoped owner rights, public network hop or additional local daemon.
- Use only RKB's existing `IndexReconciler`/mirror maintenance and VibePublish's existing provider queue/pacing. A provider outage or slow read leaves durable `pending` and must not block indexing. No duplicate scheduler.
- Freeze the key per actor + illustration + accepted revision. Persist `mirror_operation_id` and `mirror_read_operation_id` before declaring completion. A lost response/time-out is **unknown**, not permission to mint another operation: reconcile/replay the same key or continue the same read ID.
- Only after provider-backed `get` confirms exact original topic, entry, origin and provider SHA set `vibepublish_entry_ref` and provider hash. Do not garbage-collect the private Object Storage crop before this proof.
- Actor authorization must succeed before either the private Object Storage locator **or** the Telegram fallback is consulted. An archive entry or signed URL is not authorization. Public rights remain closed unless separately verified.

## Troubleshooting without guessing

| Symptom | Inspect first | Correct response |
| --- | --- | --- |
| Text found but no `metadata.illustrations` | Chunk's `illustration_ids`, region IDs vs accepted figure/caption IDs, active revision; probe exact `search→fetch` | Repair factual region projection or document a real missing link; **do not** guess by name or reimport blindly |
| Descriptor present but `illustration_fetch` fails | Caller ACL, `crop_object_id`, object hash, `vibepublish_entry_ref`, original vs provider digest | Restore through existing scoped fallback; do not publish private bytes or silently claim success |
| Illustrations never leave the queue | Real indexer SHA/PYTHONPATH and `illustration_mirror_pass_retry`; SQLite `UPDATE ... AS i` compatibility | Fix/verify one existing worker SQL boundary and its receipt; no extra queue |
| Some Telegram originals visible to owner but not RKB service | Exact legacy entry principal/topic/grant/binding/origin | Authorize **only** approved exact old entries under the technical principal; no broad owner impersonation |
| Read takes longer than a synchronous budget | Durable media-store get request key and existing operation status | Resume/reconcile **same** read ID, never send again blindly |
| Slack file hash differs from RKB crop hash | Inspect PNG chunks and pixel data; Slack may remove the 21-byte `pHYs` auxiliary chunk | Log `RKB SHA` and `Slack delivery` separately; check actual rendered pixels/IDAT when transport transforms metadata |

## Mandatory acceptance: real data, not a mock-only claim

1. **Search and association.** At least three distinct natural-language prompts; no hardcoded illustration IDs in the query. Demonstrate exact accepted evidence region and `metadata.illustrations`, model vs printed caption separation, and semantic/lexical retrieval mode. Also probe an unrelated same-page and cross-actor negative case.
2. **Actual media bytes.** Call the real authenticated MCP `illustration_fetch` for each, check image MIME and SHA of delivered canonical crop (not a web result or generated facsimile); present at least three **real image attachments** in an approved **private** owner Slack thread. One text-only general query should discover an illustration without a preknown ID. Slack transport integrity is separate from source-byte integrity.
3. **Telegram mirror.** Measure **actual active revision** `vibepublish_entry_ref IS NULL` backlog before/after; verify Telegram destination and **forum topic 4**, provider receipt/get, original vs provider hash, idempotent replay/restart and **no duplicate publications**. Preserve topic 2 for full original source files.
4. **Existing archive fallback.** Choose a naturally GC'd verified crop with no live Object Storage pointer; call the **same** `illustration_fetch` tool and verify `delivered_crop_origin=telegram_archive` and matching provider rendition hash. Do not delete real crop bytes merely for the demonstration. Test negative wrong owner/topic/entry.
5. **Deployment:** green CI and negative tests, commit and immutable deployed SHA, which **actual process** runs which module/PYTHONPATH (MCP server and indexer may use different release snapshots), live status/receipts. A merged PR is not proof of deployment. Attach evidence to existing audit report and Slack thread.

The authoritative mutable outcome is the most recent verified live receipt and [imported-source audit](../reports/imported-source-audit-20261018.md), **not** a previously green synthetic fixture. Report each capability as `verified`, `failed`, `pending` or `untested` independently. Do not collapse all to a single “works” indicator.
