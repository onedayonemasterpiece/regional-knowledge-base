# RKB illustration and source-proof pipeline

Updated 2026-10-21. The [owner alignment](../designs/rkb-owner-alignment-20261021.md) supersedes the old Vibe-only and media-hash policy. Direct Telethon in Kaggle, model reading and one owner-only import are mandatory. Normative target: [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). This is not a claim that the new Kaggle runtime is deployed. Reuse the existing source/page/figure → evidence → canonical rendition → private Telegram mirror → verified restoration chain.

## Product capabilities and artifact policy

| Capability | Entry/authority | Required result |
| --- | --- | --- |
| Discover a source illustration | `search` → `fetch` → `metadata.illustrations[]` | Correct accepted figure/caption association, exact page/revision and retrieval state |
| Fetch the illustration | `illustration_fetch(id)` | A public illustration rendition for everyone, real MCP ImageContent, actual MIME, registered media origin, caption/description and availability |
| Prove a source quotation | Existing source-proof/locator path | Authorized page/crop/stripe pixels for owner or current demo/expert/local_historian `scan_proof_read`; exact locator/accuracy and per-item outcome |
| Restore the illustration | Direct Kaggle Telethon adapter | Accessible registered topic 4 photo/document, source/figure binding; no custom download/hash comparison |
| Retrieve full original | Separate private source operation | Independently authorized exact PDF/DjVu, topic 2 and original SHA |

Apply [rights.md](../rights.md) at creation, retrieval and delivery. Public illustration policy is already authorized; no new per-figure editorial approval is required. Its authoritative artifact class comes from accepted figure identity and validated output geometry, not a caller's label. A full-page plate can be a genuine figure; an arbitrary page/quote crop cannot be made public by calling it an illustration. Exclude adjacent unrelated prose/marginalia and restricted page context. Keep authentic rights metadata unchanged; do not fabricate `public_domain_verified`.

Verified participant-category changes atomically materialize the audited `scan_proof_read` grant as defined in [rights.md](../rights.md); `claimed` does not. Rendering uses the current grant/scope, never category-name fallback. Category removal/demo expiry disables its derived grant; manual revocation is not undone by reading or replay.

The private archive location does not make a permitted public rendition owner-only. Conversely, a public rendition does not expose private archive URLs or full originals. Shared Slack stays `public_only` and may carry public figures; personal proof grants are not combined into channel access.

## One corpus and exact evidence

The corpus/proof adapters use the same PostgreSQL/pgvector on Kaggle and one accepted source revision; BGE is the only embedding model. DevCoveer keeps the existing request/controller and durable change/delivery state. Kaggle uses one outbound WSS connection to that controller. Do not add an image index, OCR/VLM service, scheduler or second semantic pipeline.

`illustration_id` is tied to the accepted source revision, physical page and `source_region_id`. Keep `caption_region_ids`, exact printed caption and separately labelled `visual_description`. Same-page location, name similarity and vector proximity do not establish image/POI identity. Street Story remains canonical POI owner.

The current `source.activate.v1` code registry uses SQL015's illustration-aware
check/apply functions for both text and illustrated sources. Native preparation
always seals its exact media sidecar; text-only sources use empty illustration
and link arrays. Activation saves that projection and its accepted-operation
origin atomically. Existing v1 command audit fields stay unchanged; the numbered
SQL ledger supplies the schema version. This describes the source contract, not
live import or Telegram mirror acceptance.

Packet publication's SQL036 media-link lookup retains only the submitted chunk
and POI-link keys while reading each accepted stage batch once. Exact page
equality and invalidation before same-batch reinsertion are unchanged: repeating
an identical page preserves links, but A → B → A does not resurrect links
removed by B. Page/figure/caption/crop and referential checks remain intact.
The historical whole-book SQL015 helper is unchanged. This bounds accumulated
link-map work to the packet, not the amount of accepted payload history read;
synthetic PG17 timings do not establish live import latency.

SQL020 treats media hashes as representation identifiers. Refreshed media
metadata uses upsert and retains its accepted source/page/figure geometry;
different media hashes or JSON serialization do not reject it. Accepted source
commands, Dataset snapshots and code artifacts keep their separate integrity
checks. Archive-backed media delivery still requires current source
eligibility, the registered archive locator and authorized pixel geometry.

The archive-backed readers and final MCP ImageContent rendering follow the same
rule: source/figure identity, geometry, current authorization and owner-held
archive proof govern delivery. A different media hash, optional hash identifier
or re-encoded PNG/JPEG/WebP does not reject those actual pixels. SHA checks on
code and accepted command receipts remain independent of media delivery.

Search material may include accepted captions and labelled visual observations without overwriting source text. An omitted descriptor can be projected only from the exact accepted shared figure/caption region, recorded as `association_basis=exact_shared_region`. A keyword or neighboring place cannot supply a missing image link. Search ranks source evidence; the agent follows its descriptor and fetches the real image.

Only GEO migrates. Old book/figure/chunk/vector projections are rebuilt by fresh MCP import, not backfilled as accepted target content. Reuse verified original bytes and existing archive infrastructure as permitted; preserve GEO minimal provenance and exact reanchor under the [cutover contract](../migrations/rkb-layers-only-cutover-20261019.md).

## Public preview rendering

Public `illustration_fetch` reads the accepted figure and physical page through
the existing source-read channel, renders that page with the same verified
original renderer as `book_pages`, then returns only the accepted figure crop
as actual JPEG ImageContent. The complete page stays internal. Ordinary source
authorization and withdrawal checks apply through the existing read issuer.
The image path does not require a Telegram mirror, registration, media hash or
archive availability state. Original acquisition/storage integrity remains separate from derivative-media
hash identifiers; page readback does not repeat a derivative SHA comparison. `fetch.metadata.illustrations[]` contains exact
accepted passage links or shared figure/caption evidence, excluding unrelated
pictures on the same page. Figure metadata includes the zero-based physical
page. Discovery uses compact descriptors outside the text metadata budget;
`illustrations_has_more` explicitly marks a bounded partial gallery. Preview
encoding fits the existing whole MCP response ceiling. The ordinary output
issuer rechecks authorization after page rendering without extending other read
tickets. This public preview is independent of archival mirror work.

DjVu decoder/ELF verification and final native-bundle verification
run through the existing cancellation-safe blocking-work helper, outside the
WSS event loop. Exact byte/ELF/source checks and page geometry remain unchanged;
this scheduling change requires native installation and concurrent page/search
verification before live acceptance is claimed. JPEG encoding keeps its existing
serialized PyMuPDF call; PyMuPDF does not support multithreaded execution.

## Source-proof and locator contract

Port the existing corpus/authorization boundary in `quote_proof` and `source_archive` to PG. An authorized nonowner proof request must reach the original through purpose-scoped internal rendering access; never impersonate the owner or grant direct source download as a side effect. Restrict the model's input and final composited pixels to the authorized class/scope. A full private page underneath a highlighted crop is a disclosure.

For a separately requested stripe, the model receives the authorized PDF/DjVu page and selects bbox/polygons; deterministic code validates and draws them. Reuse an already accepted locator only for its unchanged source/render/quote. Native text/quads may assist but are not a substitute for the selected model localization or a mandatory import phase. Preserve existing locator/validation adapters and bounded independent strip reading where visual confirmation is claimed. A transcription match, plausible bounding box or confidence score alone does not prove visual alignment.

Bind each locator to source SHA/revision, physical page, render hash/dimensions, crop offset, rotation/transform, selected quote/span and pixel scope. Preserve exact accepted locator payload and provenance. Multiple columns, rotated text, repeated phrases, line breaks and separate pages keep separate geometry. Report `exact`, `line`, `region` or `unavailable` honestly; never silently turn an exact request into a broad highlighted paragraph.

Retain the exact preview/render or an exact recoverable descriptor while any locator callback is live. Delivery ACK of a preview is not a terminal localization receipt. If exact preview recovery is impossible, create a new generation/localization and reject a callback tied to the old hash. Semantic payloads/accepted coordinates are authoritative accepted data; a parser version and source hash cannot recreate model work.

## Actual media and partial results

Return real MCP ImageContent and a MIME matching the bytes (including WebP where produced), not a metadata-only URL or hardcoded PNG. Preserve source/page/figure identity and local preview generation. Telegram delivery is verified by an accessible expected photo/document locator, not by provider-rendition SHA. Use photo by default and a high-quality document only when the original resolution requires it; determine the threshold from the actual upload path and returned dimensions. Do not add a full post-upload download to verify image quality.

Multi-page and multi-image operations retain verified allowed items and report each item as `ready`, `pending`, `unavailable`, `denied` or `failed`. Do not fail all pages because one is pending or disclose denied inventory. Use bounded item counts/continuations and preserve accepted result IDs across retry.

Public illustration/proof/original caches are distinct. Cache keys include authoritative class, source/revision, geometry/profile, policy version and applicable audience. Recheck source withdrawal/current policy and any capability/grant on cache hit, callback, retry and delivery. Late success from an obsolete policy cannot republish withdrawn pixels.

## Mirror, transport and capacity

One RKB archive uploader runs directly through Telethon in Kaggle. Downloads use the same direct route. VibePublish and DevCoveer are not bulk-byte proxies or required archive services. Reuse source/media identities and minimal pending/UNKNOWN state, not the whole Vibe framework. Current auth, session ownership and destination/topic scope remain; no extra gateway or uploader is introduced.

Full originals go to topic 2; illustration photos and conditional high-resolution documents go to **private forum topic 4** after checking the configured destination. Kaggle reads exact registered locators directly using genuine separately authorized Telegram sessions supplied for implementation. Copying a StringSession/auth key does not create an independent session, and account-wide pacing/FloodWait still applies.

Freeze publication/read keys by exact logical artifact and accepted revision. Persist operation/read IDs before success; timeout/lost reply means unknown, requiring same-key reconciliation. Archive success requires the expected media to be accessible at the registered destination/topic/message locator. Do not require custom Telegram checksum/readback downloads. Byte eviction needs that availability and no live ingest/render/locator dependency; PG recovery has its own independent integrity contract.

Reserve measured staging/cache capacity before downloads/renders/uploads; include concurrent copies and output artifacts. Keep fair bounded work under the existing queue/controller. Pending originals/mirrors cannot starve behind embeddings, and blocked work releases capacity. No second maintenance scheduler is introduced. The handoff defines bounded journal/checkpoint/backup retention; old Dataset generations do not pin an unlimited local replay log.

## Historical baseline lessons, not an obsolete target transport

The Vibe-specific mechanisms below describe the old route. Preserve their lessons about bounded progress, UNKNOWN and expired cached assets, not mandatory Vibe dependencies, provider digests or a copied retry framework. Target pacing is simple/configurable and obeys actual FloodWait;20/min is not an asserted universal limit.

The [19 October remediation](../reports/illustration-audit-remediation-20261019.md) fixed immutable first verification GET keys, missing-provider-digest failure, guarded read recovery, cooldown/fairness and source admission reserve. These are already closed baseline findings, not newly discovered defects.

In the Vibe read path, terminal blocked `telegram_transport_unhealthy` / `provider_read_deadline` may transition to a persisted next key with a 15-minute cooldown. Unknown/pending/auth/digest failures keep their operation. Verified-asset HTTP 404/410 has a separate expiry continuation. Polling errors preserve cooldown; ready work must not be hidden behind cooling entries. The existing 2 GiB admission reserve is a historical guard, not an atomic global capacity guarantee; target capacity must account for all concurrent allocations. See [read continuation](archive-read-continuation.md).

## Acceptance and diagnosis

- Use at least three natural-language searches without hardcoded image IDs. Demonstrate exact evidence/figure association and printed-caption/model-observation separation; include a nearby unrelated same-page image negative case.
- Fetch real images through the actual MCP connection, check actual pixels/MIME and accessible registered media without custom Telegram hash comparison, and deliver public renditions to an audience without a personal book grant. Separately test denied quote/page/original access and authorized owner/demo/expert/local_historian proof.
- Exercise a mislabeled page crop, actual final output geometry, warm-cache withdrawal, expired demo and stale callbacks. Return usable partial results when one page is pending or denied.
- Verify forum topic 4 native readback and naturally evicted-cache restoration through the same API. Keep topic 2 original retrieval independently private. Do not delete production bytes just to manufacture a fallback demonstration.
- Test lost replies/restart/failover with exact accepted payloads and stable keys, preview lifetime, stale locator/geometry rejection and no duplicate publications; no Telegram media checksum gate. Confirm target PG corpus access and direct Telegram reads; old tests do not prove these new paths.
- Record the actual running MCP/controller/notebook release identities and exact result receipts. A merged PR is not deployment. Preserve neighboring Vibe full-frame/Original/compositor behavior and Street Story integration.

If Slack transforms PNG metadata (the historical example removed the 21-byte `pHYs` chunk), keep source and delivered hashes separate and inspect pixels/IDAT as appropriate. Do not call a transport-rendition hash mismatch proof that the source illustration changed.

The [live acceptance report](../reports/illustration-archive-live-acceptance-20261019.md) and remediation report preserve prior real-image/original readbacks. Their measurements remain historical and do not certify fresh import, all-book recall or Kaggle reset recovery. Report each capability independently as verified/failed/pending/untested.
