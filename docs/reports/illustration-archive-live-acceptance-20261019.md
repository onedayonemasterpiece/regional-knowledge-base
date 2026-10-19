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

- Legacy originals: the independent repeat below downloaded all four approved topic-2 files through the dedicated service identity and matched each actual SHA to its immutable origin and RKB source SHA. All four returned PDF headers. This supersedes the earlier metadata-only coverage of these four grants.
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

## Independent final repeat, 19 October 2026

The final-acceptance task was executed independently against the running services. No source import, mirror PUT, grant change, database migration, service restart, image generation or Telegram publication was performed.

| Required check | Independent result |
| --- | --- |
| Actual API/indexer source | RKB `481241fa36db30e3f0e6c4c4b88fb3ce84964676`; both process CWD and PYTHONPATH agree; all 62 Python source files match the Git commit |
| Actual API/worker source | VibePublish `d1f880b24788f609398a660ba53dd8c4731b7aec`; both process CWD and PYTHONPATH agree; all 72 Python source files match the Git commit; SQLite schema 11 |
| Independent neighboring releases | Street Story `1aec5cb8a828cb54060118ec2f94d3310d09613e`; RKB POI delivery `fd967f8592f17154e3b10a7f9c752599090b77c6` (63 Python files match); six services active |
| Active archive / duplicates | 151/151 exact tenant, topic 4, origin and native observed SHA; 121 service-owned + 30 legacy-owner; zero repeated active entry refs |
| Current exact-grant authorization | 34/34 reconstructed with production `require_grant` on read-only SQLite, including current actor/binding epochs, destination, immutable revision and origin; 30 image + 4 source grants |
| Service authority | Non-owner; scopes remain `bootstrap`, `media.store`, `status`; no owner token or user OAuth used |
| Semantic query continuation | All five requested natural-language queries reached `main_state=ready`, `retrieval_mode=bge`, retaining each original `main_job_id` after cold start |
| Search → evidence → real image | Zoo plan, monkeys, potent cross, historical postcard and Green Gate all returned source-linked native PNG ImageContent; images visually inspected |
| Actual archive image bytes | Eight files: three active legacy-owner + five service-owned; all `delivered_crop_origin=telegram_archive`, PNG magic and actual SHA equal both delivered and native provider SHA |
| Four legacy originals | Four real service-authorized PDF downloads, 15,453,787 / 1,574,088 / 12,752,172 / 17,013,878 bytes; each SHA equals immutable origin and canonical RKB source SHA |
| Live access refusal | A known owner-owned RKB archive entry outside the exact grant set returned `media_store_entry_not_found`; no private content returned. Scoped topic-2 list returned only eight service-owned entries, without legacy-owner library enumeration |
| Retained Original / compositor | Existing 1448×1086 unannotated Original matches its art byte SHA and all decoded pixels. Existing 3000×4000 source-frame candidate preserves source dimensions and every compared pixel below its annotation; saved provenance says `generation_dispatch=not_sent` |
| Worker independence | Current unit has soft `Wants`/`After` on API and no API `Requires`, `PartOf` or `BindsTo`; no restart performed |
| POI delivery | Existing outbox event is `delivered` with saved `unresolved_identity` receipt, unchanged idempotency identity; transport delivery is not accepted POI identity |
| Focused repository checks | RKB: 28 tests passed for exact association, cross-actor refusal, unrelated same-page refusal, durable GET/expiry and SQLite mirror fences. VibePublish: 119 passed for exact grants, legacy origin, media-store recovery, source-frame/EXIF and Original recovery |

Search quality has explicit limits: the plan and postcard ranked first, the potent cross and exact Green Gate evidence second, and the monkey-bearing evidence third. Other higher-ranked hits were inspected and rejected as irrelevant; returning a related book is not proof of the requested subject. Green Gate linkage is `association_basis=exact_shared_region`, with its actual printed caption, never a Wooden Bridge substitution. Printed captions and model observations remain separate. No evidence claims all-book retrieval-quality acceptance.

All 151 active crops already lacked an Object Storage pointer naturally. The repeat therefore exercised the required real Telegram restoration path without deleting or fabricating crops. It does not claim a fresh Object Storage download. Wrong topic, revoked/replaced grant, other tenant/actor and unrelated same-page cases are repository regressions; the live denial above was an existing ungranted entry, not a grant revocation experiment. No foreign-user source was opened.

Private evidence is retained in the managed directory `/home/dev/artifacts/regional-knowledge-base/20261019T085314Z-illustration-final-acceptance-20261019` (mode 0700). It contains original MCP image bytes, descriptors, evidence/query IDs, provider/native joins, all four original-file readback hashes, authority and runtime checks. Source scans, private IDs, native message links and credentials are excluded from this public report. Original PDFs were hashed in memory rather than copied into artifacts. Free disk was approximately 2.5 GiB after these checks; no large database copy was made.

**Outcome:** the scoped illustration pipeline is live-verified, including active legacy restoration and all four approved legacy-original reads. This documentation update requires no runtime release: RKB API/indexer remain on the verified #144 release; newer `main` and the separately released POI delivery are preserved. Mass ingestion, unfinished source revisions, all-book semantic quality and Street Story editorial quality retain their independent acceptance requirements.
