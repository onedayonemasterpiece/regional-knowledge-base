# Regional Knowledge — robust book import: dedupe, illustrations, visual search and private mirror

Date: 2026-10-04

This is the implementation task **after** automatic post-finalize indexing is
merged/deployed.

Read first:

- `docs/reports/robust-book-import-dedup-illustrations-media-decision-20261004.md`
- `docs/reports/automatic-post-finalize-indexing-acceptance-20261004.md`
- `docs/reports/accumulative-knowledge-graph-mvp-acceptance-20261004.md`
- `docs/ingestion.md`
- `docs/integrations.md`
- `docs/poi-integration.md`

Also verify the prerequisite VibePublish task:

- repo: `onedayonemasterpiece/vibepublish`
- prompt:
  `docs/prompts/rkb-media-store-origin-rate-limit-20261004.md`

Do not start from the older SHA embedded in this prompt. Inspect current main,
the final automatic-indexing acceptance report, current production runtime and
current VibePublish runtime first.

## Goal

A user can upload a book and get one durable logical source with complete
ChatGPT-reviewed text/visual evidence, automatic E5/BGE indexing, retrievable
illustrations and an asynchronously verified private Telegram media mirror.

Repeated upload of the same source must not create duplicate logical books.

## Phase A — exact-source duplicate protection

Current source-file-ID idempotency is insufficient because the same PDF bytes
can arrive later under a new chat attachment ID.

Implement exact-source identity per owner using the downloaded PDF SHA-256.

Required product behavior:

~~~text
same owner + same bytes + default
  -> reuse existing logical document
  -> no second document

same owner + same bytes + import still in progress
  -> return/resume existing in-progress ingestion

same owner + same bytes + duplicate_policy=new_revision
  -> same document_id
  -> new staged revision

different owner + same bytes
  -> separate ACL/provenance root
~~~

Use a clear typed option such as `duplicate_policy=reuse|new_revision` or
equivalent. Default is `reuse`.

The DB must enforce the exact-source invariant under races. Before adding a
unique constraint/index, audit current production for existing same-owner
duplicate source hashes. Never silently merge historical documents in a
migration.

Do not silently overwrite title/authors/metadata merely because a duplicate
source is detected.

### Gause invariant

The corrected future reimport of document

`7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9`

must be capable of using the exact original PDF and creating a **new revision
on this document**, not another logical document.

Do not perform the full Gause reimport in this Codex task.

## Phase B — complete illustration semantics

Reuse the existing StageIllustration / figure-region / crop pipeline. Do not
replace it.

Extend the typed staging model minimally so ChatGPT can provide an optional
visual description for an illustration when useful:

- `visual_description` (bounded text);
- explicit provenance marker, fixed/validated as model observation rather than
  source text;
- optional language if already useful in existing contracts; do not build a
  translation subsystem.

Exact printed captions stay in caption source regions and remain source evidence.

Validation requirements:

- every visually reviewed page can explicitly contain zero or more figures;
- a staged illustration must reference a real figure region;
- its bbox crop must hash and persist;
- caption refs must stay on authorized source regions;
- image-only pages are supported;
- if visual review sees a materially relevant figure, it must either be staged
  or have an explicit bounded non-material/exclusion reason so silent omission
  cannot masquerade as complete review.

Do not add backend OCR/VLM/image caption generation. ChatGPT supplies semantics
while reading the page image.

## Phase C — illustration retrieval and reading

Today chunk fetch exposes illustration IDs but not enough material to use them.

Make `fetch(chunk)` return authorized rich illustration descriptors:

- `knowledge://illustrations/<id>`;
- kind;
- page ID / physical page / bbox where allowed by current model;
- exact printed caption text;
- optional model visual description labelled `model_observation`;
- crop SHA-256;
- visibility/rights fields appropriate to the caller;
- verified VibePublish entry ref if one exists.

Add one narrow read-only MCP tool (for example `illustration_fetch`) only if
needed to return the exact authorized crop as model-visible image content.
It should transport bytes from the existing canonical crop object; it must not
run OCR, image understanding or another model.

The regular Live profile does not need a bulk image dump.

## Phase D — textual retrieval material for visuals

Do **not** add CLIP/image embeddings in this task.

E5/BGE should index a deterministic per-chunk search material:

~~~text
source chunk text
+ exact captions of linked illustrations
+ optional visual_description
~~~

Keep source text and retrieval augmentation separate.

Add an explicit digest such as `search_material_sha256` / `embedding_input_sha256`
to the accepted E5/BGE indexing identity instead of overloading the source
`text_sha256`.

After the automatic-indexing task's final code is known, extend it rather than
building a second indexing worker.

Requirements:

- only active chunks whose search material changed are re-embedded;
- unchanged chunks are skipped;
- source text hash still verifies source evidence;
- search/fetch must never present `visual_description` as a quotation;
- partial/stale visual embeddings obey the same safe degraded retrieval rules
  as other partial indexing.

Add retrieval acceptance where:

- a caption phrase finds the correct chunk;
- a semantic query describing what is visibly shown finds the correct chunk;
- at least one German-source / Russian-query case succeeds;
- a misleading model description cannot be used as source-backed historical
  fact without the actual crop/caption evidence.

## Phase E — private illustration mirror through VibePublish

Do not implement Telegram directly in Regional Knowledge.

First verify deployed VibePublish supports:

1. immutable media-store origin metadata;
2. shared <=20 Telegram media files/images per rolling 60 seconds per connection;
3. durable defer/recovery and request replay.

If that prerequisite is not production-ready, finish the linked VibePublish
prompt first. Do not work around it with sleeps in Regional Knowledge.

For every active private illustration eligible for the owner's private mirror:

- canonical bytes = RKB illustration crop;
- send as Telegram DOCUMENT;
- stable request key derived from owner/document/revision/illustration ID;
- origin.system = `regional_knowledge`;
- origin.ref = `knowledge://illustrations/<id>`;
- optional origin.sha256 = crop hash, as provenance only;
- configured target thread = `https://t.me/c/4368830579/4`;
- mirror asynchronously after activation;
- never block/fail book activation because Telegram/VibePublish is unavailable;
- write `vibepublish_entry_ref` after verified native message/topic/DOCUMENT readback;
- restart resumes missing mirrors;
- replay never creates a second Telegram item.

Keep the exact provider destination alias in deployment/runtime configuration,
not hardcoded application source.

A crop already mirrored for the same canonical illustration/revision is complete.
Do not globally merge two semantically distinct illustrations merely because
their bytes happen to match.

Owner correction, 2026-10-04: image SHA equality and visual-similarity checks are
not mirror acceptance gates. Do not add a new perceptual matching pipeline for
delivery. Compression/cropping must not block the mirror. This correction does
not change the exact PDF-source deduplication requirement in Phase A.

## Telegram rate-limit acceptance

The VibePublish prerequisite owns the hard <=20/rolling-60s provider rule.

For RKB integration, prove that a batch larger than 20 is safely queued/deferred
and eventually mirrored without duplicate effects. Do not fill the real owner
topic with 25 synthetic junk files just to test the limiter.

Use:

- deterministic/fake provider acceptance with >=25 media items for rate math;
- a small real production canary of 1-3 illustration documents in the exact
  topic above;
- later corrected Gause import as the first natural large real pacing exercise.

## Phase F — end-to-end acceptance

Create a small owned synthetic PDF fixture that contains:

- normal text page;
- page with figure + printed caption;
- image-only page;
- at least 2 chunks linked to illustrations.

Use the real public product ingress/stage/validate/finalize path.

Acceptance must prove all of the following:

1. First import creates one logical document.
2. Re-upload identical bytes with a different attachment/file ID and default
   policy creates **zero** additional logical documents.
3. Two concurrent same-owner starts of identical bytes still produce one logical
   document.
4. Same bytes under a second actor do not collapse ACL/provenance roots.
5. Explicit `new_revision` creates a new revision on the same document ID.
6. Figure crops are persisted with exact hashes.
7. Image-only source page is represented, not silently dropped.
8. Printed caption is source evidence.
9. Model visual description is stored/read back only as model observation.
10. Chunk fetch returns useful illustration descriptors.
11. Exact crop can be read by ChatGPT through the authorized narrow image read
    surface.
12. Caption and visual-description queries retrieve the linked chunk.
13. German-source/Russian-query visual retrieval works on a bounded fixture.
14. Automatic E5/BGE indexing reacts to changed search material with no manual
    backfill scripts.
15. Unchanged source chunks are skipped.
16. Private VibePublish mirror reaches
    `https://t.me/c/4368830579/4` for the small real canary.
17. Native private-topic/DOCUMENT delivery and origin metadata are verified;
    source-image SHA equality is not an acceptance gate (owner correction).
18. Replay creates no duplicate Telegram document.
19. VibePublish outage/restart does not fail or duplicate the book import.
20. No public media publication occurs.
21. No paid/external inference API is used.
22. No server-side OCR/VLM is introduced.
23. Full RKB tests and CI are green; if VibePublish changes were required, its
    full focused regression/CI is green too.
24. Exact production main/runtime readback is captured.

Archive/remove synthetic active fixtures after acceptance without deleting their
audit receipts.

## Existing Gause evidence to preserve

Do not misdiagnose the old revision as an image-free book.

Prior source audit established that pages 145-174 contain image material and
sampled pages have meaningful captions missing from the old searchable snapshot.
That old revision also contains clipped text.

This task fixes product mechanics only. The next ChatGPT content task will
re-read the original Gause source, follow all continuations, visually review
every page, stage its actual illustrations/entities/aliases, and finalize the
corrected revision on the existing document ID.

## Do not build

- no graph database;
- no new ontology;
- no separate OCR service;
- no vision inference service;
- no CLIP/image vector space yet;
- no direct Telegram client in Regional Knowledge;
- no public image rights promotion;
- no generalized media scheduler;
- no semantic auto-merge of byte-different editions;
- no full Gause semantic parsing in Codex.

## Documentation

Update ingestion, integrations and MCP docs.

Create final report:

`docs/reports/robust-book-import-dedup-illustrations-media-acceptance-20261004.md`

At the end return:

- exact main/runtime SHA(s);
- duplicate-import acceptance counts;
- synthetic visual fixture results;
- illustration search/read results;
- automatic indexing deltas;
- VibePublish mirror receipt(s);
- evidence that the <=20/min provider budget is enforced by VibePublish;
- CI;
- explicit YES/NO: ready for ChatGPT to perform the corrected Gause reimport.
