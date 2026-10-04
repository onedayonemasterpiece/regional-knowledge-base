# Robust book import after automatic indexing — dedupe, visual evidence and media mirror

Date: 2026-10-04

## Why this is the next import hardening slice

The automatic post-finalize E5/BGE indexing task is currently in progress and
must finish independently. Do not mix this follow-up into its dirty worktree.

A fresh review of the current product found three additional import gaps that
matter before the corrected Gause reimport:

1. exact duplicate source bytes uploaded under a new attachment/file ID can
   create a second logical document;
2. the current old Gause revision omitted material visual evidence even though
   the ingestion model already has illustration/page-region primitives;
3. the private Telegram illustration topic currently has no mirrored
   illustrations, and the provider must be paced to the owner's <=20 media
   files/images per rolling minute requirement.

This document chooses the smallest architecture that closes those gaps without
adding OCR, a vision service or a second retrieval platform.

## 1. Duplicate import identity

### Current behavior

Ingress first reuses an ingestion by `owner_user_id + source_file_id`.
After downloading the PDF it computes `source_sha256`, but a new attachment
ID with the same bytes can still receive a new `document_id`.

### Required behavior

Exact source identity is:

~~~text
(owner_user_id, source_sha256)
~~~

not the ephemeral chat attachment ID.

For one owner and exact same PDF bytes:

- default import reuses the existing logical document and does not create a
  duplicate;
- if the existing import is still in progress, return/resume that work;
- if it is finalized, return the existing document/revision as a duplicate hit;
- an explicit `reprocess_existing` / `duplicate_policy=new_revision` starts a
  new revision on the **same document_id**.

This explicit reprocess path is required for the corrected Gause import: the
same original PDF must be re-read with the repaired source/visual workflow,
without creating a second Gause document.

Different owners remain separate ACL/provenance roots even for identical bytes.

A byte-different scan/edition is not silently merged merely because metadata
looks similar. Metadata similarity may produce a review hint later, but exact
hash dedupe is the MVP hard guarantee.

The DB transaction must protect concurrent same-source starts. Do not rely on
a Python pre-check alone.

## 2. Existing illustration infrastructure is the base, not something to replace

Regional Knowledge already supports:

- figure regions;
- staged illustrations;
- caption and nearby-region refs;
- chunk illustration refs;
- deterministic illustration IDs;
- canonical source-page renders;
- exact bbox crop to PNG;
- crop SHA-256;
- `rkb_illustrations`;
- `knowledge://illustrations/<id>`;
- POI media evidence envelopes.

Keep it.

The old Gause revision is not evidence that the source has no pictures. The
source audit already found image-bearing pages and meaningful captions absent
from the old searchable projection.

## 3. Visual understanding belongs to ChatGPT

No server-side OCR, captioning model, VLM or image semantic pipeline is added.

During ingestion ChatGPT must visually review source pages and stage:

- figure region;
- illustration kind;
- exact printed caption regions when present;
- nearby relevant text regions;
- chunk<->illustration links;
- optional model-authored visual description when the caption/context alone is
  insufficient to make the visual discoverable.

A model-authored description is not a quotation from the book. Store and return
it with explicit provenance such as `model_observation`.

An image-only page is valid source material. Native text absence must not make
the page disappear.

## 4. Search material, not image vectors

Do not introduce CLIP or a separate image-vector space now.

E5/BGE remain text encoders. For each chunk, build deterministic retrieval
material from:

~~~text
source chunk text
+ exact captions of linked illustrations
+ optional model visual descriptions
~~~

The canonical source text remains separate and unchanged.

Embeddings are keyed by a search-material/input digest in addition to source
text/revision identity. Changing a caption/visual description invalidates only
the affected active chunk embeddings and the existing automatic indexing path
reconciles them.

This lets queries such as a Russian description of an illustration retrieve a
German-source chunk without pretending that model-authored text was printed in
the source.

## 5. Illustration read surface

A normal evidence fetch should return rich illustration descriptors rather than
only bare IDs:

- illustration ID/reference;
- kind;
- page and bbox;
- exact caption text;
- optional model visual description + provenance marker;
- crop SHA-256;
- rights/visibility metadata appropriate for the caller;
- VibePublish media-store entry ref when mirrored.

Add one narrow read-only illustration operation if necessary so ChatGPT can
request the exact authorized crop as model-visible image content. Reuse the
existing object store and authorization boundary; do not make signed object-store
URLs part of the model contract.

## 6. Telegram/VibePublish is a secondary private mirror

Regional Knowledge remains canonical for crop bytes, source/page/bbox, ACL and
rights.

VibePublish/Telegram is only a secondary durable private media mirror. Use
`vibepublish_media_store put`, as DOCUMENT, with stable origin:

~~~text
origin.system = regional_knowledge
origin.ref = knowledge://illustrations/<id>
origin.sha256 = <optional crop sha256; provenance only>
~~~

Persist `vibepublish_entry_ref` only after verified VibePublish/provider
readback.

Owner correction (2026-10-04): mirror identity uses the illustration resource and
request key, without comparing image SHA or requiring visual matching. Verify
native message, private topic and DOCUMENT delivery. Preserve crop hashes only
as technical provenance; do not build a new image-deduplication pipeline here.

Mirroring is asynchronous and must never block book activation.

The target private topic for current Regional Knowledge illustrations is:

`https://t.me/c/4368830579/4`

The exact destination binding is runtime configuration; do not hardcode a hidden
provider alias in source.

VibePublish owns the provider-side rate budget. Regional Knowledge must not open
its own Telegram session or duplicate that limiter.

Prerequisite VibePublish prompt:

`onedayonemasterpiece/vibepublish/docs/prompts/rkb-media-store-origin-rate-limit-20261004.md`

It verifies/deploys immutable origin metadata and a shared connection-level
budget of <=20 Telegram media files/images in every rolling 60 seconds.

## 7. Rights and privacy

Private import does not confer public redistribution rights.

- Private illustration mirror stays private.
- Do not publish book illustrations to public channels.
- Mirror only the exact crop, not whole PDF/page renders unless the user
  explicitly asks.
- A VibePublish ref is identity, not authority.
- Revoked source access must stop exposing the mirror ref through Regional
  Knowledge even if Telegram still contains the stored private document.

## 8. Sequence

1. Finish and production-accept the in-progress automatic indexing task.
2. Verify/deploy the VibePublish origin + <=20/min provider budget prerequisite.
3. Implement exact-source dedupe/new-revision semantics.
4. Complete the illustration staging/read/search-material/mirror path.
5. Acceptance with synthetic PDFs and a very small real Telegram canary.
6. Only then perform the corrected Gause reimport as a new revision of its
   existing document.

The corrected Gause reimport is intentionally a later ChatGPT-led content task,
not Codex semantic parsing.
