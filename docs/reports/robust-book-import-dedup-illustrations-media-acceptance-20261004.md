# Robust book import and private visual evidence acceptance — 2026-10-04

Product acceptance passed on the existing E5/BGE production stack. No historical
Gause reimport, OCR/VLM, CLIP, paid inference, new graph platform or second index
queue was introduced. RKB PR #33 delivers the change; its final delivery receipt
records the canonical main SHA and exact-runtime readback after merge.

VibePublish prerequisite PR #32 is merged/deployed at
`5064be343fcc5f89a2eedc6887fe4d4116743b9d`: server/worker match main, all 264 tracked
files match the release, ledger schema is 7, all PR/main CI jobs passed, and public
OAuth/MCP native DOCUMENT/topic/origin/replay readback succeeded.

## Source identity and historical preservation

Before migration, production audit found one historical duplicate source group:
two inactive roots. Both were preserved separately; no history was merged or
deleted. Such an ambiguous source fails explicitly rather than silently choosing
a book. Clean source roots and all new roots receive owner/SHA uniqueness;
owner/source advisory locking and DB transactions protect concurrent starts.
Additive migrations 015/016 passed twice in isolated PostgreSQL/RLS. Production
application preserved corpus/vector counts and digests.

The actual public attached-PDF ingress acceptance produced:

| Case | Result |
| --- | --- |
| Two concurrent same-owner first starts | 1 document, 1 ingestion |
| Identical bytes under another file ID | 0 additional documents |
| Same bytes under another actor | Separate private root |
| Explicit `new_revision` | Revision 2 on the same document; title retained |

The existing Gause document has one exact-source root. Its future original-PDF
`new_revision` path therefore preserves its document identity. No Gause content
was parsed/reimported by this task; the old clipped text and missing visual
evidence remain for the subsequent ChatGPT-led source review.

## Actual source/visual and retrieval acceptance

An owned three-page synthetic PDF entered through public OAuth HTTPS tooling:
complete normal text, a drawn blue lighthouse with exact printed German caption,
and an image-only bicycle page with no native/printed text. All source renders
were visually inspected before staging. Two chunks link real staged figures.

`fetch` returns page/bbox/caption/rights/visibility/crop provenance and the labelled
`model_observation`, with verified private mirror reference. `illustration_fetch`
returns model-visible ImageContent from the authorized canonical crop. Its bytes
match the stored crop digest; a different actor's read is denied. Image-only
source fetch remains empty rather than presenting its description as a quote.

The first exact-main readback found a captioned-crop serialization defect: native
PostgreSQL UUID caption references reached the direct ImageContent tool's JSON
metadata. The descriptor now normalizes that UUID array to strings; regression
calls the actual MCP tool for both captioned and image-only crops. Final delivery
requires both real public crop calls to pass, beyond the initial image-only read.

All three queries found the appropriate control chunk in actual `bge_lexical`
retrieval: printed `Blauer Leuchtturm am Meer`, visual `Fahrrad mit zwei roten
Raedern`, and Russian `Красный велосипед с двумя колёсами и зелёной рамой` against
the German source/description.

Activation automatically installed 3 E5 and 3 BGE vectors. A controlled update
to one active synthetic chunk's search material installed exactly one E5/BGE
replacement; the other two chunks retained their vector timestamps. Source text
and its SHA stayed independent. The deliberately unsupported date added to that
model observation did not become printed evidence: source fetch stayed empty and
the descriptor retained `model_observation`. Existing SQL snapshot guards reject
stale/partial input identities and preserve safe degraded search.

There were 4 document jobs total (3 original + 1 changed input); finalized replay
created zero new jobs. No manual backfill command was used. After acceptance the
synthetic roots were archived without deleting audit receipts; existing corpus
and vector digests were unchanged.

## Private mirror, recovery and provider pacing

The configured owner-scoped VibePublish OAuth resource grant passed actual refresh
rotation and public scoped read. Credentials are mode 0600 in runtime state;
no user bearer is forwarded. Alias/topic are runtime configuration.

Both RKB crops reached the requested private topic as DOCUMENTs. Two explicit
same-key replays returned the original operations. Scoped VibePublish ledger
readback found exactly two origin roots and two native documents: zero duplicates.
Together with the VibePublish prerequisite canary, this task created only three
real canary documents. No public publication occurred.

For controlled production recovery, the VibePublish processing worker was stopped
before activation. The book finalized and all E5 vectors installed while both
mirrors remained pending. The RKB indexing owner restarted, then the VibePublish
worker resumed. Both mirrors completed without duplicate effects. This was an
intentional dependency fault test, not an unplanned outage/root-cause claim.

Image sameness is not a new SHA/perceptual gate: the illustration resource and
request key bind the mirror. Crop SHA remains provenance/storage integrity.
VibePublish retains its established evidence for the actual uploaded DOCUMENT.

The RKB 27-item fake-boundary acceptance proved missing-work recovery, owner
restriction, wrong-topic rejection, durable operation IDs, restart/replay and
eventual completion with <=20 admissions per rolling 60 seconds. VibePublish's
own real durable-budget regression verifies pre-upload admission, shared ordinary
publication/private-media connection pacing, failed-upload accounting, independent
connections, unchanged request keys and recovery. No bulk live spam was used.

## Verification and retained evidence

Full local RKB verification: 122 tests passed with real isolated PostgreSQL/RLS,
one existing Starlette deprecation warning. RKB PR CI passed Python 3.12/3.13;
the final main run and immutable-source/public readback are linked in the final
PR delivery receipt. VibePublish's complete PR/main CI passed Python 3.12/3.13.

Private source renders, controls, audit IDs and receipts remain in the managed
retained task directory under `/home/dev/artifacts/regional-knowledge-base/`;
no books, crops, private metadata, tokens or signed URLs are committed.

**Ready for ChatGPT's corrected Gause reimport: YES**, after deploying this PR's
canonical main as recorded in the final receipt. ChatGPT must read the original
source, follow continuations, review every page and stage the actual captions,
illustrations and graph evidence as a new revision of the existing document.
