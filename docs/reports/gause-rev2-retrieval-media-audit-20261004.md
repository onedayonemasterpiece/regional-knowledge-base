# Gause revision-2 retrieval/media audit — 2026-10-04

## Scope

Production acceptance of the completed 174-page reimport of Fritz Gause,
*Кёнигсберг в Пруссии. История одного европейского города*, plus the Telegram
source/illustration mirror and retrieval quality. This report records the baseline
that motivates the bounded follow-up changes on
`chatgpt/gause-retrieval-media-quality-20261004`.

## Production baseline

Document `7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9` has active revision **2**.
The exact archived source is SHA-256
`10328207e2664aabef78201fb997e18f3ea8e48f8d7d56a98f3d6832b21ddb65`,
12,752,172 bytes, and provider readback binds it to the configured source topic.

Revision 2 contains **742 chunks**. Character-shape diagnostics:

- p50: **650** characters; mean: 838.7; p90: 1,801;
- **125** chunks are under 200 characters;
- **295** are under 500 characters;
- **57** are over 2,000 characters;
- **0** chunks span multiple pages;
- **30** chunks carry illustrations.

The absence of multi-page chunks is an import-policy outcome, not a schema
limitation: the existing staged chunk contract already accepts ordered region
references from more than one page.

## Retrieval stress evidence

Before local E5 coverage completed, the effective mode degraded to lexical-only.
A 91-item stratified exact-substring sample achieved **88/91 hit@8**. The
12 natural-language questions did not retrieve their expected evidence in this
conjunctive lexical mode.

After E5 reached **742/742**, the same active revision in `fast_e5` achieved:

- exact sampled lookup: **78/91 hit@1**, **90/91 hit@3**, **90/91 hit@8**;
- curated natural-language questions: **12/12 evidence hit@8**;
- the set included rare names, dates, historical episodes and illustration
  captions (Euler/seven bridges, Hieronymus Roth, plague 1709–1710, Plan Braun,
  Agnes Miegel, House of Technology/Hans Hopp, main station/1929).

A deliberately harsh adjacent-chunk probe concatenated terms from two neighboring
chunks. With E5 it returned at least one target for **22/46** probes and both
targets for **5/46**.

That boundary probe is diagnostic only. It is a synthetic conjunctive query and
does **not** prove missing source text or justify blanket overlap. Its value is
that, together with 125 micro-chunks, 57 long chunks and zero multi-page chunks,
it demonstrates that revision 2 was segmented mainly by PDF/native blocks rather
than by coherent retrieval passages.

BGE was still indexing at the baseline checkpoint. After BGE reached **742/742**
and the effective mode switched to `bge_lexical`, the same harness improved to
**90/91 hit@1, 91/91 hit@3 and 91/91 hit@8** on exact sampled lookup; the
12/12 semantic-question score remained intact. The synthetic boundary diagnostic
improved to **30/46 any-target@8** and **11/46 both-target@8**, but still confirms
that mechanical block segmentation leaves avoidable boundary sensitivity.

The harness intentionally ran up to eight retrievals concurrently to stress the
path. Its multi-second latency figures are therefore concurrency-stress timing,
not a claim about normal single-query interactive latency.

## Telegram findings

The source archive status is genuinely provider-verified: the exact Gause PDF is
readable from the configured source topic. The user-facing artifact is difficult
to recognize, however, because the stored document had no `source_filename`;
the upload therefore used a generic filename and an opaque
`knowledge://documents/.../source` caption instead of the book title/author/year.

Illustration crops are also provider-verified in the illustration topic. Several
Gause plates are printed sideways inside otherwise portrait source pages. The
source crop geometry is correct; a separate explicit presentation orientation is
required. Presentation rotation must not alter the reviewed source bbox or pretend
the source itself had a different orientation.

## Product assessment

The source/evidence model is sound and the E5 semantic path is already useful:
the curated semantic sample is 12/12. The current Gause revision is nevertheless
**fit with material limitations, not the target end-state**, because segmentation
can separate continuous passages at PDF block/page boundaries and can exceed the
encoders' 512-token input caps.

The smallest robust improvement is not a new vector store or a parallel
"search-window" table. It is to use the existing multi-region/multi-page chunk
contract correctly:

- compose coherent semantic passages rather than mirroring native PDF blocks;
- target roughly 800–1,800 characters when practical;
- attach short body fragments to surrounding context;
- keep genuine continuations together across page boundaries;
- avoid chunks conservatively likely to exceed the 512-token encoder budget;
- use bounded overlap only when it represents a real semantic boundary;
- allow headings, captions and image-only evidence to remain shorter.

Character thresholds are quality heuristics only; they are never claimed to be
token counts.

## PR #39 review follow-up

Independent review caught three release blockers before deployment and they were
corrected on the branch:

- VibePublish re-encodes image ingress, so provider-rendition SHA-256 is now kept
  separately from the canonical RKB display-crop digest; archived fallback reads
  verify the provider rendition rather than incorrectly requiring byte identity
  with the pre-upload PNG.
- the readable source caption retains the immutable knowledge source URI, so
  provider text search/readback still finds the entry;
- source-archive caption/filename are frozen in durable document state before
  admission. A pre-upgrade uncertain request with no saved operation ID replays
  the legacy payload under its existing idempotency key; new requests freeze the
  new readable payload. This prevents cross-version idempotency conflict without
  risking a duplicate send.

Migration 019 is the only schema migration changed; the historical 001 migration
was restored byte-for-byte. Constraint existence checks are relation-scoped.
Encoder-budget diagnostics now measure the actual augmented search material
(source plus printed captions/model observations) rather than source text alone.

## Acceptance target after the follow-up

1. Exact source text/region coverage and evidence hashes remain intact.
2. User-visible source archive post is recognizable by title/author/year and a
   safe human filename while exact bytes and immutable origin remain unchanged.
3. Explicit cardinal presentation rotation produces upright delivered
   illustrations while source bbox/source-crop identity remains preserved.
4. Normal fetch exposes revision plus ordered region/page provenance for
   multi-page evidence.
5. Chunk validation emits non-blocking fragmentation/encoder-budget diagnostics.
6. A new Gause revision is semantically rechunked and reindexed; its regression
   suite measures exact lookup, real-question evidence@8, curated cross-boundary
   evidence, illustration retrieval and degradation behavior separately.