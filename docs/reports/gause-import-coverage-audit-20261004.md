# Gause import coverage audit — 2026-10-04

**Revision 1 is a demonstrably incomplete source projection.** PR #26 compares
encoders on that fixed incomplete corpus; its Recall is not whole-book extraction
coverage. This read-only audit prepares a transport/validation fix without
changing production, importing a revision, or introducing OCR/ASR/LLM services.

## Evidence and method

Reused retained benchmark corpus SHA-256
`f1b3a74797e2cc11b82fbcdcb4611e5b8db871491244e9b3b584c91459d5171f`.
Read active metadata and authorized private source objects through the existing
backend, using a read-only SQL transaction and no embedding provider. Active
revision remains **1**, with **174 metadata pages and 712 chunks**. All active
UTF-8 projection byte slices equal the saved chunk texts in the original ordering.

Verified original PDF SHA-256
`10328207e2664aabef78201fb997e18f3ea8e48f8d7d56a98f3d6832b21ddb65`
and active projection SHA-256
`dcfbeb7a98a6dfa8c90e15e38605869cfae80f15d39eb540113efd84e780eed6`
against object metadata. Hash equality establishes object/transport identity;
it does **not** establish semantic completeness.

For every physical page, extracted deterministic native text/blocks from the
original PDF with PyMuPDF. Compared each complete stripped source block with
active chunks on that page, and separately compared its first 1000 characters.
No OCR was run. Page/region manifest includes metadata presence, native text,
exact clipped-prefix evidence, full-native-block matches, image presence,
coverage status, staged region IDs/bboxes and visual-review requirement. No PDF,
book text, page images, document matrices or private object keys enter Git.

## Observed coverage

| Observation | Result |
|---|---:|
| Metadata pages | 174 |
| Pages with native text / retrieval chunks | 144 / 144 |
| Snapshot characters / maximum chunk length | 461233 / 1000 |
| Exact 1000-character prefixes of longer native blocks | 200 |
| Pages containing those proven clipped previews | 120 |
| Suffix volume beyond those 200 previews | 131480 characters |
| Pages where all native blocks match full chunks exactly | 18 |
| Pages with semantically proven complete source coverage | **0** |
| Image-only pages without retrieval chunks | **145–174 (30 pages)** |

The suffix volume sums omitted portions of matched source blocks. It is **not**
a count of unique missing book characters: text can repeat elsewhere; layout,
whitespace and extraction differ. Exact comparison is deliberately conservative;
trimmed or otherwise transformed previews can fall outside the 200 exact matches.
Neither the 18 full-native-match pages nor listed page IDs prove that all image
text, captions, footnotes, layout and relations were read.

| Physical page (one-based) | Native characters | Imported characters | Direct clipping evidence |
|---|---:|---:|---|
| 1 | 157 | 151 | No exact clipped prefix |
| 2 | 839 | 837 | No exact clipped prefix |
| 3 | 1470 | 1000 | Yes: contents block is a preview |
| 4 | 5016 | 3554 | Yes: source blocks have missing suffixes |

The absence of chunks on pages 145–174 is not proof of blank pages. Direct
ChatGPT visual inspection of retained renders on **145, 151, 160, 168 and 174**
confirmed pictures with substantive captions: an Albrecht portrait, a militia
assembly, a university anniversary, the main railway station and the House of
Technology. The last two provide precise dates/person information unavailable
from their imported page text. All remaining image-only pages need visual review;
these five samples are not a completed semantic revision or caption transcription.

## Confirmed mechanism and proposed bounded fix

The existing renderer used `text[:MAX_NATIVE_BLOCK_TEXT]` with a 1000-character
cap without disclosing original length or truncation. The staged graph/chunks
contain the corresponding previews. Original source-to-projection comparison
confirms the loss at this boundary; character totals alone were not the basis
for this finding. It does not prove that *every* semantic defect has that cause.

PR #26 now prepares:

- Explicit block length, offset/end, truncation and continuation metadata. The
  same authorized `book_pages` tool reads one block portion, at most 1000
  characters, with deterministic repeat semantics. The page's block cursor also
  exposes blocks beyond the 80-block preview cap. Page/image batches and the
  24000-character page-text cap remain bounded.
- Explicit staged `source_material` and `source_review_note`. Page IDs, previews
  and full native text alone cannot pass completeness validation. Every page,
  including a textless image page, needs a model visual-review attestation and
  note. This is an explicit attestation, **not server proof of semantic truth**.
- Tests for a block longer than 1000 characters, multiple/repeated continuations,
  invalid continuation offset, textless image page, and rejection of staged
  preview/unreviewed/full-native-only material. Existing behavior tests supply
  explicit review of their synthetic fixtures.

Long extracted material still needs bounded semantic regions, correct ordering,
caption/figure relations and retrieval chunks. The transport fix does not
transcribe scans or retrospectively repair an active graph. Existing staged
jobs require source review/restaging before finalization; active revisions are
unchanged. Deployment and a complete ChatGPT semantic reimport are outside this
request and remain necessary before claiming whole-book completeness.

## Retained private evidence and incident state

`/home/dev/artifacts/regional-knowledge-base/20261004T053456Z-retrieval-validation-20261004/`
contains `.artifact.json` (retained for audit/PR review), `page-region-coverage.json`
(one entry per page and staged region), `coverage-summary.json`, verified source
PDF/projection, source graph snapshots, native extraction and five page renders.
The original benchmark lab remains intact. No credentials were copied.

Incident registry opened `inc_9dbbd671a9dd99f713518be2`, but subsequent reads/notes
were rejected with project-ownership/work-binding conflicts, including the
registry's auto-linked OAuth incident. Those rejected operations are not claimed
as saved observations or live remediation. No alternate access was attempted;
this report and retained evidence are the durable observations. The production
coverage defect remains unresolved and the incident must not be declared closed.
