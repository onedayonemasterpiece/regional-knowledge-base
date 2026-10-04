# E5/BGE canonical main reconciliation — 2026-10-04

Accepted E5 fast-tier and Kaggle CPU BGE main-tier are consolidated in canonical
main. The selected warm path remains **BGE + lexical**. No graph implementation,
book reimport, model research, paid inference or queue/lifecycle rewrite was done.

## Canonical integrations

| PR | Result | Canonical merge |
|---|---|---|
| [#27](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/27) | E5 merged first | `c2a6259e06d026b0becadd63139c42c87fda2a45` |
| [#28](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/28) | Retargeted to main, synchronized after E5, CI passed, then merged | `81a18b92af4154690e9b8bcea4a2d02bb2f5216b` |
| [#29](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/29) | Narrow source transport/provenance/validation port merged | `f03751d3b03a9bb0910831dc275b1c190b47ad1b` |
| [#26](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/26) | Closed as superseded only after #29 reached main; original branch/history preserved | Evidence head `d987eedac2396a4ecd65fd9acbbb0691c6c18bfb` |

The [evidence index](embedding-benchmark-evidence-20261004.md) links five historical
reports/aggregates, copied byte-for-byte from #26, and immutable experiment code.
Benchmark scripts and private fixtures were not imported into production.

## Runtime readback of integrated code

Deployed integrated code SHA: `f03751d3b03a9bb0910831dc275b1c190b47ad1b`.
MCP, E5 and BGE controller process working directories resolve to that immutable
release. E5's existing `current` pointer resolves to the same source; runtime
model assets, credential files and durable queue were preserved. The final
report-only merge is deployed and read back separately; its exact SHA and final
CI/readback are recorded in the reconciliation PR handoff rather than embedding
a self-referential commit identifier in this file.

- E5 ready; `LocalE5Embedder`, one actual encoder process, CPU quota 1 CPU,
  memory cap 1 GiB, no swap, about 625 MB cgroup memory during initial readback.
- Dedicated external embedding configuration absent; no paid fallback selected.
  MCP process maps contain no libtorch/onnxruntime/tokenizers libraries.
- Migration 010/011 tables and RPCs present. Active chunks: **747**;
  matching revision/source-hash E5 vectors: **747**; BGE vectors: **747**.
  Legacy vectors: **907**, digest unchanged from accepted migration evidence
  (`b373d802fd8b2b7817fb2893400d9f22`).
- Public OAuth MCP search/fetch passed. After idle expiry, normal demand started
  one Kaggle CPU worker: initial E5 answer/fetch took 1.205 s, BGE+lexical ready
  including polling took 64.383 s, three fetches succeeded, zero failures.
  User OAuth token was rejected at worker claim with 403; temporary OAuth family
  revoked. This is a reconciliation smoke, not renewed latency acceptance/SLA.

## Source completeness transport

Only ten focused source/test/doc paths from #26 were ported, plus retained reports.
Native block reads are bounded to 1000 characters with repeatable
`text:page:block:offset` continuation, original length/offset/end/truncation and
explicit native-preview metadata. Blocks beyond the initial block cap remain
addressable. Source PDF integrity verification and actor authorization precede
material access.

Existing production source was read through both the normal owner ACL backend
and **public OAuth MCP `book_pages`** without reimport. A clipped block continuation
returned offset 1000/end 1195/original length 1195; repeated calls were identical,
bounded, and retained `requires_visual_review=true`. No private text, page image,
source title or identifier was retained in public evidence.

Provenance is explicit: unreviewed / preview / full_native / visual_reviewed.
Validation rejects page IDs alone, native previews, full native extraction,
and visual_reviewed without a nonblank review note. A review marker is a gate,
not proof that a human/model actually reviewed every source feature. Image-only
pages still need visual review; existing active projection was not repaired.

Full suite on integrated main: **96 tests passed**, with one existing Starlette
alias deprecation warning. Focused tests cover clipping, continuation replay,
source reconstruction, malformed cursors, block-cap overflow, image-only pages,
authorized transport and provenance validation. Integrated main CI on Python
3.12/3.13 is [green](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37189978266).
BGE standalone retargeted CI is
[green](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37189574529).

## Preserved acceptance and remaining limits

The [E5 acceptance](fast-e5-production-acceptance-20261004.md) retains historical
1/5/10-user public search/fetch p95 **0.879 / 1.412 / 2.121 s**, one CPU / <=1 GiB
and lexical fallback. These measurements do not establish a new SLA.

The [BGE acceptance](bge-kaggle-multilingual-hybrid-acceptance-20261004.md) retains
747/747 backfill/idempotent replay, BGE+lexical selection (all-three worse on the
fixed fixture), approximately 73.7 s historical first cold readiness, useful-work
30-minute lease, pre-11-hour succession, fencing/single-start/recovery evidence.
Accelerated lifecycle checks are not an 11-hour soak. Positive partial labels
support reported recall/MRR, not precision, nDCG or unsupported-answer safety.

The Gause source audit still reports an incomplete active projection: 200 clipped
native previews and 30 image-only pages without chunks. This transport fix enables
correct future review/reimport; it does not silently repair those gaps.

Retained operator evidence:
`/home/dev/artifacts/regional-knowledge-base/20261004T083703Z-reconcile-e5-bge-main-20261004`
(no automatic expiry). Earlier accepted E5/BGE evidence remains preserved.

The existing accumulative graph prompt is untouched. Repository/runtime
prerequisites are reconciled; it is safe to start that separate execution task
once final exact-main deployment/CI readback in the handoff is green. Its own
source-review, authorization and acceptance gates remain required.
