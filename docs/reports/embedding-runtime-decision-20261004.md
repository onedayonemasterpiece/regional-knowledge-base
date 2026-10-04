# Embedding runtime decision — 2026-10-04

## Decision

For the first production retrieval stack of Regional Knowledge Base:

1. **Fast / always-ready semantic tier:** multilingual E5-small INT8 on DevCoveer.
2. **Main semantic tier:** pinned BGE-M3 on Kaggle CPU, now deployed and accepted; use BGE + lexical when warm. E5 remains independent for cold/pending results.
3. **Client-side inference:** proven technically viable on one representative Android device, but **deferred from production**. It is not needed while the server-side E5 tier fits the resource envelope, and avoiding client inference reduces battery, thermal and device-compatibility risk.
4. **Lexical retrieval remains available** and is fused with E5 where it materially helps short names/toponyms.
5. **No paid or implicit external embedding API fallback is allowed.**
6. Different embedding models are different vector spaces. Query vectors must never be compared with document vectors produced by another encoder/runtime contract.

This document records the outcome of the DevCoveer benchmark, retrieval follow-up and device experiments. It does not claim that the current Gause book projection is complete; that is a separate known ingestion defect documented in the source coverage audit.

## Evidence already recorded

Primary reports:

- [DevCoveer small encoder benchmark](devcoveer-small-embedding-benchmark-20261004.md)
- [Retrieval validation follow-up](embedding-retrieval-validation-followup-20261004.md)
- [Gause import coverage audit](gause-import-coverage-audit-20261004.md)
- [Completed E5 production acceptance](fast-e5-production-acceptance-20261004.md), PR #27
- [Completed BGE/Kaggle multilingual acceptance](bge-kaggle-multilingual-hybrid-acceptance-20261004.md), PR #28
- PR #26: https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/26

### DevCoveer benchmark

All candidates were measured on CPU under a hard 1 CPU / 1 GiB envelope.

| Candidate | Warm query p95 | Full retrieval p95 | Cgroup peak | Original fused Recall@10 | Multi-evidence @10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| E5-small INT8 | 23.10 ms | 35.01 ms | 757.49 MiB | 96.43% | 5/5 |
| EmbeddingGemma Q4 | 177.31 ms | 194.99 ms | 638.64 MiB | 98.21% | 4/5 |
| POTION mmap | 3.18 ms | 16.85 ms | 1024 MiB | 75.00% | 1/5 |

Interpretation:

- E5 is the best measured fit for an **always-ready fast tier**: small assets, low startup latency, substantial memory headroom below 1 GiB, and strong known-evidence recovery.
- Gemma remains a valid quality candidate; the follow-up did not establish a universal E5 quality win.
- POTION is rejected as the default fast encoder for this workload because retrieval quality and memory behavior are not competitive enough despite very low warm latency.

### Retrieval-quality caveats

The follow-up evaluation deliberately preserves uncertainty:

- the original active book projection is incomplete;
- known-evidence Recall is not answer accuracy;
- pooled relevance judgments are partial;
- neither E5 nor Gemma has a calibrated abstention threshold;
- vector retrieval returns nearest passages even for unsupported questions;
- production lexical search is weak for long natural-language Russian questions and mostly helps short keyword/name queries;
- E5 document embeddings show batch-size sensitivity in the current ONNX path; the exact production encoding contract must therefore be pinned and reproduced.

The production decision here is about the fast availability tier, not a claim that E5 is the strongest possible historical-research encoder.

## Device experiment

A public diagnostic page in Projects Hub ran the same pinned E5-small INT8 export through CPU/WASM on a representative Samsung S21 Ultra-class Android device. No production search path was changed.

### First run — empty browser cache

- model load: 70.53 s;
- launch to first embedding: 73.40 s;
- cache hits/misses: 0 / 8;
- warm query p50/p95: 79.60 / 98.85 ms;
- four-request queued p95: about 345 ms;
- no UI frame gap over 100 ms.

Offline scoring of the device-computed query vectors against the private 712-chunk benchmark matrix produced:

- fused Recall@10: **96.43%**;
- natural-question fused Recall@10: **95.83%**;
- all required evidence for original composed questions at top-10: **5/5**;
- mean device/server top-10 overlap: **96.33%**.

The browser/server numerical-vector compatibility gate was intentionally strict and failed for some fixture cases, but the actual retrieval outcome was effectively equivalent at top-5/top-10. Product compatibility must therefore be judged primarily by retrieval behavior, not exact component equality.

### Second run — cached assets

- model load: **5.16 s**;
- launch to first embedding: **6.48 s**;
- cache hits/misses: **4 / 0**;
- warm query p50/p95: **79.75 / 99.93 ms**;
- four-request queued p95: about **346 ms**;
- maximum UI frame gap: **17.9 ms**;
- no UI frame gap over 100 ms.

All 30 query token sequences and all 30 x 384 query-vector components were identical between the two phone runs. The phone execution is therefore stable across repeated cached runs on this device.

### Device conclusion

Client inference is feasible, but it is not currently justified as the default production path:

- server E5 already fits inside the agreed 1 GiB / 1 CPU envelope;
- client inference consumes device CPU/battery and may cause thermal effects;
- Android/WebView/browser behavior differs by device;
- Projects Hub voice operation already has device-side audio responsibilities;
- one strong phone does not establish fleet-wide reliability.

Keep the browser/device harness as retained research evidence and a future optimization path. Do not ship model inference to clients until server capacity or product requirements make it valuable.

## Target production architecture

```text
query
  |
  +-- if BGE Kaggle tier is READY:
  |       BGE-M3 + lexical -> evidence
  |
  +-- otherwise:
          E5-small INT8 on DevCoveer + lexical -> fast evidence
          |
          +-- ensure one Kaggle warm-up request exists
```

### Fast tier — DevCoveer

- one always-ready E5 encoder process;
- pinned INT8 model/export and preprocessing contract;
- one inference CPU;
- memory limit at or below 1 GiB;
- bounded request queue;
- initially serialize model inference rather than multiplying model processes;
- no autoscaling until real contention is observed;
- model/query cache is optional and should be added only if measured useful;
- no external inference fallback.

### Main tier — Kaggle CPU

Implemented and accepted in production:

- BGE-M3, CPU only;
- durable queue outside the notebook;
- single-start protection so simultaneous users do not create multiple notebooks;
- 30-minute idle lease after the last useful request/work;
- heartbeat does not extend the useful-work lease;
- planned rotation before 11 hours of notebook lifetime;
- during rotation at most one serving worker plus one warming successor;
- short interactive queries take priority over long import batches;
- cold state is a normal state, not an error.

When BGE is warm, E5 does not need to be executed for every request unless later measurements show value in multi-encoder fusion.

## Measured production acceptance

E5 production acceptance is complete; one encoder stays within 1 CPU / 1 GiB.
Actual public search/fetch p95 at 1/5/10 users was **0.879 / 1.412 / 2.121 s**.
The original 5-user <1 s and 10-user <2 s product targets were missed; the E5
report records the concrete PostgreSQL/hydration bottleneck. Completion does not
mean those latency targets were met.

BGE uses a private Kaggle CPU worker and durable DevCoveer queue. Backfill covers
**747/747 authorized active chunks** with source hash/revision checks; replay
skips all 747 with zero submitted/written jobs. Separate typed E5/BGE tables leave
907 legacy vectors intact. Exact BGE space is
`bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1`, 1024-d FP32 CLS/L2, no prefixes,
512-token truncation, query/document batch 1. Pinned model revision is
`5617a9f61b028005a4858fdac845db406aefb181`; no inference runs in MCP.

Seven same-corpus ablations on **40 DE/RU queries** (38 positive, two unsupported;
16 paired needs; original German evidence, no evidence translation) select
**BGE + lexical**: Recall@10 **82.89%**, MRR@20 **.7757**, multi completeness
**6/9**. BGE alone gives **77.63%**; all-three gives **67.11%**, MRR **.5819**,
multi **5/9**. Do not execute E5 on every warm request: it adds work and loses
known-evidence quality on this fixture. Dual encoders were evaluated in parallel,
with separate spaces and RRF, not by mixing cosine scores.

For 12 paired natural-language needs, German-source DE/RU BGE+lexical Recall@10
is **95.83% / 87.50%** (8.33 pp loss); including four short-name pairs, the gap is
18.75 pp. Lexical improves short German names but does not itself bridge Russian
aliases. Person/event/place readiness favors BGE+lexical; explicit exact/current/
historical alias signals remain separate and caller-scoped, with no POI merge.
Judgments are agent-authored known positives, not exhaustive independent labels;
Precision/nDCG and calibrated abstention cannot be claimed. Both vector models
return nearest passages for unsupported premises. This is one narrow German
source, not a universal multilingual quality result.

Actual warm production backend and public OAuth MCP each passed **90/90 requests**,
30 per concurrency level. HTTP p95 at 1/5/10 users: **1.778 / 5.105 / 9.164 s**.
At ten users BGE queue p95 is **7.927 s**, inference **.259 s**, database **.204 s**,
fusion/metadata **.062 s**, hydration **.543 s**. Functional acceptance is complete;
a subsecond warm SLA is not established.

First cold demand → ready was **73.686 s**; later cold **75.370 s**. Ten public
cold users received E5 evidence + starting/job ID at p95 **2.595 s**, then main
packs at p95 **92.587 s** including client polling. Warm calls wait at most ten
seconds before returning fast evidence + pending. Same-query, actor-bound job
polling is available in full search and Live evidence search.

Useful demand/work renews a **30-minute** lease; heartbeat never renews it.
Planned **10h45m** succession allows one serving + one warming worker. Real
rotation accepted 120 queued jobs, with 43 pending at handoff; successor ready in
68.142 s. Worker loss recovered a claimed job at attempt 2; stale results were
403. An actual intentionally failing private CPU notebook returned ERROR;
controller preserved jobs and E5 evidence. Provider unavailability and idle/
lifetime deadlines also passed controlled tests. Boundary timestamps were
accelerated; no eleven-hour wall-clock soak or natural provider outage is claimed.

See the [BGE report](bge-kaggle-multilingual-hybrid-acceptance-20261004.md) and
[operator runbook](../operations/bge-kaggle.md) for complete measurements,
contract, ACL/migration proof, private-evidence checksums and recovery procedure.

## Vector-space storage requirement

The current legacy embedding column must not be reused by dimensional coincidence or by overwriting its model identity.

Production now uses separate E5 384-dimensional and BGE-M3 1024-dimensional embedding tables (migrations 010 and 011). Both preserve these invariants:

- vector-space identifier is stored with the vector;
- dimensions are enforced;
- model/export/preprocessing revision is identifiable;
- search explicitly chooses the matching space;
- an unavailable space degrades safely rather than comparing incompatible vectors;
- existing legacy vectors remain readable during migration but are not silently mixed into E5/BGE search.

## Source completeness remains independent

The active Gause revision has 174 metadata pages / 712 chunks, but source coverage audit proved:

- 200 imported chunks are clipped 1000-character previews of longer native blocks;
- pages 145-174 are image-only in the native PDF layer and have no retrieval chunks;
- sampled image pages contain substantive captions.

Therefore productionizing E5 improves retrieval over the **current active projection**, not whole-book completeness.

PR #26 prepares bounded continuation/source-review mechanics. After that transport is deployed, ChatGPT still needs to perform a complete semantic source review and create a corrected book revision. That work must not be replaced by server OCR/LLM parsing.

## Next implementation step

Implement the **minimal accumulative knowledge graph + bidirectional entity/POI
discovery** using existing PostgreSQL/Supabase and the accepted retrieval stack:
[execution prompt](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/prompts/accumulative-knowledge-graph-mvp-after-bge-20261004.md).

Keep evidence-scoped people/events/threads and canonical Street Story POI
references, bounded alias-triggered discovery, ambiguity review and one-hop
navigation. Do not introduce a separate graph database, automatic identity merge
or backend LLM extraction. Graph readiness does not repair the known source
coverage defect.
