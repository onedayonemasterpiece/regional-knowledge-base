# Embedding runtime decision — 2026-10-04

## Decision

For the first production retrieval stack of Regional Knowledge Base:

1. **Fast / always-ready semantic tier:** multilingual E5-small INT8 on DevCoveer.
2. **Main / higher-quality tier:** BGE-M3 on Kaggle CPU, to be implemented separately after the fast tier is operational.
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

Planned separately:

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

## Concurrency hypothesis to verify next

The isolated DevCoveer benchmark already measured four queued E5 requests with low latency, but production acceptance must include the whole path:

- encoder queue;
- query embedding;
- PostgreSQL/pgvector retrieval;
- lexical branch/fusion;
- evidence hydration;
- API/MCP response.

The next acceptance should measure **1, 5 and 10 concurrent end-to-end searches**.

Start with one encoder worker and inference concurrency = 1. Do not add multiple model processes unless measurements require it.

Initial product target:

- 5 concurrent users: p95 end-to-end fast search comfortably below 1 second;
- 10 concurrent users: no OOM/crash and bounded queue behavior; target p95 below 2 seconds;
- encoder remains within the 1 CPU / 1 GiB envelope.

These are acceptance targets, not previously measured facts.

## Vector-space storage requirement

The current legacy embedding column must not be reused by dimensional coincidence or by overwriting its model identity.

Production needs an explicit E5 384-dimensional vector space and, later, a separate BGE-M3 1024-dimensional vector space. The implementation may use a verified multi-space embedding table or separate typed vector fields, but must preserve these invariants:

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

The immediate implementation task is:

**Productionize E5-small INT8 as the always-ready DevCoveer fast semantic tier and perform real 1/5/10-user end-to-end concurrency acceptance.**

Do not implement Kaggle/BGE in the same task. Once the fast tier is deployed and measured, the next implementation task is the Kaggle CPU BGE worker/orchestrator.
