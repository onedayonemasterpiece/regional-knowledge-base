# E5-small INT8 is the recommended fast encoder on DevCoveer

Measured 2026-10-04 for Regional Knowledge Base. **Use E5-small INT8 with the
existing lexical branch and equal-weight RRF as the fast/cold-start encoder.**
It recovered 96.43% of required evidence at top-10 over all 28 answerable
questions, 95.83% on the 24 natural questions, and all evidence for 5/5 composed
questions. Final warm p95 was **23.10 ms**, measured encoding + SQL + ranking +
fusion p95 **35.01 ms**, and new-process launch to first embedding **1.62 s**.
Lifetime cgroup peak was **757.49 MiB** under a hard 1 GiB / 1 CPU envelope.

Gemma Q4 has slightly higher overall evidence Recall@10 (98.21%), but warm p95
is 177.31 ms and it misses one required fragment of the Ottokar question.
POTION mmap is fastest on warm queries, but its fused Recall@10 is 75.00%,
complete multi-evidence recovery is 1/5, and it touches the 1 GiB cgroup ceiling.
Its ordinary Model2Vec loader OOMs. E5 offers the strongest fit to the fast
preview goal on this fixture; this does not establish a universal multilingual
quality winner. Production configuration and vectors were not changed.

## How to read the measurements

POTION rows below mean **FP32 memory-mapped weights + official Model2Vec
StaticModel constructor and encode**, not a successful ordinary
`StaticModel.from_pretrained` load. E5/Gemma query/startup columns use their final
fresh-process live pass; corpus measurements and lifetime peaks include the
complete earlier corpus pass. Initial query measurements remain in the JSON
(E5 p95 25.05 ms; Gemma p95 187.65 ms). Latency samples were not pooled or chosen
by minimum. Disk file totals include tokenizer/config/model-card files.
Download durations refer to successful per-model sessions; the interrupted initial
partial download is excluded. They are network observations, not startup latency.

Loaded memory and per-phase query peaks in the memory table come from the full
corpus process. Its later warm queries retain earlier corpus allocation buffers.
The final query-only pass is stored separately, with its own memory samples.
The cgroup column is the largest recorded lifetime peak across these passes.

## Model files and startup

| Candidate | Dimensions | Files MiB | Download s | Load s | First embed ms | Launch → first embed s |
| --- | --- | --- | --- | --- | --- | --- |
| e5 | 384 | 129.12 | 27.79 | 1.35 | 19.30 | 1.62 |
| gemma | 768 | 208.61 | 44.61 | 2.37 | 104.91 | 2.72 |
| potion | 256 | 506.39 | 107.00 | 4.06 | 69.76 | 4.40 |

## Memory

| Candidate | Loaded RSS MiB | Loaded PSS MiB | One-query sampled peak RSS MiB | Corpus sampled peak RSS MiB | Lifetime RSS high-water MiB | Cgroup peak MiB |
| --- | --- | --- | --- | --- | --- | --- |
| e5 | 467.68 | 459.61 | 595.75 | 595.48 | 596.88 | 757.49 |
| gemma | 457.86 | 448.24 | 558.28 | 567.46 | 567.55 | 638.64 |
| potion | 707.02 | 698.05 | 746.64 | 760.16 | 760.16 | 1024.00 |

## Query latency

| Candidate | Requests | Warm p50 ms | Warm p95 ms | Warm max ms | Full retrieval p95 ms | 4-client queued embedding p95 ms | 4-client queued embedding max ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| e5 | 104 | 15.06 | 23.10 | 25.33 | 35.01 | 69.21 | 107.24 |
| gemma | 104 | 140.59 | 177.31 | 192.01 | 194.99 | 614.77 | 742.06 |
| potion | 104 | 0.33 | 3.18 | 12.40 | 16.85 | 2.04 | 12.85 |

## Full-corpus throughput

| Candidate | Batch size | Chunks | Encoding s | Chunks/s | Sampled RSS peak MiB |
| --- | --- | --- | --- | --- | --- |
| e5 | 1 | 712 | 80.07 | 8.89 | 507.79 |
| e5 | 4 | 712 | 153.89 | 4.63 | 595.48 |
| gemma | 1 | 712 | 835.70 | 0.85 | 567.46 |
| gemma | 4 | 712 | 1255.59 | 0.57 | 546.61 |
| potion | 1 | 712 | 46.05 | 15.46 | 756.97 |
| potion | 4 | 712 | 46.26 | 15.39 | 760.16 |

## Quality: 24 answerable natural questions

| Candidate | Mode | n | Recall@1 % | Recall@5 % | Recall@10 % | MRR | All evidence@10 % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | lexical | 24 | 0.00 | 0.00 | 0.00 | 0.0000 | 0.00 |
| e5 | vector | 24 | 70.83 | 89.58 | 95.83 | 0.8240 | 100.00 |
| e5 | fused | 24 | 70.83 | 89.58 | 95.83 | 0.8240 | 100.00 |
| gemma | vector | 24 | 68.75 | 89.58 | 97.92 | 0.8417 | 80.00 |
| gemma | fused | 24 | 68.75 | 89.58 | 97.92 | 0.8417 | 80.00 |
| potion | vector | 24 | 45.83 | 77.08 | 79.17 | 0.6197 | 20.00 |
| potion | fused | 24 | 45.83 | 77.08 | 79.17 | 0.6193 | 20.00 |

## Quality: 4 known-evidence keyword controls

| Candidate | Mode | n | Recall@1 % | Recall@5 % | Recall@10 % | MRR | All evidence@10 % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | lexical | 4 | 25.00 | 75.00 | 100.00 | 0.5000 | — |
| e5 | vector | 4 | 50.00 | 50.00 | 50.00 | 0.5163 | — |
| e5 | fused | 4 | 50.00 | 100.00 | 100.00 | 0.6000 | — |
| gemma | vector | 4 | 75.00 | 75.00 | 100.00 | 0.7857 | — |
| gemma | fused | 4 | 75.00 | 100.00 | 100.00 | 0.8000 | — |
| potion | vector | 4 | 25.00 | 25.00 | 25.00 | 0.2646 | — |
| potion | fused | 4 | 50.00 | 50.00 | 50.00 | 0.5417 | — |

## Quality: all 28 answerable questions

| Candidate | Mode | n | Recall@1 % | Recall@5 % | Recall@10 % | MRR | All evidence@10 % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | lexical | 28 | 3.57 | 10.71 | 14.29 | 0.0714 | 0.00 |
| e5 | vector | 28 | 67.86 | 83.93 | 89.29 | 0.7801 | 100.00 |
| e5 | fused | 28 | 67.86 | 91.07 | 96.43 | 0.7920 | 100.00 |
| gemma | vector | 28 | 69.64 | 87.50 | 98.21 | 0.8337 | 80.00 |
| gemma | fused | 28 | 69.64 | 91.07 | 98.21 | 0.8357 | 80.00 |
| potion | vector | 28 | 42.86 | 69.64 | 71.43 | 0.5690 | 20.00 |
| potion | fused | 28 | 46.43 | 73.21 | 75.00 | 0.6082 | 20.00 |

## Answerability controls

| Candidate | Population 2026: top cosine | Tram fare 2026: top cosine | Min top cosine on answerable natural questions | Max top cosine on answerable natural questions |
| --- | --- | --- | --- | --- |
| e5 | 0.8236 | 0.8304 | 0.8320 | 0.9114 |
| gemma | 0.5216 | 0.3103 | 0.4363 | 0.7676 |
| potion | 0.4651 | 0.4042 | 0.4849 | 0.7416 |

## Product gates and the decision

| Gate | E5 INT8 | Gemma Q4 | POTION FP32 mmap |
| --- | --- | --- | --- |
| One CPU enforced, GPU disabled | PASS | PASS | PASS |
| Strict cgroup peak < 1 GiB | PASS | PASS | FAIL: touched 1024 MiB |
| Observed process RSS < 1 GiB | PASS | PASS | PASS |
| Loaded RSS < 800 MiB | PASS | PASS | PASS |
| Warm query p95 ≤ 1 s | PASS | PASS | PASS |
| Measured encoder + SQL + ranking/fusion p95 ≤ 2 s | PASS | PASS | PASS |
| Completed successful-loader run without crash/OOM | PASS | PASS | PASS |
| Default loader without OOM | PASS | PASS | FAIL |

Best safe measured corpus batch size was **1 for all three candidates**.
POTION batch 1 vs 4 differs by only 0.20 s here, so that ordering is not a strong
batch-size optimization claim. No further tuning is needed to select E5.

Gemma is a viable higher-latency alternative, not rejected for inability to run:
it fits the limits and recovers the unnamed Roth/prisoner paraphrase which E5
misses. E5 instead recovers both required Ottokar fragments; Gemma finds only one
in top-10. E5's 129.12 MiB assets, 1.62 s startup and 23.10 ms warm p95 favor the
always-ready preview role. Gemma's slightly better average does not erase the
composed-query trade-off. Main exact retrieval still requires its own later
benchmark; BGE-M3/Kaggle was not implemented or measured here.

POTION is rejected as the default fast encoder for this workload. Its very low
warm latency does not offset the loss of required evidence, incomplete recovery
on 4/5 composed questions, larger 506.39 MiB assets, higher loaded RSS and
ordinary-loader OOM. The mmap attempt demonstrates that it can execute, but not
comfortable memory headroom or adequate coverage on this fixture.

E5 vector-only finds the known keyword-control evidence in 2/4 cases; fusion
finds it in 4/4. In particular, short names/toponyms can otherwise promote broad
headings. This is why the recommendation includes lexical RRF. The natural
paraphrase about the prisoner who refused to ask for mercy remains a miss for
E5 (3/4 paraphrase questions hit at top-10); Gemma and POTION hit 4/4 in that small
slice. This limitation is visible rather than averaged away.

## Queries without a supported answer

All three vector-only and fused modes return ten passages for **both** negative
controls; lexical returns none. Returning a passage is not evidence that the
book answers a 2026 population or current tram-fare question. No abstention
threshold was fitted. Gemma's population-control cosine falls inside its range
for answerable queries. E5's highest negative is only about 0.0016 below its
lowest answerable top score; two easy out-of-period negatives do not validate a
safe threshold. Raw cosine scales must not be compared across models.

A downstream answer must verify evidence and allow an unsupported-answer
outcome. This benchmark measures retrieval, not answer generation or a production
confidence policy. It does not claim false-positive control for a deployed app.

## Reproducibility, retained evidence and cleanup

- Harness and lock: [scripts/benchmarks/README.md](../../scripts/benchmarks/README.md)
  and `small_embedding_requirements.txt`. The isolated runtime measured ONNX
  Runtime **1.30.0**, tokenizers **0.23.2**, numpy **2.5.3**, Model2Vec **0.9.0**,
  psutil **7.2.2** and psycopg **3.2.13**.
- Public, small machine-readable measurements:
  [devcoveer-small-embedding-benchmark-20261004.summary.json](devcoveer-small-embedding-benchmark-20261004.summary.json).
- Questions and evidence:
  [small_embedding_questions.json](../../scripts/benchmarks/small_embedding_questions.json).
- Winner reference:
  [small_embedding_compatibility.json](../../scripts/benchmarks/small_embedding_compatibility.json),
  ten multilingual strings, query/document roles at batch 1, exact preprocessing,
  384 dimensions, pinned file hashes, normalized FP32 byte hashes, token IDs and
  proposed tolerance comparisons. PWA/Android compatibility is not claimed yet.
- Full JSON, vectors, private corpus, memory traces, manifests and systemd journals:
  `/home/dev/artifacts/regional-knowledge-base/20261004T001513Z-small-embeddings-20261004/`.
  Consolidated result: `devcoveer-small-embeddings-20261004.json`.
  This managed directory is **retained pending review**, with private directory
  mode 0700 and corpus mode 0600; there is no automatic expiry.

One known model cache is retained at that directory's `models/` for offline
reproduction/review of the three measurements: **844.12 MiB**. The shared isolated
venv/runtime is **207.61 MiB**, reported separately rather than charged three
times. There are no incomplete/partial model files or duplicate model copies,
and one private corpus snapshot. An interrupted partial download was replaced
by the completed file. Unrelated pre-existing lab/project untracked artifacts
were not touched. `dev-artifacts clean --project regional-knowledge-base` was a
dry run with **0 expired candidates**; no retained evidence was deleted.
Free disk at consolidation was **25.22 GiB**.

Validation: all source hashes and page provenance verified; existing lexical RPC
matches the reproduced SQL; independent matrix-based ranking/metric recomputation
matches worker MRR; **77 project tests passed** (one existing Starlette deprecation
warning). Tests cover prefixes, normalization, reference dimensions/hashes,
known ranking metric cases, RRF and fixture validity. No ML dependency was added
to production or required by the test suite.

## Next client test

Use the same pinned E5 INT8 export in a Projects Hub PWA/WebView CPU/WASM
compatibility benchmark. Match all ten query/document vectors and token IDs
against the committed fixture using its reference batch size, then measure
startup, peak memory, 100 warm queries and four queued requests on representative
client devices. Explicitly test batching/padding differences; tolerances are
verification targets, not proven browser/Android agreement. Test native Android
against the same fixture in a separate local-device/Android CI run. Do not
replace production vectors or implement automatic fast-to-main switching as
part of that compatibility check.

## Verified source and resource envelope

The live read found document `7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9`, active
revision **1**, **174 physical pages** and **712 chunks**. Its private text
projection was fetched through the existing backend object store. The corpus
JSONL SHA-256 is
`f1b3a74797e2cc11b82fbcdcb4611e5b8db871491244e9b3b584c91459d5171f`.
Corpus text and binaries are outside Git in managed, private artifact storage.

Repository base: `4faa1f68f6bfc56bc610cb59c3a50a898c8cca91`.
The inspected service release path was based on
`fda71420bfb38919418defdeccc3d7bc2e51ab7e`.
Production embedding configuration resolved to `LexicalOnlyEmbedder`.
The benchmark requested no production restart, config/schema mutation or vector
write. Database benchmark transactions used `SET TRANSACTION READ ONLY`;
existing search RPC validation explicitly supplied a NULL query vector.
No shared embedding/API key was used. All inference was local and CPU-only.

Host: two visible CPUs, Intel Xeon Gold 6248R @ 3.00 GHz, Linux 7.0.0-28,
Python 3.14.4. Each candidate ran separately in a transient user systemd service:
`MemoryMax=1G`, `MemorySwapMax=0`, `CPUQuota=100%`, taskset affinity to CPU 0.
Workers recorded actual cgroup settings `memory.max=1073741824`,
`memory.swap.max=0`, `cpu.max=100000 100000`. These were hard limits, not merely
an observed target. ONNX used the CPU provider, one intra/inter-op thread,
sequential execution and no spinning; BLAS threads were 1. Tokenizer parallelism,
Model2Vec multiprocessing, GPU visibility and HF online access were disabled.

The process-tree profiler sampled RSS/PSS with a requested 20 ms wait between
samples; reading smaps adds overhead, so the actual cadence is longer.
Per-phase sampled peaks can miss short spikes. The process high-water RSS and
kernel cgroup lifetime peak are also retained. Cgroup memory includes more than
RSS, including charged file cache. A failed OOM checkpoint is not a PASS:
terminal systemd status controls.

## Pinned models and contracts

| Candidate | Public export / model | Exact revision | Contract |
| --- | --- | --- | --- |
| E5-small | [Xenova/multilingual-e5-small](https://huggingface.co/Xenova/multilingual-e5-small), `onnx/model_quantized.onnx` | `761b726dd34fb83930e26aab4e9ac3899aa1fa78` | 384d; `query: ` / `passage: `; attention-mask mean over `last_hidden_state`, then L2; 512 tokens |
| EmbeddingGemma | [onnx-community/embeddinggemma-300m-ONNX](https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX), `onnx/model_q4.onnx` + external data | `5090578d9565bb06545b4552f76e6bc2c93e4a66` | Native 768d; `task: search result \| query: ` / `title: {title or none} \| text: `; exported `sentence_embedding`, then L2; 2048 tokens |
| POTION | [minishlab/potion-multilingual-128M](https://huggingface.co/minishlab/potion-multilingual-128M), `model.safetensors` | `73908c3438cf03b6a01bcb9611d62b23d0726f08` | 256d; official Model2Vec mean pooling / normalization; no prefixes; max 512 tokens; FP32 table |

All three exports downloaded anonymously without a new license acceptance or
credentials. The E5 graph contains integer/dynamic-quantization operators;
Gemma Q4 contains `MatMulNBits`. Gemma uses its documented sentence output,
not an arbitrary hidden state. Matryoshka 256 was not measured.

No document exceeded the tested context cap: observed maximum lengths were
315 tokens for E5, 450 for Gemma and 323 for the POTION tokenizer.
The two ONNX models use the pinned tokenizer's BOS/EOS postprocessor and right
padding with masked pad IDs 1 (E5) / 0 (Gemma). E5 ignores document titles;
Gemma uses the actual chunk title. Vector spaces are independent; no cross-model
similarities or production halfvec comparisons were made.

## Questions, rankings and metric definitions

The committed fixture contains **30 questions**: 24 answerable natural questions,
four short keyword controls and two unanswerable 2026 controls. Source evidence
was read and verified before scoring; the fixture records chunk IDs, source text
hashes, physical page indices and brief paraphrased verification notes.

The seeds needed additional evidence: the E5-name question requires the naming
passage and Ottokar's military role; the university question requires both the
three objectives and the separate 1544 opening passage; the Roth question uses
the resistance and Warsaw passages rather than treating a short biographical
judgment as a complete answer. Five question labels require multiple evidence
groups. Their groups describe required facts, not independent statistical trials.

Vector-only uses brute-force cosine over 712 locally encoded, L2-normalized
vectors. Lexical-only reproduces the actual active-revision SQL:
`websearch_to_tsquery('simple', query)` and `ts_rank_cd`, with a document filter,
verified against `rkb_hybrid_search(query,NULL,NULL,20)`.
RRF uses equal branch weights, `k=60`, top-100 per branch and ascending chunk-ID
ties; it never sums raw similarities from different models.

Recall@k is macro required-evidence-group recall: fraction of required groups
with an acceptable chunk in top-k, averaged over answerable questions. MRR uses
the first relevant rank in the full available ranking. All-evidence@10 is the
fraction of the five multi-evidence questions with every group recovered.
Unanswerable questions are excluded from recall/MRR and inspected separately.
The JSON also contains conventional any-evidence Hit@k and per-class results.

The natural questions all produced empty lexical rankings. This is the actual
current AND/simple-tokenization baseline; it is not an OR/BM25/stemmed baseline.
All four keyword controls produced lexical matches. The controls target a
specific verified topical chunk, so exact-chunk misses do not establish that
all retrieved topical passages are irrelevant. We report them separately and
do not choose a model solely from the combined average.

Quality indexing uses the final tested batch of the corpus pass (batch 4);
throughput chooses the fastest safe tested batch independently. The compatibility
reference uses batch 1. Quantized inference can vary slightly with batching;
quality with an independently built batch-1 index was not measured.

Warm latency uses 104 sequential requests after five warmups. Four-client latency
uses 26 groups of four simultaneous submissions to one encoder worker, includes
queue wait and measures embedding requests. Full retrieval timing additionally
includes actual read-only SQL round trips on a persistent connection, cosine
ranking and RRF. It excludes MCP/OAuth HTTP transport and evidence-text fetch;
those paths were not implemented or benchmarked for a new production encoder.
Cold startup means a new process with local model files and an OS cache left
intact. It is not a page-cache-evicted or fresh-download cold start.

This is a shared host, without exclusive CPU reservation. A short tokenizer
length inspection overlapped the Gemma batch-1 pass; no correction was subtracted
from its measured wall time. The profiler's cost is included. These are observed
local timings, not capacity guarantees for another device.

This is one Russian book with author-curated, nonexhaustive qrels. Four paraphrase
questions and two negative controls are small slices. Multilingual fixture strings
verify numerical compatibility, not multilingual retrieval quality. Cross-runtime
PWA/Android agreement and general-corpus quality remain unmeasured.
