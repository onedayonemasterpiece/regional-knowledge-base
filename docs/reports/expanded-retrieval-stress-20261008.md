# Extended Regional Knowledge Base retrieval stress — 2026-10-08

**Result: production dense-only retrieval quality gate FAIL.** This is not a reason to disable the working MCP service, but it **blocks declaring unattended mass book ingestion ready**. The previous frozen `t256` benchmark remains valid as an *experimental design* result, not current-production acceptance.

## Source and measurement boundary

- Installed immutable production SHA: `50ffeb698315eb84f594ae970a9db51c75eb3355`. Vector candidates use **PostgreSQL RLS v5** under `rkb_app`. Benchmarks verify that their Python modules originate from that release, not from the older dirty development checkout.
- Active primary sources: Brünneck (rev 2, 1,396 chunks) and Gause (rev 4, 518 chunks): **1,914** active source chunks. Across all authorized searchable sources, **1,949** BGE-ready chunks; the third real source contributes another 35. Optional E5 missing 1,380 is not a BGE publication gate.
- Frozen fixture: **32 source-grounded fact families** (64 original questions, Russian/German), six variants per question = **384 supported queries**, plus 24 independent negative questions and 20 boundary-pair checks. The held-out portion contains 16 families / 32 original questions. Source/queries/document IDs and raw rankings are retained privately, never committed to Git.
- All **64 original gold proof passages exist in at least one current active source chunk**. Thus an exact known-proof miss is an actual retrieval/layout mismatch, not absent source evidence.
- Main full 384-query result uses previously pinned BGE-M3 FP32 query vectors, re-applied to the **current production vector index** via v5. This measures true dense-only top-10 evidence lookup: **no lexical prefilter, aliases, continuation neighbors, or reranker**. A gold hit requires the indexed active chunk to contain the exact normalized proof, not just the correct book. Unjudged alternate valid evidence is not exhaustively scored. Query-embedding time is not part of the frozen-vector latency figure.

## Live production evidence recovery — 384 cases

| Frozen query form | Questions | Hit@5 | Hit@10 |
| --- | ---: | ---: | ---: |
| Original | 64 | 73.44% | 78.13% |
| Orthography | 64 | 71.88% | 79.69% |
| Terse | 64 | 68.75% | 76.56% |
| Typo | 64 | 75.00% | 81.25% |
| Verbose instruction wrapper | 64 | 62.50% | 71.88% |
| Irrelevant other-book title in query | 64 | **34.38%** | **51.56%** |
| **All variants** | **384** | **64.32%** | **73.18%** |

All 384 authorized v5 candidate requests completed without vector-search errors when the pinned **FP32 query vectors were already available**. Two-way concurrency: candidate lookup + local integrity verification p50 **89.09 ms**, p95 **148.09 ms**, max 1,091.34 ms. **These are not end-to-end MCP latencies and exclude query encoding.**

### Exact held-out quality gate

| BGE-only on same 32 frozen original queries | Known-proof Hit@5 | Hit@10 |
| --- | ---: | ---: |
| Experimental `t256` two-book matrix | 28/32 = **87.50%** | 29/32 = **90.63%** |
| **Current active production passages, frozen FP32 query** | **25/32 = 78.13%** | **27/32 = 84.38%** |
| Current passages with live local INT8 query (32 sequential, exploratory) | 25/32 = 78.13% | 28/32 = 87.50% |
| **Critical gate** | **>=85%** | **>=90%** |

The current production BGE-only source-grounded gate **fails** regardless of the tested query encoder. On the same current production passages, pinned FP32 did **not** improve Hit@5/Hit@10 over the local INT8: FP32 78.13% / 84.38% versus INT8 78.13% / 87.50%. This does not establish equivalence in a larger sample.

For frozen held-out RU query to German source, FP32 Hit@10 remains **11/12 = 91.67%**; the German-query/German-source slice is **10/12 = 83.33%**. Small source-language slices should not be generalized beyond these cases.

All **384 exact gold ranks** were unchanged by restricting scope from all authorized books to only the two principal sources. The third real document is **not causing this regression**. Differences concern the active passage/vector representation and possibly known-proof matching, not unrelated catalog noise.

## Passage topology and boundary checks

| Source layout | Chunks | Median length | p95 length | Share >1,100 characters |
| --- | ---: | ---: | ---: | ---: |
| Current two active books | 1,914 | 882.5 chars | **1,581.3 chars** | **26.54%** |
| Experimental `t256` | 2,343 | 815 chars | 993.9 chars | 0% |
| Current Brünneck | 1,396 | 698 chars | 1,156.5 chars | 10.32% |
| Current Gause | 518 | **1,271 chars** | **1,741.4 chars** | **70.27%** |

On 48 original Brünneck source-proofs, current passage lengths 600–899 chars have Hit@5 **22/26 = 84.62%**, whereas 900–1,199 chars have Hit@5 **12/20 = 60%**. This is **correlation on a small sample, not causal proof of an optimal exact character cutoff**. The system's semantic ~256 encoder-token target and exact final-input <=512-token ceiling remain unchanged.

The separate frozen **20 boundary-pair** test on `t256` yields BGE either-chunk Hit@5 **17/20 = 85%** but both-chunks Hit@5 only **8/20 = 40%**. Neighbor/context recovery and semantic segmentation require targeted checks. This pair test uses experimental t256 IDs; it is **not** a production-neighbor acceptance test.

## Sustained live query-encoder availability

A second 384-case run used the **actual pinned local INT8 query encoder** plus production v5, with two concurrent callers and a bounded stop rule. It **stopped after 128 of 384 cases** because **8 requests failed** (seven HTTP errors, one connect timeout). Partial-run p95 including live encoding was **1,066.74 ms**, encoder p95 **936.49 ms**; the unsuccessful run must never be treated as a complete quality sample. An earlier 32-case bounded slice had two encoder/connection errors.

At the failure time the shared 2-vCPU server had load averages around 4–11. The encoder queue had peak depth 2, no full-queue rejections and a short ~1-second deadline. This is a **contention/deadline resilience** issue, distinct from the successful pinned-vector PostgreSQL search. The local code's request deadlines and fallback behavior must be evaluated under contention; simply increasing a timeout beyond the product's hard 2-second budget is not an acceptable fix. Neither unrelated developer processes nor other application services were stopped.

## Vector-plane capacity state

A current read-only Supabase inventory measured a **75,806,387-byte database**, equivalent to **15.16% of the 500,000,000-byte *planning* quota** in the project's requirements documentation. Current relations: 4,936 minimal anchors, 3,932 BGE rows, 2,666 E5 rows. **BGE uses vector(1024) plus conventional HNSW**, not the proposed compact halfvec/binary-quantized index.

The separately retained 100,000-row *temporary synthetic* halfvec/HNSW benchmark projected approximately **345 MB steady** and **352 MB with 2,000 pending rows**, but that schema has **not** been production-migrated or accepted under real mass-ingestion traffic. The actual applicable provider quota, steady headroom, reimport peak, HNSW recall and concurrent p95 at ≥100,000 active chunks remain protected scale gates.

## Release/product decisions

1. **Do not designate the current corpus as mass-ingestion-ready.** The exact active production dense-only Hit@5/Hit@10 gate fails, irrespective of green BGE vector counts or a fast PostgreSQL v5 function.
2. Run a **non-destructive staged** semantic re-chunk/re-embed trial for both books against the same frozen family-held-out fixture, aiming for ~256 actual encoder tokens per coherent passage and complete page/region provenance. Compare source-grounded BGE-only Hit@5 ≥85%, Hit@10 ≥90%, language slices ≥85%, per-book evidence acceptance, and backend/public concurrency p95. Activate a revision **only after** the applicable gates pass; preserve active sources during the trial.
3. Independently harden the **local BGE query encoder queue/deadline/fallback** for sustained tool calls under co-located 2-vCPU contention. Publish failure rate, first-attempt p95, and degraded/ready state separately; do not hide errors behind retries or a lexical rescue.
4. Pilot compact halfvec + binary-quantized HNSW in isolated/rollback-ready scope. Measure 100k capacity including index/TOAST/system overhead and peak reimport, candidate recall versus exact scan, query-embedding time, and agent-facing latency.
5. Keep the existing public MCP and indexed books available: this stress report is a targeted quality/capacity **release-gate failure**, not evidence that normal retrieval is unavailable or that the RLS v5 speedup regressed.

**Reproducibility:** Frozen cases, raw source/text, query embedding arrays, raw result IDs, case-level failure traces, both actor-scope comparisons and capacity experiments remain in the managed, owner-restricted Regional Knowledge Base artifact store on DevCoveer. This public report contains only non-sensitive aggregate findings. No synthetic documents, vectors or book revisions were written to production during the new retrieval tests.
