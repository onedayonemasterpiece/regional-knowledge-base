# Vector-only retrieval and chunk-size audit — 2026-10-05

Status: completed seven-variant E5 audit, complete BGE current-corpus ablation, held-out evaluation and a measured small-passage operating recommendation. No production rollout or full BGE size-grid validation is claimed. The machine summary is generated only from saved completed measurements.

## Scope and evidence boundary

The operator requested an audit of existing chunk instructions, measured size selection, genuinely vector-only retrieval, and lexical-search latency/scaling. Production source text, revisions, ACL, indexes, model selection and schemas are unchanged. The lab reads only three explicitly selected, authenticated-owner sources. New BGE inference uses the existing pinned CPU queue with bounded document demand; no document vectors from this experiment are installed into production.

The initial corpus has **1,449 chunks**: source A (Russian, 174 physical pages, 518 chunks), source B (German, 478 physical pages, 896 chunks), and source C (German distractor, 40 pages, 35 chunks). A and C were active; B's sampled revision was still pending vector publication. An offline experiment containing B must not be presented as successful production retrieval of B.

Private source text, question text, source identities, judgments and vectors are retained outside Git. This repository contains generic code, synthetic tests and anonymous allowlisted aggregates only. The private run directory is the managed `vector-only-chunk-audit-20261005` artifact created at `20261005T164539Z` under the project's central artifact root. It is retained, not age-expiring.

## Existing instructions and concrete gaps

The checked implementation baseline is `2045a4e38c158a151749d375e27420207ce3328f`.

`docs/ingestion.md` and `docs/mcp.md` already request coherent semantic passages, approximately **800–1,800 characters** when practical, preserving real sentence/paragraph continuations across pages. This is not a license to treat each page as an embedding or to copy native blocks mechanically. The audit did not establish an older exact 700–1,000-character rule.

The operational MCP instructions in `server.py` and the ordinary `book_ingest` tool description do not expose that size guidance or a real tokenizer budget to the agent. `stage_graph.py` warns only when augmented material exceeds **2,400 characters**. This is explicitly a character proxy, not token validation, and does not prevent inference truncation. The deployed E5 service and BGE worker both truncate at **512 tokens**. BGE-M3's native model capability of 8,192 tokens is different from this deliberately bounded deployment.

The actual embedding input is `search_material`, which uses exact source text and optional printed-caption/model-observation augmentation. Setting a region's `normalized_text` does not by itself normalize what the encoder receives. Input checks must cover the final complete embedding string, including role prefixes and augmentation, not merely a body-character count.

### Exact tokenizer measurements

These counts use the pinned, untruncated tokenizers, including E5's `passage: ` prefix. No estimate based on characters substitutes for them.

| Variant | Chunks | Median characters | Maximum characters | E5 over 512 | BGE over 512 |
|---|---:|---:|---:|---:|---:|
| Current accepted/pending projection | 1,449 | 1,298 | 2,080 | 29 | 25 |
| Same boundaries, normalized input | 1,449 | 1,286 | 2,068 | 22 | 21 |
| Character cap 700 | 3,214 | 607 | 700 | 0 | 0 |
| Character cap 1,000 | 2,250 | 881 | 999 | 0 | 0 |
| Character cap 1,400 | 1,637 | 1,256 | 1,400 | 1 | 1 |
| Character cap 1,800 | 1,380 | 1,584.5 | 1,799 | 42 | 38 |
| Token cap 256 | 2,410 | 814 | 1,096 | 0 | 0 |

In the current projection, source B accounts for **23/896** E5 overflows and source A for **6/518**. The E5 maximum is 609 tokens. Thus the absence of a character warning did not establish absence of truncation.

## Experimental design

The fixture was frozen before query inference or examination of its rankings: **64 natural questions, 32 fact families**, each represented in Russian and German. There are 24 German-source and eight Russian-source families. The development and held-out partitions each contain 16 whole fact families / 32 questions; language variants of a fact stay together. Source C contributes distractors, not questions.

Exact seed answer spans were verified against the selected source before freezing. The labels are model-authored known-evidence labels, not independent expert truth or exhaustive judgments of all alternative passages. A missing seed cannot automatically be called an unanswerable question. Negative questions and generated live-agent answer accuracy are not measured here.

The size variants use the same normalized printed page material, sentence/word-oriented cut points, no blanket overlap and preserved offsets. This isolates size *between those variants*, but is not a semantic reimport and does not merge continuations across pages. The current baseline retains its existing semantic boundaries and visual augmentation. Therefore a difference between current and a rebuilt size variant is **not a pure causal estimate of size alone**. The same-boundaries normalization control isolates one additional preprocessing effect.

E5 uses the deployed pinned INT8 model, correct query/passage prefixes, mean/mask pooling, L2 normalization and query-batch1/document-batch4 contract. BGE uses the existing pinned FP32 CPU, batch1, CLS/L2 dense-only contract. BGE sparse and ColBERT modes are not used. Ranking is an exact dot product over each **complete** experiment matrix. There is no FTS candidate prefilter, alias boost, query translation, neighbor expansion or reranker in the dense scorer. Exact ranking separates semantic quality from approximate-index recall.

Primary reported metrics are known-seed Hit@1/@5/@10 and MRR@10. Supplementary metrics permit the union of retrieved source spans and apply an equal **5,000-character context budget**, so a shorter chunk is not rewarded solely by returning less source text. The selection file is frozen from development results before held-out rankings are opened.

## Encoder and label checks

A diagnostic re-encoded six development query/passage cases. The local/deployed query vectors agreed to approximately cosine 1, as did the same-batch passage vectors; matrix ordering and passage self-nearest-neighbor checks passed. A separate read-only comparison with **553 actually installed E5 vectors** found median cosine **0.998043** against re-encoding, minimum **0.995420**, with none below 0.99. Batch composition changes therefore are observable; offline re-encoding must not be called bit-identical production vectors. Development top-ten overlap in the installed-scope check was 0.9–1.0.

Twelve retrieved pairs across four development queries were additionally read semantically. One alternate passage supported the requested year even though the more precise, frozen day/month/year seed was missing. This is retained as a separate grade-2 judgment, not silently substituted into the original metrics. The other reviewed failures distinguish wrong historical events/publication dates from sufficient evidence. These twelve judgments are not a completed relevance pool; unjudged results are not assigned zero.

<!-- MEASURED_CHUNK_AUDIT_RESULTS_BEGIN -->
## Completed size selection and held-out results

**Selected operating target: approximately 256 E5 tokens per embedding passage, with semantic boundaries and exact final-input token checks.** In this corpus that variant has median 814 characters and a maximum of 1,096. A practical agent-facing guide is roughly 700–1,100 characters, usually around 800–1,000, not a blind character ceiling. The hard deployment ceiling remains 512 tokens for each actual encoder, including role prefix and all augmentation. A self-contained sentence/paragraph may deviate from the target; source loss, broken words and unrelated topic concatenation are not permitted.

This decision was frozen from development results before held-out rankings were opened. `t256` had the highest development top-five/top-ten known-proof coverage and fixed 5,000-character-budget coverage (14/32). `c1000` had better development top-one/MRR, but lower top-five/context coverage (12/32); the selection favors a small multi-passage evidence pack for an agent rather than a single top-one hit. The held-out results retain that selection; they are not used to switch winners after the fact.

### E5 size sweep — development and held-out partitions

Every row below has a complete document matrix and the same frozen query fixture. Hit percentages refer to the preselected known source span. Test has 32 questions in 16 paired RU/DE fact families.

| Variant | Dev Hit@5 | Dev MRR@10 | Test Hit@1 | Test Hit@5 | Test Hit@10 | Test MRR@10 | Test 5,000-char coverage |
|---|---:|---:|---:|---:|---:|---:|---:|
| current | 37.50% | 0.1818 | 31.25% | 50.00% | 50.00% | 0.3786 | 43.75% |
| current_norm | 31.25% | 0.1982 | 31.25% | 53.12% | 53.12% | 0.3865 | 43.75% |
| c700 | 28.12% | 0.1940 | 34.38% | 46.88% | 56.25% | 0.3985 | 50.00% |
| c1000 | 37.50% | 0.2974 | 40.62% | 53.12% | 56.25% | 0.4524 | 53.12% |
| c1400 | 31.25% | 0.2295 | 37.50% | 50.00% | 50.00% | 0.4375 | 50.00% |
| c1800 | 40.62% | 0.2052 | 25.00% | 50.00% | 50.00% | 0.3474 | 43.75% |
| t256 | 43.75% | 0.2693 | 40.62% | 53.12% | 56.25% | 0.4518 | 53.12% |

The 256-token choice and the 1,000-character variant tie on held-out top-five/top-ten hit counts, and their MRR differs by less than 0.001. This is not evidence of a universal sharp optimum at exactly 256 tokens. It supports this small-passage operating range and an exact token guard. The 700-character cap loses some semantic completeness; the 1,800-character cap adds truncation risk and larger evidence packs without a measured quality advantage.

The selected E5 variant improves held-out Hit@10 from 16/32 to 18/32 and top-five evidence volume from an average 6,770 to 4,106 characters. The paired 16-family bootstrap gives Hit@10 difference +6.25 percentage points, 95% interval [-6.25, +18.75]; MRR difference +0.0732, interval [-0.0333, +0.1953]. The small fixture does **not** statistically establish a general quality gain. The reduction in input size and elimination of observed truncation are directly measured.

**Important BGE limitation:** the complete BGE experiment tests the current 1,449-passage corpus, not the rebuilt size grid. The target above is selected from the E5 grid and checked against both tokenizers. BGE quality for a mass 256-token re-chunking remains to be tested before a production corpus migration. No such migration is claimed or performed.

### Dense-only encoder/fusion comparison on unchanged current chunks

| Method, no lexical branch | Test Hit@1 | Test Hit@5 | Test Hit@10 | Test MRR@10 | All 64 Hit@10 |
|---|---:|---:|---:|---:|---:|
| E5 | 31.25% | 50.00% | 50.00% | 0.3786 | 43.75% |
| BGE dense | 53.12% | 75.00% | 84.38% | 0.6311 | 84.38% |
| Equal-weight E5 + BGE RRF | 34.38% | 62.50% | 71.88% | 0.4605 | 70.31% |

**BGE dense is materially stronger on this fixture; blindly fusing E5 into it hurts.** BGE reaches the known proof in the top ten for 27/32 held-out questions, whereas E5 reaches 16 and equal-weight fusion 23. Both development and holdout show this pattern. The paired held-out BGE-minus-E5 Hit@10 interval is [+12.5, +53.125] percentage points; this supports a difference on this fixture, not a universal model ranking.

The bilingual slice is particularly diagnostic. Across all 24 Russian questions about the German source B, current E5 has known-proof Hit@10 **1/24**, the selected shorter E5 variant **3/24**, BGE **20/24**, and equal-weight E5+BGE **11/24**. For the corresponding 24 German questions the counts are **21/24**, **22/24**, **22/24**, and **23/24**. Size alone does not repair the current E5 cross-language weakness. It is a property of the tested deployed INT8 configuration and source/query fixture; this audit does not isolate whether model capacity, quantization, OCR/domain vocabulary or another factor is its underlying cause.

These findings are not directly comparable to the old high-recall benchmark on a different, smaller/incomplete projection and a different question set. They do not establish a chronological regression percentage. Twelve diagnostic semantic judgments already found one acceptable alternative passage outside the exact seed label, so the table must not be reported as the accuracy of generated answers.

### Does lexical retrieval hide the problem?

The no-alias full-question FTS ablation returns any result for **1/64** questions on current chunks and **0/64** on selected shorter chunks. Current lexical-only known-proof Hit@10 is 1/64. Adding FTS leaves current BGE Hit@10 at 54/64 and equal-weight E5+BGE at 45/64; it changes early ranking on one development query but does not account for the dense quality gap. On the held-out partition it changes none of those methods' metrics. Thus this fixture does **not** support the hypothesis that lexical retrieval is responsible for all successful answers. The stronger dense encoder is doing substantial work, and the current fusion policy is a separate problem.

### Prioritized implementation follow-up (not deployed by this audit)

1. Expose the semantic-passage target and final-input token counts in the MCP ingestion instructions/validation. Count each active encoder with its pinned tokenizer, include prefixes/captions/descriptions, and reject silent truncation for newly staged data. Do not make the server invent or mechanically rewrite semantic passages. Preserve page/region provenance and original evidence.
2. Keep vector-only, lexical-only and fused ablations in the release gate. Qualify same-language and cross-language routes separately. Prefer a BGE-first warm policy over the currently unconditional equal-weight E5+BGE fusion unless a measured weighted/conditional alternative proves better. No claim that a ready E5 vector count makes its cold fallback reliable.
3. Before mass re-chunking, run the selected small-passage candidate through BGE and verify held-out direct evidence, boundary/footnote completeness and a fixed context budget. Keep existing active revisions until the new revision is fully indexed and accepted.
4. Keep FTS as a bounded optional parallel complement; improve its natural-query formulation separately from alias/phrase controls. Do not classify a fast empty AND-query as successful retrieval. A configurable branch deadline and a genuine no-lexical path are needed.
5. Remove all-chunk-ID enumeration/transmission from the live path while retaining compact actor/document/revision authorization and bounded candidate provenance checks. Measure exact-versus-ANN recall and real network latency at larger diverse-corpus scale; the synthetic FTS stress does not establish that result.
6. Set and test live response deadlines and explicit degraded states. The observed backend component timings are compatible with subsecond warm evidence lookup, but this audit is not a complete MCP/voice answer-latency or multiuser SLO acceptance. The current ten-second BGE wait and pending book-publication path need separate operational acceptance.

### Run completion and test status

All seven E5 size/control matrices, all 64 E5 query vectors, the complete 1,449-row BGE current matrix and all 64 BGE query vectors were completed and hash-recorded before their quality metrics were used. The first E5 sweep reached its one-hour execution bound after saving six complete matrices; only the remaining 256-token variant was resumed. A mixed BGE size job was stopped to finish the current corpus first, reusing same-owner byte-identical cached jobs. BGE document demand was bounded to eight outstanding audit jobs; the final audit queue was empty. No production vector installation occurred.

Focused synthetic audit tests: **16 passed**. Full local suite: **186 passed, 26 skipped**, one existing Starlette deprecation warning. Skipped integration tests were not run and are not claimed. The private evidence remains retained. Source B was still awaiting vector publication at the read-only runtime capture; completing these independent lab vectors does not publish it.

<!-- MEASURED_CHUNK_AUDIT_RESULTS_END -->

## Lexical latency is not intrinsically a full-corpus text scan

The implementation uses SQLite FTS5 `unicode61`, external content, BM25, an allowed-document/revision join and `LIMIT 100`. Ordinary queries currently combine all extracted words with **AND**; aliases use phrase matching. This is not a row-by-row substring scan. It also means a conversational question may return nothing because every word is required. The ordinary lexical query builder has no Russian/German stemming or stopword removal.

A separate on-disk experiment used 2,250, 10,000 and 100,000 rows. Larger sizes repeat the same three-source material under distinct identities: this is a posting-list/storage stress test, **not** a diverse 100,000-passage quality corpus or a thousand-real-book acceptance. SQLite cache was 32 MiB. Each control has one first query after build and three warm repeats; the table reports warm medians, not a robust high-percentile service SLO. No OS cache eviction is claimed.

| Control | 2,250 rows | 10,000 rows | 100,000 rows |
|---|---:|---:|---:|
| Two-word German topic | 0.237 ms | 1.082 ms | 35.814 ms |
| Frequent German legal word | 0.941 ms | 11.858 ms | 212.649 ms |
| Very frequent German conjunction | 5.861 ms | 21.415 ms | 459.379 ms |
| Selective Russian name | 0.047 ms | 0.137 ms | 4.198 ms |
| Two-word Russian topic | 0.115 ms | 0.307 ms | 4.031 ms |

All 64 full natural questions returned zero matches in the 1,000-character FTS experiment. These fast empty responses are **not** useful retrieval. The read-only deployed-source check did find one nonempty result among eight development questions on the different current chunk projection, so the zero statement is not generalized to every production query. Selective lexical search can be much faster than query embedding; broad high-frequency queries can be expensive. There is no measured reason to remove it merely because the book count grows. Use a bounded parallel optional branch instead of letting it hide dense failures or block a live deadline.

## Deployed dense path and scaling risks

A read-only owner-scoped sample over 553 active chunks made six actual E5 query encodings and two remote vector RPCs per query, with lexical retrieval absent from that path. Warm remote candidate calls ranged **0.126–0.212 seconds**; this is candidate retrieval, not MCP end-to-end, fetch, speech synthesis or answer generation. The separate 64-query loopback run measured E5 encoder median **55.77 ms**, p95 **72.45 ms**, under concurrent audit load. Actual BGE query CPU timings are retained separately in the machine summary. Query-vector matrix multiplication alone is sub-millisecond on this small corpus and is not presented as full request latency.

`SQLiteBackend.local_rankings` currently executes lexical lookup even when the eventual rank-fusion mode is named vector-only. Merely changing that mode would not constitute a no-lexical-computation benchmark. The audit calls the dense candidate path directly and uses an independent exact dense scorer.

A separate scaling problem exists before ANN retrieval: `candidate_metadata` enumerates **every** authorized active chunk ID/hash and transmits the full ID list to the remote vector function. The sampled 553 IDs occupy **22,120 JSON bytes**. At 100,000 IDs the same representation is approximately **4 MB per query**, a size calculation, not a measured 100,000-vector network run. Removing FTS does not remove this linear work.

Both E5 and BGE HNSW indexes exist. The observed E5 plan on the small active sample used primary-key/bitmap lookups, 553 distance candidates and sorting, not HNSW; execution was **54.344 ms**, planning **0.884 ms**. Exact search can be a legitimate small-corpus planner choice. This observation does not prove HNSW can never be selected at scale. Before scale acceptance, replace all-chunk-ID transfer with a compact authorized document/revision scope and bounded local validation of returned candidate identities, preserving SQLite authority and all access checks. Then compare actual ANN recall against exact search and inspect the large-corpus plan.

The normal warm BGE path can wait up to ten seconds before its E5 fallback. A live-service deadline and the quality of that fallback must be measured explicitly; a ready vector count is not a semantic-quality acceptance.

## Reproduction and preservation

Use `scripts/benchmarks/run_chunk_size_audit.sh` with the existing pinned environment. The sequence is prepare -> normalization control -> owner/source-verified frozen families -> complete E5/BGE matrices -> development score -> frozen selection -> held-out score -> anonymous export. Generic code records original source hashes privately and refuses mixed/partial dimensions. BGE requests reuse stable per-role/text keys; a resumed experiment must not re-enqueue different text under the same key.

`tests/test_chunk_size_audit.py` covers source preservation, split answer-span union, fixed context budget, source-specific known-seed labels, and absence of lexical/alias/reranker calls from the dense scorer. Synthetic unit success does not stand in for the actual retrieval run.

## Primary references

- Existing repository: `docs/ingestion.md`, `docs/mcp.md`, `src/regional_knowledge/stage_graph.py`, `e5_service.py`, `search_material.py`, `sqlite_backend.py`, `sqlite_corpus.py`, `vector_plane.py`, `multilingual_retrieval.py`, and `sql/021_vector_only_plane.sql` at the baseline above.
- [Multilingual E5 model card](https://huggingface.co/intfloat/multilingual-e5-small): required prefixes and the 512-token bound.
- [BGE-M3 model card](https://huggingface.co/BAAI/bge-m3): dense/sparse distinction and native 8,192-token capability, separate from this deployment's 512-token cap.
- [SQLite FTS5 documentation](https://sqlite.org/fts5.html): MATCH, token indexes, boolean semantics and ranking.
- [pgvector documentation](https://github.com/pgvector/pgvector): exact versus approximate search, filtering, index-compatible ordering and EXPLAIN.
