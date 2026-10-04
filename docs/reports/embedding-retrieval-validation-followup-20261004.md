# Embedding retrieval validation follow-up — 2026-10-04

**Decision: preserve uncertainty between E5 and Gemma because source coverage and
labels are insufficient to confirm a general quality winner.** E5 remains a
practical speed candidate; it is not validated as the quality winner. Gemma is
slower but shows better ranking stability across document batch sizes. Neither
encoder repairs lost source text or establishes correctness of Mira's answers.

Continues [PR #26](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/26)
from `f77f78427984fd970c85339c7de4f53a0f253004`; no fresh installation or full
three-model benchmark. Original 30 questions, report, vectors and measurements
remain unchanged. New questions/judgments and the original-qrel review are
separate versioned fixtures. Production corpus, revision, schema, RPC and provider
configuration were not changed; no paid inference, external judge, GPU, Kaggle,
provider canary or deployment was used.

## Source completeness comes first

The [coverage audit](gause-import-coverage-audit-20261004.md) verifies the original
PDF and active projection, not merely page counts. **200 chunks on 120 pages are
exact 1000-character previews of longer source blocks.** Their omitted block
suffixes total 131480 characters; this is not a unique missing-book percentage.
The snapshot has 712 chunks/461233 characters on physical pages 1–144. Pages
145–174 contain images but no native text or retrieval chunks. Five directly
inspected image-only pages have substantive captions. No page is declared
semantically complete from hashes, native matches or metadata alone.

A bounded transport fix in this PR discloses length/truncation and reads repeatable
1000-character block continuations through authorized `book_pages`. Staging
requires explicit model visual review and a note for every page, including
textless images; page IDs and previews cannot establish completeness. This is a
review attestation, not automatic semantic certification. No new book revision
was created. A complete ChatGPT source review remains necessary before reimport.

## New fixture and source verification

[Questions v1](../../scripts/benchmarks/retrieval_validation_questions.v1.json):
**32 new items = 20 answerable, 10 negative controls, 2 source gaps**. Queries,
answer outlines, known proofs, page locators and source hashes were frozen before
new query inference or viewing new rankings. Freeze SHA-256:
`1cce7559a2730503575143de1ce0bb0ed86909510c4ef3be98a3af0b3a63e0f0`.
Every answerable seed was checked against an original native block or its exact
stripped preview; no fact was invented for a missing source. Two visual-caption
items are `source_gap`, not unanswerable and not included in answerable Recall.
They identify missing caption material; they do not assert that the same fact
cannot occur elsewhere in the book.

New answerable slices: natural **20**, paraphrase **4**, rare-name **7**, multi-fact
**6** (overlapping slices). These six ask several facts which the same known
passage can prove; they are weaker tests of multi-*passage* retrieval than the
original five. Original slices remain natural **24**, keyword controls **4**,
paraphrase **4**, multi **5**, negative **2**. There is no tuned train/test split
or claim of an independently sampled population holdout. Positive/negative
variants of a fact share a family; never split them across independent partitions.

The ten harder negatives include incorrect dates/numbers, negated historical
claims, a confused occupation/person and an explicitly unknown detail. They
mean **no answer as premised**, not necessarily no useful evidence: a source
passage may correct the premise. The brewing date/number control is backed by
the author's explicit uncertainty, rather than a failed substring search over
an incomplete snapshot. There are only two old out-of-period negatives, retained
as historical diagnostics, not the entire negative set.

## Qrel review, blinding and judgment coverage

[Original qrel review v2](../../scripts/benchmarks/retrieval_validation_original_qrels.v2.json)
corrects q15 without rewriting v1: `d2361510` names Stein/Frey **and** explains
representative governance. The preceding paragraph is context, not a mandatory
second proof. Two fact groups may share the same sufficient passage. The change
raises original-group Recall@1 but leaves Recall@10 unchanged. Other original
proofs remain sparse; all possible alternatives were not exhausted. The unnamed
Roth prisoner question also requires adjacent referent resolution, and keyword
controls identify topical targets rather than exhaustive direct-evidence sets.

Constructed a private union of **top-10 E5, Gemma and production lexical** for all
62 items: **957 question/passage pairs, 419 unique passages**. The review pool
contains source text, query and exact locator, with no model name/rank; its order
uses opaque SHA IDs. Full study blinding is imperfect because the annotator knows
the old benchmark. [Judgments v1](../../scripts/benchmarks/retrieval_validation_judgments.v1.json)
are **ChatGPT model-assisted, not independent expert truth**; the same agent
selected questions and reviewed evidence, and no independent second annotator
was available. No regex/word-match relevance grading was used.

**134/957 pooled pairs (14.00%) have semantic judgments; 823 remain explicitly
unjudged.** The first five new answerable queries v31–v35 were fully read and
labeled (76 pairs); elsewhere only source-verified seed facts/corrections have
labels. Grades are 0 irrelevant/does not establish the requested fact, 1 useful
context, 2 direct evidence. Each judgment includes a page locator, chunk hash and
short rationale. This is a deliberately disclosed **partial pool assessment**,
not completion of an independent full-pool relevance study. Completing that
study is an outstanding evidence limitation, not silently replaced by zeros.

Exact Precision and pooled nDCG are reported only where judgments cover the
relevant ranking/pool. For all 20 new answerable questions, E5/Gemma mean judged
coverage at top10 is only **32.5%**; macro direct Precision@10 is bounded by
**10.0–77.5%**, not estimated as an exact value. Top3 coverage is 50.0%/48.33% and
direct Precision@3 bounds are 33.33–83.33%/30.0–81.67%. Overall nDCG is withheld.
Unknown is never automatically graded zero.

### Fully judged five-query subset only

Precision below uses direct evidence (grade 2), fixed denominator k; useful
precision additionally counts context (grade ≥1). nDCG@10 uses gains `2^grade−1`
and IDCG from the fully judged *pool*, not assumed exhaustive corpus relevance.
This chronological five-query subset is small and not representative.

| Method | Direct P@3 | Direct P@5 | Direct P@10 | Useful P@10 | Pooled nDCG@10 |
|---|---:|---:|---:|---:|---:|
| Production lexical | 0% | 0% | 0% | 0% | 0.0000 |
| E5 batch4 | 33.33% | 20% | 10% | 24% | 0.8438 |
| Gemma batch4 | 26.67% | 20% | 10% | 30% | 0.9030 |

Useful P@3/@5 is E5 **60%/40%**, Gemma **66.67%/56%**. Both retrieve the known
proof for all five questions, while most top10 passages are context or unrelated.
High known-proof Recall does not imply high Precision. Gemma's better pooled
nDCG here reflects useful context ranking; E5 places the staple-right proof
higher. These observations do not support a broad superiority claim.

## E5 vs Gemma on unchanged batch4 document matrices

New queries only were encoded locally. Both retained weight manifests/hashes
were reverified. Original model revisions, tokenizer files, query/passage roles,
pooling, normalization and context caps were reused. All inference ran
sequentially under **1 CPU / 1 GiB / swap=0**, CPU affinity 0, offline weights.
POTION was neither reoptimized nor rerun.

| New answerable slice | n | E5 Recall@1/5/10 | Gemma Recall@1/5/10 | E5/Gemma MRR |
|---|---:|---|---|---|
| Natural | 20 | 80% / 100% / 100% | 80% / 100% / 100% | 0.8917 / 0.8725 |
| Paraphrase | 4 | 50% / 100% / 100% | 75% / 100% / 100% | 0.7500 / 0.8000 |
| Multi-fact (same known passage) | 6 | all-evidence@10 6/6 | all-evidence@10 6/6 | see machine summary |

For the original 28 answerable items, historical fused Recall@10 remains
**96.43% E5 / 98.21% Gemma**, original multi coverage **5/5 / 4/5**. Natural 24
Recall@10 is **95.83% / 97.92%**; original paraphrase 4 is **75% / 100%**;
keyword control 4 is vector **50% / 100%**, fused **100% / 100%**. These are
known-evidence metrics on the incomplete snapshot, not exact answer accuracy.
The original qrel review has its own separately recalculated slice in the
[machine summary](embedding-retrieval-validation-followup-20261004.summary.json).

### Per-query differences on the new answerable set

Evidence rank, lower is better; known-proof Recall@10 ties for every item.
All 20 per-query values and original items are in the machine summary.

| Query/fact | E5 proof rank | Gemma proof rank | Observation |
|---|---:|---:|---|
| v34: denial of staple right | 2 | 5 | E5 places the explicit denial earlier; context is distinct from proof |
| v37: two helpers of Polish recognition | 1 | 2 | E5 earlier; only sparse labels for surrounding candidates |
| v38: effect of Thirty Years' War | 2 | 1 | Gemma earlier on a paraphrase |
| v45: bankrupt firms, 1929–1931 | 3 | 2 | Gemma earlier on a precise-number fact |
| v47: museum opening/place/first symposium | 2 | 4 | E5 earlier on a composite question |

MRR wins/losses/ties: **E5 3, Gemma 2, ties 15**. Do not infer an encoder's causal
failure mechanism from these ranks alone. Family bootstrap (20 answerable fact
families, 10000 resamples, fixed seed 20261004) gives Gemma−E5 MRR **−0.01917**,
95% percentile interval **[−0.1025, 0.0625]**. Recall@10 differences are all zero,
so its bootstrap interval is trivially [0,0]; that says nothing about unseen
questions, missing source or sparse labels. No chunk/latency-repeat resampling
and no forced significance claim.

For the ten new negative controls, both vector methods return ten passages and
have no calibrated abstention. E5 retrieves known corrective evidence in **10/10**,
Gemma **9/10** (v53 misses the explicit staple-right denial). This is evidence
availability, not proof that Mira corrects the false premise. The two source-gap
queries also return passages, without proving the absent captions were recovered.

Production lexical stays `simple`/`websearch_to_tsquery` AND with `ts_rank_cd`,
active revision and the existing RPC. All **32 new full queries have empty
lexical results**, checked against the NULL-vector production RPC. Thus new
fusion equals vector retrieval and is **not an observed ensemble improvement**.
No second lexical algorithm or production search change was introduced.

## Batch1 vs batch4: tolerances and ranking sensitivity

Recomputed only the necessary 712 document vectors per encoder at batch1; kept
saved batch4 documents and identical query vectors for all 62 queries. No input,
source, role prefix, pooling, model/hash or normalization changes. Before the
comparison, fixed the existing fixture gates at maximum component difference
**0.002** and minimum cosine **0.999**. They were not relaxed after failure.

| Comparison | E5 INT8 | Gemma Q4 |
|---|---:|---:|
| Maximum absolute component difference | 0.0240526 | 0.00378356 |
| Minimum per-document cosine | 0.9924339 | 0.9995936 |
| Fixed vector compatibility gate | **FAIL both criteria** | **FAIL component criterion** |
| Top1 changed, 62 queries | 6 | 0 |
| Mean top10 overlap (intersection/10) | 88.87% | 100% |
| New 20 Recall@10, batch1 / batch4 | 100% / 100% | 100% / 100% |
| Original 28 fused Recall@10, batch1 / batch4 | **94.64% / 96.43%** | 98.21% / 98.21% |
| Original five multi, batch1 / batch4 | **4/5 / 5/5** | 4/5 / 4/5 |

E5 q20's second proof (`fef12c8c`, original demolition question) moves from
batch4 rank9 to batch1 rank11; the first proof remains rank6. This loses required
fact coverage at10. Batch1 E5 improves new MRR to0.9375 and Recall@1 to90%, while
losing that original multi fact: batch sensitivity is not uniformly degradation
or improvement. Gemma's document components differ beyond the fixed threshold
but its measured top10 rankings remain unchanged. Both conclusions matter;
equal dimensions alone would have hidden them.

These are batch consistency results, not client compatibility results. Do not
assume a batch1 index reproduces the historical batch4 quality report. Keep a
fixed index construction contract and rerun relevant ranking/compatibility gates
before client/native interoperability acceptance. The new jobs reached cgroup
peaks E5 **635.46 MiB**, Gemma **613.41 MiB**; these are separate follow-up job
observations, not replacements for historical peaks or latency figures.

Historical latency remains E523.10ms/Gemma177.31ms warm p95. Both fit the requested
CPU/memory budget. Latency does not decide retrieval quality; source/label
limitations plus batch sensitivity prevent confirmation of the old E5 quality
recommendation. POTION's prior OOM/ceiling and weak retrieval evidence stand.

## Answer correctness and device compatibility

**Mira's final answer correctness was not evaluated.** No provider generation or
paid judge was invoked. Retrieval can supply a useful passage without ensuring
all facts, accurate attribution, premise correction or faithful final citation.

**Device evaluation is pending: no user device-run JSON was supplied.** No phone
model/browser measurements or acceptance are claimed. When provided, validate
`rkb-device-embedding-run.v1`, hashes/versions, all30 unique qIDs,384 finite
components and L2 norms; evaluate *device query vectors* against the same retained
E5 document matrix using brute force and original RRF, without new inference.
Compare Recall/MRR/multi/overlap/rank changes with native queries. Preserve
failed/interrupted runs in stability denominators. Desktop smoke is not phone
acceptance, `deviceMemory` is not measured process RAM, and any ADB profile is a
separate optional measurement.

## Validation, reproduction and remaining evidence

Local suite: **87 passed**, one existing Starlette deprecation warning. Focused
tests cover bounded/repeated source continuation, backend authorization/cursor
transport, blocks beyond the 80-block preview cap, image-only pages, staging review, graded metrics with unknowns,
shared evidence groups, invalid grades/duplicate ranks and versioned fixtures.
No ML dependency was added to the production package or CI.

Private retained evidence:
`/home/dev/artifacts/regional-knowledge-base/20261004T053456Z-retrieval-validation-20261004/`.
It contains the source coverage manifest, freeze receipt, blinded pool, full
rankings/coverage summary, new query and batch1 matrices, cgroup job logs and
batch measurements. Original lab/cache/runtime/evidence remain intact. No large
cache duplicate, evidence cleanup or source publication was performed. Commands
and filenames are documented in the benchmark README.

The concrete outstanding limits are: complete semantic source review/reimport
(not authorized here), **823 remaining pooled judgments** and independent
second review, stronger multi-passage/holdout questions, final-answer evaluation,
and device JSON. These limits preserve uncertainty; they do not invalidate the
measured comparison on the fixed snapshot or justify concealing a failed gate.
