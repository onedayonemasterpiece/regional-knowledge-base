# Kaggle CPU BGE multilingual production acceptance — 2026-10-04

**Accepted:** one operational private Kaggle CPU BGE worker, durable priority
queue, source-bound 1024-d index and deployed multilingual retrieval. Select
**BGE + lexical** when warm; keep the independent E5 + lexical fast tier for
cold/pending/unavailable main results. Extra E5 warm encoding is not justified
by this fixture. Source completeness and unsupported-answer abstention remain
separate product gaps.

PR [#28](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/28)
is stacked on completed E5 PR
[#27](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/27).
It continues that implementation; no E5 model research, OCR, reimport, client
inference, graph implementation or paid inference was added.

Runtime accepted at `acfc3d5f2e1a43b1406039dcd344bc405896c147`; warm backend/HTTP
measurements were at `89daac414ad4a6b2bc9338ce7e3ba5189343c730` with the same
model/ranking contract. The later runtime adds bounded telemetry, alias input,
structured boundary logs and current Kaggle SDK enum handling. Cold public MCP
and actual provider startup-failure recovery were verified at the later runtime.
[Machine-readable aggregates](bge-kaggle-multilingual-hybrid-acceptance-20261004.json)
include private evidence checksums. No private text, document/chunk IDs, vectors,
credentials, model binaries or device JSON appear in this report/aggregate.

## Vector and authorization contract

- Model `BAAI/bge-m3`, exact revision
  `5617a9f61b028005a4858fdac845db406aefb181`.
- Space `bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1`, **1024 dimensions**.
- CPU FP32, first-token CLS pooling, L2 normalization; no query/document prefix;
  tokenizer truncation at 512 tokens, batch 1 for both; four intra-op threads,
  one inter-op thread. This is a bounded 512-token production contract, not a
  claim of using the model's full context. Long source tails may be excluded.
- Kaggle packages pinned/checked: Torch 2.11.0+cpu, Transformers 5.16.1,
  Tokenizers 0.23.1, Huggingface Hub 1.29.0. GPU/TPU disabled in private notebook
  metadata and model explicitly placed on CPU.
- `rkb_chunk_embeddings_bge` stores space/model revision, active source revision
  and exact original-byte SHA256. Ranking excludes stale hash/revision entries.
  Legacy 768-d, E5 384-d and BGE 1024-d storage remain separate.
- Migration 011 was applied twice on isolated pgvector/PostgreSQL fixtures;
  mixed dimension/space rejection, ordinary actor RLS denial, independent branch
  diagnostics, stale revision exclusion and rollback preservation all passed.
  Production migration preserved **907 legacy vectors and 747 E5 vectors**.
- Backfill first authorizes active chunks under `rkb_app` actor RLS, then binds
  object lookup to their object/document identities. Original byte hash is
  checked before sending text, authorization/hash/revision rechecked on upsert.
  **747/747** authorized active chunks backfilled. Actual second run:
  **747 skipped, 0 submitted, 0 written**. Completed query replay likewise
  preserves job, attempt count and result without another model call.
- Both ranking and evidence fetch preserve the source-owner/grant model. A
  completed encoding job grants no source access; hydration rechecks ACL.
  Worker receives only a temporary run-bound broker credential, not DB/S3/user
  tokens. Worker credential against MCP: **401**; user OAuth token against
  worker protocol: **403**; expired worker heartbeat/result: **403**.

## Source scope and relevance method

The retrieval corpus is the same **747 owner-visible active chunks** for every
ablation: the existing 712-chunk Russian projection and **35 genuine original
German chunks** from one historical medical source. All 35 German original-byte
hashes were verified. Evidence was read/scored in German, without translating
passages for scoring. This source exercises people, study/service events,
participant references and a historical/current city name; it is not a broad
German regional-history corpus.

Final fixture: **40 queries**, 38 answerable and two unsupported/false-premise
DE/RU questions; **16 paired information needs** with identical DE/RU evidence
groups. The original 32 natural-language cases contain 12 paired needs and six
spelling/transliteration/ambiguous-name variants. Eight short entity-name queries
were added explicitly to exercise lexical matching: strict AND full-question
FTS returned zero candidates on the natural questions. Results for these two
families are kept separate. Targets were not changed to favor a model.
Event questions distinguish an **announced** ceremony from an unverified completed
event; final scoring checks each query encoding hash against that wording.

Source verification is agent-authored, not independent human relevance judging.
Known-positive evidence groups support Recall and MRR; other pooled candidates
remain **unjudged**, not negative. Consequently **judged Precision/nDCG are not
reported**: the labels do not support those metrics honestly. A next independent
judging round can change the quality conclusion. Generic entity queries can
legitimately retrieve other Russian-source mentions; failing to retrieve this
German target is a measured target miss, not proof that every other hit is wrong.

Recall@k is the macro-average fraction of required evidence groups hit per
answerable query. A group can contain equivalent source passages. MRR is first
known-positive reciprocal rank at cutoff **20**. Multi-evidence completeness is
all required groups found at top 10, over nine composed cases. All branches
retrieve depth 100 from the same ACL-visible corpus; fusion is RRF k=60 with
stable ID ties. Query E5 and BGE encodings ran concurrently during dual-encoder
benchmarking. Raw vectors/cosines are never mixed across spaces.

## Seven measured ablations

All answerable cases, including short names; percentages are known-evidence
Recall, not answer accuracy.

| Ranking | R@1 | R@5 | R@10 | MRR@20 | Multi complete@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lexical | 7.89% | 10.53% | 10.53% | 0.0855 | 0/9 |
| E5 | 36.84% | 46.05% | 52.63% | 0.4606 | 4/9 |
| BGE | 57.89% | 73.68% | 77.63% | 0.7223 | 6/9 |
| E5 + lexical | 42.11% | 48.68% | 57.89% | 0.5020 | 4/9 |
| **BGE + lexical** | 63.16% | 78.95% | 82.89% | 0.7757 | 6/9 |
| E5 + BGE | 38.16% | 52.63% | 64.47% | 0.5327 | 5/9 |
| E5 + BGE + lexical | 43.42% | 57.89% | 67.11% | 0.5819 | 5/9 |

On the 30 answerable natural questions, BGE/BGE+lexical R@10 is **91.67%**,
E5/E5+lexical **60.00%**, and dual/all-three **75.00%**. On eight short entity
queries, R@10 is lexical **50%**, E5/BGE **25%**, E5+lexical/BGE+lexical **50%**,
dual **25%**, all-three **37.5%**. Thus lexical adds **5.26 percentage points**
overall to BGE without another encoder. Adding E5 to that warm path loses
**15.79 points** overall and reduces multi completeness 6/9 to 5/9.

### Language loss

| Ranking | All DE R@10 | All RU R@10 | Paired DE R@10 | Paired RU R@10 | Paired DE−RU gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lexical | 21.05% | 0.00% | 25.00% | 0.00% | 25.00 pp |
| E5 | 84.21% | 21.05% | 87.50% | 25.00% | 62.50 pp |
| BGE | 81.58% | 73.68% | 78.12% | 71.88% | 6.25 pp |
| E5 + lexical | 94.74% | 21.05% | 100.00% | 25.00% | 75.00 pp |
| **BGE + lexical** | 92.11% | 73.68% | 90.62% | 71.88% | 18.75 pp |
| E5 + BGE | 89.47% | 39.47% | 87.50% | 46.88% | 40.62 pp |
| E5 + BGE + lexical | 94.74% | 39.47% | 93.75% | 46.88% | 46.88 pp |

On the **12 paired natural information needs**, BGE+lexical DE/RU R@10 is
**95.83% / 87.50%**, a loss of **8.33 pp**; paired MRR **.8917 / .8073**.
E5 loses **66.67 pp** R@10; dual/all-three loses **37.50 pp**. Short-name lexical
benefit is mostly German and increases the total paired gap. Russian spelling
alone does not guarantee historical German alias resolution. Do not interpret
the smaller raw-BGE gap as stronger absolute recall in both languages.

### Graph readiness and alias diagnostics

The 28 person/participant cases include repeated people across passages/events,
German personal names queried in Russian, event-by-participant, and
person+event+place needs. Graph-case R@10: lexical **10.71%**, E5 **53.57%**, BGE
**76.79%**, E5+lexical **57.14%**, **BGE+lexical 83.93%**, dual **66.07%**,
all-three **69.64%**. This supports BGE+lexical for the next discovery acceptance,
with explicit alias/identity review still needed.

Exact/current/historical aliases are separate phrase-FTS branches with
`branch`, `rank` and `matched_alias` diagnostics, alongside E5/BGE/lexical.
No embedding-query string expansion or POI identity merge occurs. Auxiliary
alias fusion is reported separately in the JSON. With **manually supplied,
case-scoped source-verified person/city aliases**, BGE+lexical+alias signals reach
R@1 **64.47%**, R@5 **93.42%**, R@10 **100%**, MRR **.8414**, multi **9/9**.
This is an **assisted experiment**, not autonomous alias discovery acceptance:
the fixture supplies the relevant German/Russian mappings. The production
service accepts explicit caller aliases and returns independent diagnostics;
it does not generate those mappings or resolve canonical POI/person identity.
An earlier unscoped shared-city alias experiment harmed natural-question
ranking. Alias inputs must be relevant to the need and reviewed. This branch was
**not** enabled globally in the selected seven-mode warm path.

Top-10 overlap with BGE is intersection/10, averaged across all 40 queries:
lexical **3.00%**, E5 **34.25%**, E5+lexical **34.25%**, BGE **100%**,
BGE+lexical **98.25%**, dual **61.25%**, all-three **60.25%**. This measures
ranking agreement, not relevance. Full per-language Recall@1/5/10, MRR and
paired-family results are in the aggregate JSON.

### Unsupported questions

Lexical returned no candidates for the two negatives; every vector/fused mode
returned ten nearest passages. **No generated answer exists in this service**,
and no calibrated abstention threshold was implemented or claimed. A retrieved
biographical passage does not validate the false premise. Future answer/graph
consumers must require exact evidence for each asserted fact/relation.

## Actual concurrency and latency

Each warm level ran **30 distinct requests**, in synchronized batches of 1/5/10,
through production PostgreSQL actor RLS and private object-store hydration.
Backend and public OAuth MCP were measured separately, not simultaneously.
HTTP includes deployed edge, OAuth verification, search and three parallel
fetches. All **180 warm requests succeeded**, every initial response was main
ready; no OOM/crash/paid fallback. Warm initial response equals main response:
E5 is deliberately not encoded when BGE is ready within its ten-second budget.

| Users | Backend p50 / p95 / max | Public MCP p50 / p95 / max | Queue peak |
| --- | ---: | ---: | ---: |
| 1 | 1.350 / 1.680 / 1.901 s | 1.361 / 1.778 / 1.957 s | 1 |
| 5 | 3.168 / 4.953 / 4.974 s | 3.097 / 5.105 / 5.387 s | 5 |
| 10 | 4.893 / 9.065 / 9.214 s | 5.264 / 9.164 / 9.280 s | 10 |

Backend stage p95, seconds:

| Users | BGE queue | BGE inference | DB rankings | Fusion + metadata | Hydration |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | .362 | .258 | .246 | .070 | .564 |
| 5 | 3.805 | .292 | .197 | .062 | .564 |
| 10 | 7.927 | .259 | .204 | .062 | .543 |

Queue delay excludes inference and reflects serialized model jobs/broker
transport. Quantiles of stages cannot be added to reconstruct total quantiles.
This meets functional concurrency acceptance, not a subsecond warm product SLA.
At ten users, queue wait dominates observed latency. The bounded warm wait
returns E5 + pending if a burst exceeds ten seconds; main jobs remain durable.

**Actual first cold demand → ready: 73.686 s**. A later cold run measured
**75.370 s**. No assumed one/two-minute estimate was used. Public cold acceptance
started ten concurrent queries: **10/10 immediately received E5 evidence plus
starting/job ID**, fast-pack p50/p95 **2.257 / 2.595 s**. Reissuing the same query
and job ID yielded **10/10 BGE+lexical main packs**, p50/p95 **85.891 / 92.587 s**
from initial demand, including five-second client polling and hydration. Initial
RPCs did not wait for those main results. Temporary OAuth test families revoked.

Controlled loss acceptance separately measured five cold requests: E5 pack p95
**1.827 s**, E5 encoder **18.06 ms**, encoder queue **34.25 ms**. Ten requests
while provider dispatch was suspended: E5 pack p95 **2.219 s**, encoder
**19.26 ms**, encoder queue **70.76 ms**. DB/object hydration remain substantial
fast-tier costs, consistent with the existing E5 acceptance limitation.

## Lifecycle and failure acceptance

The durable SQLite queue resides outside Kaggle and survives service/release
restart. Query jobs precede document jobs; document backfill window 64, overall
pending limit 256, one active claim per serving worker. Run and claim nonces fence
replayed/late results. The controller claims one dispatch and reconciles a stable
notebook slug after an unknown upload result instead of creating another version.
Actual claim timestamps prove **64 fixture queries** ran ahead of older pending
document jobs during the 747-chunk backfill (up to 64 older documents bypassed;
maximum query queue wait **.603 s**). This supplements the deterministic priority
test; an already executing document job is not interrupted mid-inference.

| Scenario | Evidence and outcome |
| --- | --- |
| Simultaneous cold demand | Ten real owner requests created **one** run, all got E5 packs; transaction/thread replay tests also passed. |
| Useful-work lease | 1800 s after demand/claim/completion; unit test advances repeated heartbeats/maintenance without renewal. Actual live notebook idle timestamp accelerated to 1801 s: stopped, empty queue, heartbeat **403**. |
| Planned rotation | Current lifetime timestamp accelerated to **10h45m**. Real CPU successor ready in **68.142 s**; old worker served during warm-up. Maximum serving+warming **2**; 120 queued jobs, **43 pending at handoff**, all done. Old credentials then **403**. |
| Worker disappears mid-job | Real claimed job fenced by expired liveness; reset to pending, completed by replacement at **attempt 2**. Five cold users kept E5 evidence. Replacement readiness wait **66.453 s**; late old completion **403**. |
| Kaggle unavailable | Controller deliberately suspended so no provider dispatch was possible. Ten concurrent queries got E5/starting; jobs durable; no paid provider. This is an injected unavailable boundary, **not a naturally observed Kaggle outage**. |
| Worker startup failure | A real private CPU notebook intentionally raised before loading the model. Provider returned **ERROR**, controller detected it in **32.478 s** from demand, retained jobs; E5 pack **1.283 s**, `unavailable`. Recovery succeeded; 60 s cooldown tested, then accelerated for this acceptance run. |
| Duplicate/replayed job | Actual backfill replay 747 skips/zero jobs; actual completed-query replay preserves job/attempt/vector result. Payload changes are rejected. |
| Lost launch response | Durable `dispatching` claim cannot be reclaimed; stable provider reference is reconciled. Deterministic test covers the ambiguous dispatch boundary; no real lost-network upload is claimed. |
| Credential/protocol failures | Worker cannot access MCP, OAuth user cannot claim worker jobs, stale result/heartbeat denied; wrong revision/space, nonfinite/mixed-size vectors and oversized payload rejected. No arbitrary execution route. |

**Clock acceleration is explicit:** no thirty-minute or eleven-hour wall-clock
soak is claimed. Actual notebooks, cold starts, queued handoff, broker fences,
source reads, queries and recoveries were exercised. The short lifecycle test
does not establish fleet reliability or provider long-duration uptime.

Kaggle CPU worker sample: RSS **2.050 GiB**, HWM **3.706 GiB**, PSS **2.049 GiB**,
four inference threads; startup model process **67.700 s**, separate from queue
demand-to-ready. E5 stayed one process with unchanged PID through the controlled
lifecycle, cgroup peak about **622.32 MiB**, under its existing 1 GiB/1 CPU limit.
MCP/controller perform no model inference. Previous fenced notebooks were
confirmed COMPLETE at Kaggle; the final serving notebook RUNNING and queue empty.

## Verification, operation and remaining limits

Full local suite: **90 passed**, one existing Starlette deprecation warning.
CI Python **3.12 and 3.13 passed** at accepted runtime
[run 37186932531](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37186932531).
Migration proof, production before/after preservation, source hashes, actual
backfill replay, backend/HTTP concurrency, lifecycle and runtime readback are
retained with checksums. Final PR documentation/aggregate commit is also checked
by CI. See [operator runbook](../operations/bge-kaggle.md) for flags, private
runtime paths, narrow protocol, job polling, backfill and rollback.

The active Gause projection remains incomplete: previously audited preview
clipping and image-only pages were not repaired by embedding this projection.
The German fixture is one narrow source, judgments partial, full-question lexical
weak, short current-name-to-German-target retrieval still imperfect. Newly
finalized source revisions require authorized BGE backfill; stale embeddings are
excluded in the meantime, and E5 remains available. The new queue is intentionally
small and local to this service, not a generic compute platform.

Private evidence is **retained**, not age-expiring, at
`/home/dev/artifacts/regional-knowledge-base/20261004T065157Z-bge-kaggle-multilingual-20261004`.
It contains original German text and private fixture/vectors/IDs; do not publish
it. Only the explicit aggregate JSON is committed.

Next implementation prompt: use the accepted E5/BGE+lexical stack for the
[minimal accumulative graph + bidirectional entity/POI discovery](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/prompts/accumulative-knowledge-graph-mvp-after-bge-20261004.md),
with evidence-scoped person/event/thread/POI-reference relations, alias-triggered
bounded discovery, ambiguity review and one-hop reads in existing PostgreSQL.
No separate graph database.
