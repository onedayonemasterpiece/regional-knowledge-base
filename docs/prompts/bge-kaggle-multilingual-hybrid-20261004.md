# Regional Knowledge — Kaggle CPU BGE-M3 + multilingual hybrid retrieval

Date: 2026-10-04

This is the next implementation task after the DevCoveer E5 production acceptance
is complete.

Read first:

- docs/reports/embedding-runtime-decision-20261004.md
- docs/reports/multilingual-hybrid-poi-retrieval-decision-20261004.md
- the completed docs/reports/fast-e5-production-acceptance-20261004.md when it exists
- docs/poi-integration.md

Do not start if the E5 production acceptance is incomplete; report that
prerequisite instead of rebuilding E5.

## Goal

Add BGE-M3 on Kaggle CPU as the main semantic retrieval tier and determine
empirically whether the warm path should use BGE + lexical or E5 + BGE + lexical.

The result must support multilingual retrieval, especially German source text
queried in Russian or German.

Do not implement POI reverse discovery in this task; only make the retrieval layer
and benchmark ready for it.

## Constraints

- Kaggle CPU only; no GPU.
- No paid embedding/inference APIs.
- Existing E5 fast tier remains available and independent.
- BGE and E5 are separate vector spaces; never compare or mix raw vectors.
- Merge rankings with RRF or another explicit rank-based fusion only.
- Do not put model inference inside the MCP process.
- Keep queue/orchestration small; no generic compute platform.
- Do not ship model inference to phones/PWA.
- Preserve source/user ACLs before evidence leaves Regional Knowledge.
- Preserve the existing source-owner/grant model.
- No server-side OCR/LLM book understanding.

## BGE vector space

Use pinned BGE-M3 dense embeddings as the first main-tier implementation.

Create an explicit 1024-dimensional BGE space that can coexist with legacy
768-dimensional vectors and E5 384-dimensional vectors.

Store and verify space identifier, exact model revision, preprocessing contract,
dimensions, and source text/hash/revision identity.

Backfill only authorized active chunks needed for acceptance, using Kaggle CPU.
Make the operation resumable/idempotent.

## Kaggle worker lifecycle

Implement one temporary BGE worker controlled by a durable queue outside Kaggle.

Required behavior:

- first demand atomically transitions stopped -> starting;
- simultaneous requests do not launch duplicate notebooks;
- while starting, E5 fast tier continues serving requests;
- once ready, BGE consumes interactive query jobs;
- short query jobs have priority over long document-embedding batches;
- after the last useful work/request, keep the worker warm for 30 minutes;
- heartbeat/liveness does not itself extend this useful-work lease;
- if demand continues, lease moves forward;
- rotate before 11 hours of worker lifetime;
- during planned rotation allow at most one serving worker plus one warming
  successor;
- unfinished durable jobs survive worker loss/rotation;
- duplicate/replayed jobs are idempotent.

The externally reachable temporary Kaggle worker endpoint is acceptable. Use a
short-lived run-bound credential and a narrow embedding protocol; do not expose
arbitrary execution.

Measure actual cold startup. Do not assume one or two minutes.

## Warm retrieval modes to compare

For the same query and ACL-visible corpus, evaluate:

1. lexical only;
2. E5 only;
3. BGE only;
4. E5 + lexical;
5. BGE + lexical;
6. E5 + BGE;
7. E5 + BGE + lexical.

Run E5 and BGE in parallel when testing dual-encoder warm retrieval.

Do not automatically enable all three branches. Select the smallest warm path that
materially improves retrieval quality.

## Multilingual benchmark

Create a source-verified test set that includes genuine German source passages.

At minimum include paired information needs:

- German source / German query;
- German source / Russian query;
- historical German POI name in query;
- current Russian POI name when the source uses only a German historical name;
- German orthographic/spelling variants;
- transliteration variant;
- ambiguous historical name;
- multi-fact question;
- unsupported/false-premise question.

Where a Russian and German question express the same information need, they must
have the same evidence target.

Do not translate evidence passages into Russian for scoring. Score against the
original German source evidence.

Report per-language and cross-language Recall@1/5/10, MRR, multi-evidence coverage,
top-k overlap, judged Precision/nDCG where labels actually support it, and
unsupported-answer behavior separately.

The benchmark must explicitly answer:

- How much quality is lost when a German source is queried in Russian?
- Does E5+BGE rank fusion improve over BGE alone enough to justify the extra E5
  query work?

## Graph-readiness retrieval cases

The IdeaHub voice requirement now includes an accumulative graph of people,
events, historical threads and POI references. Do **not** implement that graph in
this task, but make the retrieval acceptance representative of its future
discovery needs.

Add source-verified cases for:

- the same historical person appearing in multiple passages/events;
- a German personal-name spelling queried in Russian;
- an event queried through a participant name rather than the event wording;
- a current Russian POI name locating German historical source text;
- one information need involving person + event + place.

Report whether BGE, E5 or their fused ranking is most useful for these cases.
Keep exact alias matching as a separate signal rather than hiding it inside model
scores.

## Named entities and future POI use

Include historical/current-name retrieval tests because this stack will later back
POI reverse discovery.

Do not solve POI identity by blindly expanding strings in the embedding query.
Preserve exact alias lexical branches and vector semantic branches as separate
signals, then fuse candidates.

Return enough ranking diagnostics for later POI acceptance to know whether a
candidate came from exact/current alias, historical alias, E5, BGE, or lexical
text search.

No automatic POI merge is part of this task.

## Concurrency

Measure at least:

- 1, 5 and 10 concurrent user searches with BGE warm;
- cold BGE with concurrent requests while E5 serves fast results;
- worker rotation with queued requests.

Report E5 latency, BGE latency, fusion/hydration latency, end-to-end initial fast
response, end-to-end main result, queue depth, cold startup, and worker
lifetime/lease transitions.

Do not hide the cold state behind a long hanging request. The consuming service
must be able to know that the main result is pending/starting while fast E5
evidence is already available.

## Failure acceptance

Verify Kaggle unavailable, worker startup failure, worker disappears mid-job,
30-minute idle expiry, planned pre-11-hour rotation, duplicate simultaneous start
requests, stale/late worker result, E5 remains available throughout, and no paid
provider fallback.

## Documentation

Update the embedding runtime decision with measured BGE results and document the
multilingual retrieval acceptance, exact BGE vector-space contract, Kaggle
lifecycle/queue operations, and failure/degraded modes.

Create:
docs/reports/bge-kaggle-multilingual-hybrid-acceptance-20261004.md

Do not commit model binaries, private book text, credentials or user device JSON.

## Definition of Done

Done only when:

1. one BGE-M3 Kaggle CPU worker is operational through the durable queue;
2. single-start protection is proven;
3. 30-minute useful-work lease is implemented/tested;
4. pre-11-hour rotation is implemented/tested;
5. BGE document/query vectors are in the same pinned 1024-d space;
6. E5/BGE spaces cannot be mixed accidentally;
7. German-source German-query and German-source Russian-query retrieval are both
   actually tested;
8. all seven retrieval-mode ablations are measured;
9. a measured warm-path decision is made;
10. 1/5/10 warm concurrency is measured;
11. cold BGE still yields immediate E5 fast retrieval;
12. no paid inference API is used;
13. project tests are green;
14. deployed behavior is verified, not merely committed;
15. final acceptance report with actual numbers is committed.

At the end return a short verdict and one next-step prompt for the **minimal accumulative knowledge graph + bidirectional entity/POI discovery** implementation, using the accepted multilingual retrieval stack. Do not propose a separate graph database.
