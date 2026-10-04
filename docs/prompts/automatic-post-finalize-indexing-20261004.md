# Regional Knowledge — automatic post-finalize E5/BGE indexing

Date: 2026-10-04

This is an implementation task for Codex.

Read first:

- `docs/reports/post-graph-product-next-step-20261004.md`
- `docs/reports/accumulative-knowledge-graph-mvp-acceptance-20261004.md`
- `docs/reports/fast-e5-production-acceptance-20261004.md`
- `docs/reports/bge-kaggle-multilingual-hybrid-acceptance-20261004.md`

Use current canonical `main` and current production runtime. Do not repeat model
research, graph implementation or reconciliation.

## Goal

Make a newly finalized/activated book revision become semantically searchable
without an operator manually running E5/BGE backfill commands.

After activation, automatically reconcile missing vectors for the active revision:

- local E5 384-d fast space;
- BGE-M3 1024-d main space through the existing durable Kaggle queue.

## Current observed gap

PR #31 new-document acceptance required explicit backfill commands. They correctly
skipped the existing corpus and wrote only the new missing vector, proving
idempotent incremental behavior, but a normal product import must not depend on
operator commands.

Treat that as the exact gap to close.

## Constraints

- No paid/external embedding APIs.
- No new embedding models.
- No graph redesign.
- No Neo4j/RDF/general scheduler.
- No client/mobile inference.
- No semantic book parsing in backend/Codex.
- Preserve E5/BGE vector-space identity and dimensions.
- Preserve current BGE + lexical warm retrieval decision.
- Preserve interactive BGE query priority over document batches.
- Do not block source activation on Kaggle/BGE readiness.
- Do not require a user/operator to run backfill scripts after import.

## Smallest implementation

First inspect how the current graph worker, BGE controller, finalize/activation
path and backfill scripts work.

Prefer reusing an existing background loop or a very small bounded
index-maintenance worker/timer. Avoid another durable queue if missing-vector
state plus the existing BGE queue is sufficient.

Required behavior:

1. Detect active chunks missing E5 vectors.
2. Encode them through the accepted local E5 sidecar.
3. Upsert only matching active-revision E5 vectors.
4. Detect active chunks missing BGE vectors.
5. Enqueue existing BGE document jobs idempotently.
6. Let the existing Kaggle worker/controller process those jobs.
7. Repeat/recover until the active revision has complete vectors or a real
   operational blocker is visible.
8. Ignore inactive/archived revisions for active-readiness accounting.
9. Never delete historical/legacy vectors as part of normal reconciliation.

Do not continuously re-encode unchanged chunks.

## Trigger

A successful revision activation must cause indexing to begin automatically.

It is acceptable for a periodic reconciler to be the durable safety net, but the
normal trigger should make work start promptly rather than waiting for a long
maintenance interval.

Activation/finalize replay must remain idempotent.

## Retrieval readiness

Preserve safe degradation:

- BGE complete/ready -> BGE + lexical;
- BGE incomplete/unavailable, E5 complete/ready -> E5 + lexical;
- E5 incomplete/unavailable -> lexical.

Do not use an old revision's vectors to represent the new active revision.

If partial vector coverage exists during indexing, either search only correctly
matched active vectors with an explicit partial state or remain on the previous
safe degradation mode. Do not silently claim main retrieval is ready.

## Status surface

Expose actor-safe operational/readiness fields sufficient to answer:

- active chunk count;
- E5 ready count / missing count;
- BGE ready count / missing count;
- indexing state: pending/running/ready/degraded;
- BGE worker state;
- effective retrieval mode.

Avoid private source/title/text leakage and avoid exposing another actor's corpus
inventory.

## Acceptance

Use an owned temporary synthetic document/revision, clearly marked as transport
control rather than historical evidence. Prefer more than one chunk so batching
and partial progress are actually exercised.

Acceptance sequence:

1. capture existing active corpus/vector counts and digests;
2. import/stage/validate/finalize the temporary document through the real public
   product path;
3. **do not invoke E5/BGE backfill scripts manually**;
4. observe automatic E5 indexing;
5. observe automatic durable BGE enqueue and completion;
6. prove final vector counts for that active test revision are complete;
7. search through fast/main modes and fetch evidence;
8. replay finalize/activation and prove no duplicate semantic effects/jobs;
9. restart/interrupt the indexing owner once and prove recovery from missing
   vector state;
10. if practical, interrupt/recover one BGE document job using the existing
    accepted worker recovery path;
11. archive the synthetic control and prove it leaves the active historical
    corpus/readiness counts;
12. verify the pre-existing historical corpus/vector digests are unchanged.

Also measure:

- activation -> first E5 vector;
- activation -> E5 complete;
- activation -> BGE job visible;
- activation -> BGE complete;
- number of new E5 writes;
- number of BGE jobs/writes;
- skipped unchanged chunks;
- duplicate jobs after replay: must be zero semantic duplicates.

Do not turn one timing into a universal SLA.

## Gause is not part of this task

Do not reimport Gause here.

The old Gause active projection is known incomplete. This task prepares the
product so that the **next ChatGPT-led corrected reimport** automatically becomes
searchable without operator intervention.

## Tests

Add focused tests for:

- activation automatically triggers indexing;
- E5 idempotent missing-vector reconciliation;
- BGE idempotent job enqueue;
- restart/retry;
- active-vs-inactive revision accounting;
- stale vector rejection;
- partial readiness/degradation;
- interactive BGE priority preservation;
- actor-safe status;
- no external provider fallback.

Run the full project suite and exact-main CI.

## Documentation

Update operational/import docs so a normal user import no longer documents a
manual backfill step.

Create:

`docs/reports/automatic-post-finalize-indexing-acceptance-20261004.md`

Record exact production acceptance evidence and remaining limitations.

## Definition of Done

Done only when:

1. a newly activated revision automatically begins E5/BGE indexing;
2. no manual backfill command is used in acceptance;
3. E5 local indexing is idempotent and bounded;
4. BGE uses the existing durable Kaggle queue;
5. interactive BGE jobs retain priority over import batches;
6. service/worker restart recovers;
7. replay creates no duplicate semantic work;
8. active-readiness status is accurate and actor-safe;
9. search degrades safely during partial indexing;
10. archived/inactive controls do not pollute active readiness;
11. no paid inference API is used;
12. full tests/CI are green;
13. exact production main is deployed/read back;
14. the acceptance report is committed.

At the end return:

- exact main/runtime SHA;
- acceptance timings/counts;
- evidence that no manual backfill was used;
- CI;
- whether the product is ready for ChatGPT to perform the corrected Gause
  reimport.
