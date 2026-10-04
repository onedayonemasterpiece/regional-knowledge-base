# Regional Knowledge Base — production fast E5 tier + concurrency acceptance

Date: 2026-10-04

This is an **implementation task for Codex**, not another architecture review.

Read first:

- `docs/reports/embedding-runtime-decision-20261004.md`
- PR #26 and its reports:
  - `docs/reports/devcoveer-small-embedding-benchmark-20261004.md`
  - `docs/reports/embedding-retrieval-validation-followup-20261004.md`
  - `docs/reports/gause-import-coverage-audit-20261004.md`

Use the actual latest repository/runtime state. Reuse the measured model files/contracts and existing code. Do not rerun the full embedding zoo.

## Product goal

Make **multilingual E5-small INT8 on DevCoveer** the always-ready fast semantic retrieval tier for Regional Knowledge Base, and prove its real end-to-end behavior with 1, 5 and 10 concurrent searches.

This fast tier exists so users still get semantic retrieval while the later BGE/Kaggle main tier is cold or unavailable.

The later BGE-M3/Kaggle implementation is **out of scope** for this task.

## Decisions already made

- Fast encoder: multilingual E5-small INT8.
- DevCoveer only for this task.
- CPU only.
- One loaded encoder process initially.
- Inference concurrency starts at 1 with a bounded queue.
- Hard target envelope: <= 1 CPU and <= 1 GiB RAM.
- Client/PWA/Android inference is deferred.
- No paid/external embedding API calls.
- No implicit fallback to OpenAI, Google, Cloudflare or any other provider.
- Lexical retrieval remains available; E5 + lexical fusion is allowed/expected where it helps.
- Different encoders are different vector spaces. Never compare an E5 query vector with legacy/OpenAI/Gemma/BGE document vectors.

## Important current caveat

The active Gause revision is known to be incomplete as a source projection. Do not pretend this task repairs book completeness.

This task may backfill E5 vectors for the currently active chunks so the production fast tier can be tested, but that does **not** certify the book as completely imported. A later ChatGPT semantic reimport will create a corrected revision after the source transport/review changes are deployed.

## 1. Resolve the actual baseline safely

At the start:

1. inspect current `main`, PR #26, CI, migrations, runtime and production DB schema;
2. do not overwrite parallel work or uncommitted worktrees;
3. if PR #26 is still open, reuse its implementation/evidence rather than duplicating it;
4. do not silently merge unrelated changes merely to simplify the task;
5. ensure the already-established rule remains true: no shared API key can trigger external embeddings.

If a prerequisite from PR #26 is required for this task and not yet on main, use the smallest safe integration path and document exactly what was consumed.

## 2. Run E5 as a separate local service

Do not load the ML model inside the MCP process.

Implement a small local encoder service/process on DevCoveer with:

- pinned E5-small INT8 model/export from the benchmark;
- pinned tokenizer/model file hashes;
- exact preprocessing contract from the compatibility fixture;
- query prefix `query: `;
- passage prefix `passage: `;
- correct pooling and L2 normalization;
- 384 dimensions;
- CPU only;
- inference threads restricted to one CPU;
- memory limit <= 1 GiB;
- listen on localhost/unix socket only;
- no arbitrary model selection or arbitrary code execution;
- health/readiness endpoint/state;
- bounded request size;
- bounded queue;
- deterministic error when queue is full.

Start with one model process and one active inference at a time. Do not create a process pool unless acceptance proves it necessary.

The service should survive normal Regional Knowledge MCP restarts independently. Its lifecycle must be operationally explicit and testable.

## 3. Introduce an explicit E5 vector space

The existing legacy embedding storage is not an acceptable place to silently put E5 vectors.

Implement the smallest verified production schema that supports an explicit E5 384-dimensional space and can later coexist with BGE 1024.

Acceptable implementation shapes include:

- a dedicated chunk-embedding table keyed by chunk + embedding-space, with a verified dimension-specific index; or
- separate typed vector fields/spaces if that is materially simpler and safer in the actual Supabase/pgvector version.

Do **not** choose an abstract framework merely for future elegance.

Required invariants:

- E5 vectors are dimension-checked at 384;
- every vector has an explicit stable space identifier;
- the space identifies the pinned model/export/preprocessing contract;
- query search explicitly names/selects E5 space;
- incompatible vectors can never be compared;
- absence of the requested space degrades safely;
- legacy 768-dimensional vectors are not deleted as part of this task;
- later BGE 1024 can be added without destructive E5 migration.

Before applying a production migration, prove it locally/test environment and provide rollback/compatibility evidence.

## 4. Backfill the active corpus using local E5 only

Using the local encoder service or the exact same pinned local runtime:

- embed all active retrieval chunks that need E5 vectors;
- do not use any external provider;
- make the operation resumable/idempotent;
- associate each vector with source text/hash and embedding-space identity sufficiently to detect stale vectors;
- do not re-embed unchanged chunks unnecessarily on retries.

For the current test book, verify the expected active chunk count from live state rather than hard-coding 712 as truth.

After backfill, prove:

- every active searchable chunk expected to have E5 has one valid 384-d vector;
- no vector belongs to the wrong text/hash/revision;
- no legacy vector was overwritten;
- rerunning the backfill is safe and does not create duplicates.

## 5. Wire the production fast-search path

Regional Knowledge search must be able to execute:

```text
query text
 -> local E5 query vector
 -> E5 vector search over matching E5 document vectors
 -> existing lexical branch
 -> simple rank fusion
 -> existing evidence hydration / ACL behavior
```

Preserve the existing narrow evidence-returning MCP contract.

Expose enough internal/result metadata to know which retrieval mode actually served the request, for example:

- `fast_e5`;
- `lexical_only`;
- later-reserved `main_bge` (do not implement BGE now).

Do not expose implementation noise to end users unless useful.

If E5 service is unavailable:

- do not call any external embedding provider;
- degrade to lexical/evidence-safe behavior;
- expose the degraded mode operationally.

Do not hold requests indefinitely waiting for the encoder.

## 6. Queue and concurrency behavior

The purpose of this task is to answer the real product question: what happens when several users arrive at once?

Use one encoder process, inference concurrency 1, and a bounded queue initially.

Perform real end-to-end tests through the actual production-like search path with:

- 1 concurrent user;
- 5 concurrent users;
- 10 concurrent users.

Each request must include:

- local E5 encoding;
- DB vector retrieval;
- lexical branch/fusion where configured;
- evidence hydration;
- normal ACL/auth path as far as practical in the acceptance harness.

Measure separately:

- queue wait;
- encoder time;
- database/retrieval time;
- evidence hydration time;
- total end-to-end latency.

Report p50, p95, max and failures for each concurrency level.

Also measure:

- encoder process RSS/PSS/peak;
- cgroup/service memory peak if available;
- CPU use;
- maximum queue depth;
- number of encoder processes;
- number of external embedding calls: must be zero.

### Initial acceptance targets

Treat these as product acceptance targets, not historical facts:

- 5 concurrent requests: p95 end-to-end fast search < 1 second;
- 10 concurrent requests: no OOM/crash, bounded queue, target p95 < 2 seconds;
- memory remains <= 1 GiB;
- encoder remains constrained to <= 1 CPU;
- no request accidentally launches another model process.

If 10-user target fails, diagnose first. Do not immediately add replicas. Show whether the bottleneck is encoder, DB, hydration or queue.

## 7. Failure/restart acceptance

Test at least:

1. encoder service already warm;
2. encoder process restart with weights already local;
3. Regional Knowledge MCP restart while encoder remains healthy;
4. encoder unavailable;
5. queue full;
6. repeated identical query;
7. service/host sees several simultaneous first requests.

Required behavior:

- no paid/provider fallback;
- no duplicate encoder-process storm;
- no data corruption;
- bounded failure/retry result;
- lexical fallback remains available when E5 is down;
- normal recovery after encoder returns.

Do not build distributed orchestration. This is one host and one fast encoder.

## 8. Retrieval regression

Use the existing frozen benchmark fixtures and saved corpus evidence where valid.

After production wiring:

- rerun a bounded E5 retrieval regression against the deployed fast path;
- verify known-evidence metrics have not materially regressed because of storage/index/RPC changes;
- explicitly account for the known E5 batch-size contract;
- use the exact production document-encoding batch contract that will be retained.

Do not claim answer accuracy or whole-book completeness.

## 9. Operational status

Add a small operational status surface that reports at least:

- fast encoder configured;
- encoder ready/not ready;
- embedding space/version;
- queue depth or busy state;
- active E5 vector count for current active revisions;
- degraded lexical-only state.

Do not expose secrets, private document text or model filesystem paths to unauthorized users.

## 10. Documentation and retained evidence

Update documentation with:

- actual deployed fast-tier architecture;
- exact E5 space identifier and preprocessing contract;
- service lifecycle;
- queue behavior;
- failure/degraded behavior;
- production migration/backfill procedure;
- rollback;
- measured 1/5/10-user results;
- known source-completeness limitation;
- explicit statement that client inference remains deferred.

Save a final report:

`docs/reports/fast-e5-production-acceptance-20261004.md`

Also save a compact machine-readable acceptance summary if useful.

Do not commit model binaries, private corpus text, user device JSON or secrets.

## 11. Tests

Add/retain tests for at least:

- vector-space mismatch rejection;
- 384 dimension validation;
- preprocessing/prefix/normalization contract;
- queue bound;
- one-process/single-flight startup behavior;
- lexical fallback when encoder unavailable;
- no shared-provider-key fallback;
- idempotent backfill;
- retrieval fusion;
- status surface;
- migration compatibility/rollback assumptions.

Run the full project suite.

## Explicitly out of scope

Do NOT implement in this task:

- Kaggle;
- BGE-M3;
- 30-minute Kaggle lease;
- 11-hour Kaggle rotation;
- Projects Hub/WebGPU/Android inference;
- mobile model download;
- answer-generation scoring;
- automated OCR/LLM book understanding;
- reimport of the incomplete Gause book;
- autoscaling/multiple E5 replicas unless acceptance proves a real need.

## Definition of Done

Done only when:

1. E5-small INT8 runs as a separate always-ready local DevCoveer service.
2. It is hard-bounded to <= 1 CPU and <= 1 GiB RAM.
3. Production has an explicit non-legacy E5 384-d vector space.
4. Active chunks are idempotently backfilled with local E5 vectors.
5. Search uses matching E5 query/document vectors and never mixes spaces.
6. External embedding API calls are zero.
7. Search safely degrades to lexical when E5 is unavailable.
8. 1/5/10 concurrent end-to-end acceptance is actually measured.
9. 5 concurrent p95 is < 1 s, or a concrete measured blocker is documented.
10. 10 concurrent requests do not crash/OOM; target p95 < 2 s is reported honestly.
11. Failure/restart/single-start behavior is verified.
12. Frozen retrieval regression is rerun through the production-like path.
13. Full tests are green.
14. Production deployment is verified rather than merely committed.
15. Final acceptance report is committed with actual numbers.
16. No phone/client inference or Kaggle/BGE code is introduced.

## Working style

- Do not start with another broad audit.
- Reuse PR #26 and existing benchmark evidence.
- Do not rerun all candidate models.
- Do not install a second embedding platform if the small local service is sufficient.
- Keep the production change narrow and reversible.
- Measure before adding replicas/caches.
- Preserve the no-paid-inference invariant.
- Stop only on a real external blocker, and document the exact evidence.

At the end return:

1. deployed fast-tier status;
2. 1/5/10 concurrency table;
3. memory/CPU table;
4. retrieval regression result;
5. exact commit/PR;
6. link to `docs/reports/fast-e5-production-acceptance-20261004.md`;
7. one short next-step prompt for Kaggle/BGE **only if** this acceptance is complete.
