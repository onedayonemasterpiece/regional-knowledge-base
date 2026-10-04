# E5 production fast-tier acceptance — 2026-10-04

Production is running the separate pinned E5 INT8 encoder and ACL-protected 384-d retrieval. Acceptance was executed, including public authenticated MCP traffic. **Latency targets are not met:** 5-user HTTP p95 is 1.412 s and 10-user p95 is 2.121 s. The definition-of-done exception is supported by measured DB/hydration bottlenecks below; this is not a claim that the latency targets passed. No request failed in the normal 1/5/10 tests, and no encoder OOM occurred.

## Deployment and contract

- Repository: `onedayonemasterpiece/regional-knowledge-base`; implementation PR [#27](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/27), reusing benchmark [#26](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/26), without repeating the model survey.
- Actual immutable deployed source: `0c9746490c445c720365c28ce79dc9fb2053ed66`. Later acceptance scripts/tests/report in this PR do not change the deployed encoder/MCP/SQL implementation. Main baseline: `45766804cd271f742932ee5b9a3f1eb3f614bf1d`.
- Independent user units: `regional-knowledge-e5.service` and `regional-knowledge-base.service`. Encoder listens only on `127.0.0.1:8767`; MCP uses explicit local fast-tier configuration. Public authenticated MCP search/fetch were verified at `https://knowledge.kenigevents.ru/mcp`.
- E5 model `Xenova/multilingual-e5-small`, revision `761b726dd34fb83930e26aab4e9ac3899aa1fa78`, CPU ONNX INT8, pinned tokenizer/model hashes checked before readiness. Exact hashes and deployment/rollback procedure: [fast-e5.md](../fast-e5.md).
- Space: `e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1`. Query `query: ` batch1; passage `passage: ` per-document text_start/id batch4, final shorter batch allowed. BOS/EOS, right padding ID1, masked mean, max512 tokens, float32 L2 normalization.
- One process, one active native inference worker, bounded FIFO10. Full queue returns429; deadline returns503; MCP falls back to lexical. Requests do not instantiate models. No numpy/tokenizer/ONNX imports or model loading in MCP.
- Operational CLI reports configured=true, ready=true, matching space, active E5 count747, degraded=false; public `/fast-tier/health` reports ready fast_e5 without private details. A stale service-env PYTHONPATH initially caused404 on the new route; the exact immutable path was fixed and the route and authenticated tools were verified after restart.

## Storage, migration and backfill

`sql/010_fast_e5.sql` adds a separate vector(384) table with exact-space/source-hash/revision/batch-hash checks, cosine index and RLS. The security-invoker RPC uses matching E5 vectors plus the existing simple/websearch AND lexical branch, RRF60 and stable ID ties. No legacy space is mixed into E5 retrieval.

Isolated PostgreSQL17.10 + pgvector0.8.2 verification passed: migration twice, wrong dimension/space rejection, owner access and denied actor, stale source exclusion, RRF60 fusion, NULL-vector lexical retrieval, and rollback preserving legacy chunks. Live PostgreSQL17.11 uses the same pgvector0.8.2. SQL hash: `c8ae9a155acb591d788d93d47268016e97731858e122a5be087b94cb16b4abb1`.

Production additive migration and final identity check preserved **907 legacy rows/vectors**, including identical ID/vector/space digest. Backfill encoded187 deterministic batches and wrote747 active vectors. A second actual run encoded0 batches, wrote0 vectors and skipped747. Exact source hashes are verified before encoding and rechecked atomically against active revision at write time. New ingestion records `fast_e5_backfill_required`; the operator backfill remains necessary for newly finalized revisions.

## Actual concurrent search and evidence hydration

Backend acceptance: 60 requests at each level, synchronized waves of 1/5/10 logical users, all30 frozen queries repeated twice. Each request runs the ordinary production backend owner actor bridge, `rkb_app` RLS, local encoding, actual remote Session Pooler SQL, and authorized fetch of the first3 results from actual object storage. No service-role user search, mocked DB or reconstructed in-memory retrieval is used. Pools/connections are warmed; no query/evidence cache or replicas were added. All simulated users share the existing owner ACL and data; this does not test ten different account registrations.

Public HTTP acceptance: 30 requests per level, all30 frozen questions once, through the public edge, real bearer-token verifier, MCP search and up to3 parallel MCP fetch calls. An operator-minted temporary owner token family is held only in memory and revoked in finally; this verifies server authentication, not browser consent UX. The six production tools are unchanged. The HTTP total includes edge/auth/MCP overhead; internal stage timings below belong to the backend run.

| Users | Backend n | Backend p50 / p95 / max ms | Public HTTP n | HTTP p50 / p95 / max ms | Failures (backend / HTTP) |
|---:|---:|---:|---:|---:|---:|
| 1 | 60 | 527.6 / 977.0 / 1069.1 | 30 | 526.0 / 879.2 / 1247.7 | 0 / 0 |
| 5 | 60 | 1018.5 / 1339.2 / 1684.5 | 30 | 1060.3 / 1412.3 / 1472.9 | 0 / 0 |
| 10 | 60 | 1753.8 / 2112.2 / 2199.8 | 30 | 1757.2 / 2121.4 / 2126.9 | 0 / 0 |

All normal requests used `fast_e5`; no overload fallback hid an acceptance failure. Totals include hydration. Nearest-rank percentiles are calculated from every request, including negative controls; an unanswerable control may still return related passages because no answer generator/abstention scorer is implemented.

| Users | Queue p50 / p95 / max ms | Encode p50 / p95 / max ms | DB p50 / p95 / max ms | Hydrate p50 / p95 / max ms |
|---:|---:|---:|---:|---:|
| 1 | 0.0 / 0.1 / 0.1 | 13.0 / 30.2 / 99.8 | 232.7 / 256.2 / 264.4 | 272.2 / 716.9 / 825.8 |
| 5 | 26.8 / 55.8 / 65.3 | 13.4 / 17.5 / 20.2 | 471.3 / 509.2 / 532.2 | 497.2 / 801.0 / 1134.0 |
| 10 | 63.7 / 139.3 / 159.9 | 14.0 / 22.4 / 30.7 | 616.0 / 875.7 / 891.0 | 1001.2 / 1191.6 / 1287.9 |

### Measured latency blocker

At 5 users, E5 queue p95 is 55.8 ms and inference p95 is 17.5 ms, while DB p95 is 509.2 ms and hydration p95 is 801.0 ms. At 10, DB p95 is 875.7 ms and hydration p95 is 1191.6 ms. Stage percentiles are not additive; their paired totals are reported above.

Actual EXPLAIN ANALYZE through the ordinary ACL context (10 repetitions): server RPC execution p50 177.4 ms, p95/max184.2 ms; RPC roundtrip p50 189.9 ms; SELECT1 roundtrip p50 9.7 ms; pool+actor setup p50 39.9 ms. Thus the server-side ACL/fusion SQL itself consumes substantial time; this is not solely network RTT or encoder startup. Profiling of internal SQL operators is still needed before selecting a safe optimization.

A measured pool16 experiment still yielded backend p95 1.268 s at 5 and 2.015 s at 10, and the Session Pooler rejected new connections with `EMAXCONNSESSION`, explicitly limiting session clients to 15. The experiment closed its pool; **production remains at max6**. It does not justify raising production connections to 16 or adding encoder replicas. Next performance work should profile SQL/RLS and hydration fan-out without weakening authorization. Latency remains an operational limitation, not a completed optimization.

## Memory, CPU and process count

Hard encoder limits verified from its live cgroup: cpu.max `100000 100000` (=1CPU), affinityCPU 0, memory.max1073741824 (1GiB), memory.swap.max0. ORT uses sequential execution and intra/inter-op1; the process has2–3 OS threads (main/worker/runtime), not multiple inference workers.

| Phase | RSS / VmHWM MiB | PSS MiB | Cgroup peak MiB | Encoder CPU seconds for the phase |
|---|---:|---:|---:|---:|
| 1 users,60 requests | 490.5 | 478.0 | 448.0 | 0.858 |
| 5 users,60 requests | 490.6 | 478.1 | 448.1 | 0.814 |
| 10 users,60 requests | 490.7 | 478.2 | 448.2 | 0.787 |
| Passage batch4,512-token cap | 659.1 | 646.7 | 621.4 | 1.298 s inference wall time |

Encoder PID stayed constant throughout each normal load run. Actual `/proc` enumeration after restart/stress found exactly one model process. Cgroup memory.events reported oom=0 and oom_kill=0, with no quota throttling in the retained normal/stress snapshots. RSS/PSS and cgroup accounting measure different memory sets; all remained below the hard limit. Batch4 resource check uses synthetic max-length passages and also ran under the live 1GiB cap. MCP remained a separate process (about100 MiB cgroup peak during public HTTP acceptance).

External embedding API calls are zero: the MCP fast-tier client permits only the fixed loopback endpoint and never selects a provider fallback; the encoder uses verified local files with offline settings. The acceptance/status counters report 0 and the reviewed implementation contains no external embedding path in fast mode. This is a code/configuration-backed invariant, not a claim of packet-capture auditing.

## Failure and lifecycle acceptance

- Encoder restart: readiness recovered from local pinned weights in 2.170 s with a new PID. No download occurred.
- MCP restart preserved the independent encoder PID; subsequent public OAuth/search/fetch smoke passed.
- Encoder stopped: application returned lexical_only with 3 hydrated evidence items in 0.975 s, with 0 model processes. Recovery returned fast_e5.
- Ten simultaneous systemd start calls plus ten initial application requests:10 successful lexical searches during cold startup; exactly one ready encoder process afterward.
- Actual40-request queue overflow: {'200': 9, '429': 29, '503': 2}; observed queue never exceeded 10; maximum request duration 2.653 s. HTTP429 is a bounded rejection,503 is the bounded queue deadline, not a process crash.
- Additional occupied-queue test:10/10 application queries safely returned lexical_only with 0 application errors. After the queue drained, normal fast_e5 search/evidence recovered.
- Five identical queries produced exactly equal vectors and retained the same PID; no query cache was added.

## Frozen retrieval regression and evidence limits

Actual local E5 → production DB/RLS RPC was rerun on all30 frozen questions (28 answerable,5 multi-evidence questions). Regression asks20 ranked results internally, with candidate depth 100 to match PR #26; normal MCP keeps 8 and acceptance hydrates at most 3. These are retrieval metrics, not generated-answer correctness or assurance that the first3 evidence items contain every required fact.

| Measure | Prior pinned batch4 benchmark | Production-like regression |
|---|---:|---:|
| Macro evidence-group Recall@1 | comparison focuses on @10 | 67.86% |
| Recall@5 | — | 91.07% |
| Recall@10 | 96.43% | 96.43% |
| MRR | — | 0.791071 |
| Multi complete at 10 | 5/5 | 5/5 |

Production includes747 active chunks versus the frozen Gause corpus712, so other authorized documents are possible distractors. Known-evidence Recall@10/multi completeness did not regress. Ten fixed multilingual strings, both query and passage batch1:20/20 vectors matched exactly (max difference0, cosine1). Stored712 Gause batch4 vectors match the retained matrix: max absolute component difference7.403e-9, minimum cosine0.9999999999999996; source hashes also matched. No new model comparison was run.

**Source is still incomplete.** The prior source audit found200 exact1000-character prefixes on120 pages,131480 omitted suffix characters, and image-only physical pages145–174 without searchable text chunks. This task did not reimport, OCR or repair that projection. Coverage747/747 means the existing active chunks have valid vectors; it does not mean every book page/fact is represented. Previously expanded judgments remain partial and model-assisted (823/957 unknown), so no general quality-winner claim is made. Client inference remains deferred.

## Verification, evidence and rollback

Full local project suite:79 tests passed (Python3.14; one existing Starlette deprecation warning). Focused tests cover dimension/finiteness/normalization, fixed prefixes, space rejection, bounded single-worker queue/start, unavailable lexical fallback, no shared-provider-key fallback, changed-batch/idempotence behavior and safe status serialization. Real isolated DB checks cover fusion, ACL, migration compatibility and rollback; actual unchanged backfill confirms zero inference/writes. GitHub Python3.12/3.13 checks are reviewed on the final PR head separately from the deployed runtime SHA.

Private run evidence is retained under `/home/dev/artifacts/regional-knowledge-base/20261004T060943Z-fast-e5-production-20261004` with its managed retention manifest. Public aggregate JSON: [fast-e5-production-acceptance-20261004.json](fast-e5-production-acceptance-20261004.json). It includes SHA256 hashes of the exact measured evidence files; private per-request IDs, corpus/model binaries, credentials and tokens are not committed. The original retained pinned cache/runtime is an active dependency of the model service and must not be removed while its symlinks are used.

Rollback procedure is documented in [fast-e5.md](../fast-e5.md): disable the fast client, restore the old MCP immutable release/PYTHONPATH/unit and restart MCP; extra E5 storage can remain dormant. Only after callers are disabled may its own rollback SQL remove the E5 RPC/table/index. Legacy data survives. The saved pre-change unit is retained; service.env/secrets were never copied into evidence.

No Kaggle/BGE, phone/client inference, ingestion/OCR overhaul, provider-key fallback, answer-generation scoring, autoscaling or replica/cache platform was added.
