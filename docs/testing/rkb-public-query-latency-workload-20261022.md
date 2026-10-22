# RKB public-query latency — paired corpus profiles and censored metrics (2026-10-22)

**Status: isolated source + synthetic correctness only. Live v105 schema11 does not yet contain accepted book search evidence; REAL MCP/BGE/PG baseline and source-fix before/after NOT RUN.** Runtime read slots, credentials, authorized public MCP client and Kaggle queries remain the sole responsibility of Шкипер/Claude. This is not a new benchmark service or framework.

## Single existing public path and observations

The operator uses the existing authenticated public MCP `search` / `knowledge_search` request: `KaggleSourceBackend._search` → `ReadAuthority` → `SourceReadGate` → shared RAM `SharedReadAdmission` → owned WSS QUERY → resident `KaggleWorkerQuery` (tokenizer/BGE, PG lexical/dense/hydration, serialization) → final retained public output gate and MCP response. Existing `ReadTelemetry` / `query_telemetry` records correlate the exact `ReadTrace` and safe controller/native stages, may drop records, and grant no new permission. No alternative client, BGE encoder, PG connection, inference sidecar, scheduler or credentials may be introduced.

The bounded leaf `scripts/benchmarks/public_query_latency_leaf.py` accepts **only** the operator's *already authorized* public MCP query callable, privately held eight-question fixture, independently existing read-only corpus version readback, approved eight-case metadata gold manifest and safe output sink. The offline tool only joins existing sanitized telemetry; neither path can launch the provider. These raw client samples do not contain query/evidence text, credentials, private exception strings, or raw runtime identity.

## Two explicitly separate corpus profiles

| `corpus.profile` | Minimum native readback / honest claim |
|---|---|
| `limited_first_book` | **Exactly one accepted real book source** and at least one visible evidence passage, independently confirmed as accepted by owner. Measures only *that first book*. Conclusion `LIMITED_FIRST_BOOK_BASELINE_ONLY_NOT_REPRESENTATIVE` on complete useful source-backed observations. Not a production-scale / cross-book benchmark. |
| `comparison_corpus` | At least **two accepted book sources** and **32 visible passages** plus independently verified source-gold. This is the expanded same-workload before/after profile, **still not production-scale acceptance** without separate quality/capacity gates. |

A source count is an anti-empty check, **not proof** that an accepted source is a genuine book or that its evidence quality is representative. Both profiles require the owner's trusted nonsecret checkpoint: source-manifest SHA, hashed DB generation, schema revision, applied_seq==projection_seq, visibility epoch, accepted/visible counts, actual loaded source release SHA, and owner handoff ref. The readback **before and after each run must be byte-for-byte identical including profile and release SHA**; a change invalidates that run. Across two separately authorized before/after runs, `assert_comparable_receipts` requires **exact same profile, corpus/version/generation, question SHA**; reviewed source SHA and handoff ref may differ but are explicitly recorded, not interpreted as data changes. A comparison across different profiles is **rejected**, not reported as a gain.

The last independently verified v105/schema11 read-only receipt showed only canonical GEO commands, **not an accepted book**; Opus's current live state must be checked afresh before a public query slot. Neither profile is authorized by a merely READY API or a synthetic first-book receipt.

## Frozen source-quality sidecar (metadata-only; no fabricated visual gold)

Operator must first provide exact accepted source/edition and independently human-reviewed evidence for the selected genuine book. The source quality [acceptance matrix](rkb-first-book-acceptance-matrix-20261022.md) is separate from performance. A new pure validation call inside the existing leaf checks the structured `gold_manifest` before issuing **any** public workload query. The manifest is codec `rkb.first-book-query-gold.v1`, state `human_reviewed`, same corpus profile, database generation and source manifest SHA as the existing owner readback, a real edition/reviewer SHA, **eight ordered cases** q01–q08, with exact SHA-256 of each private question, canonical source ID/content revision, nonempty exact locator digest(s), and source human-review digest. Allowed evidence bases are `human_reviewed_source_text`, `human_reviewed_actual_pixels`, `approved_story_body`, `owner_verified_bibliography`. **OCR-only/preview/unreviewed/wrong source** cannot be promoted to approved visual gold. This validates binding and structure, not authenticity of human approval; only Opus's external approval/readback makes a fixture eligible.

The manifest **must not be created from synthetic text/locator fixtures for production use**. It is never written to Git or raw latency logs. The leaf emits only `gold_fixture_sha256` in the run receipt, and before/after comparison refuses changes in question or gold manifest SHA. Correct source-revision/locator quality of actual returned evidence is verified separately by the authorized operator; a fast hybrid response without true expected evidence is not a useful product answer.

## Bounded workload and exact timeouts

The same **eight owner-frozen source-grounded positive questions** `q01`…`q08` appear in both A/B observations, with aggregate canonical question SHA-256 and exact human-reviewed gold manifest digest saved; actual question/evidence content stays private. For each cohort (serial concurrency **1**, then bounded concurrent **2**), each question has **2 serial warmups** and **6 measured repeats**: **32 warmups + 96 measured = 128 requests** total, max entire run **450 seconds**. No extra alias calls, automatic retries, client sessions or hidden BGE/PG bypass. Errors and gate closures remain in the measured denominator.

Three distinct deadlines must never be conflated:

- The existing server `_search` and `kaggle_query.execute_query` enforce a **2-second maximum** absolute search/evidence deadline. This is the *backend's time budget*, not a guarantee of a 2s complete client response.
- The existing product thresholds are **warm backend p95 ≤1,000ms** and **public MCP p95 ≤1,500ms**, with hard evidence deadline **2,000ms**. The leaf does not change these targets or claim acceptance.
- The leaf's **3-second client wait guard starts only upon entering the actual public MCP callback, after the local harness semaphore**. If this timeout fires, the observed time to cancellation is a **right-censored lower bound on hypothetical uncensored completion**, never an exact 3-second completion time or proof that server-side compute stopped. A callback that catches the cancellation and returns after the deadline remains **right-censored**, not a late successful completion. The two owner read-only corpus readbacks also have bounded 3-second callback waits, but they are **not benchmark search samples**. A *remote* TimeoutError received before the client deadline is an **observed failed terminal**, not the 3-second caller censor. Other denials, deadline failures, cancellations and timeouts are preserved.

Each `public_client_sample` contains both `callback_wall_ms` (**public MCP call excluding local benchmark semaphore**, including server admission/network) and `wall_ms` (**from creation of local task to terminal, including harness semaphore wait**), plus exact `client_semaphore_wait_ms`. The reported public MCP SLO comparison must use callback-only latency; the schedule-inclusive latency is shown separately and never silently drops local queue delay. Native controller `admission_ms` remains included in callback time and is **not** the local semaphore. Concurrency=2 process-wide CPU cannot be allocated to one request: per-request `client_cpu_ms` is null, and cohort total CPU/peak RSS are distinct from Kaggle worker CPU/RSS.

## Correct success/timeout/failure statistics

The reporter retains **all 48 measured calls per cohort** regardless of status. It counts `public_success_count`, `failure_count/rate`, `timeout_count/rate` (includes caller timeout and completed server deadline), `caller_timeout_censored_count`, `server_deadline_completed_count`, and `right_censored_count`; terminal and gate-refusal reason distribution remains visible. Warmups remain stored and their censored count is disclosed, but their latency does not enter measured p50/p95.

- `success_only_p50_public_callback_ms` / `success_only_p95_public_callback_ms` measure **only completed, useful hybrid public responses**, not the whole cohort. Schedule-inclusive success-only p50/p95 are separate.
- `all_requests_completion_p95_public_ms` is **null whenever any measured call is right-censored**; survivor percentiles must never be labeled all-query p95. The separate `all_requests_p95_public_lower_bound_ms` uses observed times to failure/censor as a **lower bound**, not a claim of completed request distribution. All-censored cohort → success-only p50/p95 `null`, all-completion p95 `null`, timeout/failure rate **1.0**.
- Independently joined native/controller stages have their own `n`, p50 and p95 **only when exact attempt/generation and telemetry loss gates pass**. `dispatch_to_result_ms`, `search_ms`, `encode_ms` and SQL stages have overlapping durations and **must not be summed**. An absent stage is UNKNOWN, never a zero. If user-visible success lacks an exact stage join, the overall outcome is `INCONCLUSIVE_OR_CENSORED` for profiling.
- Public client CPU and RSS are measured locally; native Kaggle CPU/RSS and isolated transport cost are **null/UNKNOWN** unless the operator supplies separate already authorized observational counters. Never claim these from a subtractive wall-time residual.

Both profile-specific successful terminal labels explicitly deny production-scale acceptance. A partial success or timeout cannot produce a valid performance acceptance claim merely because its surviving requests have a low p95.

## Operator-only use after corpus-ready and read slot handoff

```python
from scripts.benchmarks.public_query_latency_leaf import run_public_workload
receipt = await run_public_workload(
    owner_private_questions,                  # q01..q08; same across A/B
    query_public=existing_authorized_mcp_query, # no new auth/transport
    corpus_readback=existing_owner_readback,  # no new PG/provider calls
    emit=approved_sanitized_jsonl_sink,
    gold_manifest=approved_human_reviewed_source_gold,  # private exact 8-case source/revision/locator hashes
)
# Store this small receipt separately from the current ReadTelemetry sink;
# the public question texts and original source evidence remain private.
```

Existing telemetry/callback logs can then be joined **offline**, without a live operator action:

```bash
python scripts/benchmarks/public_query_latency_leaf.py \
  --receipt /approved/operator/this-run-receipt.json \
  --before-receipt /approved/operator/previous-run-receipt.json \
  --client /approved/operator/client-events.jsonl \
  --telemetry /approved/operator/controller-native-events.jsonl \
  --report /approved/operator/report.json \
  --raw /approved/operator/raw-sanitized-samples.jsonl
```

For the first run omit `--before-receipt`. The optional before receipt verifies profile, **question fixture, gold manifest** and corpus identity against the later run while recording differing code SHAs. Input max 2,048 JSONL records, 8KiB per line, strict field allowlists and finite timing values; outputs are small nonsecret synthetic-friendly metadata. A successful synthetic test **cannot authorize** live reads or prove latency gains.

**Gate status:** current v105 book corpus/positive-gold and runtime read slot are not approved for this run. Owner's accepted first real book permits the *limited* profile only after proof; the later *expanded* profile requires a corpus matching its own bounds. Only after actual measured baseline with real useful search and independently reviewed top bottleneck may one small semantic/authority-preserving source fix proceed. PR209's Python microkernel before/after is **not actual public MCP end-to-end**; PR209/212/215, mandatory CI and live runtime remain untouched.
