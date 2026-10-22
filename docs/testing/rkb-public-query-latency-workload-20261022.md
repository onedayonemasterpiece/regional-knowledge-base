# RKB public-query E2E latency workload — operator handoff (2026-10-22)

**Status: SOURCE + SYNTHETIC ONLY; real Kaggle baseline, bottleneck decision and before/after: NOT RUN.** The current warm v105 schema11 is empty of accepted book content. Empty-corpus requests are not representative search measurements. Sole operator Шкипер/Claude retains all runtime, provider, credential, model and PG actions.

## Reuses existing actual public path and telemetry

The existing MCP public `search` / `knowledge_search` route uses `KaggleSourceBackend._search` → `ReadAuthority` + `SourceReadGate` → `SharedReadAdmission` → existing WSS QUERY → resident `KaggleWorkerQuery` (BGE tokenizer/encode + PostgreSQL lexical/dense/hydration) → final output-retention authority and transport. `ReadTelemetry` already emits `read_attempt_started/dispatched/terminal` plus native `kaggle_read_completed` and `kaggle_read_adapter_terminal` events, joined by the actual public `ReadTrace`. Loss/cancellation is possible; these are advisory signals and never admission receipts.

The new **single leaf** `scripts/benchmarks/public_query_latency_leaf.py` is a bounded callable wrapper **over the owner's injected existing public MCP query**, plus an **offline joiner** over the existing telemetry, **not another client/transport/PG/model/scheduler/benchmark framework**. The owner supplies its own authorized callback, immutable private question fixture, trusted read-only corpus readback, and safe output sink. There are no imports, network connections, client credentials or provider controls in the leaf.

## Reproducible bounded plan

- Fix one accepted corpus and complete read version: schema_revision, loaded release SHA, pseudonymous database generation, visibility epoch, applied_seq==projection_seq, source-manifest SHA, accepted source and visible passage counts, and the independent sole-operator handoff ref. Read back the **same** state before/after; a change invalidates comparison. The synthetic lower bound (≥2 accepted sources, ≥32 visible passages) only rejects trivial empty-data runs: **separate owner evidence/gold quality proof is necessary for representative corpus acceptance**.
- Freeze **8 real owner-supplied questions**, ids `q01`…`q08`, same bytes/corpus/version across A/B. Do not store question text; keep only canonical aggregate fixture SHA-256. The owner must separately attest answers/source revisions and expected positive evidence; merely `hybrid` status cannot establish search quality.
- Two cohorts, `serial` (concurrency **1**) and `concurrent2` (concurrency **2**). Per cohort per question: **2 warm-ups + 6 measured repetitions**. Total **32 warm-ups + 96 measured = 128 public requests**; no automatic retries, no new worker or queue. Existing query evidence deadline 2s; client wrapper hard timeout 3s per call and entire workload ≤450s.
- Every raw client sample records only `case_id, phase, warmup, trace_id, attempt_id, request_ref, status, reason, mode, evidence_count, wall_ms, client_cpu_ms, client_rss_kib`. Client process CPU is **not attributable per call when concurrency=2**, so per-call CPU is null there and a single per-cohort process CPU/peak RSS record is emitted. **These are not native worker CPU/RSS.** Шкипер must supply independently existing read-only native CPU/RSS before/after if available; otherwise **UNKNOWN**, never guessed.
- The offline analyzer joins controller/native stage records with exact runtime-incarnation / connection / turn refs (writes only their salted-looking digest, not original run IDs). It refuses unknown/private field names, nonfinite metrics, duplicates; missing/lossy/mismatched telemetry becomes `inconclusive`. All denied, cancelled, timed-out, pending, capacity/authority/output-gate failures remain in the denominator. Warm-ups are retained as raw samples but excluded from p50/p95. End-to-end p50/p95 use wall monotonic time from the **actual public caller**.

## Interpret stages without double counting

| Readout | Scope / caveat |
|---|---|
| `client wall_ms` | True client→public MCP→result incl. transport, once operator provides a real client |
| `auth_ms, binding_ms, dispatch_gate_ms, finish_gate_ms` | Existing controller intervals |
| `admission_ms` | Wait from admission entry to dispatch, not queue length |
| `dispatch_to_result_ms` | Includes WSS/native/execution; *not* separate network time |
| `tokenize_ms, encode_ms` | Native token count and encode incl. BGE forward/formatting/wait |
| `lexical_ms, dense_ms, hydrate_ms` | PG candidate/hydration waits, not independent PostgreSQL CPU |
| `search_ms, serialize_ms, total_ms, adapter_ms` | Overlapping outer and nested stage windows; **never sum** |
| `result_to_terminal_ms` | Result-to-final public output terminal |
| Native CPU/RSS, isolated transport | **UNKNOWN/null unless separately observed by authorized owner** |

Production gate thresholds remain warm-backend p95 ≤1000ms, public MCP p95 ≤1500ms and hard evidence deadline ≤2000ms; none is proven by a synthetic run or missing corpus. Reported status/result/evidence/generation and source-gold correctness require separate owner proof.

## Operator-only runnable use

Already authorized owner binds the callback from its **existing** MCP session; `corpus_readback` must return the already supported operator's read-only version receipt. **Illustrative only** — not a live launch authorization:

```python
from scripts.benchmarks.public_query_latency_leaf import run_public_workload
receipt = await run_public_workload(
    owner_private_questions,                  # exact q01..q08 -> immutable text
    query_public=existing_public_mcp_query,   # no new MCP transport
    corpus_readback=existing_owner_readback,  # no new PG / provider call
    emit=approved_sanitized_jsonl_sink,
)
# Persist receipt separately; collect existing ReadTelemetry JSONL using
# the operator-approved read-only sink (not a new listener).
```

Once the exact same run's client JSONL, existing controller/native telemetry JSONL and receipt are available, the **offline** command is:

```bash
python scripts/benchmarks/public_query_latency_leaf.py \
  --receipt /approved/operator/workload-receipt.json \
  --client /approved/operator/public-client.jsonl \
  --telemetry /approved/operator/read-telemetry.jsonl \
  --report /approved/operator/latency-report.json \
  --raw /approved/operator/latency-raw-samples.jsonl
```

Inputs bounded to ≤2048 telemetry records of ≤8192 bytes each. Output contains no question/evidence/private actor text, model tensors or credentials. Source tests create only synthetic observations and never access Kaggle.

## Current blocker / after baseline

Live v105/schema11 has **no accepted book corpus**, and Шкипер has not provided a corpus-ready + read-slot handoff for this exact workload. **No real public calls, CPU/RSS metrics or before/after were collected.** This leaf is ready for owner review; *after* the first correctly observed, source-backed baseline, choose the **largest measured** stage and propose one semantic-preserving source fix with independent review. PR209 remains CI green but without E2E gains established; do not touch shared PR212/runtime or restart Kaggle for this isolated benchmark.
