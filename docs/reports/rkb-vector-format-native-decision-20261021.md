# RKB: vector literal / Go–Rust narrow native decision (21 October 2026)

**Status: MEASURED, Rust NO-DEPLOY; Python source-only improvement, not deployed.**
This closes **one micro-kernel evaluation**, not the whole Go/Rust work package or the warm-MCP performance gate. Owner contracts and the original Kaggle implementation handoff remain authoritative and unchanged.

## Source and CI receipts

| Evidence | Immutable identity |
|---|---|
| Common baseline | `main f2c6ad0ba9df283a08240ee31de5ae771c2820eb` |
| Isolated Rust prototype (not in this PR) | `caeaaa3d70f260a3813a72fcf6e9520b0aba0472`; pinned Rust 1.85.1, zero Cargo dependencies, in-process `cdylib` |
| Rust parity + A/B/C CI | [run 39689859409](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39689859409), PASS; [artifact 12387423883](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39689859409/artifacts/12387423883), 14-day retention |
| Compiled Rust library SHA-256 | `904f92f96c48661947a125be99c28020bd89a0d9da8b373e5733f2e0d5421e01`; 443,744 bytes. Binary is **not** included in this PR or runtime bundle |
| Python two-file fix, measured source | `a3d4338b8e60b9c7d42a02a10c05e2d167fc4583` |
| Actual A/D source-path CI | [run 39691439134](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39691439134), PASS; [artifact 12388124178](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39691439134/artifacts/12388124178), 14-day retention |
| Installed real-package import / existing regressions | [run 39692017426](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39692017426), PASS at `0e4a1104361fb5acc7a65e52c43170fb8fb33ef6`; [artifact 12389315649](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39692017426/artifacts/12389315649) |

## Method and measurements

**A/B/C: synthetic isolated micro-kernel, GitHub-hosted Ubuntu / Python 3.12.15.**
Each variant had 45 warm-ups, a 30-call smoke, then **1,200 calls per variant** over **23 deterministic synthetic 1,024-component vectors**, including normalized FP32, sparse, signed zero and decimal edge cases. A and B execute the unmodified Python `vector_literal` function bodies extracted from the pinned source. A formats **twice** (discard first string); B formats **once** (ideal Python one-pass comparator). C executes strict Python type/length checks, Python-list → contiguous 1,024 `f64` copy, ctypes call into Rust, output-buffer allocation, result copy and ASCII decode. **All C copies, Python conversion, FFI and output allocation are included**. No separate process or IPC was introduced. The strict numeric, error and byte-for-byte representation parity suite passed before performance.

| A/B/C variant | Wall p50 / p95 (µs) | Process CPU p50 / p95 (µs) | Process peak RSS (KiB) |
|---|---:|---:|---:|
| A — original Python twice | 1305.880 / 1333.360 | 1304.897 / 1331.907 | 22,756 |
| B — original Python once | 630.016 / 645.048 | 629.043 / 644.116 | 22,604 |
| C — Rust plus all Python/FFI costs | 661.051 / 677.356 | 660.079 / 676.283 | 23,136 |

C internal **wall p50**: Python conversion + allocations **180.909 µs**, FFI + Rust formatting **471.640 µs**, output copy/decode **2.804 µs**. Separately measured cold `ctypes.CDLL` library load **0.294137 ms**; it is neither per-request nor Kaggle cold startup. C p95 was **5.0% worse than B** and RSS peak **532 KiB higher** on this run. A→B p95 reduction was **51.6%**. CI runner variation, no cross-run pairing, sampling and instrumentation constrain inference.

**A/D: measured changed Python path, separate hosted CI.** Replayed the *actual source-extracted nested* `kaggle_query.execute_query.checked_encode` plus downstream `kaggle_search.vector_literal`; A is old early **validate-and-format + final validate-and-format**, D is **early strict validate without discarded text + final validate-and-format**. Thus D retains two validations; this is **not** ideal B. Both had 40 warm-ups, bounded smoke and **1,200 calls of each path**, **42 deterministic synthetic vectors**, randomized interleaved A/D order on one runner.

| A/D variant | Wall p50 / p95 (µs) | Process CPU p50 / p95 (µs) |
|---|---:|---:|
| A — original executable path | 1230.865 / 1260.190 | 1229.470 / 1258.132 |
| D — actual two-file Python patch | 761.418 / 780.347 | 760.111 / 779.050 |

D reduced *this synthetic local-path* wall p95 by **38.1%**. Joint A/D process RSS before/after: **29,024 / 29,196 KiB**; joint `ru_maxrss` before/after: **28,960 / 29,088 KiB**. These are **not** independent per-variant RSS measurements. A/B/C and A/D absolute latency figures must not be compared across distinct runners/harnesses.

The measurements use `time.perf_counter_ns`, `time.process_time_ns`, `thread_time_ns` where available and Linux process RSS / `ru_maxrss`. They **exclude** real tokenizer, Torch/BGE residency/inference, PostgreSQL/pgvector, admission, WSS, output authority, end-to-end useful-answer time and Kaggle session lifecycle. **No claim of warm MCP p95 or Kaggle deployment speedup is made.**

## Chosen implementation and protected behavior

**Rust NO-DEPLOY:** native parity is not an advantage by itself, and complete Rust FFI/copy cost did not beat fair Python B. No Go/Rust runtime sidecar, packaging, scheduler or new transport is justified by this test.

**Python patch (only two production files):**
- `src/regional_knowledge/kaggle_search.py` Git blob `01b06d3f8691761504689537449fca95ae77eff6`: one `validate_vector_values(values)` helper checks list membership, exact `int|float` element type excluding bool, pinned dimension, finite values and L2 norm through the unchanged `bge_contract.validate_vector`. `vector_literal` still calls this helper and emits identical numeric text.
- `src/regional_knowledge/kaggle_query.py` Git blob `c40152ec887c8f387f7be0ecc464ec79fb406d2c`: `checked_encode` now calls the helper without discarding an assembled pgvector string; it still rejects invalid output **before hybrid success**, maps encoder/validation failures to `DenseUnavailable` and propagates `asyncio.CancelledError`. Downstream `vector_literal` independently validates future direct callers.

**Actual import and regression receipt:** The installed project `regional_knowledge.kaggle_query` and `kaggle_search` imported successfully; real module helper identity, return shape and strict invalid vector handling passed. Existing suites `test_kaggle_query.py`, `test_kaggle_search_scope.py`, `test_kaggle_pg_search.py`, `test_kaggle_worker_query.py`, `test_kaggle_query_roundtrip.py`: **57 passed, 26 skipped, 0 failed** (83 collected); the skips explicitly require opt-in real PostgreSQL/container fixtures, including two PG roundtrip cases. Additional source-extracted parity/cancellation/invalid-dense tests: **9 passed**. No PG, GPU/model, Kaggle session or provider was started. No mandatory `ci.yml` changes.

## Integration boundary

Source can be reviewed/merged only with the broader owner-approved runtime bundle. Runtime owner **Claude** must take the **two exact Git blobs** above, preserve auth/leases, model and numeric fidelity, then verify the actual installed Kaggle PG/search path and recovery/current-runtime revision under a separately authorized integration. This report and isolated test workflow are CI/doc evidence only; **the Rust prototype and compiled binary must not be imported into production**. As recorded, **NO merge, NO deployment, NO Kaggle restart, and no real PG/Kaggle acceptance** were performed for the benchmark decision.
