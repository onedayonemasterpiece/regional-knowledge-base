# RKB A2 — warm read-handler boundary (2026-10-21)

**Status: source/fixture only. Not a live patch, controller operation, production rollout or complete DBG-1 acceptance.** This change is isolated from frozen PR #170 (cold-demand/MCP) and from the import/media and expert workflow executors.

## Precisely implemented

- The existing `TargetRuntime.prepare` still initializes the one PostgreSQL pool and uses the already supplied resident BGE encoder. It wraps only the **stateless read callback** in `ReadHandlerSlot`, with a pinned context of database generation, schema/function/executor revisions, model fingerprint, dependency fingerprint and query codec.
- A *trusted internal maintenance caller* can stage an already delivered **source-reviewed**, byte-pinned Python module from a private 0700 direct-child directory. It opens the module via no-follow descriptors, reads and hashes a single exact byte sequence, compiles/executes those same bytes, constructs a candidate using retained context references, and tests it before activation. There is **no public code-upload, shell, executable RPC or arbitrary filesystem path interface**.
- Candidate compatibility prevents replacement of the existing pool acquire callable, tokenizer, telemetry collector or source-provenance policy. A bounded activation waits for in-flight read callbacks to finish, swaps a single reference, probes useful output before reopening dispatch and rolls back on failure/timeouts. The readback exposes generation, **verified bundle vs loaded module** SHA kind, current hash, revision/context fingerprints, active/read-drain state. Repeated activation is capped; exceeding the cap requires a controlled process restart.
- `kaggle_target_launch.MODULES` now includes the new module. The generated Kaggle notebook's manifest contains the module's exact bytes and digest. This fixes a packaging gap that a direct unit test would miss.

The wrapper holds neither the BGE encoder/model nor the PostgreSQL pool and invokes no model loader or installer during a callback swap. This is an **architectural property and fixture check**, not a live count of actual Kaggle model reloads/downloads.

## Bounded verification

`PYTHONPATH=src:tests pytest -q tests/test_kaggle_warm_read.py tests/test_kaggle_target_runtime.py tests/test_kaggle_launch_envelope.py`

Tests include source-file digest mismatch, symlink/nested-directory refusal, no second path-read after hash verification, relative module imports, pinned resource identity, in-flight drain, first-probe failure, bounded timeout and rollback, single PG pool lifetime, and exact Kaggle source-bundle inclusion.

The separate `tests/test_kaggle_source_bundle.py` subprocess isolation suite uses `sys.executable -I` to exclude user-installed packages. On this DevCoveer host `/usr/bin/python3 -I` lacks third-party `pydantic` and `psycopg`, so three independent baseline isolation tests abort on those imports, not on warm-handler content. Do not claim that suite passed on this host; repeat in the pinned project/CI interpreter.

## Required before live application / product sign-off

1. **Artifact transport + authority:** bind one exact approved code-artifact digest and version to an existing controller's authenticated maintenance operation, using an already supported scoped byte-transfer path. Merely publishing a file to Kaggle is not activation. Do not let a public `search`/MCP caller choose Python source, constructor or local path.
2. **Remote operation receipt/reconcile:** assign an exact operation ID, generation and dependency fingerprint, and retain the previous handler for lost-ACK reconciliation/rollback. Wire this through the existing WSS/maintenance owner; do not create a parallel controller or arbitrary remote shell. Keep durable mutation replay/SQL migration outside this stateless switch.
3. **Live readback:** verify loaded module digest/generation and an actual authorized `QUERY` after activation in the same run, compare model load count/download bytes and the exact PostgreSQL/model fingerprints before/after. A failed candidate or first probe must leave previous serving behavior and recovered accepted commands intact.
4. **Dataset/PG persistence:** independently demonstrate exact Kaggle Dataset base restore, native PostgreSQL readiness, compatible replay tail and weekly independent Yandex backup. This is a separate A3 acceptance, **not** delivered by the warm read handler.

No Kaggle launch, stop, native PostgreSQL cluster, BGE local installation, remote mutation or deployment is performed by this source patch.
