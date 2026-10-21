# RKB A2 — warm read-handler boundary (2026-10-21)

**Status: source/fixture only. Read-only WSS maintenance readback is implemented; actual patch activation, production rollout and complete DBG-1 acceptance remain NOT RUN.** This change is isolated from frozen PR #170 (cold-demand/MCP) and from the import/media and expert workflow executors.

## Precisely implemented

- The existing `TargetRuntime.prepare` still initializes the one PostgreSQL pool and uses the already supplied resident BGE encoder. It wraps only the **stateless read callback** in `ReadHandlerSlot`, with a pinned context of database generation, schema/function/executor revisions, model fingerprint, dependency fingerprint and query codec.
- A *trusted internal maintenance caller* can stage an already delivered **source-reviewed**, byte-pinned Python module from a private 0700 direct-child directory. It opens the module via no-follow descriptors, reads and hashes a single exact byte sequence, compiles/executes those same bytes, constructs a candidate using retained context references, and tests it before activation. There is **no public code-upload, shell, executable RPC or arbitrary filesystem path interface**.
- Candidate compatibility prevents replacement of the existing pool acquire callable, tokenizer, telemetry collector or source-provenance policy. A bounded activation waits for in-flight read callbacks to finish, swaps a single reference, probes useful output before reopening dispatch and rolls back on failure/timeouts. The readback exposes generation, **verified bundle vs loaded module** SHA kind, current hash, revision/context fingerprints, active/read-drain state. Repeated activation is capped; exceeding the cap requires a controlled process restart.
- `kaggle_target_launch.MODULES` now includes the new module. The generated Kaggle notebook's manifest contains the module's exact bytes and digest. This fixes a packaging gap that a direct unit test would miss.

The wrapper holds neither the BGE encoder/model nor the PostgreSQL pool and invokes no model loader or installer during a callback swap. This is an **architectural property and fixture check**, not a live count of actual Kaggle model reloads/downloads.

## Same-session WSS loaded-version readback (source-only)

The existing authenticated `RecoveryPhaseDispatch` + `WssSnapshotPhases` now has a **read-only** `read_handler_status(binding)` operation. It uses the existing durable `recovery_phase` job, compute permit, exact run/nonce/incarnation, current connection generation and ACK/reconciliation. Every poll has a fresh operation key, so a previously cached receipt cannot masquerade as the current loaded version. The request arguments must be **exactly empty**; no caller controls a Python path, module bytes, constructor, code evaluation or model/PG resource handle.

After `TargetRuntime.prepare` has completed, the same `WorkerRecoveryPhases` holds a reference to the already created in-process `ReadHandlerSlot`. Before preparation, the readback fails closed. The response exposes only the slot's loaded SHA256, code origin (`verified_bundle` or `module`), activation generation, pinned database/model/dependency/revision fingerprints, in-flight count and switching flag. A readback is not a command to stage or activate an update. The normal WSS provider and exactly one PG/BGE runtime remain authoritative.

The corresponding fixtures exercise direct worker-side exact run binding, absence before preparation, refusal of unexpected module-path arguments, distinct polls before/after a real in-process activation, end-to-end durable admission, and actual ASGI/WebSocket DISPATCH→RECOVERY_PHASE_RESULT→ACK. They make **no Kaggle provider calls**.

## Bounded verification

`PYTHONPATH=src:tests pytest -q tests/test_kaggle_warm_read.py tests/test_kaggle_target_runtime.py tests/test_kaggle_launch_envelope.py`

Tests include source-file digest mismatch, symlink/nested-directory refusal, no second path-read after hash verification, relative module imports, pinned resource identity, in-flight drain, first-probe failure, bounded timeout and rollback, single PG pool lifetime, exact Kaggle source-bundle inclusion and existing owner-authenticated ASGI/WSS readback. For the readback extension use `PYTHONPATH=src:tests pytest -q tests/test_kaggle_recovery_phases.py tests/test_kaggle_generated_restore.py tests/test_kaggle_warm_read.py tests/test_kaggle_target_runtime.py tests/test_bge_shared_channel.py tests/test_kaggle_restore_startup.py tests/test_bge_channel.py`.

The separate `tests/test_kaggle_source_bundle.py` subprocess isolation suite uses `sys.executable -I` to exclude user-installed packages. On this DevCoveer host `/usr/bin/python3 -I` lacks third-party `pydantic` and `psycopg`, so three independent baseline isolation tests abort on those imports, not on warm-handler content. Do not claim that suite passed on this host; repeat in the pinned project/CI interpreter.

## Required before live application / product sign-off

1. **Artifact transport + authority:** bind one exact approved code-artifact digest and version to an existing controller's authenticated maintenance operation, using an already supported scoped byte-transfer path. Merely publishing a file to Kaggle is not activation. Do not let a public `search`/MCP caller choose Python source, constructor or local path.
2. **Mutation receipt/reconcile:** extend the existing maintenance owner with an exact staged-code operation ID/generation/dependency fingerprint and retained prior handler for lost-ACK reconciliation/rollback. The existing read-only WSS status phase does **not** authorize changing code. Do not create a parallel controller or arbitrary remote shell. Keep durable mutation replay/SQL migration outside this stateless switch.
3. **Live post-activation evidence:** invoke the now source-implemented loaded-version status phase against the actual warmed Kaggle run, verify loaded module digest/generation and an actual authorized `QUERY` after activation, and compare model load count/download bytes and exact PostgreSQL/model fingerprints before/after. A failed candidate or first probe must leave previous serving behavior and accepted commands intact.
4. **Dataset/PG persistence:** independently demonstrate exact Kaggle Dataset base restore, native PostgreSQL readiness, compatible replay tail and weekly independent Yandex backup. This is a separate A3 acceptance, **not** delivered by the warm read handler.

No Kaggle launch, stop, native PostgreSQL cluster, BGE local installation, remote mutation or deployment is performed by this source patch.
