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

**Two reviewed lifecycle fixes:** candidate download, stage or useful probe rejection now settles as a fixed, secret-free `RECOVERY_PHASE_RESULT` with `value={status:rejected,reason:read_handler_maintenance_rejected}` and receives the usual `RESULT_ACK`. The Dev maintenance caller converts that successful transport receipt to a bounded `SnapshotError`, but the notebook `BgeWssTransport._encode` does **not** raise/close the model or PG owner. Retrying the same operation key returns the identical retained rejection, never a second side effect. Unauthorized attachment errors *before* phase dispatch still fail closed at the transport boundary. The test uses real `BgeWssTransport._encode` and follows the rejection with a fresh healthy worker readback.

The warm slot registration is also order-independent: a single `TargetRuntime._bind_read_handler_maintenance` method is invoked both after `prepare` and after `configure_recovery`. `NEW_EMPTY_GENERATION` runs `prepare→configure_recovery`, whereas a restored owned database runs `configure_recovery→wait_initialized→prepare`. Both connect the same actual slot/PG/model once; no new model load, WSS endpoint, or code-upload mechanism. The regression exercises the **generated empty snapshot entrypoint** and the real method against a retained synthetic native worker.

## Bounded verification

`PYTHONPATH=src:tests pytest -q tests/test_kaggle_warm_read.py tests/test_kaggle_target_runtime.py tests/test_kaggle_launch_envelope.py`

Tests include source-file digest mismatch, symlink/nested-directory refusal, no second path-read after hash verification, relative module imports, pinned resource identity, in-flight drain, first-probe failure, bounded timeout and rollback, single PG pool lifetime, exact Kaggle source-bundle inclusion and existing owner-authenticated ASGI/WSS readback. For the readback extension use `PYTHONPATH=src:tests pytest -q tests/test_kaggle_recovery_phases.py tests/test_kaggle_generated_restore.py tests/test_kaggle_warm_read.py tests/test_kaggle_target_runtime.py tests/test_bge_shared_channel.py tests/test_kaggle_restore_startup.py tests/test_bge_channel.py`.

The separate `tests/test_kaggle_source_bundle.py` subprocess isolation suite uses `sys.executable -I` to exclude user-installed packages. On this DevCoveer host `/usr/bin/python3 -I` lacks third-party `pydantic` and `psycopg`, so three independent baseline isolation tests abort on those imports, not on warm-handler content. Do not claim that suite passed on this host; repeat in the pinned project/CI interpreter.


## Authenticated same-run stage → activate → readback → rollback

The **existing** `RecoveryPhaseDispatch/WssSnapshotPhases` now exposes the
bounded maintenance phases `read_handler_stage`,
`read_handler_activate`, `read_handler_rollback` and the previous
`read_handler_status`. These use the existing durable `recovery_phase`
document compute permit, exact current run/nonce/incarnation/WSS generation,
original idempotent operation key, source maintenance owner and durable
RESULT_ACK. There is **no new launcher, public MCP code upload, arbitrary
Python/SQL RPC, control-socket uploader or second scheduler**.

A trusted deployment/code-review operator selects an exact immutable source
artifact and its reviewed Git SHA. `WarmHandlerPatchApproval` validates their
canonical metadata but is *not itself cryptographic proof of Git review*;
approval **must** be established by the trusted controller/operator before it
calls `stage_read_handler`. Only `BlobRef` key/size/SHA/ETag and Git commit
identifier cross the WSS maintenance envelope. The existing scoped
`SnapshotTicketIssuer` issues a conditional, short-lived HTTPS GET for that
immutable restricted S3 blob. The existing `NativeSnapshotTransfer` checks
the ticket operation, allowed origin, ETag, exact size and SHA before
returning its exclusive `/kaggle/temp` file. There is no source text on
the WebSocket; the worker is capped to **512 KiB** for this patch class.

After the download, the worker **rechecks the exact WSS binding and loaded
generation**. It atomically moves the verified file inside a fresh private
0700 subdirectory; `ReadHandlerSlot.stage` opens O_NOFOLLOW, hashes its own
descriptor bytes, compiles those same bytes, constructs the candidate with
the already retained PG acquire/tokenizer/telemetry/source-provenance
references and executes a bounded invalid-query contract self-test.
Unsupported context, changed run, misplaced artifact and arbitrary local
path/constructor fields fail closed. No model weights or PostgreSQL process
are created by stage.

Activate rechecks the original loaded SHA and generation, drains active
read callbacks, switches one pointer, and probes the live native PostgreSQL
catalog/writer binding using the same pool, followed by the finite
invalid-query callback contract. Probe failure or WSS-generation revocation
**restores the old handler before reopening reads**. A fresh
`read_handler_status` then reads the actual new module SHA, generation,
database/model/dependency/revision pins and switching state, and the
operator must submit a **new authorized useful QUERY** in that same run for
product acceptance. A later functional failure can invoke the same durable
`rollback_read_handler`; an incompatible dependency/model/PG context
requires a controlled worker restart instead of forced activation.

An expected stage/probe rejection returns a typed **rejected maintenance receipt**, not an uncaught task exception that shuts down the warm model-owning worker. The native phase traps errors only for `read_handler_stage/activate/rollback` after authentic run/connection verification and persists the fixed `{status:rejected,reason:read_handler_maintenance_rejected}` result under the original operation key. The original `BgeWssTransport._encode` then sends a normal `RECOVERY_PHASE_RESULT` and receives `RESULT_ACK`; it does not call `close_runtime` for a bad candidate. The Dev caller raises the fixed `SnapshotError` **after** that durable ACK, with no arbitrary error text. Cancellation and an invalid transport/attachment *before* phase execution still fail closed. A repeated rejected operation returns its saved rejection rather than downloading/re-running the candidate; the previous handler and PG/BGE remain usable, including for a fresh loaded SHA readback. This is verified through actual `BgeWssTransport._encode` plus acknowledged WSS maintenance fixtures.

A single `TargetRuntime._bind_read_handler_maintenance` installs this control regardless of actual startup order. Under `NEW_EMPTY_GENERATION` the generated notebook executes `prepare → configure_recovery` so the control binds **after** preparation; for `VERIFIED_INITIALIZED` it executes `configure_recovery → wait_initialized → prepare` so it binds **after** the native PG proof. Repeated binding is idempotent and neither path recreates the resident PG pool or BGE. Both the actual `TargetRuntime` ordering and the generated fresh notebook composition are exercised by tests.

A lost stage/activation ACK is **not permission to issue a new patch key**
or restart Kaggle: the same operation is retained/cached; request a fresh
readback, reconcile SHA/generation, then retry only its original operation
when the existing safe reconciliation contract allows it. Presigned GET
URLs may be renewed for the same immutable object without replaying code.

**Source validation:** the test suite exercises the actual durable admission
and WSS DISPATCH → signed GET ticket → worker stage → activate → fresh
readback → RESULT_ACK, no duplicate transfer on repeated operation,
oversized/forged artifact refusal, staleness after a slow download, and
automatic rollback when WSS generation changes during the activation
probe. Both `kaggle_warm_read.py` and
`kaggle_warm_maintenance.py` are included in the exact generated notebook
source bundle and hashes.

**Not yet live:** no actually warmed Kaggle worker/PG/model-load counters,
signed artifact approval record, actual provider network transfer, new
authorized useful query after activation, or production rollback has been
run or verified here. All source tests are fixtures; a source-only PR is not
product acceptance. The running RKB recovery remains owned by Шкипер.

## Required before live application / product sign-off

1. **Independent approval + authentic transfer:** the trusted deployment owner must attest the reviewed commit+blob SHA and keep source approval out of any public RPC. Exercise the scoped signed GET against the actual provider and verify no external module body/path crosses the WSS control frame.
2. **Mutation receipt/reconcile:** verify exact retained operation/capability identity in the real model-owning worker with a lost WSS ACK and status readback, without new provider push; preserved durable accepted-command original replay remains independent of this stateless patch.
3. **Live post-activation evidence:** check loaded module SHA/generation and an actual authorized useful QUERY against source-backed PG, and compare BGE load count/download bytes and PostgreSQL/model fingerprints before/after. Source-only catalog probe and invalid-query self-test do not establish useful-result acceptance.
4. **Dataset/PG persistence:** independently demonstrate exact Kaggle Dataset base restore, native PostgreSQL readiness, compatible replay tail and weekly independent Yandex backup. This is a separate A3 acceptance, **not** delivered by the warm read handler.

No Kaggle launch, stop, native PostgreSQL cluster, BGE local installation, remote mutation or deployment is performed by this source patch.
