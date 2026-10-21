# RKB A2c — readback of actual PG and resident BGE identity

**Source only / not yet live accepted (2026-10-21).** A2c is a narrow,
separate follow-on from reviewed A2 PR #185. Its purpose is to give the
existing owner-side warm acceptance driver observations it can compare
*before and after* a patch. No replacement model loader, PG pool, WSS
connection, provider client or artifact transfer is introduced.

## Source of truth and security

The existing run-authenticated, compute-permit-bound
`WssSnapshotPhases.read_handler_status(binding)` and
`WorkerRecoveryPhases` append `owned_runtime` to the already returned
loaded code SHA/generation. The status callback is attached by the **same**
`TargetRuntime` only after its resident PG+model were prepared; no caller
supplies a process ID, connection string, SQL statement, model counter,
artifact digest or local path on the WSS frame.

The callback obtains the following **actual owned observations**:

- `pg_system_identifier` and `pg_postmaster_started_at`, from a **fresh
  read-only transaction** on the existing RKB native socket using the
  retained `rkb_bootstrap` PostgreSQL owner identity and the fixed query
  `SELECT system_identifier::text,
  pg_postmaster_start_time()::text FROM pg_control_system()`. Both the whole
  operation (2 seconds) and server-side statement (1 second) have bounds.
  This opens one ephemeral admin connection, **not a new PG pool**, and
  does not create, restart or migrate PostgreSQL.
- `pg_process_pid` comes from the actually owned native process handle,
  including the retained recovery candidate on restored PG. If its PID or
  liveness is unknown, the PG observation reports `unavailable` rather than
  promoting a guessed identity.
- `resident_model_load_count` from the already loaded
  `ResidentEncoder.model_load_count`, validated against the retained
  `PinnedTokenizer.count.__self__` identity. It is NOT a prediction from
  the model SHA; the original BGE model loader increments this value.
- `process_model_load_count` from the generated notebook's actual
  `__main__._MODEL_LOAD_COUNT` counter, incremented by `_load_model()`.
  If missing or invalid, the whole owned observation is `unavailable`.
  If different from the bound resident value, the observation is
  `inconsistent`, not a valid warm-activation PASS.
- `resident_model_context_ref` is one opaque per-`TargetRuntime` process
  correlation marker; it cannot stand alone as a cryptographic identity,
  model-hash proof or provider-execution receipt.
- Actual `runtime_process_pid` and authenticated `runtime_incarnation` are
  read beside the PG identity; both must remain unchanged between the
  first and subsequent same-run observations.
- If the **top-level** `RUN_CONFIG.stage0` includes Stage0, the generated
  Kaggle worker preserves cumulative counters from the real
  `prepare_stage0` return:
  `stage0_actual_counters.invocation_count`, `downloaded_bytes`, and
  `install_count`. Its real prepare call already differentiates verified
  cached bytes from downloaded bytes, and an actual offline pip install
  from a no-op. The generated worker records the counters only upon
  successful Stage0 return. The independent generated-worker
  `_RKB_STAGE0_ATTEMPTS` is incremented **before** each call, while
  `invocation_count` counts only actual successfully completed calls.
  Any failed or cancelled call (including invalid missing measurements)
  sets a **sticky** `_RKB_STAGE0_INCOMPLETE=True` for this worker lifetime.
  The flag cannot be cleared by later successful calls, because an earlier
  partially applied download/install may have unmeasured side effects.
  Readback requires `incomplete is False`, a real positive attempt count,
  and `completed_invocation_count == _RKB_STAGE0_ATTEMPTS`; otherwise it
  returns fixed `stage0_runtime_counters_unavailable`. This rejects the
  sequence success #1 → partial failure #2 → success #3 instead of falsely
  treating #3 as proof all earlier effects were measured. Both counters
  and the sticky flag are checked **again after the awaitable PG identity
  probe**; a newly begun Stage0 attempt during PG I/O invalidates the
  entire observation. No separate download/install
  or reconnect loop is started by readback. Missing, malformed or uninitialized counters
  return `stage0_runtime_counters_unavailable`, **never zero**. Generated
  `compose_target` intentionally **removes `stage0` from `target_runtime`**:
  only the top-level field controls the encoder's Stage0 lifecycle. This
  source fix is exercised with the actual generated notebook RUN_CONFIG,
  not a fabricated nested Stage0 test object.
- The typed logical database/writer generation are included alongside the
  PG system/start IDs, not substituted for them.

If the PG query is unauthorized, times out or cannot verify its fixed
expected result, `owned_runtime` reports
`{status:unavailable,reason:owned_pg_observation_unavailable}` without
returning a SQL exception, DB password, private query text or source rows.
The existing WSS readback continues to return loaded code SHA; a
diagnostic failure **must not terminate the BGE/PG worker**. WSS
generation is rechecked after the await to prevent a stale owner from
reading the observation. **If the connection generation changes during
PG I/O**, `read_handler_status` returns only the fixed
`{status:unavailable,reason:runtime_readback_stale}` value, discarding
both the old loaded SHA and all PG/model/Stage0 metrics, while the existing
`BgeWssTransport._encode` still completes normally. This protects PG/BGE
from fatal diagnostic-only readback exceptions on WSS reconnect. A real
transport regression exercises the changed generation *during* the await.
Production useful search and current native capability checks are still
separate gates.

## Not-yet-observed instrumentation and acceptance

Stage0 `install_bytes` is only a **planned budget** and must not be used as
an observed download count. The *existing* `prepare_stage0` function
already returns actual `downloaded_bytes` and the boolean whether an offline
installation occurred; the generated notebook now retains those measured
values cumulatively. Its readback is therefore comparable before/after
patching, but **only for Stage0 actions actually executed in this process**.
An unknown or non-Stage0 installer must not be relabelled as zero.
The readback also does not prove a SQL migration was applied correctly;
the separate native SQL migration/readback owner and useful QUERY are still
required for that acceptance check.

Actual target acceptance remains a distinct owner-run E2E: readback the
same run/PG/model context → stage reviewed artifact → activate same slot →
readback new SHA and unchanged native PG identity/model counters → send a
**new authorized useful QUERY** with evidence IDs/text hashes → prove no
re-install/re-download/PG restart and retained acceptable command history.
No manual Kaggle Save/Stop or provider API changes occur in A2c.

## Tests

`PYTHONPATH=src:tests pytest -q
tests/test_kaggle_owned_runtime_readback.py
tests/test_kaggle_recovery_phases.py
tests/test_kaggle_target_runtime.py
tests/test_kaggle_generated_restore.py
tests/test_kaggle_warm_read.py`

Synthetic PG connection/counter tests are deliberately labelled as such;
they validate the **actual fixed SQL, identity binding and fail-closed
semantics**, not a live native PG 17 process. Additional tests cover current
WorkerRecoveryPhases fresh readback and its inability to turn an admin SQL
denial into fabricated counter values.
