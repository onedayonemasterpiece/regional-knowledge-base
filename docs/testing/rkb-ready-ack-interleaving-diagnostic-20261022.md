# READY_ACK interleaving — diagnostic-only real WSS regression (2026-10-22)

**Status: DIAGNOSTIC SOURCE PREPARED; ACTUAL EXECUTION NOT RUN.** Expected RED is a hypothesis until an actual test receipt exists; this is not an approved production fix. This source-only artifact is **separate from PR220**; no existing test, queue, WSS, readiness, heartbeat handler, scheduler, CI or runtime code is edited. Do **not** add it to the default mandatory pytest suite or claim it is a PASS simply because PR220's ordinary CI passed. The native integrator is stopped following denials; no work on its blocked files is authorized.

## Source-level race (no claim of prior execution)

Current immutable production baseline: `bd138b2449f68cabb2cbfa3df552df8471f40c78`,
`src/regional_knowledge/bge_channel.py:209–238,372–390`:

1. Existing `dispatch()` has entered `promote()`, holds `promotion_lock`, and awaits the real `queue.accept_search_runtime(old_wrong_proof)` in a thread.
2. Existing `receiver()` handles a later **correct** `READY`, performs `runtime_readiness = frame['runtime_readiness']` and `runtime_blocked = None` **before** awaiting the same promotion lock.
3. The old dispatch result returns with `database_generation_mismatch`. Since `runtime_blocked` is now `None`, the old dispatch sends `READY_ACK(runtime_not_ready)`.
4. The receiver subsequently obtains the lock, validates its new proof and sends `READY_ACK(ready)`. The prior negative is obsolete and can be mistaken for the new READY's result.
5. `HEARTBEAT_ACK` (receiver's HEARTBEAT path) does not acquire `promotion_lock`; PR220's earlier HEARTBEAT barrier therefore does not forbid this schedule.

This ordering is **possible from source**, not automatically the root cause of PR208/PR218 historical RED. The diagnostic test must execute it to claim a real reproduction.

## Execution attempt and exact blocker

An authorized isolated test environment was not reachable in this run: the **read-only** `Codex_DevCoveer.project_probe` for the RKB repository returned **`UNAVAILABLE: McpServerError: Connection failed`** before any test launch, worktree switch, dependency installation or mutation. This is **not** an access-denied receipt, not a diagnostic test result and not an actual RED. No alternate executor, GitHub Actions workflow edit, Dev PG, model or provider was used to work around it. GitHub source commit/diff verifies the new test/doc files only. The selector below must be explicitly run by an owner-approved existing test environment before declaring reproduction.

## Explicit selector, not default CI

Diagnostic file:
`tests/diagnostics/ready_ack_interleaving_diagnostic.py`. Filename intentionally does not match `test_*.py`, so `pytest` under mandatory `ci.yml` **does not collect it**. Run only as an explicit separate synthetic WSS/SQLite-control test in an environment already authorized for these installed dependencies:

```bash
PYTHONPATH=src:tests python -m pytest -q -s \
  tests/diagnostics/ready_ack_interleaving_diagnostic.py
```

No PostgreSQL, resident BGE/model, Kaggle, Telegram, credentials, external provider, new runner or runtime action is necessary. Do **not** substitute a fake `promote()`, queue validator or WSS caller. The test uses the existing `shared_channel_run` synthetic fixture, ASGI `Starlette/TestClient`, actual `channel_route` and queue.

### Deterministic controls

- Bind a fresh WSS attachment; receive initial `WELCOME`, reconcile `RESOURCE_STATE idle` and use existing receiver-ordered `HEARTBEAT_ACK`.
- Submit model-only `READY` and wrong-database READY in sequence; prove both reject, with strict original reasons and `ready=False`.
- **After** those ACKs and a receiver-ordered HEARTBEAT barrier, attach a **test-instance-only delegate** around the real `queue.accept_search_runtime`. The delegate invokes **the original production method** then holds the *already computed second* wrong-proof result on a bounded `threading.Event`. Because the wrong-proof receiver has already returned, the new invocation belongs to unchanged independent `dispatch()`; it still owns `promotion_lock`.
- Send the new correct READY. Test-scoped CPython line-tracing in the TestClient portal thread observes the actual receiver executing the line **after** the two nonlocal assignments `runtime_readiness=valid` and `runtime_blocked=None`; it signals a bounded Event. The observer only watches genuine interpreter line events and does not replace or patch any production function. No arbitrary sleeps or probabilistic scheduling.
- Release the old validation return; observe the actual WebSocket frames. If an obsolete negative arrives before the positive, assert **RED** with the observed precise ordered statuses `['runtime_not_ready','ready']` and original `database_generation_mismatch`. If a future reviewed owner production change suppresses that negative while preserving the strict first two refusals, the same diagnostic would naturally PASS.
- Always release the test barrier, restore the per-instance delegate and remove test tracing on teardown. No production source or shared operator state is modified.

### Minimal future owner correction contract (no patch here)

A reviewed native-integrator source fix must make each READY proof versioned/serialized with the promotion decision, including the reset of `runtime_blocked`, such that completion of an earlier proof **cannot emit a negative READY_ACK for a superseded generation/proof**. One valid outcome after an ordered newer READY; model-only and wrong-proof READY must remain denied, with no early search admission, no heartbeat-assisted proof bypass and no false `runtime_verified`. Re-test this exact selector and all existing WSS/queue tests; do not silently filter negative ACKs on the test client, add retry-to-green, or increase timeouts.

Source-only finding remains distinct from any historical CI failure until reproducibly executed and archived. A failing diagnostic is **not** reason to change blocked native-integrator files without renewed owner authorization.
