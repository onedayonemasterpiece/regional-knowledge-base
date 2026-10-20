---
name: kaggle-warm-debugging
description: Debug an existing Kaggle-backed service with short warm-session iterations, identity-bound status and result reconciliation, and safe executor or PostgreSQL changes. Use for Kaggle runtime debugging, migration planning, or recovery from uncertain operations, not ordinary notebook analysis.
---

# Kaggle warm debugging

Prefer a small verified change in the existing session over repeated notebook starts. This is a workflow, not an implementation of remote execution and not permission to launch, stop, migrate, install, or delete anything. In analysis-only work, inspect code and saved evidence; leave runtime gates NOT RUN.

## Establish the actual contract

Read the project's handoff and current runtime receipts. Record only what matters for this change:
- Stable notebook owner/slug; exact provider version, run/operation ID, launch nonce and runtime incarnation. A slug is a versioned container, not an execution identity. Do not create a new slug for each iteration.
- Deployed source/launch-configuration hash separately from the executor bundle hash, schema/function revision and writer generation.
- Actual installed CLI/SDK versions and supported status, version-qualified output, pagination and stop capabilities. Repository pins, historical validation and latest upstream source are different evidence.
- Existing controller, WSS/RPC channel, resource budget, ownership leases and recovery contract. Reuse them; do not add a second controller or general remote-shell framework merely to debug.

Do not probe a mutating capability to discover whether it exists without authorization. Unsupported or unproved capabilities remain UNKNOWN and block only operations that need them. In particular, do not invent a cancel API.

## Choose the smallest useful iteration

1. Capture the failing operation's exact identity and narrow evidence before changing anything. Separate computation, side effects, result transfer and status projection.
2. Use a small disposable fixture for deterministic local checks when project resources permit. Do not silently download the full corpus or load the model locally. A disposable fixture does not need a second live database and replication stack.
3. Choose the affected boundary:
   - SQL or stored-function change: an ordered migration on the existing PostgreSQL instance. Read [migrations.md](references/migrations.md).
   - Stateless read-handler change: stage an immutable checked bundle, test it, switch generation at a safe boundary, retain the old handler if activation fails. Do not hot-swap mutation/replay semantics as ordinary code.
   - Stateful executor change: drain or reconcile accepted work, then replace only its owned process boundary under the existing supervisor.
   - Model/runtime change: reload that model process if required. Keeping BGE resident across replacement of the process that owns it requires a separate surviving model process and compatible bounded interface. A stateless read-handler swap within an unchanged model context does not require separating the model into another process; an in-process model cannot survive replacement of its owning process.
   - Lost environment or incompatible bootstrap/base dependencies: controlled recovery/new version only when necessary, not as a fallback for a missing status.
4. Read back the applied bundle/schema generation and run the smallest relevant regression. Broaden checks when shared invariants or release acceptance are affected. Do not run Restart + Run All or publish a Dataset after every edit.
5. Return a short receipt: old/new identities, changed boundary, actual checks, observed result, unresolved uncertainty and next action. Do not translate a local check into deployed or product acceptance.

Datasets are versioned snapshots/checkpoints or occasional large-state transfers, not the transport for every patch. Do not replace live PGDATA. A state switch uses an explicitly planned maintenance/recovery boundary, with resource/compatibility checks and preserved accepted writes. Do not assume updating a Dataset refreshes an already-mounted reader. For temporary Datasets or large file transport, read [transfers-and-cleanup.md](references/transfers-and-cleanup.md): prefer direct data transfer and retire temporary quota consumers when their actual dependencies end.

## Handle uncertain outcomes without duplicate work

Read [lifecycle.md](references/lifecycle.md) when a push, update, commit, download or Stop has an uncertain outcome.

- Preserve the original operation identity. Lost ACK, status timeout, empty download and WSS disconnect do not prove that execution or side effects failed.
- Reconcile exact operation/version/result before retry. If a newer version occupies the same slug, do not attribute its status or files to the older run.
- Validate result bytes and expected required artifacts independently of COMPLETE, heartbeat or a manifest. Prefer the installed SDK's proven pagination rather than duplicating it.
- Use bounded event/log tails with sequence/cursor and explicit gaps. Keep durable operation receipts separate from disposable diagnostics. WSS events are the fast path; bounded provider polling repairs missing observations, not every workflow stage.
- Stop request, cancellation acknowledgment, resource deletion, credential revocation and compute termination are distinct outcomes. Do not release a shared session merely because its TTL expired.

## Project-specific limits

Take credentials, concurrency caps, data retention, Saved/accepted semantics and allowed tests from the current project contract, not this skill. For RKB, read [rkb-profile.md](references/rkb-profile.md); its Telegram restrictions are not universal Kaggle requirements.

## Evidence and scope

[Evidence map](references/evidence.md) records dated lessons and reusable code boundaries. Use it to avoid known mistakes, then check the actual selected versions. Existing examples do not establish a generic hot-update or remote-stop capability. Do not claim performance improvement without comparable measured results.
