# Identity, results and scoped Stop

Use this reference for lost ACKs, unknown launches, status/output disagreement, or cleanup.

## One operation, several observations

Retain stable notebook slug and distinct provider version, launch nonce, runtime incarnation, task/run ID, operation ID and writer/executor generation where available. Do not substitute kernel ID, notebook version or slug for a provider execution/session ID.

Before a consequential dispatch, persist its exact intent and idempotency identity using the existing controller's store. A retry with the same identity must reconcile the original effect, not create a replacement run. A source lookup exception means unknown, not absent. A newer unmatched slug revision is a conflict/uncertainty barrier, not the old operation's result.

Capability evidence should identify the actual package version and adapter path. Prefer supported version-qualified APIs; if the selected SDK only resolves current slug, use identity checks and attested output to reject wrong results. Such checks do not make an old execution's provider status observable. Check for a race between separate metadata, status and download calls.

## Results and logs

Declare required outputs before the operation. Read back each required artifact's identity, bytes, size and hash and any semantic checks needed for the task. The manifest is evidence to compare, not self-authenticating proof of completeness. An allowed optional artifact being absent is different from a missing required result.

Use an anchored expected filename or version-qualified output when supported. Traverse all needed pages, or rely on the installed SDK's verified automatic traversal. Absence of an explicit loop in the caller alone does not prove lost pages. A selective single-file receipt is not proof that every provider output was retrieved.

Keep these distinct:
- computation finished;
- side effect committed;
- result artifact complete;
- result transferred;
- terminal receipt delivered;
- resource cleanup completed.

Failure after publication or a database commit must not rerun the side effect. Recover delivery or reconcile its receipt instead. COMPLETE without the required artifact is not successful completion.

For warm debugging, use existing WSS/RPC events with exact identity, monotonic sequence/cursor, reconnect replay and bounded bytes. Include a truncation/gap marker when diagnostics were discarded. Preserve operation/terminal evidence according to the project recovery contract. A fsynced JSONL spool alone supplies neither a bounded ring buffer nor remote durability. Poll only to reconcile missing/ambiguous observations, with bounded time/backoff and no blind resubmission.

## Stop and cleanup

A scoped Stop names the exact operation/run/incarnation. Distinguish requested, acknowledged, terminal and UNKNOWN. Neither notebook deletion/inventory absence nor WSS loss nor lease TTL proves the compute session died. An upstream method name alone is not a working stop mechanism: obtain exact execution identity, supported stop behavior and same-execution terminal readback.

For an executor-local stop, the supervisor must own and reap its children, drain bounded output pipes and verify that owned descendants exited. Prohibit unmanaged daemonization rather than assuming process-group signals catch detached descendants. Do not put resident PG/model or unrelated work into that executor's kill group.

Stop PostgreSQL using its own bounded shutdown procedure. A pg_ctl timeout can precede eventual success, so reconcile PID/start identity, socket and database identity before any new Start. Do not delete postmaster.pid or spawn a second PG merely because an ACK was lost. Smart, fast and immediate shutdown have different effects; killing the postmaster is not reliable orderly child cleanup.

A revoked Telegram authorization may prove that an old worker can no longer use that session; it does not prove compute termination. Do not generate or revoke credentials merely because this reference describes that distinction. Shared-session handoff requires the project's terminal/ownership proof or explicitly authorized, independently verified revocation. Preserve UNKNOWN otherwise.

References: [PG pg_ctl](https://www.postgresql.org/docs/17/app-pg-ctl.html), [PG shutdown](https://www.postgresql.org/docs/17/server-shutdown.html), [Kaggle issue 1169](https://github.com/Kaggle/kaggle-cli/issues/1169). The issue is a reporter's observation, not a provider guarantee.
