# Version-qualified provider observation

The installed Kaggle 2.2.4 `kernels_status()` parses a requested version but does
not populate the native SDK request's `version_label`. Calling it with a version
therefore still observes the latest slug. The compatibility controller now uses
the existing SDK's `ApiGetKernelSessionStatusRequest` with an explicit `v<N>`
label and rejects unbound versions or an unsupported request shape. It never
falls back silently to latest-by-slug status.

During Stage 0, kagglesdk 0.1.37 accepted a version-qualified read with `v<N>`;
the corresponding bare numeric label returned 404. No launch, stop or deletion
was performed. This agrees with the upstream CLI's
[`_kernel_version_label`](https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/api/kaggle_api_extended.py)
normalization. Provider errors are observations, not proof of absent compute.

The response exposes a phase, not an independently verified execution identity.
A version-qualified COMPLETE is not alone permission to release a Telegram
lease or declare a scoped Stop complete. Apply the separate execution-binding,
freshness and contradictory-evidence gates in `kaggle_readiness` before target
runtime ownership/termination acceptance. These helpers do not repair all
legacy launch/expiry semantics or certify target serving readiness.

Launch failures now retain the fixed boundary, exception type and bounded
numeric HTTP/provider codes, with a fixed source-size reason when supplied by
the provider. URLs, response messages, credentials and request bodies are never
logged. A local preparation failure before `kernels_push` is known not sent and
can be marked failed. Any exception or invalid receipt after calling that
boundary stays unknown and must be reconciled; logging an error does not
authorize a second push.

## Exact saved-session persisted-log retirement

The native read adapter in `regional_knowledge.kaggle_provider_terminal` pins
`kaggle==2.2.4` and `kagglesdk==0.1.37`. The inspected SDK's
`kernels/services/kernels_api_service.py`, `get_kernel_session_logs_stream`,
documents live SSE while a session runs and a persisted `application/json` log
fallback after session termination. The CLI's `kernels_logs_stream` and
`_iter_blob_lines` describe the same completed-session fallback. Primary sources:
[SDK service](https://github.com/Kaggle/kagglesdk/blob/main/kagglesdk/kernels/services/kernels_api_service.py),
[CLI implementation](https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/api/kaggle_api_extended.py).
These upstream links are navigation; the adapter supports the inspected package
versions only, not arbitrary future installations.

For a **previously bound saved version**, the controller requests source, native
status, session log stream, then source again, each with the identical explicit
`version_label=v<N>`. Source must contain the exact stored RUN_CONFIG run ID and
launch nonce (including an absent nonce for legacy HTTP), metadata must identify
the stored owner/slug, and both source hashes must match. Native status must be
ERROR or COMPLETE. The *stream* response must be HTTP 200 with JSON content type
and a nonempty array of valid native `{stream_name,time,data}` records. Ordinary
output/download JSON, error envelopes, empty logs, SSE, EOF and cancellation ACKs
are not proof. Missing/invalid/oversized evidence leaves UNKNOWN.

The adapter bounds logs to 256 KiB and each source response to 2 MiB (observation
budgets, not book limits). It wraps only its fresh pinned SDK client's send
boundary to bound response bytes **before SDK JSON decoding** and applies short
connect/read timeouts plus a POSIX main-thread ITIMER_REAL deadline spanning the
entire observation. The alarm interrupts headers or buffered body reads even if
bytes keep arriving below the inactivity timeout. Unsupported threads/platforms,
a blocked SIGALRM, or an already occupied alarm fail closed before network I/O.
The prior handler is restored and this timer disarmed on all exits. No background
reader thread/process survives a timeout. The wrapper invokes the session's
selected HTTPAdapter.send directly with the SDK-prepared/authenticated request,
not Session.send: Requests may consume redirect bodies while preparing a next
request even when redirect following is disabled. All 3xx responses are closed
and rejected before reading their body; Location is never followed. It closes responses
and restores the client's send method on every exit. Requests that exceed the
budget fail closed. Begin/end wall times must be finite and ordered, beginning
after the stored heartbeat; monotonic time bounds collection. Retirement requires
proof no older than 30 seconds and an unchanged run/version/nonce/incarnation,
connection generation, channel state and heartbeat under the same transaction.
Only a persisted already-expired historical row can be selected or retired.
Stale READY/starting/draining and attachment-conflict rows remain on the existing
live reconciliation path; elapsed lease time does not expand this adapter scope.

This is provider-managed *saved-session termination*, not independent VM
forensics or closure of other interactive/batch sessions at that notebook.
[Kaggle documents separate sessions and concurrent batch runs](https://www.kaggle.com/docs/efficient-gpu-usage).
No numeric version is treated as a session ID. Status alone remains insufficient.
Package/shape mismatch, source mismatch and 404 never establish non-creation.
In particular old10893, with no bound provider version, remains UNKNOWN.

Only the internal adapter mints proof; no MCP/operator boolean can mark a row
terminal. `BgeQueue.retire_provider_reservation` persists a compact immutable
receipt and retires that exact reservation atomically, including known-version
rows that never attached, without inventing an incarnation. It does **not**
modify jobs, control-spool operations, accepted inputs/history, model decisions,
apply/outcome state, writer ownership or publication intents. Accepted GEO seq1
with apply SENT_UNKNOWN / outcome NOT_SENT and source_history recovery remain
unresolved. Current/successor pointers stay in place when a durable claimed
command/recovery or writer publication is unresolved; unrelated pointers and
shared owners are never cleared. The adapter never writes status=expired, so it
cannot collide with the real rkb_writer_run_fence publication trigger to force a
live row into retirement. After closure, maintenance and idempotent retirement
revisit at most the current/successor pointers under the same domain guards.
Once the existing finite lease/recovery machinery requeues a claim, or native
writer reconciliation settles a publication, these pointers can advance and
preserve durable demand. This finalizer does not settle or replay domain work. Compute closure is not Saved, SQL rollback,
delivery completion or authority to replay an accepted choice.

A surviving shared-admission process releases sent RAM debt on an attachment
change only after an indexed read of the exact old incarnation's persisted native
terminal receipt. Same-run reads do not acquire a new receipt check or writer lock.

The existing controller visits at most two history rows per iteration through a
durable SQLite rowid cursor. UNKNOWN/unmapped rows are skipped, not cleared;
failures cannot starve later known versions. Exhausted cycles back off 30 seconds.
A crash before cursor advancement repeats a read; a committed terminal receipt
is idempotent across retries/restarts. The main loop no longer duplicates the
history observer with latest-source terminal probes. Healthy successor admission
and the stable-slug no-duplicate-push fence are unchanged. The separate historical
status predicate work is not included here.

Validation is synthetic: native request selectors/response modes, invalid bodies,
freshness/CAS, 101 mixed historical rows, cross-process RAM-debt reconciliation,
retry/restart, real writer publication triggers and native settlement, finite
claim-lease requeue/pointer liveness, actual loopback HTTP slow-drip/redirect
responses, unresolved accepted seq1/source_history preservation and existing
queue/admission regressions. No live provider, model, runtime database, Stop,
launch, rollout or product acceptance is implied.


## Development continuation

This service is still being built. Ordinary owner-directed development launches
need no additional recovery approval. If expired historical reservations prevent
progress, the local operator may call `BgeQueue.prepare_debug_launch` for one
exact pending candidate and stable notebook reference. It records the exact
fenced historical identities and a bounded lease. `launch_claim` consumes that
intent in the same transaction as the provider dispatch claim. New reservations,
identity changes, active old channels, claimed jobs, active writer or unsettled
publication prevent that claim. No public RPC or automatic enrollment exists.

Old outcomes remain UNKNOWN with their original rows and accepted work. A lost
Save response reconciles the same candidate; the consumed intent cannot fund a
replacement run or an automatic retry. Individual native terminal receipts remain
the only evidence that an old provider execution terminated. The ordinary
all-history guard remains in effect without this explicitly recorded dev intent.
