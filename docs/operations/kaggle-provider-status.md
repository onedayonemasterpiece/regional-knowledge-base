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

For a stored owner/slug/version, native ERROR, COMPLETE and
CANCEL_ACKNOWLEDGED retire the compute reservation. This is the coordinator's
2026-10-24 operational decision: writer and WSS generation fences remain the
write authority. A caller's Stop request or an unqualified latest-slug status
is not a terminal observation. Compute retirement does not settle domain work
or prove independent VM shutdown.

Launch failures now retain the fixed boundary, exception type and bounded
numeric HTTP/provider codes, with a fixed source-size reason when supplied by
the provider. URLs, response messages, credentials and request bodies are never
logged. A local preparation failure before `kernels_push` is known not sent and
can be marked failed. Any exception or invalid receipt after calling that
boundary stays unknown and must be reconciled; logging an error does not
authorize a second push.

## Exact-version status retirement

`regional_knowledge.kaggle_provider_terminal` requests only
`ApiGetKernelSessionStatusRequest` with the stored owner, slug and explicit
`version_label=v<N>`. ERROR, COMPLETE and CANCEL_ACKNOWLEDGED are terminal;
RUNNING, QUEUED, CANCEL_REQUESTED and unknown phases retain the reservation.
Missing versions, unsupported version selectors and provider failures remain
UNKNOWN. There is no latest-version fallback or guessed session ID.

Installed package version numbers, source downloads and log availability are
not prerequisites. In particular an unavailable or empty persisted log cannot
block an otherwise exact terminal observation. Logs remain diagnostics and are
never requested by this observer. The existing receipt identifier is preserved
so already persisted receipts remain readable; new receipts contain identity,
status and observation timestamps, without invented source/log hashes.

The native status response is bounded to 16 KiB before SDK decoding. The
existing send wrapper closes responses, rejects redirects without reading their
body, disables verbose payload logging and restores the client's transport on
exit. Short connection/read timeouts and a 12-second total observation deadline
bound provider latency. Unsupported signal/thread contexts fail before I/O.
The fresh transactional CAS still requires an observation at most 30 seconds
old and the same run, provider version, nonce, incarnation, connection generation,
channel state and heartbeat. Attachment conflicts and unsettled publication
still prevent retirement.

These checks describe saved-version status and reservation bookkeeping, not
independent host forensics or closure of other interactive sessions at the slug.
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

The automatic controller observes at most two current owners per iteration.
Detached launch history is not probed automatically and does not gate launch.
An explicit bounded historical diagnostic sweep can still use the durable SQLite
rowid cursor: UNKNOWN/unmapped rows are retained, exhausted cycles back off for
30 seconds, and a crash repeats only a safe read. Native terminal receipts stay
idempotent across retries. Healthy nominated successor admission and one-shot
claiming of the current request remain in place.

Validation covers version-qualified terminal and nonterminal phases, unavailable
source/log endpoints, unsupported selectors, provider errors, freshness and
identity CAS, cross-process RAM-debt reconciliation, idempotent receipts,
publication/claim preservation and bounded real HTTP transport. No local test
implies a live launch, restore, Saved checkpoint or successful product read.



## Development launch scope (owner clarification 2026-10-21)

Archived run history is diagnostic. Old UNKNOWN rows alone do not block a new
ordinary development launch and need no exception registry. Only the current
request, current/successor owner, shared compute owner and active writer/publication
retain launch authority. Claiming the current pending request is atomic and
one-shot; an unknown Save response is reconciled for that same request, never
blindly submitted again. Accepted data, writer fences and original historical
outcomes remain intact. Automatic reconciliation observes current owners; a
bounded historical sweep remains an explicit diagnostic operation.


Physical restore reconciles retained local source jobs only after native tail
replay, exact remote outcome persistence and final initialized-database verification.
The original accepted bytes, actor, writer identity and attempt remain unchanged;
the stored apply/outcome receipts settle the job before ordinary dispatch resumes.
An unreconciled job must not be redispatched with its historical writer on the
new executor. Native terminal logs diagnose channel closure; a generic
`revoked_run` channel reason is not evidence that the bearer token was revoked.

The owner policy dated 2026-10-23 starts the existing successor warmup at 8 h 15 min, with handover and confirmed native stop required before nine hours. This threshold schedules warmup; actual handover continuity, writer pause duration and predecessor stop time require a live receipt. STOP remains the existing planned-handover work, not an additional cancellation mechanism.
