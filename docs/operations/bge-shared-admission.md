# Shared ephemeral read admission

This is an opt-in controller unit, not deployed product wiring. The default
`BgeQueue` HTTP/durable-WSS behavior remains unchanged. Target startup calls
`queue.configure_shared_reads(existing_read_authority)` once, outside the ready
read path. It acquires a nonblocking process flock on the canonical queue path
and persists an enable fence. A second instance/process cannot own RAM admission;
even instances opened before opt-in must reject direct legacy claims, persisted
QUERY intake, and attachment/heartbeat changes. A closed controller leaves that
fence and its resource owner intact. The queue directory and canonical database
path must be shared by every controller; separate database copies are separate
capacities and are not a supported multiple-ASGI-worker arrangement.

The helper is `queue.shared_reads`. It owns bounded RAM consumers and one actual
compute slot for QUERY and existing durable document jobs. It has no transport,
model, second dispatcher, cloud journal, or user identity registry. Ready reads
use current authority reads and bounded queue/attachment snapshots; they never
take a SQLite writer lock or persist a row, fairness counter, result, or receipt.

## Controller calls

All async/sync consumer methods run on one controller event loop. SQLite and
participant checks execute in worker threads. Startup/close are lifecycle calls.

```python
admission = queue.configure_shared_reads(read_authority,
    max_reads=256, max_person_reads=8, max_dialog_reads=2, max_read_streak=8)

ticket = await admission.begin_read(verified_authority, typed_query,
    dialog_ref=trusted_dialog_ref, deadline=absolute_monotonic_deadline)
result = await ticket.result
admission.cancel_read(ticket)  # also invoked when the future is cancelled
```

Only server-resolved, identical `VerifiedReadAuthority` objects authenticate a
read. The QUERY projection must match person, context reference, auth revision,
and trusted destination exactly. The adapter supplies the dialog reference and
fresh unique request ID, never public tool arguments; request IDs must not be
reused within a connection generation. The helper checks the current socket
generation, original turn generation, minimum sequence, and the earlier of the
absolute deadline and QUERY's remaining budget. Absolute remaining time cannot
exceed two seconds. At most `max_reads` consumers are retained, including sent
reads awaiting output. The default pending RAM caps are eight per canonical
person across all verified routes and two per trusted dialog within that person.
`AdmissionLimit.code` identifies `read_capacity`, `person_read_capacity`, or
`dialog_read_capacity`; these are configurable RAM bounds, not model quotas or
evidence of the four-user product capacity gate. The adapter releases an unused
ReadAuthority context on rejection. Parent-owned internal subqueries share the
parent ticket/accounting; do not admit a second user turn for each child.
Deadline/cancel removes a consumer and its pending RAM quota without releasing
actual compute. Results still need the separate source/withdrawal/output gate.

The existing WSS dispatch loop calls:

```python
work = await queue.next_channel_work(run_id, token, current_generation)
if work is None:
    await queue.wait_channel_work(.25)
```

An idle free slot immediately reserves one available item; there is no batch
fill wait. Fair selection chooses the least recently served canonical person,
then dialog. Multiple provider identities or dialogs cannot multiply that
person's share. When both classes remain eligible, at most `max_read_streak`
consecutive read reservations precede a document reservation; oldest documents
get the forced turn. Startup adds the `(state,kind,actor,created,id)` index.
Selection seeks one oldest pending head for each registered trusted canonical
document actor (at most 256), never the completed journal or an actor's whole
pending tail. A long legacy tail cannot hide another eligible actor. Read fairness state is
RAM only and may reset on process restart.

`ChannelWork.resource` contains `run_id`, `runtime_incarnation`, `resource_kind`
(`read` or `document`), `resource_id`, `compute_claim`, and original
`dispatch_generation`. Its `query` is the typed QUERY with remaining time
deducted; compatible protocol versions also receive its controller-owned
`compute_claim`. Its `document_payload` is a read-only mapping with the existing
durable claim/text/space payload. The root WSS encoder adds compute claim and
dispatch generation to DISPATCH. QUERY is still QUERY, with no new wrapper.

The sender must retain its immediate current-attachment check before delivery.
Call `admission.mark_work_sent(work)` **before** attempting socket send. An
uncertain send holds the slot. `discard_unsent(work)` is valid only when no
network send was attempted. It refuses a potentially sent reservation. The
sender must discard a reserved read if `mark_work_sent` rejects its canceled or
expired consumer. `await admission.finish_read(run_id, token,
current_generation, typed_result)` can deliver only a sent, current, live
consumer with exact request/turn/generation and sufficient observed cursors.
It never releases compute. RESULT for a document still uses `queue.complete`;
that transaction also never releases compute.
The channel calls `admission.durable_result_settled(job_id, durable_claim,
status)` after the existing `queue.complete` outcome (`accepted`, `duplicate`,
or `stale`). A document RELEASED arriving first creates one separate bounded
result debt for its still-live durable claim. This blocks replay/new document
assignments until exact settlement or a read-only observation of done/reassigned/
expired claim, while available reads may use the free actual compute slot.
Retained RELEASED restores that debt after restart. A fresh idle probe without
an actual RELEASED creates no result debt, so a never-received document dispatch
can retry its original durable assignment.
`admission.read_consumer_active(work)` is a bounded RAM observation for the
existing channel to send CANCEL_QUERY once a consumer disappears/expires. It
does not change resource ownership or create a new dispatch mechanism.

## Resource reconciliation

Startup begins UNKNOWN. Persisted ownership records only the owned run and
runtime incarnation at initial baseline, not each ephemeral claim. The current
authenticated attachment reports `resource_state(...,
runtime_incarnation=..., state='idle'|'busy', resource_kind=...,
resource_id=..., compute_claim=..., dispatch_generation=...)`. Idle omits all
claim fields. Busy includes the exact actual claim and original dispatch
generation. A restart recovers busy debt, loses old ephemeral consumers, and
never replays QUERY. An initial idle baseline is allowed only for the owned
current run; thereafter another run/incarnation cannot erase old ownership.
Known competing incarnations retain the existing permanent attachment fence.

Actual completion calls `resource_released` with that full claim descriptor.
The **current** generation authenticates the control, while the original
generation and compute claim identify the slot. A reconnected current socket
can settle old compute; the original stale socket cannot. The exact latest
release is idempotent, including replay while a newer claim is active; it cannot
free that newer claim. Other wrong releases fail closed. After controller
restart, a retained release for the persisted owner is recorded without making
UNKNOWN idle: a same-owner baseline/probe is still required. Worker RESULT, timeout, cancellation, disconnect, lease
expiry, or run expiry are not release evidence. The worker retains its last
RELEASED descriptor across reconnect until a subsequent assignment acknowledges
progress. Ordinary idle cannot release a known busy claim.
`resource_released` returns True for a newly observed settlement and False for
an exact idempotent replay. Only a new settlement invalidates the outstanding
probe; the channel may renew that challenge once without a replay/probe loop.

A socket may lose a dispatch before the worker receives it. To reconcile that
case, `await admission.begin_resource_probe(run_id, token,
current_generation)` returns one unpredictable nonce. Send RESOURCE_PROBE on
that current socket. The worker's ordered reader must record work ownership
before scheduling compute, fence superseded readers, and reply RESOURCE_STATE
with `probe_nonce` and its actual current idle/busy state. Only that outstanding
nonce, same run/incarnation/generation, and unchanged slot version can reconcile
idle debt. A new reservation/send/release/discard or attachment invalidates the
probe. Queued ordinary idle frames, reused nonces, and a different run cannot
free a slot. This is a fresh actual-worker observation, not an expiry heuristic.
The worker replays a retained RELEASED before replying idle to a probe as well
as on reconnect, preserving the distinction between completed/pending-result
work and a dispatch it never received.

`await admission.confirm_terminal(expected_identity, capability_receipt,
terminal_observation, max_age_seconds=...)` is an internal alternative for a
lost resource. It binds stored run/incarnation/provider/version/nonce and uses
the existing exact native terminal validator, including fresh observation after
the last heartbeat. A valid proof expires the old run before clearing its owner
baseline. Stop ACK, deletion, missing status, and unverified adapter capabilities
leave UNKNOWN/debt intact. Caller-created boolean termination assertions are
not accepted.

## Existing durable authority and remaining integration

Trusted intake registers an existing internal `RequestAuthority` through
`await admission.register_document_actor(context)`, which verifies current
`Participants.require(context, 'import')`. Queue actor UUIDs alone do not grant
authority. The selected document rechecks the same canonical actor before the
existing guarded durable claim transaction. Missing/revoked contexts leave all
accepted rows and original identities intact. After restart, existing durable
intake must restore its accepted context references; this unit deliberately
does not create a durable authority registry or reinterpret token expiry as
loss of an accepted command. Unprojected legacy QUERY rows are also retained;
they are not target ephemeral requests and are not dispatched by this unit.

The existing `bge_channel` route now uses this helper when explicitly configured,
including typed QUERY results, exact resource controls and fresh reconnect
probes. Legacy durable encoding remains the default. A native loopback
ASGI/aiohttp test connects the real route/admission/resident transport/query
adapter to disposable PostgreSQL with stub inference. It verifies bounded
hybrid evidence, exact source revisions and release independent of result.
This is local synthetic integration, not a Kaggle/model or public MCP acceptance.

Server public admission/output gates and restoration of durable accepted-intake
authority remain pending. No provider, model, production database, grant, or
runtime changes were performed.
