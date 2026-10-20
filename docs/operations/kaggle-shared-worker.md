# Shared QUERY and durable worker transport

Source baseline: `4e3bd5a61fd44a144b00fab760ec29ae9346d263`. This unit extends the
existing outbound WSS worker without adding a dispatcher, model process, identity
authority or public listener. The repository instructions and installed Kaggle
skill/profile/migrations reference were read. Tests use synthetic wire peers and
stub synchronous encoders only. Model/provider/PG bootstrap and deployment remain
NOT RUN.

`BgeWssTransport(config,load_encoder,diagnostics,query_handler=None,...)` and
`run_wss_worker(...,query_handler=None)` retain legacy behavior by default.
Controller-owned `config['shared_reads']=True` requires an async handler before
network connection or model load. The trusted injected handler receives
`await query_handler(frame_dict,encode=resident_async_encoder)` and returns a
QUERY_RESULT dictionary. The native typed adapter must validate the full Query
and call Kaggle-local tokenize, SQL and hydration; this transport does not
authenticate actor fields or implement retrieval itself. Factory injection and
controller/server routing still require explicit integration.

The worker establishes ATTACH/WELCOME and sends actual RESOURCE_STATE before
starting heavy model load. Subsequent READY, heartbeat, probes and result delivery
share the same socket. The runtime incarnation remains stable across reconnect
and is already bound by ATTACH.

Shared assignments have one exact resource descriptor: `resource_kind` (`read` or
`document`), `resource_id` (request ID or job ID), `compute_claim` and original
`dispatch_generation`. Resource control frames carry the current authenticated
`connection_generation`. A new durable DISPATCH requires its dispatch generation
to equal that current generation; replay of an existing exact durable assignment
keeps its original descriptor. Changed replay payloads or resource descriptors
fail closed. Legacy DISPATCH frames and durable RESULT/RESULT_ACK stay compatible
when shared reads are disabled.

QUERY uses the existing strict frame plus controller-populated `compute_claim`;
there is no READ_DISPATCH wrapper. The transport checks bounded identifiers,
integer generations excluding booleans, the16KiB input and1000-character/4096-byte
query bounds, and deadline1–2000ms. Nested Query validation remains the trusted
typed handler's contract. Identical active read identity, claim and exact payload
are deduplicated. Changed payloads fail closed. New assignment while actual work
is busy fails closed rather than becoming a second local queue.

The injected async encoder creates an independent Task that holds one shared
`asyncio.Lock` across the existing resident synchronous encoder in `to_thread`.
The worker retains and observes every such Task until actual completion. Both
document and read paths use this same primitive. Consumers await it through
`shield`: deadline, cancellation or lost socket cannot cancel the native Task or
release its lock. Handler code must use this injected encoder and must not call a
second model or place provider/model retries in cleanup.

CANCEL_QUERY matches exact request/connection/turn identity and cancels the read
consumer once, discarding its pending result. Stale cancellation cannot affect
another read. A disconnected session also cancels/discards its read consumer and
SQL output once; it preserves native compute debt and the same warm model.
Repeated outer shutdown cancellation is absorbed while started load, handler,
document and native tasks drain. SQL cleanup belongs to the injected handler;
the worker waits for that handler Task to finish before releasing its resource.

QUERY_RESULT has no RESULT_ACK and is never replayed after a session changes.
Bounded callback failures produce `unavailable/internal_error` with no exception
text, evidence or retrieval mode. Deadline expiry similarly produces unavailable.
Completed retrieval permits only hybrid mode; full strict result/evidence,
cursor and dependency validation remains the native typed callback's obligation.
Exact continuation reads are a separate internal operation and must not be passed
off as hybrid searches. The result bound is256KiB.

RESOURCE_RELEASED is emitted only after the relevant document/read Task and all
native forward Tasks finish. A result, deadline, cancel, ACK, lease expiry or
disconnect is not compute release. A completed durable result can remain pending
RESULT_ACK while a subsequent read uses the already released encoder; the durable
result remains intact and resends under the existing semantics.

The last exact release descriptor is retained across reconnect and sent before
idle RESOURCE_STATE, until the next accepted assignment implicitly acknowledges
it. Control frames have sender priority over queued results. RESOURCE_PROBE is
handled by the same ordered reader and echoes its bounded `probe_nonce` with
actual current state. The existing controller helper must bind this challenge
to the same run/incarnation/current generation and slot version and mark work
ownership before network send. This can reconcile a dispatch lost before receipt;
ordinary idle state or elapsed lease alone cannot prove that loss. These frames
report this worker's observed local completion, not native provider termination.

State is bounded to one active read, one ephemeral result, one current resource,
one last release, the existing durable result/ACK state and tracked native tasks.
No unbounded history, separate registry or admission authority is introduced.
Live loaded-model/tokenizer identity, actual PG cleanup and controller integration
require their own verification; synthetic transport tests do not establish them.
