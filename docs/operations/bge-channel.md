# Durable BGE worker channel

`bge_channel.channel_route(queue)` adds the native Starlette websocket route
`/bge-worker/channel` to the existing MCP ASGI application. The existing
`BgeQueue` remains the run credential, attachment and durable claim authority.
It transports existing encoding jobs and, with explicitly configured shared
admission, internal QUERY frames. Public routing and current source-output
authorization remain separate integrations. Local socket tests do not establish
deployed Kaggle WSS egress.

The notebook connects outbound over WSS with its existing run-bound
`Authorization: Bearer …` credential. It sends ATTACH before loading the model.
ATTACH is a bootstrap attachment request, separate from the future full runtime
HELLO in `kaggle_protocol.py`. Every frame has integer `protocol_version: 2`.

| Direction/type | Fields beyond type/protocol_version |
| --- | --- |
| Worker ATTACH | `run_id`, controller-issued `launch_nonce`, stable-per-bootstrap `runtime_incarnation` |
| Server WELCOME | server-assigned `connection_generation`, `provider_version` string or null |
| Worker READY | `connection_generation`, exact `space`, `model_revision` |
| Server READY_ACK | `connection_generation`, `status` (`provider_pending` or `ready`), `provider_version` string or null |
| Worker HEARTBEAT | `connection_generation`, optional bounded `diagnostics` object |
| Server HEARTBEAT_ACK | `connection_generation`, `provider_version` string or null |
| Server DISPATCH | `connection_generation`, existing job `id`, `claim`, `kind`, `texts`, `space` |
| Worker RESULT | `connection_generation`, `job_id`, `claim`, `space`, `vectors`, optional `timings` (`encoder_seconds`) |
| Server RESULT_ACK | `connection_generation`, `job_id`, `claim`, `status` (`accepted`, `duplicate`, `stale`) |

Unknown native provider version permits control only. READY may arrive during
that period and receives `provider_pending`; the server retains that validated
model readiness. Once the controller records the native provider receipt, the
same socket receives `READY_ACK/ready` and can receive DISPATCH. The notebook
does not invent a provider version and does not need to reconnect for this race.

The queue allocates connection generations. Same-incarnation reconnect fences
the previous socket; a competing incarnation blocks dispatch until reconciliation.
The HTTP compatibility claim path cannot dispatch beside a WSS attachment.
Detach uses the exact generation and does not silently switch dispatch to HTTP.
An expired claim returns RESULT_ACK/stale without killing the warm worker.

After a lost result ACK, the worker retains the exact job/claim/vector and sends
READY then replays RESULT under the new WELCOME generation. Existing queue
completion determines accepted/duplicate/stale; no replacement job or second
inference is created by the transport. A duplicate ACK confirms only the local
encoding queue outcome, not domain Saved or remotely durable acceptance.

There is one receive coroutine and one bounded sender. Control frames precede
queued DISPATCH frames, while an already sent frame remains subject to TCP
ordering. Control/result frames are capped at 64 KiB; DISPATCH permits 256 KiB
to carry the existing 40,000-character document input without truncation.
The send queue holds at most eight frames (a conservative 2 MiB byte bound),
and queue/send waits at five seconds. A native ASGI websocket server should also
cap incoming message bytes before application delivery. Cross-process pending
jobs and attachment fences reconcile at 250 ms; SQLite calls run off-loop.
The worker owns encoding and keeps socket control responsive while it computes.

Close codes: 4400 invalid/oversized/unsupported frame or channel timeout; 4403
invalid/revoked run authority; 4409 stale/busy/conflicting attachment. A close or
ACK does not prove provider compute termination. Logs contain event/status/run
generation only, never credentials or text/vector payloads.
Unexpected server failures close with 1011 and log only their exception type.

Verification: `tests/test_bge_channel.py` exercises the actual ASGI websocket
route with a queue stub and a real-queue roundtrip: auth, early receipt race, job roundtrip/notification,
generation fencing, competing incarnation, lost ACK duplicate, heartbeat while
encoding, stale claim, full-size Unicode input and malformed/oversized frames. Transactional attachment
and exact duplicate behavior belong to the real BgeQueue tests. Provider/runtime
WSS egress, model residency and deployment acceptance remain NOT RUN here.

## Target runtime proof

Target controller setup must call `queue.expect_search_runtime(run_id, expected)`
before the first ATTACH. `SearchRuntimeExpectation` freezes six artifact hashes,
database generation, schema/function/executor revisions and the minimum applied
sequence. The generated launch bytes and actual artifact manifests must supply
these facts; setting matching strings is not artifact verification. The setup
API is immutable, with an inert exact retry. It is not a warm update barrier.

For such a run, READY additionally carries `runtime_readiness`: all six observed
hashes, semantic revisions, database generation, applied/projection sequences,
visibility epoch and explicit text/dense readiness. Archive/proof readiness is
independent. Model-only READY, stale projections or a mismatched artifact cannot
promote the channel. `READY_ACK/runtime_not_ready` reports bounded reason codes
while control remains available. Native provider version must also be known.
Identity comes from the current authenticated run/nonce/incarnation/attachment,
never fields supplied inside this proof.

Reconnect requires proof for the new attachment generation. A changed or missing
proof after promotion closes the attachment with4409; arbitrary READY cannot
replace an ordered migration/drain barrier. Invalid or exact duplicate proof
checks are read-only. A first valid binding performs one setup write, outside
normal ready QUERY delivery. The resource busy/idle/release path also avoids a
SQLite writer transaction once the run is bound.

Absent expectations preserve legacy and explicit injected-handler test behavior;
they do not establish target readiness. Target launch/public activation must
install this binding and require `runtime_verified`. Actual loaded artifacts,
PostgreSQL probes, current withdrawal authorization and live acceptance remain
separate evidence. `tests/test_bge_runtime_readiness.py` checks these binding and
reconnect rules through the real queue and ASGI route.
`tests/test_kaggle_query_roundtrip.py` also connects prepared READY through the
same native loopback WSS route, shared admission, resident transport and actual
disposable PostgreSQL query. Its artifact hashes/tokenizer/encoder are explicitly
synthetic; it proves wire composition and frontier binding, not loaded artifacts.
