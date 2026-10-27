# Outbound worker and existing ASGI integration

The source implementation registers `/bge-worker/channel` on the existing
`KnowledgeMCPServer.streamable_http_app`, sharing the BgeQueue instance used by
the HTTP compatibility routes. No second listener process, job authority or
provider controller is introduced. The ASGI websocket implementation is an
explicit `websockets>=16,<17` dependency; configure the existing ASGI server's
websocket frame limit at deployment as well as the application bounds.

The existing controller's `worker_source` creates private notebook bytes from
the common worker template. With explicit `RKB_BGE_TRANSPORT=wss`, it persists a
launch nonce before push and embeds `bge_wss_transport.py` into that same
template. It derives WSS/443 from the existing HTTPS worker authority, without
credentials in URLs. The legacy HTTP default remains for independently running
consumers during this draft; it is not target WSS acceptance. No deployment or
notebook push was performed by this source implementation.

One ATTACH/WELCOME establishes authenticated control before `load_encoder`
imports Torch or loads weights. The existing pinned CPU FP32/1024 BGE loader is
shared by both transports, and now checks the complete input length without
silent token truncation. No BGE weights or inference run on DevCoveer. Actual
Kaggle package/egress/runtime readiness remains a Stage 0 check. The optional
existing ingest dependency `aiohttp` provides the notebook's TLS websocket
client; absence is a bootstrap failure, never an HTTP fallback.

`BgeWssTransport` maintains one model-load task, one active forward and exact
unacknowledged result across bounded jitter reconnect. A lost ACK resends the
same job, claim, vectors and timings under the new server generation. Receiving
the same durable DISPATCH cannot start another forward while it is active,
pending, or already acknowledged. The worker remembers only one completed
job/claim pair, sufficient for this single-forward bounded transport. A changed
claim may wait in one slot while an expired forward finishes; cancellation or
stale results do not imply that its native CPU work stopped.

There is one receive coroutine and a four-frame priority sender. Heartbeats and
READY precede queued results. Model loading, encoding and diagnostics run off
the event loop. Authority/contract closes stop the worker rather than starting
a different transport or run. Connection retries never call `kernels_push`,
reload the model, regenerate the incarnation, or resume ephemeral QUERY.

The cooperative worker summary explicitly records
`provider_termination_verified=false`. Neither socket close, drained Python
threads nor an exited worker establishes native provider termination. Exact
execution Stop/capacity and unknown publication outcomes remain controller
integration work. Existing controller lifetime/launch behavior is not silently
claimed to satisfy the target lifecycle contract.

The historical `cooperative_worker_exit=true` field means only that worker
main's outer `finally` was reached; retained summaries with that flag do not
prove successful transport, completed resource cleanup, or provider COMPLETE.
New summaries also record `transport_outcome=returned|raised|cancelled` and a
bounded exception class in `error_type` for raised/cancelled outcomes. Exceptions
and cancellation still propagate. Even `returned` leaves exact provider terminal
status to the existing native observation; it is not provider termination proof.

An attached, version-bound READY worker reaching the useful-idle deadline
enters `draining` while its heartbeat and hard lifetime remain valid. The first
maintenance, channel-status or STOP observation commits that same transition;
ordinary pre-deadline observations remain read-only. It stops
serving new reads/claims, retains existing claimed work, and uses the same
checkpoint/STOP path as a planned drain. An unresolved writer publication
keeps its existing recovery fence. The idle clock cannot retire the provider
reservation; only exact native terminal evidence does that. Disconnected,
conflicting, unversioned or failed cold starts keep their existing expiry rules.

Local evidence uses actual uvicorn/ASGI, aiohttp websocket, BgeQueue and result
waiters with a stub encoder, plus deterministic lost-ACK/reconnect/slow-load
fixtures. The native socket check is loopback without TLS and does not establish
Kaggle WSS egress, warmed model performance, public ephemeral search, fair
canonical-person admission, accepted/Saved recovery, or production readiness.
