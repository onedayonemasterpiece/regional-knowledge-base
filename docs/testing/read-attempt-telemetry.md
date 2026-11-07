# Read attempt diagnostics

`ReadTelemetry.begin_attempt(operation, trace_id=...)` creates a fresh attempt ID
and 32-hex request ID before admission. Retries and reconnects create a new
attempt while retaining the logical trace ID. `attempt.correlation()` exposes
only `trace_id`, `attempt_id`, and SHA-256 `request_ref`; raw request IDs do not
enter diagnostics. The public trace can join native events through `request_ref`.

Bind dispatch with the existing controller's `ReadRuntime(run_id,
runtime_incarnation)`, connection generation and turn generation. A different
attachment cannot overwrite this scope. A rejected rebind makes the terminal
diagnostic inconclusive. Call `finish_attempt` exactly once for the actual final
outcome, including refusal or cancellation before dispatch. Repeated finish
calls do not emit another terminal observation.

The existing runtime factory binds `bind_read_runtime(runtime)` around the
native callback. `kaggle_read_completed` retains its historical stage fields but
declares `plane=executor_stage, terminal=false`. The separate
`kaggle_read_adapter_terminal` event reflects the adapter's final typed status
and reason after input validation, serialization and deadline checks. An
invalid identity produces no request reference and a fixed diagnostic reason.
Cancellation describes the consumer outcome and does not prove compute death.

Timing fields are local monotonic durations. `total_ms`, `search_ms` and their
components overlap; `timing_semantics=overlapping_do_not_sum` forbids summing
them. Neither host wall clocks nor separate stage percentiles establish E2E.

The outbound worker adds `transport_ms` to the existing adapter event immediately
before writing its result to the socket. It measures local receive-to-write
duration, including adapter work and waiting behind earlier writes. It overlaps
`adapter_ms`; it excludes socket-send duration and both network legs. The
timestamp travels with the existing outbound queue item, so releasing/pruning a
completed read cannot lose the observation. Missing diagnostics leave the reply
unchanged. This measurement becomes live with the next ordinary worker release;
it is not evidence that latency has improved.

Both collectors remain finite and lossy. They use nonblocking enqueue, never
per-read durable storage or awaited logging. The next successfully enqueued
event carries cumulative `loss_before` counters and becomes `inconclusive`
after any loss. Missing runtime identity is also inconclusive. An observation
is never a durable receipt or proof of complete trace coverage. Labels and
stages use finite allowlists; query text, actor/grant/context data, credentials,
arbitrary exceptions and compute claims are excluded.

Focused local coverage is in `tests/test_query_telemetry.py`: concurrent
identical queries, retry/reconnect isolation, early refusal/cancel, adapter
serialization/deadline outcomes, blocked/full/failed sinks, and a held SQLite
writer. These checks do not establish real WSS/PG latency or deployment.
