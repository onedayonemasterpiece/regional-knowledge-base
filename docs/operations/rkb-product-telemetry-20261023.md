# Product telemetry: bounded async read/import spans (2026-10-23)

This is **diagnostic-only**; never a domain audit, ControlSpool, provider ACK, Saved receipt, or replay source. It does not change authorization or public response bodies.

## Scope

ReadTelemetry continues its existing bounded controller collection. ProductTelemetry.try_emit(record, level='standard') has a fixed schema, fail-open put_nowait and one daemon writer. A blocked, full or failed sink increments loss counters and cannot gate search/fetch or import.

QUERY_RESULT is transmitted without draining/serializing native read_events. Once socket.send returns, a separate task drains the existing native collector off-loop, validates correlation and stage whitelists, then posts events to the local sink. Other/older producers of optional read_events are protocol-compatible. Domain results, receipts and accepted outcomes remain unchanged.

Controller search, Kaggle native search stages, outer authorized fetch and original acquire/archive/verify/pages all use monotonic durations. Native stage/seal/materialize call spans and confirmed activation apply are distinguished; time ranges overlap and must never be summed. Inter-host wall clocks do not establish end-to-end stage timings. Import `applied`/provider ACK is **not Saved**. Without independent Dataset coverage/readback this PR emits no invented Saved event.

Only observable calls/actions are logged, never model private reasoning. Fields are allowlisted: event, phase, status, level, timing_semantics, duration_ms, trace_id, attempt_id, run_id, request_ref, operation_ref, connection_generation and turn_generation. request_ref/operation_ref are SHA-256. No query/book content, pages/originals, signed URLs, tokens, bearer, identities or CoT. Fields not on the whitelist are counted as failed, not accepted.

## Mode, bounded segments and archive

RKB_PRODUCT_LOG_MODE=standard (default) or debug at process start. RKB_PRODUCT_LOG_DIR points to a private local directory (default /tmp/rkb-product-telemetry). Standard: terminal/status transitions. Debug: additional phase stages. Producer returns only successful queue admission, **not persisted log proof**. Inspect emitted/dropped/failed/queued/sink_disabled counters for incomplete traces.

The queue is bounded at 128 by default, each JSONL file at 256 KiB, and local open+sealed files together have a 32 MiB budget. On disk full, worker I/O is disabled and subsequent events dropped. No fsync/retry storms. Segments seal by atomic rename into sealed/, with size+SHA256 in name; sealed_manifest() checks the bytes and marks archive_due after 72 hours (three days). Old sealed files are **never deleted** without separately verified archival, even after the hot horizon. They still consume the byte budget, so logging eventually drops rather than unbounded growth. Three-day hot retention is a target, not an impossible guarantee when storage fills.

The next separately authorized archive package must select a specific private Kaggle Dataset/quota and an existing transfer mechanism with Shkiper, verify size/hash + Dataset version readback, and only then design GC. This PR makes no Dataset/provider effects, deletes no logs and creates no credentials/permissions.

Resource-Реализация owns /proc/cgroup sampling and heartbeat. Integrate through this same ProductTelemetry.try_emit interface and a small explicit resource metadata whitelist, not another disk writer. The adapter is fail-open and independent of query permits.

## Validation / rollout

Source tests cover malformed/private payload rejection, debug/standard, full queue, blocked drain/sink, ENOSPC, cancellation, exceptions and 72-hour sealed preservation. CI is not live Kaggle proof, p50/p95, throughput, or successful model book import. Overhead is not zero. Opus owns functional review/merge, Critic complexity review; only Shkiper may deploy onto the existing warm runtime and validate measured outcomes.