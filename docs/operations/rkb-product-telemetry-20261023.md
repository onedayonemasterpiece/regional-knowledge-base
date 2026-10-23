# Product telemetry: bounded async read/import spans (2026-10-23)

This is **diagnostic-only**; never a domain audit, ControlSpool, provider ACK, Saved receipt, or replay source. It does not change authorization or public response bodies.

## Scope

ReadTelemetry continues its existing bounded controller collection. ProductTelemetry.try_emit(record, level='standard') has a fixed schema, fail-open put_nowait and one daemon writer. A blocked, full or failed sink increments loss counters and cannot gate search/fetch or import.

QUERY_RESULT is transmitted without draining/serializing native read_events. Once socket.send returns, a separate task drains the existing native collector off-loop, validates correlation and stage whitelists, then posts events to the local sink. Other/older producers of optional read_events are protocol-compatible. Domain results, receipts and accepted outcomes remain unchanged.

Controller search, Kaggle native search stages, outer authorized fetch and original acquire/archive/verify/pages all use monotonic durations. Native stage/seal/materialize call spans and confirmed activation apply are distinguished; time ranges overlap and must never be summed. Inter-host wall clocks do not establish end-to-end stage timings. Import `applied`/provider ACK is **not Saved**. Without independent Dataset coverage/readback this PR emits no invented Saved event.

Only observable calls/actions are logged, never model private reasoning. Fields are allowlisted: event, phase, status, level, timing_semantics, duration_ms, trace_id, attempt_id, run_id, request_ref, operation_ref, connection_generation and turn_generation. request_ref/operation_ref are SHA-256. No query/book content, pages/originals, signed URLs, tokens, bearer, identities or CoT. Fields not on the whitelist are silently removed, with a numeric ignored_fields count; their names/values never enter the file. Invalid optional identifiers are omitted with a counter, not written or treated as an event failure. Invalid known numeric measurements are rejected and counted as failed.

## Mode, bounded segments and archive

RKB_PRODUCT_LOG_MODE=standard (default) or debug at process start. RKB_PRODUCT_LOG_DIR points to a private local directory (default /tmp/rkb-product-telemetry). Standard: terminal/status transitions. Debug: additional phase stages. Producer returns only successful queue admission, **not persisted log proof**. Inspect emitted/dropped/failed/queued/sink_disabled counters for incomplete traces.

The queue is bounded at 128 by default, each JSONL file at 256 KiB, and local open+sealed files together have a 32 MiB budget. On disk full, worker I/O is disabled and subsequent events dropped. No fsync/retry storms. Segments seal by atomic rename into sealed/ with unique JSONL filenames; no SHA or manifest is calculated on the read path or by the sink. The operator's three-day hot-log horizon is a retention requirement for the separately authorized archive step, **not** a running archive/delete scheduler in this PR. Older sealed files are **never deleted** without separately verified archival. They continue to occupy the local byte budget, so logging can drop when full rather than silently deleting the only copy. This first PR cannot guarantee three days of new writes when that disk budget is exhausted.

The next separately authorized archive package must select a specific private Kaggle Dataset/quota and an existing transfer mechanism with Shkiper; read sealed filenames/bytes, calculate size/hash at transfer, verify Dataset version readback, and only then design GC. This PR makes no Dataset/provider effects, deletes no logs and creates no credentials/permissions.

Resource-Реализация owns /proc/cgroup sampling and heartbeat. Integrate through this same ProductTelemetry.try_emit interface and a small explicit resource metadata whitelist, not another disk writer. The adapter is fail-open and independent of query permits.

## Validation / rollout

Source tests cover private/unknown field omission, bad numeric rejection, debug/standard, full queue, blocked drain/sink, ENOSPC, cancellation, exceptions and old sealed JSONL preservation without archive or deletion. CI is not live Kaggle proof, p50/p95, throughput, or successful model book import. Overhead is not zero. Opus owns functional review/merge, Critic complexity review; only Shkiper may deploy onto the existing warm runtime and validate measured outcomes.