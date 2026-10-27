# Product telemetry: bounded async read/import spans (2026-10-23)

This is **diagnostic-only**; never a domain audit, ControlSpool, provider ACK, Saved receipt, or replay source. It does not change authorization or public response bodies.

## Scope

ReadTelemetry continues its existing bounded controller collection. ProductTelemetry.try_emit(record, level='standard') has a fixed schema, fail-open put_nowait and one daemon writer. A blocked, full or failed sink increments loss counters and cannot gate search/fetch or import.

The native query adapter carries its existing fixed stage records in the already supported `QUERY_RESULT.read_events` sidecar. It reads only its finite in-memory collector with bounded `get_nowait`; it never invokes an arbitrary exporter or performs I/O before the reply. The controller validates correlation and stage whitelists into its existing log sink. The transport's optional drain callback remains after socket send, independently of this sidecar. A completed ordinary hybrid query returns `ok`; normal bounded ANN/top-k policy is not runtime degradation or a claim that recall acceptance passed. The private acceptance request retains its explicit recall limitation. Domain receipts and accepted outcomes remain unchanged.

Controller search, Kaggle native search stages, outer authorized fetch and original acquire/archive/verify/pages all use monotonic durations. Native stage/seal/materialize call spans and confirmed activation apply are distinguished; time ranges overlap and must never be summed. Inter-host wall clocks do not establish end-to-end stage timings. Import `applied`/provider ACK is **not Saved**. Without independent Dataset coverage/readback this PR emits no invented Saved event.

Only observable calls/actions are logged, never model private reasoning. Fields are allowlisted: event, phase, status, level, timing_semantics, duration_ms, trace_id, attempt_id, run_id, request_ref, operation_ref, connection_generation and turn_generation. request_ref/operation_ref are SHA-256. No query/book content, pages/originals, signed URLs, tokens, bearer, identities or CoT. Fields not on the whitelist are silently removed, with a numeric ignored_fields count; their names/values never enter the file. Invalid optional identifiers are omitted with a counter, not written or treated as an event failure. Invalid known numeric measurements are rejected and counted as failed.

## Mode, bounded segments and archive

### Import call and command observations (2026-10-27)

The existing SDK `call_tool` boundary times `book_pages`, every `book_ingest`
command and `fetch`, including validation, backend authorization, waits and
output binding. It excludes HTTP middleware before that boundary, final HTTP
delivery and client/model time. `fetch.terminal` now uses this boundary instead
of timing only the backend await. Successful tool return is `returned`, not
proof of APPLIED or Saved; fixed `result_state`, `next_action`, `result_status`
and `error_class` retain pending/unknown/error distinctions without error text.

`import.terminal` and nested `import.phase` share the request trace and hashed
`ingestion_ref`. The native admission observation supplies hashed
`operation_ref`, exact command sequence, component, batch, canonical packet
bytes and page/region/chunk/unit/story counts. Page calls include actual JPEG
bytes and physical page index. No page body or image bytes enter the log.
`ingestion_ref` hashes the ingestion ID; `operation_ref` always hashes the
operation key, rather than using the same field for two unrelated identities.

`command.phase` records first locally committed observations of remote journal
acceptance (`seal/acknowledged`), an exact native apply receipt
(`dispatch/applied` or `rejected`), and a verified remote durability receipt
(`saved/saved` or `rejected`). Exact repeats emit no second transition. A later
native `import.phase` reconciliation is a separate observation, not the native
commit timestamp. Join using operation hash and sequence; a background receipt
may have no originating call trace. Logs themselves are not receipts and cannot
establish durability when missing from the canonical path.

`call_started_unix_ms` and `event_time_unix_ms` are captured by the producer;
the sink's `observed_unix_ms` is its later write time. Durations use monotonic
time. Missing fields/spans remain unknown. Inter-call gaps include client,
model and orchestration time and are not pure model reading time.

The additive metadata whitelist accepts only fixed enums, hashes and bounded
integer counts. Existing queue/disk budgets remain in force. A disabled sink
also emits one content-free `product_telemetry_sink state=disabled` warning
with loss counters to the existing journal, so a full log cannot appear as a
zero-duration import. There is no automatic archive or deletion. Rollout must
explicitly enable the existing mode and select a private managed log directory;
source/CI completion alone does not mean runtime logging is enabled.

RKB_PRODUCT_LOG_MODE=standard (default), debug or off at process start. RKB_PRODUCT_LOG_DIR points to a private local directory (default /tmp/rkb-product-telemetry). Set an explicit managed directory for devserver diagnostic runs. Standard: terminal/status transitions. Debug: additional phase stages. Producer returns only successful queue admission, **not persisted log proof**. Inspect emitted/dropped/failed/queued/sink_disabled counters for incomplete traces.

The queue is bounded at 128 by default, each JSONL file at 256 KiB, and local open+sealed files together have a 32 MiB budget. On disk full, worker I/O is disabled and subsequent events dropped. No fsync/retry storms. Segments seal by atomic rename into sealed/ with unique JSONL filenames; no SHA or manifest is calculated on the read path or by the sink. The operator's three-day hot-log horizon is a retention requirement for the separately authorized archive step, **not** a running archive/delete scheduler in this PR. Older sealed files are **never deleted** without separately verified archival. They continue to occupy the local byte budget, so logging can drop when full rather than silently deleting the only copy. This first PR cannot guarantee three days of new writes when that disk budget is exhausted.

The next separately authorized archive package must select a specific private Kaggle Dataset/quota and an existing transfer mechanism with Shkiper; read sealed filenames/bytes, calculate size/hash at transfer, verify Dataset version readback, and only then design GC. This PR makes no Dataset/provider effects, deletes no logs and creates no credentials/permissions.

Resource-Реализация owns /proc/cgroup sampling and heartbeat. Integrate through this same ProductTelemetry.try_emit interface and a small explicit resource metadata whitelist, not another disk writer. The adapter is fail-open and independent of query permits.

## Validation / rollout

Source tests cover private/unknown field omission, bad numeric rejection, debug/standard, full queue, blocked drain/sink, ENOSPC, cancellation, exceptions and old sealed JSONL preservation without archive or deletion. CI is not live Kaggle proof, p50/p95, throughput, or successful model book import. Overhead is not zero. Opus owns functional review/merge, Critic complexity review; only Shkiper may deploy onto the existing warm runtime and validate measured outcomes.
