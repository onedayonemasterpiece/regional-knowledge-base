# MCP read profiling

The existing `search` and Live `knowledge_search` responses include optional
`read_trace`: an opaque correlation ID and a local monotonic
`backend_elapsed_ms`. This measures awaiting the backend only. It excludes
authentication before dispatch, MCP serialization, connector transport, client
queueing and the assistant's reasoning. It is never a client E2E measurement.

The same ID accompanies a bounded `read_completed` diagnostic containing mode,
result count and available allowlisted backend stage durations. No query text,
person identifier, bearer, exception text or arbitrary metadata is recorded.
Diagnostics use one bounded RAM queue per existing server and a daemon logging
consumer. A blocked/failed sink does not hold a read, event loop or compute
permit. Overflow increments `dropped`; sink/intake errors increment `failed`.
These lossy records are neither accepted-work recovery nor a Saved receipt.
The CLI enables INFO only for the specific diagnostic logger; embedding hosts
may configure that logger themselves. Restart may lose pending diagnostics.

For comparison, the external client measures monotonic tool dispatch through
receipt of the tool result and retains the returned trace ID. Compare this
duration with `backend_elapsed_ms` and server stages from the same call. Do not
subtract unsynchronized timestamps on different hosts, add individual p95s or
infer a percentile from a handful of probes. Separate ready evidence from
starting/degraded/empty responses; record cold-start and actual worker/job IDs
when available. A second query completing after a cold-start trigger does not
make the first response a successful dense retrieval.

Tests deliberately block the logging sink, fill the queue, fail the sink/startup
and cancel a backend. They verify unaffected results/cancellation, trace context
cleanup and bounded diagnostic content. These are local checks, not a deployed
MCP latency measurement. The running legacy backend has no such trace field
until this source is deliberately released.

Owner clarification on 2026-10-20 makes actual BGE plus lexical execution
mandatory for public search. The compatibility MCP adapter rejects a pending,
lexical-only or vector-only backend response with explicit
`hybrid_search_required` unavailability, retaining its job locator and trace.
It does not deliver the partial evidence as a successful search. Legacy BGE
ranking configuration now always enables its lexical branch; this does not
introduce E5 into the target or establish target quality/latency acceptance.
Kaggle QUERY executes both SQL branches locally and must likewise fail explicitly
when dense computation is unavailable. Bounded hybrid candidate recall may
remain explicitly incomplete after both branches actually run.
The existing public `story_search` likewise defaults to hybrid and refuses
lexical-only or unavailable/stale-vector discovery; direct saved-story readback
retains its separate contract. Internal branch probes and exact source/catalog
reads do not claim to be completed hybrid search.

Independent review found that the legacy story registry's `hybrid` parameter
previously skipped FTS whenever a dense result existed. Compatibility serving
now actually runs FTS and dense candidate retrieval independently and fuses at
most 200 candidates per branch. Its existing Python vector scan remains legacy
code, explicitly outside the Kaggle indexed target and scale acceptance. The
response marks bounded fusion incomplete. Tests include an unembedded lexical
hit and a differently worded dense hit together, excluding another person's
private story from both branches.
