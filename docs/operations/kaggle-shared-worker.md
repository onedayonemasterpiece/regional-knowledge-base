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
An already queued completed result keeps its original identity when a subsequent
read is accepted on that same socket; replacing the current consumer cannot drop
it. The server must still check the original ticket, deadline and current output
authority. Completed read output is queued before its release can authorize new
work; sender priority still favors control frames already in the bounded outbox.
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
actual current state. A retained release is replayed before this probe reply as
well, so completed work awaiting durable RESULT cannot be mistaken for work never
received. The existing controller helper must bind this challenge
to the same run/incarnation/current generation and slot version and mark work
ownership before network send. This can reconcile a dispatch lost before receipt;
ordinary idle state or elapsed lease alone cannot prove that loss. These frames
report this worker's observed local completion, not native provider termination.

State is bounded to one active read, one ephemeral result, one current resource,
one last release, the existing durable result/ACK state and tracked native tasks.
No unbounded history, separate registry or admission authority is introduced.
Live loaded-model/tokenizer identity, actual PG cleanup and controller integration
require their own verification; synthetic transport tests do not establish them.

## Explicit resident preparation

Source unit A starts at `04bbf6dcfaed71fa833020c2c7df871b2188a887` and adds the
optional async `prepare_runtime(encoder,run_id=...,launch_nonce=...,
runtime_incarnation=...)` hook. It runs once, after early authenticated ATTACH,
initial RESOURCE_STATE and the existing model load. Reconnect retains that same
prepared context, encoder and already owned pool. Direct `query_handler` injection
and legacy behavior remain compatible. The hook requires explicit shared mode;
it is not selected from a client field or environment import string.

The hook returns exact `PreparedRuntime(query_handler,CandidateReadiness)` types
from `regional_knowledge.kaggle_bootstrap`. Target READY and target dispatch stay
closed until preparation completes. Its READY adds only `runtime_readiness` with
the following exact fields: `hashes` containing all six `code`, `launch_config`,
`executed_source`, `dependency`, `model`, `schema` SHA256 values;
`database_generation`, `schema_revision`, `function_revision`, `executor_revision`;
`applied_seq`, `projection_seq`, `visibility_epoch`; `text_search_ready`,
`dense_search_ready`, `archive_read_ready`, `proof_ready`. No actor, user, run,
provider or credential appears inside this observation. Counters are bounded
nonnegative PostgreSQL bigint values and exclude booleans. Text/dense readiness
requires a caught-up projection and known visibility cursor. Archive/proof can
remain unknown independently. The payload is at most16KiB and revalidated before
serialization. Existing model-only READY is unchanged without this opt-in hook;
it cannot by itself certify target database/search readiness.

`verify_artifacts(root,entries,max_total_bytes=...)` accepts a bounded explicit
inventory of `PreparedArtifact(ArtifactRequirement(name,size,sha256),role)`.
It streams exact declared bytes, checks count/total budgets, rejects traversal,
symlink components, missing/changed/nonregular files and mismatched hashes, and
closes every descriptor. The model directory must contain exactly its declared
tokenizer/config/weight files. Its tokenizer.json must match the canonical pinned
tokenizer SHA. Code files stay outside that model directory. The owning factory
must keep these content-addressed directories immutable across verification/load;
hashing cannot fence an arbitrary concurrent filesystem writer.

The worker template supports explicit controller-owned `prepared_model` config
with exactly `root`, `entries` and `max_total_bytes`. In this path it verifies
artifacts before loading, pins the tokenizer.json file explicitly, uses only the
local model directory and sets `local_files_only=True,trust_remote_code=False`
for model/tokenizer loading. There is no download/install fallback in this path.
`main(prepare_runtime=...)` rejects missing/malformed inventory or disabled shared
mode before entering the loader, preventing a target configuration from falling
through into legacy network-capable loading. Legacy `main()` stays compatible.
It returns a callable `ResidentEncoder` exposing the actual locally loaded fast
tokenizer, verified artifact binding, model directory and actual load counter.
The effective native tokenizer backend must match the pinned tokenizer.json
structure (apart from per-call padding), with truncation disabled. Any observed
settings difference fails closed; real pinned-runtime compatibility remains a
Stage0 check. The tokenizer settings document is bounded to64MiB.
Its nonblocking `count` uses `to_thread` with complete special-token-inclusive,
no-truncation/no-padding token IDs. The same resident encoder remains the only
model; this unit does not load a model in tests or on DevCoveer.
Counting and inference serialize access to the same mutable fast tokenizer.
Existing native read stages split successful query counts into
`tokenize_executor_wait_ms`, `tokenize_mutex_wait_ms`, `tokenize_work_ms` and
`tokenize_resume_ms`. These partition count wall time; they are included in
`tokenize_ms`, not additional latency to sum with it. No source text is logged.
Cancellation drains any started native count, including repeated cancellation,
before handler cleanup may relinquish the worker resource.

`prepare_resident_runtime` composes that handle with `acquire` from the already
owned initialized **Kaggle** PG pool, `probe_database`, the actual launch config,
executed-source path plus explicit Stage0 path-equivalence attestation, and an
explicit installed dependency inventory. It verifies that code inventory paths
cover the actual loaded target modules. Observed hashes come from actual verified
file bytes, canonical actual launch config, actual installed dependency/runtime
fingerprint and actual native catalog bytes returned by the probe. They are not
copied from the controller's expected six hashes. The executed-source expected
hash belongs outside the source bytes it hashes; the controller compares it.
The dependency inventory must cover torch, transformers, tokenizers,
huggingface-hub, psycopg, psycopg-pool, aiohttp and pydantic; missing packages or
incomplete coverage fail preparation. No dependency is installed by the check.

The trusted `probe_database(acquire=...)` callback returns bounded native catalog
bytes plus the exact readiness fields above excluding hashes. It must verify the
existing DB generation, semantic revisions/catalog/index contract, actual
applied/projection frontier and native text+dense readiness under finite query
budgets. It must not initialize, restore or migrate an unknown DB. This unit
neither constructs a PG process/pool nor updates a readiness frontier. Existing
ordered recovery/apply/prepare-frontier contracts remain the only mutation path.
The helper creates the existing PinnedTokenizer and KaggleWorkerQuery only after
artifact and native probe checks pass, forwarding the existing finite telemetry
queue unchanged. Each read returns its own bounded diagnostic sidecar, including
SQL health reads; the transport writer does not drain that collector again after
sending. The caller owns pool shutdown and actual probe completeness.

Physical placement remains thin SQLite control/short operation journal and rights
on DevCoveer; corpus, indexes, pool and complete hybrid retrieval remain in Kaggle
PostgreSQL beside the resident BGE. No Dev PG/model/data fallback is introduced.
Control stays responsive while preparation runs. Session loss does not restart
preparation; outer shutdown drains started load/preparation through repeated
cancellation before returning. Native probes/preparation need explicit finite
budgets; cancellation/expiry is never treated as proof of completion.

Controller integration B must persist expectations before promotion, bind the
observation to the authenticated current run/incarnation/provider version and
call the existing readiness validators before routing target QUERY. This unit
does not enable that factory or supply a provider/PG capability receipt. Tests
use clearly marked synthetic tokenizer-pin/dependency overrides and artifact/PG probe stubs,
and prove control-before-preparation, no premature READY, actual byte/hash/path
checks, exact wire bounds, off-loop complete counting, context reuse and drain.
Actual loaded artifacts and provider runtime remain NOT RUN; disposable native
PG verification is described below.
The old monolithic runtime has no verified live installation hook; initial
adoption without a model reload is explicitly unverified. No eval, reload,
second dispatcher, model service or public listener is added.

`NativeDatabaseProbe` supplies the concrete read-only probe over an exclusively
checked-out, idle, tuple-row connection from that existing Kaggle pool. It uses
one repeatable-read read-only transaction, a total deadline including acquisition,
local statement timeouts, bounded named-cursor fetches, SQL row limits and byte
limits. It observes the existing recovery/read-frontier singleton and PostgreSQL
17 / pgvector 0.8.2, native 1024-dimensional vector support, required SQL 001–005
functions with their scalar/table result contracts, enabled fences, valid/ready
GIN and HNSW indexes and native text-search configuration. Missing or invalid
objects fail with fixed sanitized codes. Cancellation propagates after transaction
cleanup; the pool retains responsibility for exclusive leasing and disposal of
broken connections.

Canonical catalog bytes include actual table/column/constraint/index/trigger and
function definitions, relevant settings, privileges and text-search mappings.
Internal trigger OID-derived names are normalized. Actual semantic revisions and
migration binding are included; mutable applied/projection cursors are returned
separately, so an ordinary accepted command does not change the schema hash.
Lagging projections force both readiness flags false while preserving actual
observed cursors. Archive/proof readiness remains unknown. No corpus rows are
scanned, modified, initialized or restored. The controller must still compare the
observed catalog hash and exact database/revision binding with its independent
expectations; object presence alone does not verify trusted function semantics,
retrieval quality, product permissions or deployment readiness.

Synthetic tests exercise bounds, ABI failures, deterministic hashing and cleanup.
Opt-in native cases reuse only the existing guarded disposable PG fixture and
install SQL 005 there; they include catalog mutation/hash, frontier lag, missing
or invalid indexes and lock-timeout cleanup. These tests do not authorize probing
or changing a product database. Runtime artifact/model/provider observations and
public delivery remain separate acceptance work.
The root serial native run now verifies these cases on PostgreSQL17/pgvector,
including rollback and reuse after an actual held-lock timeout. Locale facts
come from `pg_database.datcollate/datctype`, rather than unavailable locale GUCs.
The timeout assertion requires the deadline/cancellation reason and an actual
wait, so an unrelated early SQL error cannot satisfy it.

Normal search latency targets are measurements, not rejection deadlines. A live
normal Query waits in the existing single-slot fair admission queue and executes
without an elapsed-time rejection, including native forward and SQL. Cancellation,
withdrawal, binding changes and actual channel loss still terminate the consumer;
actual compute cleanup retains its existing ownership. Exact reads, acceptance
checks, SQL health and explicitly delegated caller deadlines retain their separate
contracts. Output tickets retain their existing finite lifetime after completion.
No additional worker, queue, retry, parallel forward or transport is introduced.
Owner intent: https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1793281299333039;
protected requirements change: `op_303a11fa1e9fc253df59ef33`. Source and synthetic
validation do not establish deployment or live latency acceptance.
