# Durable BGE worker attachment authority

This slice carries the existing durable BGE encoding queue over an outbound
worker channel. It does not implement ephemeral QUERY, product Saved/accepted
durability, PostgreSQL execution, native compute Stop or runtime deployment.

The trusted controller calls `prepare_channel(run_id)` before provider push.
It persists and returns a server-generated launch nonce, stable on retries
while the run is starting and launch is pending/dispatching. A nonce is private
runtime configuration, never a report or log field. The run's existing token
remains its credential; neither migration nor reconnect rotates it.

`attach_channel(run_id, token, launch_nonce, runtime_incarnation)` authenticates
the run, checks its exact nonce and binds the first observed incarnation.
The incarnation is a worker claim under that token, not native provider proof.
HELLO may arrive before `launch_record` persists the native provider version;
attachment then returns `provider_version=None`. Claim dispatch waits for that
native receipt. Returned known versions are strings.

Each attachment increments the SQLite `connection_generation`. Every WSS claim,
completion and heartbeat supplies that generation. A reconnect of the same
incarnation fences the old socket; its exact durable job/claim can complete on
the new generation. Repeating an already committed completion returns
`duplicate`, so a lost ACK never requires repeating inference. A different
vector/space payload under the same completed claim is rejected; diagnostic
timings are excluded from that payload identity. A different
incarnation commits a permanent conflict and disables both attachments and HTTP
dispatch. Reattaching the original incarnation cannot clear that conflict.

After readiness on a new connection, `resume_channel_claim` reads the existing
unexpired durable claim before normal allocation. The same id/claim/input is
sent once, covering disconnect after claim commit but before the worker received
DISPATCH. This read neither creates an attempt nor renews a lease. Completed or
expired assignments do not resume. Buffered DISPATCH frames are checked against
that current claim immediately before sending and discarded if it changed.
The SQLite read and network write cannot be atomic without holding a database
lock during network I/O. Worker deduplication of its in-flight and most recently
acknowledged claim handles a completion racing that final check/write boundary.
This recovery applies only to legacy durable encoding jobs, never ephemeral QUERY.

`channel_status` uses a query-only snapshot and validates authentication,
generation and conflict state. It returns state, readiness and provider version
without renewing leases or persisting expiry. `detach_channel` clears only the
matching active socket. It never clears incarnation, generation or conflict,
and never hands dispatch back to HTTP. Socket loss proves no compute termination.

HTTP dispatch remains available to runs never attached to WSS. Preparing a nonce
alone does not take ownership. An outstanding unexpired HTTP claim blocks the
first attachment with `AttachmentBusy`; claim and attach serialize in the same
SQLite writer authority. No HTTP dispatch or result acceptance is allowed after
the first WSS attachment, including after disconnect. Control-authenticated
heartbeats without a generation remain compatible; WSS heartbeats pass their
generation for atomic fencing.

`StaleAttachment`, `AttachmentBusy` and `AttachmentConflict` are typed
`PermissionError` subclasses for HTTP 409 handling. Invalid/revoked run authority
remains `RevokedRun` (403). Catch attachment exceptions before generic permission
errors. `StaleClaim` continues to describe an ended job assignment.

The migration only adds columns to existing run rows. Run IDs, credential hashes,
provider receipts, queued/claimed jobs and completed cache rows stay intact.
An already compatible constructor probes schema read-only, preserving concurrent
observation while an unrelated SQLite writer holds its transaction.

Local verification: `tests/test_bge_attachment.py` exercises real SQLite
connections, reconnect fencing, persisted incarnation conflicts, concurrent
HTTP/WSS ownership, pre-receipt dispatch exclusion, lost-ACK completion replay,
read-only observation and baseline-schema migration. These checks provide no
provider/runtime or product acceptance receipt.
