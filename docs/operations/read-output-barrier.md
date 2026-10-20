# Canonical source restriction intake

`control_spool.py` adds one source-only canonical write hook inside the existing
BgeQueue SQLite database. It reuses Participants, the immutable HandlerRegistry,
RecoveryJournal acceptance, and atomic PG apply. It owns no dispatcher, service,
identity registry, provider adapter, second journal or public MCP route. The
current gate source baseline is `10cc6d5f09e3c8faf64bfffcfce96e988b6b9f38`;
the installed Kaggle debugging skill/profile and migration/lifecycle/evidence
references were read from
`/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging`.
The repository records its verified skill source pin as
`2102f08478e532b44f2b0e0dfcba8fb11565ac63`.

Physical placement is deliberate: DevCoveer retains this thin bounded control
hot tail, compact receipts, exact restriction intents and existing rights.
Corpus, passage bodies, bibliography, projections, FTS and vector indexes remain
the target Kaggle PostgreSQL/pgvector plane. This source-only spool accepts a
fixed withdrawal payload and source/revision identities; it cannot stage
book text, vectors or a shadow corpus. Its rows are derived exact existing
journal commands, not an independently maintained source-policy authority.

The source gate is available for the root controller to connect to all
applicable write/revoke paths and actual final issuance. Its implementation
alone does **not** enable public QUERY or prove existing serving routes safe.
A remote accepted intent is not product
`closed=true`, PG Saved, external derivative purge, or proof of live deployment.

## Existing controller API

At explicit startup, outside read serving:

```python
spool = ControlSpool(existing_queue, participants, recovery_journal, registry)
entry = await spool.stage_source_withdraw(internal_request_authority,
    resolved_exact_command, database_generation=owned_database_generation)
# Resolved imports use stage_source_import with source_import.py's pinned
# payload validation and exactly the same journal/apply/outcome path.
seal = await spool.seal(internal_request_authority, operation_key,
    predecessor=verified_head_or_none, writer_generation=current_writer)
receipt = await spool.apply(operation_key, existing_pg_connection,
    writer_generation=current_writer)
remote = await spool.publish_outcome(operation_key,
    writer_generation=current_writer)
```

The stage checks the existing current `RequestAuthority`/Participants owner
personal route and import capability. `command.person_id` must match that
canonical person; it grants no rights. No caller actor/audience/epoch fields are
accepted as authority. This is an internal hook, not a raw tool argument parser.
The controller freezes sequence, operation/request key, logical time, original
schema/function/executor revisions and database generation once. Source IDs are
canonical UUIDs. `source.withdraw.v1` requires exactly `ids={source_id: UUID}`,
`expected_revisions={eligibility_revision: positive_decimal_string}`, and exact
canonical payload bytes `{"withdrawn":true}`. Current actor checks never replace
the PG source eligibility precondition.

`DependencySpec` extends the existing static HandlerSpec. The source handler
uses `WITHDRAW_DEPENDENCY`: shared public-source write/restriction with the one
resolved source ID key. It is part of the handler fingerprint. Old specs without
dependency metadata retain their exact old fingerprints; missing metadata does
not prove independence. New classification semantics need a versioned trusted
handler key. `source.import.v1` is a distinct nonrestriction dependency: pending
imports affect completeness without falsely withdrawing the imported source.
Intake preserves the exact source ID, absent-source precondition and resolved
canonical payload. A private-draft write class is actor-scoped even if it read public
books; read dependencies must never become its public-corpus write set. Variant,
grant, case and generic mutation intake are not implemented by this spool and
fail closed.

The sole local spool is the existing queue DB; FULL-synchronous writes and a
nonblocking process ownership lock happen only during startup/mutations. A
second controller instance cannot acquire this spool. Exact command/scope/DB
bytes are immutable for an operation key; global sequence occupancy is UNIQUE.
Retries use the same bytes, keys, IDs, logical time and original semantics.

## Acceptance, UNKNOWN and outcomes

Seal, PG apply and remote outcome each have separate `NOT_SENT`,
`SENT_UNKNOWN`, and verified completion states. Before the first remote seal,
the spool commits exact bytes plus the original verified predecessor/token and
marks `SENT_UNKNOWN`. It never holds a SQLite writer lock across provider/PG
I/O. A repeated seal omitting predecessor recovers the original frozen value;
changing it is an immutable retry conflict. Exact remote membership/readback
must return before the local phase becomes `ACCEPTED`. Stop/deletion/timeout,
uploaded orphan objects, lost socket or lease expiry do not substitute for it.
An unaccepted intent rechecks current owner/import authority after acquiring the
operation lock, immediately before the new seal attempt. An already accepted
intent replays its original captured author without current-token
reauthorization; an outer receipt/output endpoint still applies current access
checks before disclosure. These are different boundaries.
For `SENT_UNKNOWN`, the spool first calls read-only
`RecoveryJournal.locate_accepted(command)`: an exact accepted sequence locator
or the current authoritative-head crash gap can prove prior acceptance without
another write. It then records that acceptance even if current user authority
has expired. Verified observed absence still requires current authority before
any new attempt; unavailable/historical gaps remain UNKNOWN. Observed absence
does not prove an old in-flight CAS cannot later commit and never erases input.

Local pending/unknown withdrawal remains a provisional restriction. Remote
acceptance makes its intent durable, including while PG is unavailable. Root's
eventual common issuance barrier must observe this state before it may promise
that new delivery is closed. A rejected domain precondition never silently
reopens an acknowledged restriction. This first phase has no restore/unwithdraw
operation and never expires a restriction by time.

`apply` calls the existing `apply_accepted` with the original DB generation and
exact accepted envelope. A lost COMMIT ACK leaves `SENT_UNKNOWN`; retry on a
fresh original-generation connection returns its recorded exact outcome.
Already accepted commands retain their original author/semantics when replayed
by the current writer; they are not replaced by a newly authenticated command.
`record_apply(key, native_receipt)` and `mark_apply_sent(key)` are also internal
seams for the existing controller. Their receipt type is not a public proof or
an authorization token. `publish_outcome` uses the existing fenced create-only
remote outcome/readback and stores a separate compact terminal receipt.

## Atomic facts and current output gate

```python
scope = await spool.scope_state(None)       # whole accepted-public source query
scope = await spool.scope_state(source_ids) # at most32 server-owned finite IDs
items = await spool.barriers_for(all_item_source_ids) # at most160 =5*32
page = await spool.pending_page(after_key=cursor, limit=100)
facts = await spool.read_facts(query_sources, exhaustive_item_sources)
```

These are read-only SQLite snapshots, off the event loop, with no cloud access
or writer lock. Whole-scope counters/cursors are updated atomically with spool
transitions. Finite scope uses a covering source/state aggregate. Per-source
views seek bounded heads through indexes: latest local intent, latest remotely
accepted restriction, pending sequence and unknown sequence. A newer unsealed
intent cannot hide the older accepted key/hash/sequence/provenance. Counts and
maximum sequence alone are not proof of global history continuity.

The older diagnostic views still report `history_complete=False` and
`scope_complete=False`. Only `read_facts` can carry a verifier-owned current
coverage capability. An empty spool, JSON `complete=true`, maximum sequence or
top-K without a source never proves complete history. Query scope and the
exhaustive per-item source union are read in one SQLite transaction. Broad
zero-hit/top-K results remain degraded while any relevant source control is
pending. An explicit B-only query is unaffected by a pending A-only restriction.

`SourceReadGate(read_authority, spool, binding_current=...)` has this controller
API:

```python
permit = await gate.dispatch(verified_authority, query, trusted_runtime_binding)
# Dispatch permit.query through the existing shared admission/controller.
gated = await gate.finish(permit, remote_result)
gated = await gate.refresh(gated)  # delayed/cache output must recheck
if gate.issuance_current(gated.ticket):
    issue_already_serialized_result(gated.result)  # no intervening await
gate.release(gated.ticket)
# On cancellation/timeout before finish: gate.abandon(permit).
```

The binding is an existing trusted root object with `run_id`,
`runtime_incarnation`, `connection_generation`, `database_generation`,
`min_data_seq`, and `provider_version`. The gate replaces caller actor fields
from the identical canonical `ReadAuthority` object, using the destination's
complete key expected by `SharedReadAdmission`. It checks current authority at
dispatch and again on both sides of the final local snapshot await. It never
raises the global minimum read cursor merely because a restriction remains
unapplied. Current source restrictions remove whole dependent cross-source
items, including their text, metadata and continuations. Affected result-level
continuations are removed too. Unsupported non-source kinds, variants and grant
dependencies fail closed. This module does not implement private-draft/case IAM.

Permits/tickets are bounded RAM identity objects with deadlines; copied fields
do not confer permission. A local write changes the synchronous mutation fence
before the cancellable write begins and after it drains, including UNKNOWN.
`issuance_current` checks this revision, verifier capability and current runtime
binding without SQLite/provider work. The root must serialize before this last
fence and must perform no await between success and actual issuance. The root
owns participant mutation serialization/current authority at that boundary;
this source fence alone is not an atomic cross-thread participant-grant lock.

## Native genesis and history recovery

`NativeSourceHistoryVerifier` runs during explicit controller maintenance,
outside ordinary ready reads. It receives a bootstrap PG pool acquire function,
the actual closed root route dictionary (`source.search`, `source.fetch`,
`source.withdraw`, plus `source.import` when implemented), the current binding
callback, immutable HandlerRegistry, current writer, and independently pinned
native catalog/role-ACL hashes. `inspect_acl(connection)` returns bounded safe
metadata bytes for pin preparation: `pg_roles` safe attributes, memberships,
schema/table/column/function/default/database ACLs and role settings. It never
reads `pg_authid` or passwords. Writer/reader privilege bypass and any membership
are rejected; `rkb_bootstrap` is the explicitly trusted maintenance superuser.
An exact hash comparison additionally rejects unapproved ACL/default drift.

`establish_genesis(binding)` takes the canonical intake lock and native SHARE
locks on source/recovery/applied tables. It proves the native source/projection
tables and applied history empty, the native applied cursor zero, local intake
empty, and the exact remote journal head absent. Only then does the existing
RecoveryJournal writer-fenced CAS publish/read back an immutable generation
genesis checkpoint. The checkpoint binds stream, DB generation, sequence zero,
catalog, ACLs, handler fingerprints, route names and loaded verifier hash. A
request nonce and exact current run/incarnation/connection/provider binding
belong to the retained verifier capability. PG transaction/lease exit must
succeed before that capability is installed.

`restore(binding)` requires that exact checkpoint and traverses every manifest
and segment to genesis within explicit time/hop/command/byte budgets. It checks
contiguous sequences and exact pinned handler dependency classifications, then
streams native applied receipts as the exact accepted prefix: original command
bytes/hash, operation, generation, manifest, handler, status and outcome
bytes/hash. Existing remote outcomes must match the same native bytes. Every
native source must have an applied canonical import origin in that verified
history; raw fixture data or a preexisting corpus cannot be relabelled genesis.
It restores only compact derived per-source restriction/pending heads into the
same queue spool. It never restores corpus, bibliography or independent grants
into DevCoveer, never reruns a model choice, and provides no non-genesis snapshot
shortcut. Exceeding any verification budget keeps coverage unavailable.

`renew(binding)` can cheaply renew an expired retained capability only when
the exact previously verified pointer/token, native applied cursor, DB/writer,
catalog, ACLs, registry, actual routes and binding remain unchanged. It reads
the bounded head pointer and native metadata, without traversing old segments
or reading corpus rows. It never treats maximum sequence as a prefix proof.
Run this through existing maintenance outside search serving. A changed
accepted/applied prefix requires explicit full verification in this version.
Full genesis replay has finite command/byte limits; a permanent verified remote
checkpoint and incremental native-prefix extension remain an explicit scaling
acceptance gap. Full replay must never be introduced on the read path or as
blocking query maintenance merely to hide that gap.

Coverage expires within a bounded lifetime and is revoked by runtime generation,
route object or registry changes. The controller must call
`invalidate_source_history()`/`gate.invalidate()` **before** DDL, grants, route,
writer or generation changes, and before effects outside this source-only path.
Exclusive trusted writer/bootstrap connections and the closed root route
inventory remain necessary: a broad writer SQL privilege is not itself proof
that arbitrary SQL is routed through canonical intake. Legacy writers must be
disabled or integrated before serving. Verifier/catalog/ACL changes require an
explicit recovery-format migration; an old checkpoint is never accepted under
new bytes implicitly.

## Storage and verification limits

The default logical spool reservation is512MiB. Intake reserves command bytes,
configured maximum outcome bytes and bounded head/seal/receipt metadata before
remote acceptance; new work backpressures before it can become promised. The
4h raw retention setting never deletes active, pending, UNKNOWN or restriction
rows. This phase implements no destructive GC: raw rows and compact receipts
remain retained until a later reference-aware canonical retention integration.
Therefore this is bounded capacity/backpressure, not a claim that bounded
physical DB/WAL size or full reference-aware4h retention has been achieved.

SQL005 is an immutable ordered migration after001–004. Its pinned invoker
check/apply functions enforce exact source eligibility, update withdrawal and
use the existing same-transaction frontier proof. It preserves readiness only
if the pre-transaction projection was already current; unready/stale state
cannot become ready by withdrawing a source. Effect, eligibility invalidation,
applied receipt/cursor and frontier promotion commit or roll back together.
Deployment must still verify exclusive writer/DDL grants and actual loaded
function hashes. There is no SQL from caller payload and no model/provider or
Python callback inside the PG transaction.

Synthetic SQLite/journal/output-gate/native-boundary fault tests run without
providers; the gate also enters the actual `SharedReadAdmission` with the real
canonical authority. Fake native adapter tests are explicitly not PG acceptance.
Real PG tests are opt-in
in `tests/test_source_withdraw.py`, reusing the strict root-owned fixture; they
must run serially with other rkb_v4 suites. This agent did not execute PG,
providers, live grants/schema changes or production effects.
