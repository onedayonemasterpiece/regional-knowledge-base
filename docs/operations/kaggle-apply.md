# PostgreSQL apply ledger and semantic replay

This source unit supplies an atomic PostgreSQL apply boundary in the existing
`rkb_v4` schema. It does not deploy a Kaggle worker, introduce a dispatcher,
authorize a person, publish a remote outcome or issue product Saved. The installed
Kaggle skill/profile/migration reference and handoff §6 were read against
`a496ece14358538979063135e2bee33bd8dc470b`. The prerequisite public journal
`verify_accepted` API came from `8db2497` (local cherry-pick `861d686`).

## Installation and trusted state

`sql/kaggle/002_recovery.sql` is an immutable installation artifact after
`001_search.sql`. It creates tables without `IF NOT EXISTS`: running it twice is
an error, not silent drift acceptance. Execute it through the existing ordered
migration authority. It adds no roles, credentials, grants to callers or
`SECURITY DEFINER` functions; table access is revoked from PUBLIC. Deployment
must deliberately grant the existing trusted writer the required access.

`initialize_database(connection, database_generation=..., writer_generation=...,
revisions=Revisions(...), applied_seq=0)` explicitly initializes one state row and
initial writer history. It rejects an existing state even if the requested values
match. Any nonzero initial cursor is trusted recovery metadata, not evidence of
a verified base; base validation/restoration is outside this unit.

`handover_writer` compares expected/current writer under the same state row lock
used by apply. It records the predecessor, retires the old generation and assigns
the new one in one transaction. The same successful handover can be reconciled
after a lost ACK even after a later handover: predecessor history proves the old
transition while leaving the current writer unchanged. A retired or otherwise
used generation cannot be reactivated.
This is the PostgreSQL fence for the existing controller's authority, not a new
authority or automatic multi-master system. Current writer strings and command
person/route fields confer no authorization on an untrusted caller.

## Exact accepted command to database receipt

```python
receipt = await apply_accepted(
    checked_out_connection,
    journal=recovery_journal,
    seal=accepted_seal,
    command=exact_command,
    database_generation=original_target_generation,
    writer_generation=current_controller_writer,
    registry=trusted_registry,
)
```

The connection must be exclusively checked out, open, idle and use psycopg's
`tuple_row`. A local in-use check rejects overlapping calls on that connection;
the caller must also prevent unrelated tasks from sharing its pool lease. The
command is bounded and copied before the first await. `journal.verify_accepted`
proves actual remote publication and the exact command hash while PostgreSQL is
still idle. A caller-created seal dataclass is insufficient. Journal/provider
I/O finishes before entering the database transaction.

Within one transaction, apply sets finite local lock/statement timeouts and locks
the singleton state row. It checks the exact database and current writer before
looking up duplicates. Same operation key, sequence, exact command bytes/hash and
database generation returns the existing outcome without invoking a handler,
including after a later semantic migration or writer handover. Changed bytes,
preconditions, key or occupied sequence conflict. A different target generation
cannot settle the original operation's SQL outcome.

New work must be exactly `applied_seq + 1` and match the current
schema/function/executor revisions. There is no latest-handler fallback. The
trusted registry must match the command kind and route as well as those three
revisions, and the complete expected-precondition key set must match exactly.
The actual SQL precondition function checks the values against domain state.
False produces a deterministic `rejected_precondition` outcome, no domain effect,
and an atomic ledger/cursor advance. It is a recorded terminal rejection, not a
successful mutation. Invalid function contracts, SQL failures and other errors
roll back the effect, ledger, cursor and semantic transition together.

For a successful check, the same connection invokes the pinned domain function,
records its exact result in a canonical outcome, inserts immutable `applied_ops`
and advances the cursor/revisions. The outcome includes command identity/hash,
chosen IDs, request/person/route, logical time, before/after semantic revisions,
handler fingerprint, status and cursor. Handler result bytes are preserved and
hashed; fresh embeddings, renderer bytes or new model choices must not replace
the recorded canonical domain result. Database/writer generations are dispatch
and apply receipt metadata, not inputs to canonical domain outcome bytes.

`ApplyReceipt` means database apply only. The existing controller must still
publish and verify the exact remote outcome, recheck delivery fencing and apply
product authorization before Saved/result. No callback for those effects runs
inside the database transaction.

## Trusted function registry and migrations

`HandlerRegistry` is a bounded frozen mapping of `HandlerSpec`s. Each spec contains
an exact semantic key, a `PgFunction` for checking and applying, the complete
precondition key set and an optional `MigrationTransition`. Function names are
static `rkb_v4` identifiers; input bytes never become SQL or function names.
Both functions have the exact signature:

```sql
(payload bytea, chosen_ids jsonb, expected_revisions jsonb, logical_time text)
```

Check returns one scalar boolean and must be STABLE. Apply returns one scalar
bytea. Both must be SECURITY INVOKER SQL/PLpgSQL functions, with no set-returning
or procedure contract. Pass the chosen IDs, original time and exact accepted
payload directly; do not generate replacements. Deployment authors must qualify
domain objects, set a safe search path and keep these functions pure PostgreSQL:
no model/provider/network extension calls or user-supplied dynamic SQL.

Each spec pins SHA-256 of the exact UTF-8 `pg_get_functiondef` bytes. Definition,
language, security and scalar contracts are checked under the state lock before
either function runs. Build/pin the registry from reviewed deployment artifacts;
never adopt whatever definition happens to be loaded on each request. Static
versioned names and the existing exclusive DDL authority are required: every
runtime semantic/DDL transition must take the same state row lock. An earlier
hash read alone cannot fence arbitrary out-of-band `CREATE OR REPLACE FUNCTION`.

`kind=schema_transition` requires a declared `MigrationTransition` with exact
migration ID, artifact hash, predecessor and resulting revisions. The accepted
payload must equal its canonical metadata object; the current recorded migration
predecessor must match. The registered static apply function performs the actual
transactional SQL/DDL. Its domain changes, migration outcome, recorded predecessor,
resulting revisions, ledger and cursor commit together. There is no arbitrary SQL
in command payloads and no automatic application of all latest migrations first.

Replay calls `apply_accepted` in original sequence, preserving the existing
recorded IDs/model inputs/times and corresponding registry/function revisions.
An old duplicate returns its recorded outcome, even after a transition. A not-yet
applied v1 command cannot run through v2 functions. The existing controller must
still drain/reconcile all affected accepted work before admitting an incompatible
transition; this module does not own that queue or infer its completeness.

## Failures, limits and verification

Connection/commit loss raises `ApplyUnknown`, retaining the original operation
key, bytes and database generation. Retry on a new connection against the same
original generation: a committed ledger row returns its exact outcome without
another effect; an uncommitted transaction can apply once. A cancellation also
requires reconciliation rather than a claim that commit was impossible.
Transaction cleanup/rollback finishes before connection reuse; a failed cleanup
closes the connection. Local lock/statement settings are transaction-scoped.

Commands and outcomes default to 1 MiB, with configurable lower caps. Metadata is
bounded to 128 entries per mapping and finite string sizes; sequence fits positive
PG bigint. The registry has at most 64 semantic handlers. Function definitions
are bounded to 128 KiB; function result transfer and final canonical outcome have
byte limits. Oversized results roll the transaction back. Lock wait is bounded
to one second and each statement to two seconds. Handler code must remain bounded
and must not introduce a separate work/admission loop.

Focused tests use only the existing strictly verified synthetic PG17/pgvector
fixture and deterministic journal store. They verify atomic effect/ledger/cursor,
failure after effect, concurrent duplicates, exact/hash/precondition conflicts,
gaps and versions, retired writer/handover lock ordering, real COMMIT lost-ACK
reconciliation, v1→migration→v2 replay in a fresh logical generation with identical
outcome bytes, function drift, set-returning function rejection, result caps and
cancellation cleanup. Fresh-generation replay is a fixture proof of semantic
equality; it does not resolve an original-generation live UNKNOWN by itself.
No real GEO, product Saved inputs, BGE, external provider or deployment is used.

The native source executor reconciles an uncertain apply once on a fresh PostgreSQL connection, using the same accepted command, sequence and original operation key. Existing `applied_ops` duplicate lookup returns a committed receipt; model/vector preparation is not repeated. A second uncertain result remains UNKNOWN. A previously confirmed rollback rejection stays a rejection during reconciliation.
