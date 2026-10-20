# Canonical source restriction intake

`control_spool.py` adds one source-only canonical write hook inside the existing
BgeQueue SQLite database. It reuses Participants, the immutable HandlerRegistry,
RecoveryJournal acceptance, and atomic PG apply. It owns no dispatcher, service,
identity registry, provider adapter, second journal or public MCP route. The
source baseline is `cba215a33333d30c4914c3bb99dc83f75e3b0410`; the installed
Kaggle debugging skill/profile and applicable migration/lifecycle references
were read from `/home/dev/.local/share/devcoveer/auth/codex/skills`.

Physical placement is deliberate: DevCoveer retains this thin bounded control
hot tail, compact receipts, exact restriction intents and existing rights.
Corpus, passage bodies, bibliography, projections, FTS and vector indexes remain
the target Kaggle PostgreSQL/pgvector plane. This source-only spool accepts a
fixed withdrawal payload and source/revision identities; it cannot stage
book text, vectors or a shadow corpus. Its rows are derived exact existing
journal commands, not an independently maintained source-policy authority.

This phase does **not** enable public QUERY or prove withdrawal safety across
existing serving routes. Root must connect all applicable write/revoke paths
and the current output gate first. A remote accepted intent is not product
`closed=true`, PG Saved, external derivative purge, or proof of live deployment.

## Existing controller API

At explicit startup, outside read serving:

```python
spool = ControlSpool(existing_queue, participants, recovery_journal, registry)
entry = await spool.stage_source_withdraw(internal_request_authority,
    resolved_exact_command, database_generation=owned_database_generation)
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
handler key. A private-draft write class is actor-scoped even if it read public
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

## Bounded facts for the next output gate

```python
scope = await spool.scope_state(None)       # whole accepted-public source query
scope = await spool.scope_state(source_ids) # at most32 server-owned finite IDs
items = await spool.barriers_for(all_item_source_ids) # at most160 =5*32
page = await spool.pending_page(after_key=cursor, limit=100)
```

These are read-only SQLite snapshots, off the event loop, with no cloud access
or writer lock. Whole-scope counters/cursors are updated atomically with spool
transitions. Finite scope uses a covering source/state aggregate. Per-source
views seek bounded heads through indexes: latest local intent, latest remotely
accepted restriction, pending sequence and unknown sequence. A newer unsealed
intent cannot hide the older accepted key/hash/sequence/provenance. Counts and
maximum sequence alone are not proof of global history continuity.

Every view explicitly reports `history_complete=False` and
`scope_complete=False`: this source-only hook cannot attest legacy write paths
or reconstruct absent remote history. An empty list/zero count cannot become an
empty successful search or proof of visibility. A missing source in top-K does
not prove it was irrelevant; the separate query-scope check is required even
for zero-item results. Auth/profile revisions, ordered command sequence, and
database-local PG visibility_epoch remain distinct cursor domains.

The later gate must recheck current identity and unresolved relevant scopes at
actual dispatch and delivery, drop whole cross-source affected items and their
metadata, and return degraded/evidence_withdrawn even if no item survives.
Missing provenance, unknown/compacted history or unproved independence must fail
closed. An unrelated typed private-draft pending write must not block public
books. Normal unchanged ready reads must use local facts, not a second remote
roundtrip. That gate/projection and legacy writer migration are not implemented
or claimed complete here.

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

Synthetic SQLite/journal tests run without providers. Real PG tests are opt-in
in `tests/test_source_withdraw.py`, reusing the strict root-owned fixture; they
must run serially with other rkb_v4 suites. This agent did not execute PG,
providers, live grants/schema changes or production effects.
