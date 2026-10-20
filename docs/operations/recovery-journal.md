# Recovery journal foundation

`regional_knowledge.recovery_journal` implements bounded immutable command seals,
verified readback and a conditional manifest head. It is a library boundary for
the existing controller, not another dispatcher or domain queue. It has no
production storage adapter and is not wired into product ACKs. Local fixture
tests do not establish provider durability, accepted/Saved behavior or deployment.

The contract follows implementation handoff §§6.1–6.6. The installed skill,
`rkb-profile.md` and `migrations.md` were read from
`/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging`;
the handoff/source baseline was `85676094cf1a019fd34a29daa930d4ebb16eedb7`.

## Controller integration

1. Authorize intake with the existing authority. Persist/fsync the exact command
   envelopes, batch grouping and predecessor reference/token in its bounded local
   spool before calling `seal`. Resolve IDs, logical times, revision preconditions
   and model decisions once; never choose them again during replay.
2. Form a bounded group in the existing uploader. There is no accumulation timer
   here: a ready singleton seals immediately. Any controller batching window must
   be at most one second and must respect count/byte budgets. Ephemeral
   `read_query` has no journal operation; its command kind is rejected.
3. Call `seal(commands, predecessor=head, writer_generation=generation)`.
   `AcceptedSeal` proves the exact group is in the verified remote manifest
   ancestry. Transport receipt, uploaded command binding, segment or immutable
   manifest alone does not prove acceptance.
4. Apply accepted commands using the existing fenced PG transaction:
   domain effect + original-generation `applied_ops` outcome + continuous applied
   cursor. That integration is outside this module. Use the recorded typed
   rejection or exact committed outcome after an uncertain SQL ACK.
5. Call `record_outcome` with the original database generation and exact recorded
   outcome bytes plus the current writer generation. It verifies acceptance and
   publishes the create-only outcome binding through an atomic generation-fenced
   CAS, then verifies exact remote byte readback,
   returning `RemoteOutcomeReceipt`. The caller must independently prove the PG
   commit, original-generation reconciliation, authorization and delivery fence
   before a product Saved/result ACK. Neither receipt emits an ACK or verifies a
   SQL commit. Derived embeddings/render output must not replace canonical body,
   evidence, IDs/revisions, status or cursor in the domain outcome.

```python
head = await journal.head()
# Existing spool fsync must already cover the exact batch and this predecessor.
accepted = await journal.seal(
    exact_commands, predecessor=head, writer_generation=current_writer_generation,
)
# Existing PG writer records/applies the same commands and returns committed bytes.
remote_outcome = await journal.record_outcome(
    accepted, command=exact_commands[0],
    database_generation=original_database_generation,
    outcome=exact_applied_ops_outcome_bytes,
    writer_generation=current_writer_generation,
)
```

`ExactCommand` preserves payload bytes (base64 in the envelope), payload
SHA-256/length, sequence, operation/request/person/route identity, chosen IDs,
logical time and schema/function/executor revisions. `kind=schema_transition`
places migration intent in the same ordered history. Decoding and executing a
transition with the matching artifacts, draining old accepted commands and
reconciling old SQL UNKNOWN remain the existing controller/writer's job. This
module cannot safely apply old commands through the newest functions.

## Storage adapter requirements

`ConditionalRecoveryStore` is deliberately separate from the existing
`ObjectStore`: its `put_bytes` overwrites and cannot implement this contract.
An adapter must provide:

- Strong read-after-write snapshots with opaque native version tokens. Verified
  absence returns `None`; timeout/authentication/provider errors are not absence.
  `read(max_bytes=...)` must enforce the bound before downloading unbounded bytes.
- Native atomic create-if-absent for immutable objects. Existing identical bytes
  are idempotent; differing exact bytes conflict. Multipart initiation is not
  completion, and a locally computed checksum is not a provider version token.
- Native conditional head replacement against the exact prior token (or verified
  absence), with writer-generation authority enforced at the publication boundary.
  Canonical outcome creation uses the same fenced CAS with `expected_token=None`;
  it must never overwrite an existing outcome. Generation checks apply even when
  a create condition fails because an identical outcome already exists.
  A separate earlier fence check is insufficient. A retired writer may leave
  orphan bytes but must not publish a head. Authority restoration/epoch change
  needs its own verified control checkpoint and fencing integration.
- `StoreUnavailable` for uncertain transport outcomes, `StaleWriter` for a denied
  generation, and a definite false return for a failed conditional precondition.
  Map real SDK/provider errors explicitly; do not catch every provider failure as
  successful upload or emulate CAS with read-then-unconditional-write.

Real provider/bucket conditional writes, error mapping, concurrent requests,
cross-process visibility, credential scope and lost-response behavior must be
verified before wiring this into real accepted/Saved routes. No capability is
claimed for S3/Yandex/Kaggle by the deterministic memory fixture.

## Immutable layout, retries and bounds

Keys are scoped under a hash of the authorized stream. This naming is not an ACL;
the caller and storage adapter must enforce tenant/resource ownership. Every
command operation key gets an immutable exact-envelope binding, preventing
changed IDs/payloads/sequence under the same key without scanning old history.
Segments contain the exact envelopes, contiguous first/last sequence and count,
and a reference to the preceding manifest. An immutable manifest binds the
segment's SHA-256/length and predecessor; the small mutable head points to it.
Both levels are read back before publication and again when proving acceptance.

A normal successful append reads the current node and segment, writes only its
new immutable objects and CASes the head. It does not list the bucket or scan the
whole history. A raced/uncertain/repeated append walks published predecessor
references up to `max_reconcile_hops`; an exact ancestor is success, a competing
seal at that sequence is a conflict. It never overwrites a fork. Retry using the
same batch, predecessor and exact bytes; arbitrary regrouping cannot be treated
as an identical group receipt.

Lost upload ACK is reconciled by exact key byte readback. Lost CAS ACK is
reconciled by exact published ancestry, including when later groups have already
advanced the head. Absent/unreadable evidence leaves `JournalUnknown` and the
pending operation intact. Retrying UNKNOWN never means inventing another key,
changing sequence or assuming the previous attempt was not delivered. Same key
with different bytes, revisions or original database generation conflicts.
Outcome publication has no unfenced compatibility path: a retired writer cannot
reserve the canonical outcome key and thereby block the active writer. Already
accepted commands remain replayable by the current authority with their original
database generation. Lost outcome CAS ACKs require exact bounded readback; absent
or unreadable bytes leave UNKNOWN. An existing identical outcome can be reused
after a writer handover without changing its canonical bytes or database identity.

Defaults are 1 MiB encoded segment bytes, 128 commands, 1 MiB encoded outcome
bytes and 128 reconciliation hops. Head/manifest reads are bounded at 4/8 KiB;
stream names at 512 UTF-8 bytes. Encoding/base64 overhead counts against byte
limits. Hitting a traversal bound is UNKNOWN, not a partial recovery success.
Limits are explicit configurable safety bounds, not provider quota measurements.
Before base64/JSON encoding, payload and outcome lengths, aggregate identity and
metadata UTF-8 sizes, and mapping cardinality are checked against those same
configured byte budgets. Oversized mappings are rejected before traversal. Each
encoded command is also checked while forming a batch, so fixed JSON overhead
cannot accumulate beyond the segment budget. These preflights bound temporary
encoding allocations to a finite multiple of the configured caps; the final
canonical segment/outcome byte checks still apply and their bytes are unchanged.
The writer generation and final encoded head must fit the existing 4 KiB head
read budget before remote publication, preventing creation of an unreadable head.
Operation bindings add immutable objects per command and duplicate envelope
bytes in segments: measure and budget that cost before production use.

There is no deletion/compaction/retention implementation. Interrupted uploads
can leave immutable orphan bindings/segments/manifests; their existence does not
mean accepted. Orphan bindings deliberately reserve their exact identity until
controller reconciliation; do not rewrite or expire them to make a retry pass.
Real deployment must connect reference-aware remote GC, retained base cursors,
30-day verified idempotency horizons, permanent UNKNOWN references, remote
request/byte budgets and the 4-hour/512-MiB local spool admission policy. These
are prerequisites for product routing, not solved by this library's byte caps.

## Restore and verification

`head()` verifies the current node and segment only. After loss/restart, run
`verify_chain(head, stop=independently_verified_base_manifest)` or verify to
genesis before replay. Holes, overlaps, missing predecessor bytes, unsupported
codec, hash/length mismatch and malformed exact command stop recovery. A base
reference supplied to `stop` must already have an independent restore proof;
it is not inferred from a timestamp or an uploaded pointer.

`read_segment(reference)` returns the predecessor and exact canonical command
envelopes after verifying one seal. Follow references from a verified head back
to the verified base, then replay envelopes in original sequence order with the
matching pinned decoders/schema/functions/executor. A standalone orphan can pass
`read_segment`: byte integrity alone is not proof of published membership.
Retain an original command's database generation in dispatch/applied-op receipts;
querying a replacement generation cannot settle the original SQL UNKNOWN.

Focused tests use synthetic exact bytes and a deterministic conditional store:
upload/CAS lost ACKs; pre-publication crashes; CAS races and forks; writer fencing;
key/hash/revision/sequence conflicts; hole/count/byte limits; missing/corrupt
history; recovery hop caps; and remote-outcome readback distinct from acceptance.
No credentials, remote provider, actual accepted input, inference model, notebook,
database or product route participates in those checks.
