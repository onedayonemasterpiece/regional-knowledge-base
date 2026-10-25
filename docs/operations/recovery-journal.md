# Recovery journal foundation

`regional_knowledge.recovery_journal` implements bounded immutable command seals,
verified readback and a conditional manifest head. It is a library boundary for
the existing controller, not another dispatcher or domain queue. The native S3
adapter has no real-provider verification and is not wired into product ACKs. Local fixture
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
command operation key gets a fenced exact-envelope binding, preventing
changed IDs/payloads or reuse of an accepted operation without scanning old history.
Segments contain the exact envelopes, contiguous first/last sequence and count,
and a reference to the preceding manifest. An immutable manifest binds the
segment's SHA-256/length and predecessor; the small mutable head points to it.
Both levels are read back before publication and again when proving acceptance.

Each accepted manifest has a create-only proof at
`accepted/manifests/<manifest-sha256>`, containing its exact reference. Each
accepted sequence has an occupant proof containing that reference and exact
command/operation hashes. Only independently proven published membership can
create these indexes: a current head, an existing accepted-manifest proof, or a
verified predecessor reached by the durable bootstrap cursor. Uploaded orphan
bytes and caller-supplied `AcceptedSeal` dataclasses cannot create proof. Index
creation uses fenced native create-only CAS and exact readback. Sequence proofs
are completed before the manifest proof.

Every append repairs **all indexes of the current predecessor before advancing
the head**, then indexes its newly published head before returning a receipt.
A crash or lost ACK between head CAS and indexing therefore leaves a gap only
at the current head. The next appender repairs that gap before moving it into
history, so old acceptance never depends on finding it within a moving hop
window. Accepted-manifest lookup plus verification of the target segment is
constant in history length; `record_outcome` does not read intermediate segments.
A successful append has bounded work in the current/new group sizes, and never
lists the bucket or scans the whole history.

If a proof is missing, bounded fallback walks predecessor manifests only; the
target segment is validated separately. Competing accepted ranges are conflicts,
but a false CAS result with an unchanged predecessor head remains UNKNOWN:
an earlier request could still commit. It never overwrites a fork. Retry using
the same batch, predecessor and exact bytes; arbitrary regrouping cannot be
treated as an identical group receipt.

Lost upload ACK is reconciled by exact key byte readback. Lost CAS ACK is
reconciled by exact published ancestry, including when later groups have already
advanced the head. Absent/unreadable evidence leaves `JournalUnknown` and the
pending operation intact. Retrying UNKNOWN never means inventing another key
or assuming the previous attempt was not delivered. The only permitted
resequencing is an evidence-based repair of a **never accepted** binding: old and
new exact envelopes must match in every field except `sequence`, and the old
sequence's accepted occupant must be a different operation. The occupant's
manifest proof, exact segment and command hashes are checked in bounded reads.
Only then may the current authority replace the orphan binding with fenced
IfMatch on its native token. The old head precondition cannot succeed after
another accepted operation has permanently occupied its sequence. An already
accepted operation cannot move to another sequence. Missing occupant proof is
UNKNOWN pending repair/bootstrap; changed chosen IDs, model bytes, preconditions,
logical time or revisions remain conflicts. Database identity cannot change in
an outcome retry.
If a rebind times out and readback still shows the original token, its condition
is still viable: that remains UNKNOWN until exact replacement or a changed token
settles it. Seeing the old bytes alone never proves that a delayed rebind failed.
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
Operation bindings, accepted-sequence proofs and accepted-manifest proofs add
objects/requests and duplicate envelope bytes in segments: measure and budget
that cost before production use. They reuse the same authority and recovery
store; they are not a second scheduler or writer registry.

There is no deletion/compaction/retention implementation. Interrupted uploads
can leave orphan bindings/segments/manifests; their existence does not mean
accepted. Do not expire or manually overwrite a binding to make a retry pass;
use only the verified resequencing rule above. Acceptance/sequence proofs and
bootstrap checkpoints are controller-owned state; worker upload tickets must
not grant access to those namespaces. Their retention is reference-aware and
must cover retained tails, pending outcomes and idempotency consumers.
Real deployment must connect reference-aware remote GC, retained base cursors,
30-day verified idempotency horizons, permanent UNKNOWN references, remote
request/byte budgets and the 4-hour/512-MiB local spool admission policy. These
are prerequisites for product routing, not solved by this library's byte caps.

## Restore and verification

Existing pre-index history is not silently declared indexed. Under the existing
ordered writer upgrade, stop older append semantics, then call
`bootstrap_indexes(writer_generation=...)` until it returns `True`. The traversal
follows verified predecessor references to genesis, without a segment-count ceiling.
Its initial anchor comes from the actual head; its fenced
`acceptance-index-bootstrap` checkpoint retains the exact predecessor across
restarts. Lost progress ACKs re-read the checkpoint and safely repeat indexed
segments. The checkpoint is indexing bookkeeping, not another authority or
recovery base. Accepted membership is also provable from the verified published
head chain when a legacy acceptance index is absent. New appends continue to
repair their own predecessor first.

`head()` verifies the current node and segment only. After loss/restart, run
`verify_chain(head, stop=independently_verified_base_manifest)` or verify to
genesis before replay. Holes, overlaps, missing predecessor bytes, unsupported
codec, hash/length mismatch, excessive JSON nesting and malformed exact command
stop recovery with `JournalCorrupt`. A base
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
history; long recovery chains; old accepted outcomes; crash-gap index
repair; resumable legacy bootstrap; dead-orphan binding repair; late-CAS/409
reconciliation; and remote-outcome readback distinct from acceptance.
No credentials, remote provider, actual accepted input, inference model, notebook,
database or product route participates in those checks.
