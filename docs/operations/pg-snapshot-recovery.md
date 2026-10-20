# Physical source snapshot and recovery boundary

Implementation baseline: `4e4b5d0`, with source prefix extension/genesis codec
compatibility in `071bf0b` and native-safe helpers in `b4b96f6`. This is code and
synthetic fault/composition evidence. No agent-created production backup,
provider upload, real PG restore, distributed restore, or product acceptance is
claimed here. The Kaggle debugging skill/profile/migrations/lifecycle/evidence
were read from the installed `kaggle-warm-debugging` directory. Synthetic test
outputs are temporary managed artifacts under
`/home/dev/artifacts/regional-knowledge-base/20261020T150032Z-source-snapshot-recovery-20261020`.

`pg_snapshot_recovery.py` runs on DevCoveer. It owns no corpus or SQL transport.
It reuses the existing RecoveryJournal, immutable accepted envelopes/outcomes,
S3 client/prefix/writer guard, and controller maintenance authority. Native
archive bytes move directly between private provider storage and Kaggle through
existing scoped file tickets. Controller verification streams and hashes bytes
without creating a Dev shadow copy or using the 512MiB control spool. There is
no notebook bucket credential, public Dataset publisher, new service, or new
dispatcher in these modules.

`kaggle_pg_snapshot_native.py` and `kaggle_restored_database.py` are the notebook
closure. They import native PG/typed helpers and no Participants, SQLite corpus,
story registry, control spool, source-history verifier or S3 client. Native
backup binaries need actual Stage0 path/size/hash/exact PG17-version pins and
their installed dependencies. This does not claim a copied binary is portable.
Stage0 must prove the workspace is private and excluded from automatic notebook
publication; an arbitrary `/kaggle/working` directory is insufficient. Both
private storage and workspace callbacks come from trusted existing authority,
not JSON flags. Native UID/GID, disk, file, archive, unpacked, cursor, tail and
deadline budgets are explicit. The live source cluster must be dedicated RKB.

Before capture the controller closes new applies under existing maintenance
authority and takes the canonical intake lock. The source-history capability
must prove the exact drained prefix: accepted equals native applied cursor,
every accepted outcome is durable, and the authoritative head is unchanged.
Native capture runs pinned `pg_basebackup --wal-method=stream` and
`pg_verifybackup`. It records original logical DB/writer generation, actual
system identifier, PG/extension/model/tokenizer/schema/function/executor pins,
internal cursor, streamed immutable applied-prefix fingerprint, required native
backup-manifest hash, complete archive inventory/hash/size and provider native
version. It refuses live PID/standby/recovery files, symlinks, unsupported archive
members, traversal, duplicates, insufficient disk, and an advanced source cursor.
Accepted model choices and embeddings are bytes in the existing commands/base;
capture and replay perform no inference.

The controller independently reads back the exact provider stream before
sealing content-addressed physical and source-control manifests. The control
checkpoint preserves compact accepted restriction heads, including a rejected
withdrawal whose accepted intent still closes output. Empty local state and a
maximum sequence do not establish this checkpoint. An uploaded candidate is not
the current recovery pointer.

For recovery the controller persists an exact stable operation intent before
native preparation. Native preparation downloads only the exact scoped artifact
version, verifies archive/native manifest/inventory, and starts an exclusively
created private candidate. Live PGDATA is never copied or overwritten. Candidate
listeners, archive commands, preload hooks and standby modes are disabled; no
jobs/outbox/provider worker is started. Native readback must match the physical
system identifier, original writer, exact internal cursor and applied prefix.

The retained base manifest is an exact ancestor of the current accepted head.
Only the contiguous tail after that boundary is replayed, in original sequence
with original schema/function/executor revisions and pinned handler semantics.
The controller checks actual journal acceptance immediately before dispatch.
Native replay uses the existing `WorkerSourceExecutor`/`ControllerAcceptedProof`
path; it never receives the remote journal store or re-runs a model. Exact native
receipts are reconciled with remote outcomes before the next terminal boundary.
An existing different outcome, unknown acceptance, gap, unsupported retained
handler or migration executor blocks recovery. There is no latest-function
fallback. The current fixed worker supports the import/withdraw handlers it
actually contains; older revisions or ordered migrations need the corresponding
retained fixed executor and explicit barrier before use.

Only after the entire fixed old accepted tail and outcomes are reconciled does
the candidate roll the PG writer from old to new. Native catalog, generation,
revisions, projection cursor and SQL lexical/dense probes then run. The original
logical DB generation is preserved through replay so existing exact outcomes
retain their meaning; runtime/connection and writer generations fence the new
incarnation. Changing logical DB identity needs its own ordered boundary.

`VerifiedInitializedDatabase` is an identical object retained by the native
factory. Its `verify_for(database_spec, run_id, launch_nonce,
runtime_incarnation)` rechecks native state and returns only reader connection
arguments. A parsed receipt, copied dataclass, or arbitrary native-reference
string cannot create this capability. `TargetRuntime` may adopt it before pool
creation; its model object can remain owned by the existing supervisor. A Dev
`ControllerRecoveryProof` separately retains the checked native reference and
source-control checkpoint; source-history adoption requires that proof and
fresh native/binding checks.

Recovery-pointer promotion is a separate existing writer-fenced CAS/readback
after physical restore, exact tail and readiness proof. Active routing remains
the controller's separate decision. The former verified base/tail remains
retained; no remote GC is implemented here. Native process ownership and unknown
prepare outcomes remain visible, same operation keys reconcile instead of
blindly spawning another candidate, and a verified PG child exit is not a claim
about Kaggle execution termination.

The controller's typed native adapter must connect `capture`, `prepare`,
`apply`, `finish`, and `verify_initialized` to the existing authenticated owned
WSS/control lifecycle and scoped artifact transport. Root must bind its active
control writer/claim outside the retained session while the restored PG still
has its old writer during replay. The normal source-dispatch binding must not be
weakened to impersonate the retired writer. This registration, real scoped
cross-host byte transfer, native PG17 backup/restore, actual S3/Dataset readback,
and loss/reconnect drills are still acceptance work. Injected-adapter tests do
not prove those boundaries.

The existing durable jobs/admission/channel now registers `recovery_phase` with
fixed capture, prepare, apply, finish and fresh verify phases. Only a configured
retained `RecoveryPhaseDispatch` can enqueue it. The maintenance assignment uses
the actual shared document compute claim and durable receipt ACK, including before
READY; ordinary document/source/read work remains READY gated. Each native phase
retains its request hash before effects and known result across reconnect. An
unknown phase refuses blind retry. Native replay wraps the existing fixed accepted
source executor with the original database writer, then finish compares all tail
receipts before writer rollover. The current maintenance run/writer remains the
outer authority. `WssSnapshotPhases` is the concrete controller adapter.

`TargetRuntime(recovery_worker=...)` waits for that native owner's retained
`VerifiedInitializedDatabase` while the channel continues maintenance traffic.
The fixed worker needs Stage0 verified native tools, private workspace policy and
scoped direct-transfer callbacks installed by trusted composition. The wire cannot
supply filesystem paths, SQL, credentials or replace those callbacks. Native bundle
construction, provider-ticket registration and the physical fixture drill remain
separate acceptance work; the synthetic WSS tests do not prove those effects.
