# Explicit Kaggle source startup

The existing server entrypoint accepts `main(target_startup=...)`, where the value
is a `TargetServiceStartup(TargetServiceConfig(...))`. Set
`RKB_BACKEND_PROFILE=kaggle_source` to make a missing typed composition a startup
error. An explicitly supplied target startup also selects this profile. There is
no fallback to `backend_from_env`, SQLite source text, E5 or Supabase in this mode.
The ordinary `RKB_MCP_PROFILE=full|live` still selects the public tool bundle.

The config takes the existing RegionalOAuthProvider, its exact personal
Participants projection, BgeQueue, ControlSpool, current writer generation/guard
and a typed NativeHistoryProofAdapter. The spool retains the existing
RecoveryJournal and immutable handler registry. Startup reuses the queue's
canonical SharedReadAdmission, or configures it once with the same OAuth and
participant authority. It binds actual search/fetch/import/withdraw route
callables, SourceReadGate and the accepted-source dispatcher to those owners.
No credentials, grants, source corpus, second queue or storage authority are
created by the startup module.

MCP lifespan begins with public reads closed, keeps the existing WSS control route
available for attachment, and runs bounded history maintenance off the ready read
path. A verified current binding and retained native history coverage are both
required before public source search/fetch or Live knowledge_search can dispatch.
Unknown observations, proof expiration, writer/runtime changes and maintenance
failure close routing. Full-profile domain tools that have no target native
implementation return `target_domain_unavailable`; they cannot read legacy
content. Story-only capability profiles are refused. Shutdown invalidates read
tickets, drains accepted-command remote-outcome publication tasks and releases
local admission/spool ownership without clearing durable jobs or claiming compute
termination.

The native history adapter must implement fixed bounded SOURCE_HISTORY
observations over the existing maintenance WSS job path, with exact current
runtime/generation binding. It must retain native catalog/ACL/prefix proof and
verify remote journal bytes on Dev. The protocol intentionally takes no SQL,
arbitrary filesystem path, user bearer or cloud credential payload. A direct Dev
connection to Kaggle's private Unix socket is not a supported production adapter.

The fixed `WssNativeHistoryProofAdapter` verifies observations through the
maintenance channel. The explicit entrypoint still requires actual native
SOURCE_HISTORY registration, independently pinned facts,
an actually prepared/ready target runtime, pinned native closure and verified
remote recovery durability. The synthetic startup tests prove owner reuse,
OAuth-backed shared admission, full/Live routing and closure on missing proof;
they do not prove deployment or actual Kaggle startup.

## Production owner entrypoint

`server.main()` now has a fixed target entrypoint when
`RKB_BACKEND_PROFILE=kaggle_source`. `RKB_KAGGLE_SOURCE_CONFIG` must name an
existing private regular JSON file, at most 64 KiB, with exactly these fields:
`codec=rkb.source-service.v1`, `run_id`, `stream`, `database` (the actual owned
database wire descriptor), `command_handlers` (the fixed import/withdraw function
pins), `catalog_sha256`, `acl_sha256`, and `observer_sha256`. Missing or invalid
metadata aborts startup; it cannot select the legacy backend.

The entrypoint reuses the existing `RKB_BGE_QUEUE_PATH` file, configured OAuth
state/key and canonical Participants projection through the existing personal
authentication constructor, and the already configured native S3 object-store
authority. It creates no user, grant, credential, corpus file or namespace choice
from a request. The existing participant database is used only for identity and
permissions; target content is not retrieved from it. The explicit
`compose_source_service(...)` function assembles the same owners when they are
already retained by the application. Its returned value is the real
`main(target_startup=...)` input.

Construction configures the same admission before attachment/readiness and
leaves the writer inactive. Lifespan maintenance waits for accepted native READY
and the current resource baseline, activates the initial writer, and then
establishes native history coverage. Reads remain closed throughout these steps.

`QueueWriterAuthority` persists its generation, database, run, stream and exact
storage-namespace fingerprint in the existing queue/control store. It activates
only after the current native runtime proof is accepted at cursor zero, the
pre-attachment empty-generation intent matches, the actual native journal head
is absent, and local intake is empty. Exact active-owner restart does not reset
history. A retired/mismatched writer or existing accepted history cannot be
silently adopted; same-database writer rollover still needs the ordered verified
recovery barrier. Root storage/provider conditional-semantics verification is a
separate operational prerequisite, not a fact inferred from this constructor.

Every conditional S3 publication holds a cross-process advisory lock and records
the exact key, body SHA-256/length and native expected token before provider I/O.
The queue stores no raw publication body. Ordinary source read checks remain
read-only snapshots and hold no provider lock. Control handover and retirement
cannot pass an in-flight or UNKNOWN publication. Cancellation drains the native
SDK call before releasing the lock, and conservatively preserves UNKNOWN.
`writer.reconcile(store)` clears it only after exact native byte readback;
`exact_data=...` permits only a conditional retry of the originally recorded
bytes. Absence, a boolean, timeout or an unrelated storage client cannot clear
UNKNOWN. No unbounded retry loop or unconditional write is introduced.

The initial writer owner and fixed production wiring are covered by actual
SQLite/OAuth and synthetic native-S3 race/cancellation tests. They perform no
Kaggle, PostgreSQL, model or real object-store operations. Native SOURCE_HISTORY
registration, exact current function/catalog/ACL pins, real provider recovery
proof and actual public read acceptance remain required before product routing.

## Generated notebook restore owner

`TargetLaunchSpec.recovery` is an explicit `SnapshotRecoveryConfig` for
`VERIFIED_INITIALIZED` launches, or fresh launches explicitly enabling the
first-base owner below. It declares the exact bundle-relative
`postgres`, `pg_basebackup`, and `pg_verifybackup` paths and complete version
outputs, allowed HTTPS storage origins, observed nonroot UID/GID, and finite
archive/unpacked/time budgets. The existing native bundle, package closure and
command handler pins remain mandatory. Old launch wire without this optional
field remains valid; a receipt dictionary never supplies a restored capability.

The generated notebook registers one retained `DeferredRecoveryRuntime` and
`SnapshotTicketChannel` on the existing WSS transport before runtime preparation.
ATTACH/WELCOME binds its current incarnation and connection generation. After
Stage0, the owner rechecks the native bundle, grants only its verified native code
access, builds the existing `WorkerRecoveryPhases` through
`TargetRuntime.configure_recovery`, and waits for its retained verified database
capability. The reader pool, queries and READY remain unavailable until physical
verification, exact accepted-command replay and native final checks complete.
Reconnect preserves the owner/model/session and invalidates the detached
generation. Native children use only the retained bundle's verified library
paths; no ambient `LD_*` settings or cloud credentials enter the notebook.

The controller must supply its existing pre-READY `RecoveryPhaseDispatch`,
`SnapshotTicketIssuer`, and `PhysicalSnapshotRecovery` operation. The notebook
hook alone does not orchestrate that controller operation or prove production
recovery. Unknown operations preserve their original identities and remain
closed. Shutdown closes partial native candidates as well as an adopted pool;
it does not overwrite or delete an accepted cluster to restart a failed restore.

## Dev recovery startup

An initialized source-service configuration may opt into `source_restore`, using
`SourceRestoreConfig.wire()`: an exact existing snapshot manifest reference,
stable operation key, exact native/model/tokenizer/schema versions, the final
launch-config SHA, allowed storage origins and finite archive/time limits. The
source service still reuses its existing queue, participant authority, S3 client,
journal stream, handler registry and admission. No snapshot metadata creates a
readiness capability. The notebook must independently opt into the native
recovery owner described above.

The server lifespan yields its WSS route before background restoration waits for
native phases. The old persisted writer must already be retired and have no
unresolved publication. A different executor cannot take shared admission until
the existing `confirm_terminal` gate accepts a fresh, identity-bound authoritative
provider terminal observation and its verified capability receipt. Heartbeat
expiry, a Stop ACK or a new launch descriptor cannot clear that debt.

`QueueWriterAuthority.begin_restore` records the exact plan, attachment identity
and actual remote journal head in the same control database. Normal writer
checks remain false. Recovery publications retain the existing durable I/O fence
and are limited to that operation's recovery intent and exact outcome keys;
accepted-command/head/index writes are refused. UNKNOWN publication must be
resolved by native exact-byte readback. Fixed WSS phases verify the physical
base, replay the original accepted command bytes and original semantic handlers,
and verify the new writer/readiness. Independent Case/GEO commands do not create
source heads. Writer activation requires the factory-retained controller proof,
unchanged head and accepted current READY at the identical cursor. Public routing
still waits for full WSS native history coverage after that activation.

The configured snapshot must already exist. This restore composition does not
create an initial backup for a deployment whose native snapshot owner was never
enabled, and it does not silently choose or replace a published snapshot.


Fresh11 source intake also requires the exact configured GEO installation. Set
all `geo_manifest_operation_keys` before constructing startup and establishing
its initial route/genesis closure. `imports_current()` opens new source acceptance
only after the retained GEO gate has verified every accepted phase, native apply,
remote durable outcome and exact contiguous activation prefix. Configuring the
keys, READY, a manifest file or an activation ACK alone is insufficient. Ordinary
reads and GEO installation stay available while installation is pending. Already
accepted imports continue exact reconciliation after this proof invalidates.

The measured operational GEO manifest has20 commands/5,680,160 encoded command
bytes; its largest complete SOURCE_COMMAND envelope is752,274bytes. Both source
WSS boundaries already permit2MiB. The configured20 operation keys receive a
separate6MiB WSS history window for their exact fresh11 GEO command and outcome
bytes. Ordinary source/Story/Case history retains its4MiB budget; unrelated GEO
operations cannot use the window. Per-command, per-frame and command-count bounds
remain unchanged. These byte measurements used synthetic in-memory seals with
actual codecs, not remote acceptance or native application.

Cold WSS history currently verifies from immutable genesis; incremental renewal
uses an already verified prefix. This GEO bootstrap allowance does not make
unbounded cold history or post-GC GEO proof reconstruction supported. A verified
snapshot/prefix capability and compact durable GEO proof must cover those cases
before claiming restart/retention acceptance. The current GEO gate still needs
its accepted raw install rows; do not GC them while that restore gap remains.
## First empty base and restore drill

A fresh deployment can opt into `SnapshotRecoveryConfig(drill_empty=True)` and
matching Dev `SourceSnapshotConfig.wire()` under `source_snapshot`. Both retain
exact native binary/version, model/tokenizer, catalog and schema pins, the same
storage origin and writer, and explicit finite budgets. For the measured empty
schema11 base (about 53 MiB), an operator can select 128 MiB archive and 256 MiB
unpacked limits instead of the larger compatibility defaults. The native owner
checks free space before the additional download/restore and returns the actual
free-space observation plus a conservative owned peak bound. This path has no
model reload or second queue.

After the real serving PG A and ordinary native history proof are ready, the
same source-service owner retains the canonical spool exclusion for the entire
empty-prefix operation. It captures A through the existing scoped conditional
PUT ticket, streams the stored blob back for SHA/size verification and seals an
immutable candidate manifest. Public reads and all new canonical command
staging/acceptance/dispatch stay closed; exact already-accepted reconciliation and
maintenance remain available.
The fixed maintenance phases derive a distinct private socket and temporary
writer B from A's identity; caller paths and writer IDs cannot select them.
They conditionally GET the same immutable blob, verify the physical backup,
start B, check its catalog, SQL and dense readiness, and close only B. A retained
drill proof is released only after B's process terminates and socket disappears.
It is not an adoptable serving-database capability.

Before publishing the existing verified `snapshots/current` pointer, the owner
requires that retained proof, fresh native observations of A's exact catalog,
system ID, writer and text/dense frontier zero, an unchanged empty journal head,
and independent blob readback. A stays the serving writer throughout. Capture
alone cannot publish this pointer. The same SQLite control database records
pending, captured and verified states; API restart validates the exact verified
pointer and immutable base without repeating the backup or drill. An unknown
capture needs explicit `SourceSnapshotCoordinator.reconcile()` and an already
durable exact capture result. It cannot allocate another backup or temporary PG.
Unknown shutdown retains B and blocks promotion until that same owned shutdown
is proved. Scoped tests use synthetic transfer/native peers; actual Kaggle
capture, drill, provider readback and recovery remain separate acceptance.
