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
