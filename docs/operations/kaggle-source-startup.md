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

Current limit: the startup composition and fail-closed lifecycle are implemented;
the production WSS native history proof adapter is still required. The explicit
entrypoint cannot produce a working public source service without that adapter,
an actually prepared/ready target runtime, pinned native closure and verified
remote recovery durability. The synthetic startup tests prove owner reuse,
OAuth-backed shared admission, full/Live routing and closure on missing proof;
they do not prove deployment or actual Kaggle startup.
