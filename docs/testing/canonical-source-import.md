# Canonical resolved source import

`source.import.v1` is a bounded new-source activation command. It uses the existing
owner-authorized `ControlSpool`, remote `RecoveryJournal` acceptance, exact semantic
`HandlerRegistry`, atomic `apply_accepted`, and remote outcome receipt. A resolved
command is not permission: the canonical import workflow must check the current owner
personal route, verified original/archive receipt and reviewed source material before
staging. The builder performs no acquisition, provider call, model selection or inference.

`resolved_import_command` freezes a canonical payload of at most 768 KiB and 100
passages: bibliography; work/edition IDs; original SHA/size/stable provider reference and
verified receipt hash; pinned BGE/tokenizer identity; exact ordered passage IDs, locators,
text, language, normalized 1024 FP32 vectors and complete tokenizer input counts; and an
explicit research frontier ID/state/continuation. Only new source revision 1 with the
exact `absent_source=true` precondition is supported. Existing data is never overwritten.
The bound is an envelope limit. Large books require the resumable multipart staging
workflow; this command is not a complete replacement for that workflow.

`006_source_import.sql` follows the immutable 001–005 prefix. Fresh generation intents
now pin the exact ordered 001–006 list and hashes before initialization, and the source
bundle/code hash includes those bytes. It installs the check/apply functions, immutable
original provenance and persisted research frontier. `source_import_handler` registers
the exact installed native function hashes with explicit semantic revisions and shared
public-source dependency metadata. Existing initialized databases are never silently
patched: native readiness requires the new schema; an old database requires a separately
verified ordered maintenance/replay barrier. No such live upgrade is performed here.

The native check rejects invalid dimensions, non-FP32/nonfinite/unnormalized vectors,
wrong model pins, token overflow, extra keys, duplicate IDs, malformed originals or
unreachable research continuation. The apply function creates accepted/unwithdrawn source,
revision, original provenance, exact projections and reachable research continuation
before activating the revision. It prepares the next read frontier inside the same
transaction as the existing apply ledger and cursor advance. A failure rolls everything
back; duplicate replay returns the original canonical outcome. A stale projection cannot
become ready merely because an import finished. The accepted book text and bibliography
use the existing shared search path without owner filtering; original/scan/private access
continues through independent current authorization.

The original reference is an opaque stable provider receipt locator, not a signed URL or
an instruction to fetch. Neither the payload validator nor a receipt hash proves provider
storage by itself. Provider byte/hash/receipt readback remains a required upstream import
workflow check. A model result accepted earlier is replayed exactly; replay never invokes
BGE or another model.

Local checks exercise resolved-data validation, canonical bytes, registry semantics,
remote acceptance and launch-source/intent binding with synthetic fixtures. Three opt-in
native tests cover activation plus persisted continuation/outcome, duplicate-source
rejection without overwrite, and a mid-transaction failure followed by exact retry. The
native tests require the existing guarded PG fixture; this agent has not started a PG
service, installed dependencies, loaded model weights or called a provider.
