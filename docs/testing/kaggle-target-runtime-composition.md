# Target runtime composition

The controller accepts an explicit `TargetLaunchSpec` through `main(target_spec=...,
recovery_journal=...)`. Legacy launches remain distinct. Target launches require WSS,
an existing Stage0 prepared inventory rooted in `/kaggle/working`, independently
observed dependency and native catalog hashes, and an explicit owned database binding.
No environment variable silently creates this authority or supplies a new journal.

`worker_source(..., target=spec)` assembles the fixed native-safe Python modules and exact
001–006 SQL bytes into one bounded immutable source package. The generated entrypoint
extracts and verifies its content-addressed files, then actually calls
`main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose)`. The controller
computes all final source bytes before persisting `SearchRuntimeExpectation`; the
expected executed-source hash lives outside those bytes. Code, model and launch hashes
come from the declared immutable inputs; dependency/schema hashes come from the
independent Stage0 pins. READY observations must still match the persisted expectation.

Control ATTACH/WELCOME precedes model loading and runtime preparation. Preparation uses
one local-only resident BGE encoder, one bounded reader pool and native database/catalog
checks, including the actual current database/writer generation. Reconnect retains the
same model and pool. Each handler invocation binds the actual run/incarnation to the
existing lossy telemetry adapter. Shutdown drains read cleanup and every tracked native
forward before closing the pool and its owned postmaster on the same event loop.
Repeated cancellation also drains the asynchronous close task.

Fresh controller launches require an explicit typed Stage0Spec before provider
authentication or launch claim. Its download name/size/hash inventory must match every
external prepared model/native/wheel artifact; bundled Python and SQL are extracted
directly and never downloaded. Exact package versions match the dependency-hash
inventory and preserve the four protected BGE runtime pins. Native PostgreSQL binaries
must have explicit executable flags. The complete launch configuration is bounded.
After ATTACH/WELCOME, load_encoder invokes the standard-library Stage0 preparer before
psycopg/pool/pydantic/torch imports. It checks/reuses cached bytes, streams missing pinned
assets and installs only the declared offline hash-pinned wheel closure. A deferred
runtime proxy imports and constructs TargetRuntime only after that preparation and
encoder loading succeed. No downloads, installation or model load occur in Dev tests.

Fresh mode is `OwnedDatabase(mode='NEW_EMPTY_GENERATION', ...)`. Before source generation,
`persist_empty_intent` reads the injected existing `RecoveryJournal.head()` and refuses
any history or UNKNOWN. It persists an immutable run/nonce/database/writer/001–006 hash
binding in the controller store before first attachment. The caller must retain writer
and intake fencing throughout genesis. Fresh initialization creates an exclusive new
leaf beneath `/kaggle/working`; existing, partial or symlinked clusters are refused. It
rechecks exact native file hashes and pg_config-resolved vector 0.8.2 extension paths,
uses Stage0's verified nonroot UID/GID, private Unix socket only, bounded processes and
SQL waits, applies 001–006 in order and initializes cursor zero. No accepted data is
reset, deleted or silently restored. Failed partial initialization stays refused for
explicit investigation.

Bootstrap owns schema/maintenance. Writer and reader have no superuser, database/role
creation, replication, bypass-RLS or membership grants. Reader receives SELECT on serving
and read-state tables plus only the seven read-function identities; writer receives
existing table DML and executor-function access. PUBLIC loses target database/schema,
table and function authority; future bootstrap functions default to no PUBLIC EXECUTE.
The independent source-history verifier must additionally establish the native ACL and
route-registry closure before product intake can open.

`VERIFIED_INITIALIZED` mode requires an explicit receipt bound to the complete database,
schema pin and Stage0 evidence, then probes actual current native state. It never adopts
an arbitrary directory or starts PostgreSQL. Existing accepted history still requires a
real verified restore and ordered replay path. The optional restored_database hook accepts
only the native-safe owner-retained VerifiedInitializedDatabase capability, calls its
identity/generation verification before creating the pool, and closes its owned candidate
after pool draining. A serialized dictionary cannot satisfy that hook. Distributed restore
orchestration and provider verification remain separate from this composition and must
not move Dev controller/identity/storage authority into the notebook. These hooks do not
establish product acceptance, deployment or provider capabilities.

Local verification uses synthetic artifacts, existing conditional-store fixtures,
source extraction redirected into the managed fixture directory, stub native processes
and the actual WSS transport with deterministic peers. It starts no PostgreSQL/model or
provider execution. Actual Kaggle readiness, verified restore, native role/catalog pins,
output completeness and end-to-end product acceptance remain separate gates.

Skill evidence: read the installed full `kaggle-warm-debugging` package at
`/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging`, including RKB,
migrations, lifecycle, transfers and evidence references; project source pin
`2102f08478e532b44f2b0e0dfcba8fb11565ac63`. Also read the installed devserver artifact
policy. No runtime authorization is inferred from those skills.
