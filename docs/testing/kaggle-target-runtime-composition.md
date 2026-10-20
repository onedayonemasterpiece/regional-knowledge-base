# Target runtime composition

The controller accepts an explicit `TargetLaunchSpec` through `main(target_spec=...,
recovery_journal=...)`. Legacy launches remain distinct. Target launches require WSS,
an existing Stage0 prepared inventory rooted in `/kaggle/working`, independently
observed dependency and native catalog hashes, and an explicit owned database binding.
No environment variable silently creates this authority or supplies a new journal.

For a first real warm bootstrap, `bootstrap_only=True` requires both dependency
and catalog pins to be `None`. Before ATTACH, source assembly freezes the other
four hashes, exact SQL/native/package plan and finite bootstrap deadline. After
actual PG and BGE preparation, the worker emits BOOTSTRAP_OBSERVATION with bounded
native catalog bytes and actual dependency metadata, rather than READY. Reads and
normal document/source jobs stay closed. The trusted controller independently
reviews those bytes and calls `queue.finalize_bootstrap_runtime` with their exact
observation digest, full SearchRuntimeExpectation and retained review reference.
This one-time transition requires the same current attachment/incarnation/provider
version before the deadline and cannot alter any frozen anchor. WSS has no pin
finalization operation. The retained candidate can then promote without a reload;
unfinalized workers stop at the finite deadline and drain runtime cleanup. A later
active manifest change still requires the ordinary ordered update barrier.

Native child processes receive a clean PATH/LANG/LC_ALL environment. Only library
directories containing exact files in a current retained native bundle may enter
LD_LIBRARY_PATH; ambient LD_PRELOAD/LD_LIBRARY_PATH are excluded. The configured
nonroot numeric service account must resolve to the exact configured primary GID
before initdb. The same exported verified_native_environment helper is available
to owned backup/restore tools.

For the real controller invocation, serialize exactly `TargetLaunchSpec.wire()`
to a bounded regular JSON configuration file. `load_target_launch(path)` reconstructs
the typed database, Stage0 and native bundle descriptors and rejects unknown or
duplicate fields, symlinks, oversized files and changing bytes. The existing controller
accepts `main(target_config_path=path, recovery_journal=existing_journal)`; equivalently
`RKB_BGE_TARGET_CONFIG` supplies that metadata path. This selects the same controller
loop, queue and stable notebook slug. It does not create a journal, writer guard,
credentials or artifact-hosting service. A fresh target file without the injected
real journal is refused before provider authentication. The plain script without
target metadata remains the existing legacy controller.

`build_bootstrap_target` in `kaggle_bootstrap_config` combines the public protected
six-file BGE metadata, one exact wheel-lock receipt, actual native manifest bytes
and published native input receipt/download rows into the complete typed config.
It checks the actual manifest hash and all six native paths/executable flags,
retains the full wheel closure, uses an exclusive generation-derived private
`/kaggle/temp` cluster and leaves the two unavailable pins as None. Pass explicit
database/writer generations, candidate service UID/GID and identity evidence; no
secret URL is generated or logged by the builder. Serialize its `.wire()` only
into the private existing controller state directory. Target controller launches
also require `RKB_BGE_DOCKER_IMAGE=gcr.io/kaggle-images/python@sha256:<64 hex>`;
the generated launch config retains that image and provider metadata uses
`docker_image_pinning_type=original`. A mutable latest image is rejected.

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

The authenticated early environment observation includes actual Python/ABI metadata,
current effective UID/GID and bounded supplementary groups, plus local nonroot account
name/UID/GID candidates confirmed by the system numeric-account lookup used by initdb.
Only bounded `/etc/passwd` bytes are examined; password, home, shell and personal fields
are never emitted. Unavailable/oversized account metadata is reported explicitly with
no candidates. This observation neither selects an account nor grants READY or native
ownership; the controller still pins an explicit service identity and workspace. Older
ABI-only observations remain parseable.

Fresh controller launches require an explicit typed Stage0Spec and NativeBundleSpec before provider
authentication or launch claim. Its download name/size/hash inventory must match every
external prepared model/native/wheel artifact; bundled Python and SQL are extracted
directly and never downloaded. Exact package versions match the dependency-hash
inventory and preserve the four protected BGE runtime pins. The native descriptor binds
one exact top-level code manifest; its pinned package archives are also top-level code
entries. PostgreSQL paths must lie in that manifest's content-addressed prefix. The
complete launch configuration is bounded.
After ATTACH/WELCOME, load_encoder invokes the standard-library Stage0 preparer before
psycopg/pool/pydantic/torch imports. It checks/reuses cached bytes, streams missing pinned
assets and installs only the declared offline hash-pinned wheel closure. A deferred
runtime proxy imports and constructs TargetRuntime only after that preparation and
encoder loading succeed. No downloads, installation or model load occur in Dev tests.

After the encoder is available, TargetRuntime invokes the standard-library native bundle
preparer in a worker thread. It verifies the manifest, OS/ABI, exact package archives and
bounded nested file inventory before extracting an exclusive prefix or reusing a fully
rechecked prefix. Package scripts never execute. The retained VerifiedNativeBundle
receipt supplies the exact nested file pins to OwnedPostgres, which rechecks the receipt
and intent binding before using any native binary. Manifest and package hashes anchor
the nested code without expanding the top-level prepared-artifact inventory. Reconnect
retains this preparation and the existing pools.

Before nonroot PostgreSQL starts, all exact verified code files receive only the service
group read permission and declared native executable permission; exact owned parents
receive traversal permission. Owners, hashes and world permissions stay unchanged, and
group write is removed. Model/tokenizer files and private generated launch source are
excluded. The working root must already be traversable by the verified native identity.

Fresh mode is `OwnedDatabase(mode='NEW_EMPTY_GENERATION', ...)`. Before source generation,
`persist_empty_intent` reads the injected existing `RecoveryJournal.head()` and refuses
any history or UNKNOWN. It persists an immutable run/nonce/database/writer/001–006 hash
binding and native bundle descriptor in the controller store before first attachment. The caller must retain writer
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
