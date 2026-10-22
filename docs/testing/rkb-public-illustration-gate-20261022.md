# RKB public illustration output gate — isolated source handoff

**Status: unregistered source-only gate; not public media activated or Kaggle/Telegram live acceptance.**

## Provenance and exact changes

Branch base: PR217 commit 2991af968ad40bef9f6e43d181548fa25b7650c5. Managed media handoff: src_ce236841a07e116e9ace41ce. Its media-only .media-delta.patch is 97,933 bytes, SHA256 701c03106cb2a9436ddd814d08583008dd1065243fd6ce5e6b7a03e81f01a377. SQL015 SHA256 7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773.

Existing unchanged source_illustrations_native.py SHA256 b671afe8d88685e027a71b84c9977a4678332f65e6e2ab0be2bb29d85cddcf23 provides public_illustration_metadata and validate_archive_state; they are injected by the root, not reimplemented. SQL015 read_source_illustrations_v1 emits only bounded public metadata/status and strips crop_binding. Existing NativeTelegramIllustrations.observe checks registered topic-4 message presence on the native client's *current* lease. None alone is sufficient authorization.

New files ONLY: src/regional_knowledge/kaggle_public_illustration_gate.py, tests/test_kaggle_public_illustration_gate.py, this document. Server, shared SourceReadGate, WSS query enum, registry, SQL013–016, source materializer, media source, Telegram sender, existing CI and other worktrees are untouched.

## Exact root integration hook

The root must instantiate PublicIllustrationOutputGate with the **same** existing ReadAuthority and SourceReadGate, an installed native reader SHA256, plus:
- native_read(source_id, illustration_id, verified_person_id, binding): trusted bounded existing actor-scoped Kaggle reader/PG lease, returning one coherent NativeMediaSnapshot from sources/active_revision/source_visibility, source_import_origins.original_sha256, SQL014 source_image_origins and source_manifests page/figure region, SQL015 source_illustrations, source_illustration_candidates/origins and source_illustration_availability. Its public_row must correspond exactly to SQL015 read_source_illustrations_v1, with crop_binding removed. An unavailable private read role or unpinned SQL handler fails closed; do not substitute model JSON or a new connection pool.
- archive.read_opaque(native_snapshot, verified_person_id): the *existing separately authorized* archive/media owner, under current reader/session lease, rechecking registered archive scope and actual native media presence through NativeTelegramIllustrations.observe (read-only). It must mint and retain an exact ArchiveOpaqueProof object with a non-bearer opaque ref; archive.issuance_current(proof) synchronously checks that same object and lease. A caller-provided boolean, status, Telegram locator or string ref is never authority.
- render_metadata: the existing source_illustrations_native.public_illustration_metadata; validate_archive_state: the existing source_illustrations_native.validate_archive_state. The gate applies a final safe metadata-key whitelist and typed PublicOpaqueArtifact output. No raw chat/topic/message/media id, private crop binding, token, URL, original content or credential leaves the public frame.
- verified: an actual OAuth-resolved, still retained VerifiedReadAuthority, not a user identity claim. Selectors are canonical source/illustration UUIDs, never grants.

After issue(), root must call gate.issuance_current(ticket) **synchronously immediately before emitting the media frame** and release(ticket) afterward. Any await between verification and transmission requires a fresh issue() (no cached frame). This object is NOT registered with server.py or global runtime yet.

## Security sequence / tests

ReadAuthority.require/public_search and artifact_eligibility/public_illustration; first native current read; exact existing ControlSpool.read_facts/facts_current source history; separately authorized archive read/presence and opaque ticket; second exact native readback; recheck source history and both authorities after every significant await; safe existing metadata renderer; short retained issuance ticket and synchronous final lease/history/archive gate. The two native snapshots must be exactly equal; a withdrawal, different active/content/eligibility revision, wrong source original/page/region/crop/raster/receipt, changed reader generation, archive UNKNOWN or reconnect fails closed, not Saved.

Targeted synthetic pytest uses actual existing ReadAuthority, SourceReadGate and ControlSpool fixture classes, with fake native I/O and private archive owner. No real PostgreSQL, provider, Telegram, Kaggle or credential access.

**Outstanding composition (explicit):** SQL015 public reader omits private binding needed for a proof; SQL015 public grant/role and actor-scoped native private read bridge have not been registered; existing WSS/SourceReadGate has no media query type. NativeTelegramIllustrations.observe is read-only, not itself a public artifact grant. The single root integrator must compose these, verify exact native/reader grants and artifact issuance, then conduct separate real media/live acceptance. The present source gate never claims those are installed or operational.
