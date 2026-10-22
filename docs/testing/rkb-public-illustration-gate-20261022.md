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


## Independent review corrective: exact frozen RED → canonical final binding

The independently reviewed original source ref `a0e195fcf86e1760f0ee4d3fcaf7e50719d08ac2` is immutable. Separate test-only draft PR221 at `17885f76307ed7dde184ac18039250fe1b9f4082` ran the reviewer negatives on that **unchanged** source. Real GitHub CI job `119596399512` / run `39709255686` (CPython 3.12): **9 FAIL / 2495 PASS / 32 SKIP**, including top-level `archive_locator` injection, nested bbox+list mutation, issued frame replacement, released verified actor, expired OAuth context, revoked `public_search`, changed participant epoch, expiration inside renderer before final output, and a two-coroutine max_retained=1 admission race. The independent monotonic ticket-clock test already passed RED, proving it actually advanced the fake clock; it is retained.

Corrective changes remain confined to this one new gate and its own tests/doc:
1. The retained output now stores the **exact immutable canonical JSON bytes** of the public frame plus the identity of the returned `IssuedPublicIllustration`. Final synchronous `issuance_current(ticket)` compares the frame object, its exact serialized bytes and original ticket identity. Frozen Pydantic alone was *not* treated as deep immutability. Caller-added private keys, nested mutations and object replacement therefore invalidate the same output ticket before JSON publication.
2. Final issuance reuses the **existing** `ReadAuthority` context-identity lock, `authority.clock` and synchronous `authority._state` called with the same `Principal`, route, link revision and OAuth-family ref as `ReadAuthority.recheck`. It rejects released/expired contexts and changed participant revision/role before output, including changes after the last asynchronous `require`. This is not another IAM store; underlying canonical source remains the original resolver and retained OAuth/participant projection. A failed synchronous source check denies instead of reauthorizing.
3. One last no-await ticket-capacity check directly before insertion prevents two concurrent reads that both passed the earlier check from each retaining a ticket. No extra queue, scheduler or permit is introduced.

**Important:** The future root output bridge must still call `issuance_current(ticket)` synchronously **immediately before** serializing that **same** issued frame; release afterward, no intervening awaits or cached frame. A repeated check cannot authorize a newly produced/caller-modified frame or an expired actor. No common runtime/queue/readiness/heartbeat/SourceReadGate/ReadAuthority/SQL/provider code was changed. Synthetic CI validates the leaf; native archive owner and live Kaggle are still integration-owned.


## Approved next leaf: actual SQL014/015 native snapshot assembler (22 October)

**Only three paths in this scoped addition**, based on immutable PR219 corrective
`e9e0d5587a7857dbca3909c46e4a0df06f3a2c22`: new
`src/regional_knowledge/kaggle_public_illustration_native_read.py`, new
`tests/test_kaggle_public_illustration_native_read.py`, and this own manifest
append. Source, SQL, WSS, IAM, queue, runtime, roles, provider and sender stay
unchanged. The three RKB v4.1/owner-alignment/acceptance docs at
`bd138b2449f68cabb2cbfa3df552df8471f40c78` remain mandatory:
PDF/DjVu, actual image pixels, facts, real Story.body and continuation cannot
be removed for this scoped gate.

### Exact native API and one-transaction contract

```python
from regional_knowledge.kaggle_public_illustration_native_read import (
    NativePublicIllustrationReader, READ_SQL_SHA256,
    SQL014_SHA256, SQL015_SHA256,
)

native_read = NativePublicIllustrationReader(
    acquire=existing_resident_reader_pool.connection,
    binding_current=existing_verified_binding_current,
    archive_chat_id=trusted_topic4_chat_id,
    archive_scope=trusted_registered_archive_scope,
)
# Signature matches PublicIllustrationOutputGate(native_read=...)
snapshot = await native_read(source_id, illustration_id, verified_person_id,
                             existing_target_read_binding)
# Both the gate and runtime owner independently pin READ_SQL_SHA256.
```

**Actual SQL:** 1 parameterized fixed SQL statement, 1 injected existing pool
acquisition, **1 REPEATABLE READ READ ONLY transaction**, two-row ambiguity
guard, bounded 2-second deadline, current runtime/lease before and after each
significant await. It joins existing SQL001/004/006/009/014/015 tables:
`sources/active_revision/source_visibility`, `source_import_origins`,
`source_manifests` and embedded exact manifest page/figure region,
`source_image_origins/source_activation_candidates`,
`source_illustrations/source_illustration_candidates/
source_illustration_origins/source_illustration_availability`,
`applied_ops`, `recovery_state` and `read_frontier`. It binds the accepted
native operation hashes and original receipt, media candidate hash, exact
source active1/not-withdrawn, image receipt/raster, figure region/bbox,
canonical persisted crop bytes and operation hash, ready native availability,
trusted archive chat/topic and current projection/data sequence. The output
is the **existing** `NativeMediaSnapshot` class, including the exact
SQL015-style `data - crop_binding` public projection. No Telegram locator is
ever public; a stored `ready` is not independent archive permission.

**Source pins:** reviewed SQL014 SHA256
`5f8257e9fe58e2e2bdd00f67042ad17b8db007c7873dcc04f52d53be3523e855`;
SQL015 SHA256
`7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773`.
`READ_SQL_SHA256` fingerprints the actual parameterized SELECT text, **not**
an installed read handler or a running Kaggle release. Source readback and
real role/function pin approval belong to the root integrator.

### Exact **root-owned** missing integration and read grants (not implemented here)

1. **Installed Media15 + reader role:** add only the SQL013→014→015 ordered
   accepted migration and reviewed media leaf to a single root candidate. The
   existing `rkb_reader` grants for `sources`, `active_revision`,
   `source_visibility`, `recovery_state` and `read_frontier` do not grant
   this private proof SELECT. SQL015 explicitly REVOKEs PUBLIC. The root must
   review narrow source-filtered trusted native execution with at least
   `source_import_origins.payload_sha256/original_receipt_sha256`,
   `source_manifests.payload_sha256/original_receipt`,
   `source_image_origins` activation/operation/knowledge/hash columns,
   `source_activation_candidates.knowledge_sha256`,
   `source_illustrations(source_id,illustration_id,data)`,
   `source_illustration_candidates(media_sha256,knowledge_sha256)`,
   `source_illustration_origins` exact accepted receipt identity,
   `source_illustration_availability` binding bytes, status, locator and
   archive scope, and `applied_ops` accepted operation identity/status/hash.
   Existing `rkb_reader` column grants may cover a subset but **NOT** all;
   do not blindly grant private archive locator rows to a public SQL caller.
   Prefer an owner-scoped fixed native read function/role when necessary, with
   no direct read of full source content or unscoped private locator grants.
   The leaf intentionally fails unavailable when actual SELECT is denied.
2. **Existing native pool/dispatch:** in `kaggle_target_runtime.py`, bind
   `acquire=self.pool.connection` on the already verified resident PG reader
   generation; use its current binding/lease and a source/actor-scoped typed
   callback under the *existing* WSS/query worker path. A single, bounded
   `ILLUSTRATION_READ` request/result must be registered in the shared
   `kaggle_protocol.py`, `kaggle_worker_query.py`, `bge_channel.py`
   and any reader bundling list only by the one native root owner. This own
   leaf adds **no WSS route** or new pool.
3. **Real archive owner + artifact bytes:** the existing media
   `NativeTelegramIllustrations.observe` rechecks get_messages/topic4 on
   its existing exclusive lease. The root must connect it to an already
   authorized scoped pixel/crop artifact issuer/transfer and opaque
   `ArchiveOpaqueProof` object identity; status `ready` or a synthetic
   `rkb-art-...` ref does **not** prove access or return actual pixels.
   The public response must never contain a Telegram locator, token or
   original/page body; actual ImageContent needs supported verified transfer.
4. **Existing MCP final issue/refresh:** `server.py:1076`
   `illustration_fetch` is the existing public route; its legacy
   `illustrations.fetch_crop` (Supabase/VibePublish) must be replaced by
   the root native bridge, not a second tool. `TargetServiceStartup.gate`
   and `KaggleSourceBackend` must reuse the same real
   `ReadAuthority/SourceReadGate`. Existing
   `read_issuance.ReadIssuanceMiddleware` calls `gate.refresh(gated)`
   twice before the final `gate.issuance_current(ticket)`; the existing
   PR219 `PublicIllustrationOutputGate` supports
   `issue/issuance_current/release`, **not** `refresh`. The root owner must
   add an exact adapter/compatible re-issuance path at the established
   issuance boundary, never pass an already cached frame through the
   generic refresh handler. Synchronize the final gate and JSON/ImageContent
   serialization without an intervening await.
5. **Acceptance still required:** request/actor revocation between native
   reads, source withdraw/revision/eligibility, wrong original/figure/crop
   and lost archive lease must deny. Cold/no PG, missing grants and UNKNOWN
   remain unavailable. CI synthetic success and even a real PG SQL-compile
   fixture are not real archive/pixels, accepted ledger migration, Dataset
   persistence or deploy/live acceptance.
