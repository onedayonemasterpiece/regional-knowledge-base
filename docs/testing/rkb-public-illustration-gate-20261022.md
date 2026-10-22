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


## 22 October — clean Opus integration-target handoff

Canonical integration branch: `integration/rkb-native-import-20261022`, initially
`4e6a7b7ab0c90a9ff41effa37f25f92930041ccf`, **sole merge owner RKB-Opus**.
This handoff consists only of independently reviewed RKB-Риф-owned source,
tests, and this manifest:
- Original PR219 output gate `e9e0d5587a7857dbca3909c46e4a0df06f3a2c22` →
  exact gate git blob `5a1b78d7ca8ac7355eb971b892cfeae13e2b020a`.
- Original PR222 native read source `0a169aefa51d7869d7b657e259432b107479ac41` →
  exact reader git blob `b3ce02885fb33b1d2c6097b2313bf75ab2ee721f`.
- Separate PR222 populated synthetic PG17 and archive-denial test revision
  `7ec5bf739ba4bfe5e5d9117d67b2aa83009c4523` was verified in CI
  `39751280849` (Python 3.12/3.13 each 2551 PASS/32 SKIP).
- The canonical base does NOT yet contain SQL013, SQL014, SQL015 or SQL016.
  Its test import was previously coupled to
  `test_story_reconcile_ordered_013_016_pg` from another (unmerged) branch;
  the clean handoff now uses a standalone fixed **test-only** DSN/ordered-file
  validator and marks real PG tests `NOT RUN` when any exact SQL013–016
  predecessor is missing. It never fabricates SQL015 or reports these
  skips as successful Media15 installation. **Root must rerun populated PG
  tests with all four real SQL migration artifacts on the accepted candidate**.
  If all files are present but SHA014/015 differ, the assertions hard-fail.

### Additional changed-boundary synthetic regressions

All use the existing `ReadAuthority`, `SourceReadGate`,
`ControlSpool` and real native reader class with fake PG/archive I/O only:

- `test_native_media_final_issuance_refuses_real_source_withdrawal_pending_after_issue` —
  actual local ControlSpool restriction after ready issue fences cached output.
- `test_native_media_withdrawal_during_archive_await_prevents_second_read_and_public_ticket` —
  a real staged source restriction during an archive await denies before emission.
- `test_native_media_reconnect_rotates_exact_lease_identity_despite_equal_tuple` —
  a newly owned binding with equal scalar fields is **not** the prior
  retained session/current lease.
- `test_native_media_archive_lease_lost_during_await_forces_second_native_read_denial` —
  loss of existing native lease between first and second PostgreSQL snapshot
  never releases an old ready frame.
- `test_native_media_current_read_authority_revoke_at_last_renderer_before_emit` —
  synchronous final actor fence rejects revoked current access without an
  intervening await.

These demonstrate denial conditions, not production provider success. The
existing Media15 `status='ready'` row is **only native metadata**, never
independent proof that actual image bytes or an archive-owned opaque artifact
have been authorized, read and delivered. Real
`illustration_fetch → ImageContent` still needs Opus-owned installed
SQL013–016, private fixed reader grants, typed WSS, registered archive
`observe` and real current-lease pixel delivery, plus compatible
`read_issuance.refresh` handling and no-await final output fence.


## 22 October — Opus-assigned existing MCP illustration_fetch and ASGI issuance wiring

This is a **separate shared-media PR** from Opus' verified canonical integration
base `b932c15ef133d668a0eaa3570cc895e750876c77`, preserving his health
hook. The first commit imports only the five exact, independently reviewed
PR231 media leaf blobs, then changes assigned
`src/regional_knowledge/server.py` (existing tool body/target allowlist),
`src/regional_knowledge/kaggle_public_backend.py` (method on the existing
KaggleSourceBackend), `src/regional_knowledge/read_issuance.py` (optional
request-local refresh callback), and the OWN output gate's proof-bound actual
image byte read. No standalone renderer, queue/IAM/read-pool/WSS/worker,
SQL13–16 migration, role grant, upload, provider send or native runtime change.

### Concrete tool invocation / owner composition

Only **the existing** `illustration_fetch(id)` MCP tool is reused.
For the native backend its locator is
`rkb:illustration:v1:<canonical source UUID>:<canonical illustration UUID>`.
Both are *untrusted selectors*, never capabilities. An illustration UUID
alone is insufficient to establish source identity; the legacy
`knowledge://illustrations/<uuid>` route is preserved **only** on the
legacy profile and is never silently used for Kaggle native data.

The root service owner must explicitly compose and register the canonical gate
**before** `build_server(target_startup=...)`:

```python
# All objects are the existing sole owner-resolved instances, not strings.
gate = PublicIllustrationOutputGate(
    authority=startup.authority, source_gate=startup.gate,
    native_read=real_typed_native_media_reader, archive=leased_archive_owner,
    render_metadata=source_illustrations_native.public_illustration_metadata,
    validate_archive_state=source_illustrations_native.validate_archive_state,
    native_reader_sha256=approved_native_media_reader_hash,
)
startup.backend.configure_public_illustrations(gate)
# Then build_server(target_startup=startup) uses the existing illustration_fetch.
```

This owner configuration **requires** callable
`leased_archive_owner.read_opaque(snapshot, actor_id)`,
`issuance_current(retained_proof)` **and**
`read_issued_bytes(the identical retained ArchiveOpaqueProof object)`.
The method must get actual, independently authorized crop bytes from the
registered existing source/page/figure archive client **on its current lease**,
not from SQL015 `ready`, an arbitrary `rkb-art-...` ref or a user-supplied
Telegram locator. The owner returns the exact private
`IssuedArtifactBytes(proof=same_proof, data=actual_crop_bytes,
mime_type='image/png'/'image/jpeg'/'image/webp', sha256=sha256(data))`,
bound to the SAME immutable proof object, actor/source/region/crop,
current storage authority and archive scope. The gate validates proof
identity, digest, real Pillow decoder/MIME/dimensions and a 256 KiB bounded
image response, rechecks the same PG source after the bytes await, plus
current actor/source history/native lease. If this byte issuer does not exist
or returns UNKNOWN, **the media tool is not advertised** by the native
profile; no fake proof or pending image is serialized.

**No production leased archive bytes issuer is installed in the current
candidate.** The only positive ImageContent in tests uses a clearly named
`SyntheticArchivePixels` with real generated 20×20 PNG bytes and fake
external I/O; this does not establish Telegram/pixel deployment or valid
accepted-command/Dataset media lifecycle.

### Exact final issuance

`KaggleSourceBackend.illustration_fetch` validates a paired locator and
uses the SAME `ReadAuthority.resolve_oauth`,
`SourceReadGate.binding_current`, current native binding and
`PublicIllustrationOutputGate.issue`. The initial tool result is a harmless
NO-IMAGE placeholder retained **only** in
`read_issuance.capture_read_output` for the actual MCP request, and fails
closed if that request-local capture is absent.

The **existing** `ReadIssuanceMiddleware` now accepts an optional
trusted, request-local `on_refresh` for the media variant. This callback
releases the prior ticket and obtains an entirely new gate-issued
source+archive proof and actual owner-issued image bytes. Native + source/actor
are reread AFTER each archive await. The final renderer emits the existing
`CallToolResult` contract:
`TextContent` with the canonical typed safe public frame and
`ImageContent` with the bounded real verified image bytes and MIME. It does
not emit private Telegram locator/chat/topic/media IDs, document originals,
token, or credential. The ASGI middleware serializes a newly refreshed body
and checks `gate.issuance_current(ticket)` **synchronously immediately
before** its sole `await send`, then releases authority and ticket. If
revoked/stale/expired, the complete text-and-image result is replaced by
generic `unavailable/context_gap` without leaking a base64 prefix.
Existing text/case/source issuance paths continue to use their original
`gate.refresh(gated.result)` logic untouched.

### Exact remaining ROOT responsibilities and NO-GO boundaries

1. Install/approve ordered SQL013–016 Media15 source and **narrow
   accepted-provenance PG reader grants** in a single canonical target.
   Do not replace missing SQL015 with a no-op. Current integration before
   those migrations has 10 explicit SQL-dependent media PG test SKIPs.
2. Register the existing WSS/worker media-read request type into the single
   target native reader pool and bind an actual
   `NativePublicIllustrationReader` to the canonical
   `SourceReadGate`; no second pool or IAM.
3. Supply an **ACTUAL** leased archive reader that performs
   `NativeTelegramIllustrations.observe` on the exact registered
   topic-4 message and returns crop bytes via its existing supported
   transfer mechanism (not an invented Telegram API). Its
   `read_issued_bytes` must bind immutable digest, actor, scope,
   original/page/crop identity and survive same-owner reconnect/fence
   revocation. The archive output method does NOT yet exist in the
   approved source bundle; without that the tool remains intentionally
   unavailable. Do not call a purely synthetic test archive an issuer.
4. Add a real image locator producer to existing search/fetch media metadata
   and test true end-to-end PDF/DjVu page→figure→crop→actual ImageContent
   on a combined, approved source candidate; the public locator is an
   untrusted pair and must not itself grant permissions. The current route
   accepts such a locator but **nothing produces it automatically yet**.
5. On combined candidate, run changed-boundary class tests and actual
   populated GitHub PG17 fixtures (after exact migrations). Separate
   acceptance still requires real remotely durable book import (Saved),
   qualified PDF/DjVu pixels, authentic facts, Story.body and continuation,
   native Dataset checkpoint/restart and public API proof. This source
   PR alone cannot establish installed reader/issuer/live readiness.


## 22 October 11:52 UTC — source-only NativeTelegramIllustrations owner-bound crop read

**Latest product plan:** PR238 HEAD `a11a25e29720eb67caf2437751fed8461d0ea17f`,
`docs/plans/rkb-product-delivery-checklist-20261022.md` blob
`f23ce6e69b9033723697b4dce45ab7c69d7b60a0`.
M3 source-only ownership assigned by dot
`1792669652.725409`; Opus remains sole integration/live/merge owner.
Base is the reviewed PR241 `5d9c07b17568bf7aa44be13022e5e98980222766`,
not a second integration branch. The new source branch copies *only*
`src/regional_knowledge/telegram_illustrations.py` from the authorized
media delta `.media-delta.patch` SHA256
`701c03106cb2a9436ddd814d08583008dd1065243fd6ce5e6b7a03e81f01a377`,
file size 6240 and SHA256
`485a992ba7d064bc86028ce3065d5ebc392a98cd304a69bf028f440a26d2066c`,
then appends owner-bound **real native crop byte read methods**. The existing
`send_one` unknown/no-resend / registered locator lifecycle and
`observe` remain unchanged. Imported existing legacy regression tests are
from exactly the same media delta. No new login/client, downloader class,
second storage, queue, IAM, SQL, native WSS route, provider mutation or
runtime/deploy.

### Native owner-local contract: actual leased client, not fake archive proof

```python
# Reuses the single externally leased Telethon client used by send_one/observe.
archive = NativeTelegramIllustrations(
    client=existing_leased_telegram_client,
    chat_id=trusted_private_archive_chat,
    topic_id=4,
    current=existing_native_lease_current,
    archive_scope=trusted_exact_owner_scope,
)
native_proof = await archive.read_opaque(
    existing_native_pg_snapshot, verified_actor_id)
native_pixels = await archive.read_issued_bytes(native_proof)
assert native_pixels.proof is native_proof
# native_pixels.data: ACTUAL bounded image bytes from Telegram iter_download.
# native_pixels.sha256 = sha256(exact downloaded bytes).
archive.release_issued(native_proof)  # owner-side transient proof cleanup
```

`read_opaque` requires trusted accepted/current native snapshot:
source revision 1, unchanged imported original/media SHA, exact persisted
canonical crop binding and native ready operation key, configured archive
scope, actor UUID, *current* native client lease, and **actual**
`get_messages`/topic-4/media ID+dimensions readback via existing
`observe` (never an invented provider receipt). It creates an owner-local
short-lived object-identity-bound `NativeIllustrationArchiveProof`:
private actor/source/figure/crop SHA and exact archive scope; the associated
non-bearer `rkb-art-*` ref is not an authorization token. Native proofs are
bounded in RAM (64 active, 10s TTL by default); cancellation/TTL/reconnect
cannot promote a stale proof into a new read.

`read_issued_bytes` uses **exact same** leased client's existing
`iter_download(media, request_size=65536,chunk_size=65536)`, modelled after
`NativeTelegramOriginalDownloader.download`. It bounds each chunk to
64KiB, total to 256KiB, deadline 4s, checks native lease/owner+object
identity after EVERY async step and again *after* awaited iterator cleanup,
verifies the registered chat/topic/message/media/actual dimensions before
and after download, SHA256 of real bytes, Pillow decoded image format/MIME and
dimensions, and never sends/resends a provider message. Errors and private
Telethon session details fail closed. Actual bytes are internal, not JSON
locators, and the surrounding PR241 gate independently checks actor/source,
re-reads PG after bytes await and synchronously fences final output.

**Do not conflate native/local proof with Dev `ArchiveOpaqueProof`:**
`NativeTelegramIllustrations` exists in the Kaggle resident context, while
PR241 `PublicIllustrationOutputGate` and its retained Pydantic frame live in
the Dev MCP ASGI process. Python object identity cannot traverse WSS.
The native issuer returns
`NativeIllustrationIssuedBytes(proof=native_proof,data,mime_type,sha256)`,
*not* an `IssuedArtifactBytes` object accepted directly by the Dev gate.
The sole integration owner must bind the source's validated native receipt
and current exact WSS lease to the DEV-held
`ArchiveOpaqueProof` identity and convert only after independently
authorizing the operation. **No such WSS media read/bytes response is
registered in this source PR.**

### Exact shared follow-up hunks reserved for root owner (NOT EDITED)

- `kaggle_protocol.py`: bounded `ILLUSTRATION_READ` native
  request/result DTO with canonical actor/source/illustration/binding identity,
  attached run/connection generation and ≤256KiB byte receipt/sha; never use
  arbitrary user media IDs/URLs or claim native object identity as WSS proof.
- `kaggle_worker_query.py`: dispatch to this existing native leased owner
  only after the existing verified actor/source PG read, and return an
  accepted image snapshot + proof-bound byte response. No second PG pool.
- `bge_channel.py`: current WSS read owner, same-run/nonce/connection
  generation and result-size bounds; use the already selected transport.
- `kaggle_target_runtime.py`: construct/bind the SAME Telegram leased
  client instance under its exclusive `current` predicate; do not mint
  another credential/session. No shared source change in this PR.
- `kaggle_public_backend.py` / `read_issuance.py`: trusted Dev bridge
  retains its own current `ArchiveOpaqueProof`, exact native WSS result
  identity and source/lease/actor rechecks through the existing ASGI final
  issuance, mapping to Dev `IssuedArtifactBytes` only with validated bytes.
- **Do not repurpose** `NativeSnapshotTransfer` or
  `SnapshotTicketChannel`: their typed method and operation key are
  recovery-phase/Dataset-specific, not user image-read grants.

A remaining fidelity limitation is explicit: SQL015 availability persists
canonical crop *binding bytes* and registered Telegram media locator, **not
the SHA256 of the crop's exact upload byte stream**. A same-message
`get_messages` plus independently hashed download proves the actual
registered object and retrieved pixels under current scope; it does **not**
bitwise prove equality to the pre-upload uncompressed crop or rule out
Telegram's normal photo recompression. Product acceptance still needs
source crop/rendition evidence, use the existing document choice when native
dimensions require lossless archival, and a reviewed upload-byte digest/receipt
if exact-bit equality is a requirement. No fabricated persisted SHA added.

### Evidence boundaries

The source-only tests use **the actual** `NativeTelegramIllustrations`
and `NativeMediaSnapshot` classes plus fake Telethon external I/O:
real encoded PNG, real 64KiB async chunk iterator, cancellation, revoked
native lease during observe/download/stream-close, changed topic/media ID,
stale/replayed/copied proof, scope/source/original/crop mismatches,
oversize/bogus pixels, expired/current-epoch proofs, and legacy
`send_one` UNKNOWN/no-resend. They are never called actual live Telegram
archive, PG/Dataset saved, real ChatGPT image-content receipt or provider ACK.
A complete first-approved-book PDF/DjVu page→model bbox→Telegram archived crop
→MCP ImageContent→model actually sees pixels still requires root-owned
typed WSS transport and sole Opus installed E2E slot, as required by full
pinned RKB v4.1 / owner alignment / acceptance contract.
