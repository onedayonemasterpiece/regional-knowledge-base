# Canonical authority for short reads

This source unit selectively reuses the public participant and personal OAuth
implementation at PR151 commit `adf78a5`, without resuming its paused task or
importing its disagreement/request executor. Source baseline:
`8db2497290ce1935d7c45aa06dd7eb5372eaabb6`. The installed
`kaggle-warm-debugging/SKILL.md`, RKB profile and repository `AGENTS.md` were read.
No runtime database, user, grant, credential or provider was changed.

`Participants` owns the existing canonical `rkb_users` identities and
`rkb_participant_profiles`, grants, weights and external identity tables. Existing
owner personal-route administration, explicit grants, provider observations,
link revisions and tenant-qualified identity uniqueness are preserved. Categories
and expert weights never create capabilities. Every profile, disable, grant,
weight and account begin/confirm/unlink command advances the same participant
profile revision transactionally with its event and idempotency receipt. That
revision is the read `auth_revision` / auth epoch. Expiry and current canonical
user status are independently rechecked and do not depend on an epoch increment.

## Existing OAuth, explicit participant policy

Default `RegionalOAuthProvider` owner-only mode retains its existing encrypted
state/key files, client registrations, opaque credentials, resource binding,
consent, PKCE, refresh rotation/replay revocation and owner flow. Personal login is
an explicit deployment opt-in through `PersonalPolicy(existing_participants)`;
it cannot create people, profiles, external links or capability grants. The
personal credential code authenticates an already enrolled canonical person.
Owner administration can issue/revoke it through the selectively reused local
CLI; raw codes go to private files and never command output. No new OAuth server,
login identity provider, public admin route or IAM is introduced.

Web workers and that CLI share the same private stable lock inode around the
existing encrypted OAuth file. Blocking file/crypto/SQLite work in async OAuth
handlers runs through `asyncio.to_thread`. Existing file/key formats are extended
additively; the key is not replaced.

The existing `rkb_request_oauth_grants` table projects current OAuth family
revocation. Mint and refresh create/extend the projection under the auth-file
lock. Revocation disables it before an auth mutation completes; a disabled family
cannot be resurrected by refresh. The projection binds exact canonical person,
client, resource, issuer and personal credential identity/epoch. Ready access-token
verification only reads that projection; it does not create or extend it.

Opting in with pre-existing personal-policy families may require a real refresh:
legacy/unprojected families or rows lacking the new issuer field fail closed
until mint/refresh records the exact existing authority. The additive schema
initializer preserves legacy rows with an empty issuer. This source does not
run that migration or enable personal policy in a live service. Default owner-only
mode does not acquire this new migration requirement.

## Internal read resolver

Construct `ReadAuthority` once with existing initialized `Participants`, the
configured existing provider and exact server issuer/resource. It requires the
provider's explicitly configured policy to refer to that same Participants
instance. Construction and ready read resolution do not initialize schemas.

- `resolve_oauth(raw_bearer, destination=...)` asks that existing provider to
  verify the actual opaque bearer. It then verifies the exact current family
  projection, canonical person, issuer, resource, client and token expiry. Caller
  supplied AccessToken objects, JWT claims, person UUIDs or family references
  cannot authenticate a read.
- `resolve_adapter(provider=..., tenant=..., provider_subject=..., destination=...)`
  is an internal trusted adapter hook after transport identity verification.
  It resolves only the complete verified tuple through existing Participants and
  retains the current verified link ID/revision. It is not a public tool or a
  provider verifier. A raw client payload must never call it or construct its
  routing context.
- `TrustedReadDestination` comes from authenticated server routing, including its
  actual audience/destination. Group/public destinations are always public-only;
  they cannot union personal grant sets.
- `VerifiedReadAuthority` is a frozen internal object. Its unpredictable context
  reference identifies only the identical object retained in this resolver's
  bounded RAM map. Copying its fields does not construct authority. It stores no
  bearer, login code or executor credential. Default capacity is 256 contexts
  (configurable up to 4096), default lifetime 120 seconds, further bounded by the
  verified access token's expiry. Release or expiry invalidates delayed use;
  restart loses these ephemeral locators.

`recheck` opens a query-only SQLite snapshot on a worker thread and checks current
person/profile, grant expiry, external link state/revision, exact OAuth family
projection and context expiry again. Participant revision changes invalidate old
contexts. Call it immediately before delayed output; that is an admission point,
not a claim that already transmitted bytes can be recalled. The resolver performs
no durable intake, mutation, fsync or cloud seal on ready reads.

`require` exposes only current read capabilities. `artifact_eligibility` separates
accepted public text/public illustrations from restricted scan proof. Scan
eligibility requires a personal destination and current owner or verified
participant in demo/expert/local_historian categories with `scan_proof_read`.
This is **capability eligibility only**: it does not prove a resource's source,
purpose, scope or current withdrawal. Artifact class must come from trusted source
geometry/purpose, not a caller label. The existing output gate must independently
prove all resource/purpose constraints; absent proof fails closed. Full originals
and private drafts remain under their independent policy. This unit grants no
whole PDF/DjVu access.

Deferred durable RequestAuthority contexts fail closed when the separate executor
is unavailable; no executor or durable request subsystem was imported.

## Evidence and limits

Synthetic SQLite/OAuth tests cover canonical multi-provider identity,
tenant collisions, exact family projection binding, forged authority objects,
profile/grant/link/credential/family revocation and expiry during reads,
public-only destinations, independent scan eligibility, context bounds/release,
read-only projections and off-event-loop I/O. A real HTTP/PKCE test obtains a
non-owner opaque token through existing login/consent endpoints. Existing OAuth
regressions also run; the paused PR's unrelated journal/executor tests are not
ported or claimed.

Server/search/WSS wiring, source withdrawal/output dependencies, actual non-owner
client acceptance, live credentials and production deployment remain outside this
unit. No runtime policy has been enabled by committing it.
