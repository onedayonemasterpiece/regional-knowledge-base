# Existing owner-family projection migration

This LOCAL operator migration repairs the explicit infrastructure transition
from legacy owner-only OAuth to the same issuer's canonical request projection.
It never mints tokens, creates a person/participant grant, broadens scopes,
rewrites encrypted state/key, derives an issuer, or runs from token validation.

Use `OwnerFamilyProjectionMigration(existing_provider, corpus_path=old_authority)`
only with the existing private operator configuration and the existing OAuth
state/key/authority paths. No caller actor, bearer, family selector or MCP route
exists. The owner must be the current active canonical user and not disabled.
The optional attached PersonalPolicy must match that owner and authority path.
The sole API OAuth writer must already use that same PersonalPolicy/projection;
exclude legacy OAuth writers which would revoke encrypted families without
invalidating their SQL projection. This is separate from ordinary corpus workers.

`review()` takes the same encrypted-store exclusive lock used by mint/refresh/
revoke and reads a single query-only SQL snapshot. It selects only currently
live access/refresh families for the exact configured owner, exact issued
issuer/resource/scope and existing registered client. Personal/nonowner families,
missing issuer, unknown or mixed metadata remain unprojected with fixed counts.
Used-refresh/session/code metadata alone cannot authorize projection. A used
refresh record alongside a valid rotated live family is normal, not revocation.
No token key, raw family, person/client payload or secret is returned/logged.
The review contains aggregate counts and a stable plan SHA only.

The operator independently reviews that exact plan and its rejection counts.
At execution construct the same local migration, take a fresh review and compare
its plan SHA to the reviewed value, then call `apply(retained_review)`. A JSON
receipt is not a permission capability. Apply rederives source authorization
under the same auth exclusion plus one SQL write transaction before any insert.
Changed/expired/revoked source, disabled owner, disabled/conflicting SQL or a
changed plan fails closed. Missing exact rows inherit the maximum actual live
access/refresh expiry; no new duration is added. Existing shorter expiries are
never extended or enabled by migration. Normal authenticated refresh remains
the only renewal path.

One immutable aggregate receipt is committed in
`rkb_oauth_owner_projection_migrations` in the same SQL transaction. Lost ACK
reconciliation returns that original receipt for the unchanged plan. Revocation
and refresh replay still close the projected family through existing code.
Keep the receipt during infrastructure authority projection/archive; any thin DB
candidate prepared before migration must be prepared again from the final
quiesced authority, including this audit table.

Run actual dry-run first; source tests use synthetic issuance only. Root reviews
the exact live plan before applying. Then verify through the genuine installed
MCP client without extracting a token or minting a replacement family. Successful
profile continuity must match the pre-transition canonical identity. Record
normal refresh separately; do not turn successful access validation into a
claim that the refresh path ran. This migration does not prove PostgreSQL,
GEO, import, recovery, public parity or source cutover acceptance.
