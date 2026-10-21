# Thin local identity authority at target cutover

This is an offline preparation procedure, not an activation or corpus migration.
The target can keep the existing `RKB_SQLITE_CORPUS_PATH` setting while pointing
it to a small stable identity database. OAuth encrypted state/key paths and
credentials remain unchanged. The application continues using `SQLiteCorpus`,
`Participants`, `PersonalPolicy` and the same auth administration CLI; there is
no second identity service or rights authority.

## Exact projection

`python -m regional_knowledge.authority_projection` reads a consistent SQLite
read-only transaction, copies only the following explicit columns, validates
person/link references, creates an exclusive mode-0600 unreferenced candidate in
an existing owned mode-0700 directory, and reads back every copied row. It never
runs source constructors, SQL from `sqlite_master`, source triggers or content
migrations. Unknown authority tables/columns, nonempty legacy workspace/content
grants, missing owner/references and bounded-budget violations stop preparation.

| Table | Columns |
| --- | --- |
| `corpus_rows`, only `table_name='rkb_users'` | `table_name,row_key,document_id,revision,payload`; user payload only `id,status,metadata,created_at,updated_at` |
| `rkb_participant_profiles` | `person_id,revision,disabled,payload` |
| `rkb_participant_grants` | `person_id,capability,expires_at,assigned_by,assigned_at` |
| `rkb_participant_weights` | `person_id,domain,payload,revision` |
| `rkb_external_identities` | `id,person_id,provider,tenant,provider_subject,state,revision,method,confirmed_by,confirmed_at,attribution_review_required` |
| `rkb_identity_observations` | `id,provider,tenant,provider_subject,link_id,link_revision,audience,expires_at,receipt_id,used_by` |
| `rkb_participant_events` | `person_id,revision,actor_id,action,payload,created_at` |
| `rkb_participant_operations` | `actor_id,operation_key,person_id,request_hash,result` |
| `rkb_request_oauth_grants` | `id,person_id,client_id,resource,credential_id,credential_epoch,expires_at,enabled,issuer` |
| `rkb_oauth_owner_projection_migrations` | `plan_sha256,receipt_json`; exact bounded owner-family migration receipts only |

Missing infrastructure tables become empty through existing constructors; no
profile, capability, account link or request grant is fabricated. A historical
request table missing only `issuer` retains empty issuers and remains unverified
until proper refresh, matching `request_grants.schema`. Existing disabled/expired
rows and audit bytes stay disabled/expired and unchanged. Credentials and service
principal epochs/audit remain in the same encrypted OAuth store, outside this
export. Local authority backup/recovery remains the existing auth-file/SQLite
procedure: this projection adds no remote auth checkpoint or new durability claim.
Owner-family migration receipts retain their exact canonical bytes and counts;
the candidate reconstructs the fixed immutable/retained audit triggers from
trusted application code. It never copies source trigger SQL or accepts arbitrary
JSON bodies as receipts. Unknown `rkb_oauth_*` tables require explicit review.
An authority candidate prepared before a later family migration is stale and
must be regenerated at the final coordinated writer boundary.

The existing `SQLiteCorpus` constructor creates empty content/FTS/GEO table
shells. They contain no old documents, chunks, vectors, stories, variants, private
drafts, cases, contributions, source/content ACLs, receipts, control payloads or
GEO records. GEO has its separately reviewed allowlist migration. Default bounds
are 10,000 selected rows, 8 MiB logical bytes, 1 MiB per encoded row/cell and a
10-second source query deadline. No whole-corpus copy is necessary.

## Observed source and activation blockers

Read-only inspection on 2026-10-20 found a 114,008,064-byte legacy corpus database,
four active canonical users and zero workspaces, workspace members or document
grants. All eight infrastructure tables above were absent. The owner and all
active encrypted OAuth access/refresh subjects matched those four users. Nine
active refresh families had no SQL request projection. The inspection did not
change OAuth state/key metadata or invoke constructors against production.

Outstanding legacy work cannot be discarded: two `story_jobs` await an agent,
461 of 878 `story_outbox` rows await a worker, and ingestion has two processing,
three staged and one ready job (36 total). Nineteen publication records remain
(18 ready, one superseded). These are content-bound control work, not global
identity grants. Copying their bodies to the thin authority would violate the
cutover boundary. Keep the old database and existing native authorizations until
the operator explicitly resolves, cancels through supported lifecycle, or retains
those operations with their original recovery path. A candidate export does not
settle any pending/UNKNOWN operation. The seven-day old-backup policy starts only
after independently confirmed cutover; it does not authorize deletion here.

## Existing owner token continuity

Enabling target `PersonalPolicy` correctly rejects an old access token whose
family lacks `rkb_request_oauth_grants`. Ready reads do not create grants. The
normal continuity path is the client's **normal OAuth refresh** against the
same `/token`, issuer, resource and registered client, after the reviewed target
configuration points at the new authority database. The separately reviewed
[local existing-owner-family projection](oauth-owner-family-projection.md) can
also restore an unchanged live access token without refresh, by projecting only
its independently verified existing issued family. It creates no token, person
or capability, and leaves unknown-issuer families unprojected. Never ask the owner to hand
an agent broad credentials or clone/decrypt/rewrite cipher state for migration.

`load_refresh_token` and `exchange_refresh_token` use the existing encrypted-store
lock and current family/credential/replay validation. `_mint_family` keeps the
original owner subject and family, and `request_grants.save` writes the exact
issuer/client/resource and optional credential epoch to the projected database.
After a verified existing-family projection or normal successful token rotation,
the client can retry the read.
The endpoint's registered client authentication must remain unchanged; the
installed MCP endpoint requires `client_id` in the form even for Basic clients.
Expired, revoked or consumed refresh tokens must not be resurrected; a client
with no valid refresh uses the existing owner login/consent flow itself.

This is the existing two-store lifecycle, not an atomic transaction across SQLite
and the encrypted file. Projection save occurs under the auth-file lock before
cipher-state commit; failure must retain existing recovery semantics. The new
exporter does not change that ordering. Synthetic tests exercise the real HTTP
refresh route, same person/family, exact projection, `ReadAuthority` resolution,
and consumed-refresh replay revocation. They do not prove that a particular live
client will automatically refresh; actual owner MCP continuity remains NOT RUN.

## Reviewable preparation and later activation

No command below has been run against the production database. An operator may
prepare an unreferenced private candidate after reviewing this procedure:

```sh
# DESTINATION_PARENT already exists, belongs to the service user, mode 0700.
# Use a stable versioned authority location, never a disposable artifact path.
PYTHONPATH=src python -m regional_knowledge.authority_projection \
  --source "$RKB_SQLITE_CORPUS_PATH" \
  --candidate "$DESTINATION_PARENT/authority-candidate.sqlite3" \
  --owner-subject "$RKB_OWNER_SUBJECT" --prepare-unreferenced
```

The returned counts/hash are safe metadata; the database contains private
identity/audit data and must remain mode 0600. The receipt always says
`activated=false`, `legacy_control_disposition_required=true` and
`oauth_continuity_review_required=true`. It is not evidence of target readiness.

Before actual activation:

1. Review legacy pending disposition and the exact target parity/readiness gates.
2. Quiesce **all** authority writers: API OAuth mint/refresh/revoke, participant
   changes and local personal/service auth CLI. Retain the original OAuth files.
   Regenerate the projection under that boundary; any later source authority
   change invalidates an earlier prepared snapshot. Never dual-write two rights
   databases or roll back to stale grants after target auth changes.
3. Review exact current service release ancestry, existing units/drop-ins and
   target-only `RKB_SQLITE_CORPUS_PATH` change. Preserve issuer/resource, owner,
   OAuth state/key, queue/spool/control paths and PH service epoch bindings.
   Do not switch legacy content consumers to the empty target authority database.
4. Start the reviewed target composition, let the existing client perform normal
   OAuth refresh, and verify owner MCP current authority and a real non-owner
   verified-link flow. Recheck current revocation and outstanding operations.
5. Confirm deployment/cutover separately. Keep old content/read-only recovery
   evidence until pending disposition and retention prerequisites are satisfied.

The projection script never edits environment, units, grants, OAuth files, queue,
spool, accepted journal, target PostgreSQL or the old source database. There is
no activation shortcut in its CLI.
