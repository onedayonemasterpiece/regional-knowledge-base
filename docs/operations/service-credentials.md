# Local service credentials in the existing RKB auth authority

Technical service authentication uses `RegionalOAuthProvider`'s existing encrypted
state/key and the existing owner-only `personal_auth_cli` runtime. It does not use
a person, browser login, OAuth client_credentials, new IAM, service, queue or
corpus journal sequence. Human rights still come from current verified participant
links and capabilities. This implementation does not provision any live key,
configuration, link or grant.

The first fixed technical scope is `knowledge_search` from provider
`projects_hub`, one exact configured provider tenant and at most 32 exact PH
workspace IDs. A service credential cannot import, mutate, select a canonical
person or bypass the current user/source/destination gates. Owner-personal admin
checks run on each issue/list/revoke/audit action. The protected credential file
uses codec `rkb.service-credential.v1`; tokens have a distinct `rkbs_` namespace.
Only SHA-256 digests are stored in the encrypted credential records. Raw tokens
are written exclusively to the operator's mode-0600 output file and never printed.

Review the exact scope in an existing private directory, for example:

```json
{
  "operation": "knowledge_search",
  "provider": "projects_hub",
  "provider_tenant": "REVIEWED_STABLE_PH_TENANT",
  "workspace_ids": ["ws_VERIFIED_PH_WORKSPACE"]
}
```

Run from the reviewed main release using the existing protected auth environment
and interpreter. Existing corpus/auth state/key files must already exist; the CLI
cannot create a parallel identity authority. Commands below are templates and
have not been run against production:

```sh
python -m regional_knowledge.personal_auth_cli service issue \
  --service-id projects-hub-reader --scope-file /PRIVATE/ph-rkb-scope.json \
  --output /PRIVATE/ph-rkb-credential.json --days 365 --reason 'Reviewed PH read scope'
python -m regional_knowledge.personal_auth_cli service list --service-id projects-hub-reader
python -m regional_knowledge.personal_auth_cli service audit --credential CREDENTIAL_UUID
python -m regional_knowledge.personal_auth_cli service revoke \
  --credential CREDENTIAL_UUID --expected-epoch 1 --reason 'Reviewed reader revocation'
```

An output credential directory must already be private. Issue persists and fsyncs
the original credential file before mutating the same encrypted authority. A lost
response uses the identical issue command plus `--resume`; it reuses the exact
credential ID/token/scope/expiry. Never delete that file and issue a second token
to resolve UNKNOWN. Issue returns metadata only after same-store readback. To
rotate, provision a new explicit credential ID and review both descriptors; revoke
the old credential after the switch. Revocation is CAS on the expected epoch;
repeating the same ID/epoch/reason after a lost post-commit response returns the
same disabled next epoch without another audit event. A mismatched reason/epoch
conflicts. There is no un-revoke operation.

RKB's reader descriptor pins `service_credential_id` and
`service_credential_epoch`; PH pins the same fields and points `service_token_file`
at the protected issued JSON file. The service ID, scope and workspace must agree.
RKB verifies the actual presented token in its existing authority, then resolves
the human link separately. The current enabled state, epoch, scope digest and
expiry are rechecked at final HTTP issuance. A busy auth writer fails this
read-only fence closed instead of waiting indefinitely. No read writes auth state.

Durability/recovery is deliberately the existing personal-auth model: shared
process/file locking, encrypted atomic file replacement/fsync, persistent audit,
CAS and local readback. This change does not create a remote auth checkpoint or
claim that the encrypted store has a new backup/recovery guarantee. Existing
protected auth backups and restore procedures remain necessary; restoring stale
auth state can restore stale rights and must be handled by the operator's existing
recovery policy. Corpus replay cannot reconstruct missing service credentials.
Never label synthetic credential/ASGI tests as actual PH or phone acceptance.
