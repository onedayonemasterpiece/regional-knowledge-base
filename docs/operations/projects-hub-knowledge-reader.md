# Projects Hub reader activation (inert by default)

The optional `ProjectsHubReader` is mounted on the existing RKB app at
`POST /internal/projects-hub/knowledge-search` only when explicitly supplied to
`build_server`, or when main receives `RKB_PROJECTS_HUB_READER_CONFIG` with an
explicit target composition. It does not make a legacy backend compatible with
the target. No credential, link, grant, deployment or public-profile cutover is
performed by this implementation.

The loopback caller uses a dedicated service credential, never the owner's
OAuth bearer. Its private descriptor limits the existing `projects_hub` provider
tenant, PH actors, workspaces and exact RKB identity-link revisions. The descriptor
is a technical routing scope, not proof of the human link. Every request resolves
the current independently verified link through `ReadAuthority.resolve_adapter`.
The same participant capabilities, native source gate, shared admission,
absolute monotonic deadline and final HTTP issuance fence apply. A revoked human
link, technical scope or source withholds the whole evidence result. Public
forwarded addresses cannot satisfy the loopback check.

A reviewer must substitute the observed existing RKB loopback port and stable
private state paths. Neither service may point at a temporary artifact or venv.
Files are absolute, bounded regular files, mode 0600, outside source control.
The credential is a separate file holding a dedicated random service secret.
Changing either descriptor or credential invalidates the retained configuration;
a deliberate reviewed process restart loads a replacement.

RKB descriptor (`RKB_PROJECTS_HUB_READER_CONFIG`):

```json
{
  "codec": "rkb.ph-reader.v2",
  "service_id": "projects-hub-reader",
  "service_token_file": "/ABSOLUTE/PRIVATE/ph-rkb-reader.token",
  "provider_tenant": "REVIEWED_STABLE_PH_TENANT",
  "bindings": [{
    "actor_sub": "usr_VERIFIED_PH_ACTOR",
    "workspace_id": "ws_VERIFIED_PH_WORKSPACE",
    "link_id": "CANONICAL_VERIFIED_LINK_UUID",
    "link_revision": 2
  }]
}
```

Before adding a row, confirm the current RKB personal identity through the
existing participant account-binding flow: `AccountBegin(provider='projects_hub',
tenant=...,provider_subject=...)`; independent authenticated PH observation
recorded by trusted `Participants.observe_account` with audience
`rkb-account-binding`; then `AccountConfirm(method='provider_observation',
observation_id=...,expected_link_revision=...)`. Review the actual resulting
link revision and current `public_search` capability. An email, transcript,
caller-supplied canonical UUID or service credential is insufficient. The new
read route cannot create/confirm links or grant capabilities.

The paired PH descriptor and its `PROJECTS_HUB_RKB_CONFIG` key are documented in
Projects Hub's `docs/operations/regional-knowledge-provider.md`. The body codec is
`rkb.knowledge-query.v2`; the response is `rkb.knowledge-search.v2`. Request ID,
turn generation, minimum data sequence and the same-host absolute monotonic
2-second deadline pass through unchanged. Status, actual observed data/projection
sequences, prior-write-pending and context gaps are explicit. Unavailable or stale
results carry no evidence or success mode. Normal acceptance requires actual
hybrid retrieval; lexical degradation is not product acceptance.

Activation remains blocked until reviewed configuration, verified links, compatible
API target composition and target readiness exist. Preserve all currently running
features: prove each running release is an ancestor of a main candidate before
switching either service. After reviewed activation independently verify an actual
PH request, native query receipt, current-source response, revoked-link denial and
phone/non-owner behavior. Unit/synthetic ASGI tests do not establish any of those
live acceptance results. Rollback removes the optional env key and restores the
previous reviewed main release; do not delete utterance or ingestion state.
