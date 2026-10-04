# Cross-project integrations

## Principle

Synergy comes from shared identity + stable resource references + narrow service contracts, not from one database or one giant MCP.

## Wonderful Lections

Use Regional Knowledge Base for:
- sourced research while writing lectures;
- historical illustrations;
- citation/page provenance.

Wonderful Lections already has a verified VibePublish asset bridge. For media mirrored to VibePublish, Knowledge Base can return the stable media-store entry reference and the bridge can obtain a temporary verified asset. Long term, Wonderful Lections should also accept a direct `knowledge://illustrations/<id>` source after authorization.

## Street Story

Street Story is the canonical owner of the regional POI graph, POI aliases,
POI lifecycle/location identity, atomic POI claims, contradiction ledger and
expert arbitration state.

Knowledge Base remains the authority for book/journal provenance and exact
page/region evidence. It also owns the small accumulative knowledge projection for
non-POI historical entities: people, events and reusable historical threads, with
stable references to Street Story POIs. During book ingestion it may extract
evidence-backed POI fact candidates and evidence-backed person/event relations and
deliver POI evidence asynchronously after a successful finalize.

The two graphs are not competing copies: Street Story owns POI identity; Regional
Knowledge owns cross-source historical evidence/navigation and references POIs by
stable external identity.

Never copy the whole corpus into Street Story. Deliver only typed evidence events
with stable knowledge:// references and inherited access scope.

See [POI evidence bridge](poi-integration.md).

## Projects Hub

Projects Hub is the natural user-facing orchestration surface:
- one Live assistant;
- project selection/routing;
- Knowledge Base read capability alongside other project tools.

It consumes the Knowledge Base Live profile (`search`, `fetch`) through `live-interaction`. For private knowledge, Projects Hub holds a user-approved, resource-bound Knowledge OAuth delegation; it never forwards its own bearer token. Projects Hub does not become the identity provider.

## VibePublish

VibePublish is a secondary media mirror/catalog and publication layer. Regional Knowledge Base owns:
- document/page/illustration semantics;
- ACL and rights;
- object-store source.

VibePublish owns:
- its provider/media-store entry;
- Telegram/provider identity;
- exact-byte evidence;
- optional model preview.

The VibePublish media-store origin metadata binds an entry back to a stable Knowledge resource without granting Knowledge access.

## Cross-service references

Use typed stable opaque references, for example:

```text
knowledge://documents/<uuid>
knowledge://illustrations/<uuid>
vibepublish://assets/<id>
```

A reference is identity, not authority. Receiving a reference never bypasses the target service's authorization check.

## Authorization pattern for integrations

For private user data the integration itself must be authorized:

```text
Wonderful Lections / Street Story / Projects Hub
        |
        | OAuth client of Knowledge resource
        v
shared application/platform identity
        |
        | same stable platform user UUID, Knowledge resource
        v
Regional Knowledge MCP/API
```

This makes cross-product synergy explicit and revocable. A service identity is
not a substitute for the user's grant.

See also [accumulative knowledge graph decision](reports/accumulative-knowledge-graph-decision-20261004.md).

## Implemented entity/POI discovery MVP

`graph_fetch` returns one authorized entity and at most 20 one-hop evidenced
relations; `graph_related` reuses accepted retrieval around its authorized aliases.
`graph_stage` writes model-authored candidates/aliases with exact evidence. These
three narrow tools are available only in the regular MCP profile; Live stays
search/evidence-only. No graph dump or automatic narrative service exists.

Production uses a read-only adapter to Street Story's local canonical identity
projection (`RKB_STREET_STORY_POI_DB`), reading only POI identity/name aliases.
No end-user bearer, book text or private claim is sent to another MCP. The adapter
never creates POIs and never copies claims/location/lifecycle records. Alias-set
versions on already referenced canonical POIs schedule bounded reverse discovery.
A remote resolver/delegated intake transport remains a future integration; no
unauthorized private-source delivery is enabled by this local adapter.
