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
POI lifecycle/location identity, atomic POI claims, and the contradiction ledger
and expert arbitration state **within its canonical POI model**.

Knowledge Base remains the authority for book/journal provenance and exact
page/region evidence. It also owns the small accumulative knowledge projection for
historical entities: people, organizations, events, historical threads and
source-side places. A source-side place can exist without a map, coordinates or
a Street Story POI. Model-selected physical POI references enrich the same entity;
see the current [location-first policy](location-registry.md). During book ingestion it may extract
evidence-backed POI fact candidates and evidence-backed person/event relations and
deliver POI evidence asynchronously after a successful finalize.

The two graphs are not competing copies: Street Story owns POI identity; Regional
Knowledge owns cross-source historical evidence/navigation and references POIs by
stable external identity.

Never copy the whole corpus into Street Story. Deliver only typed evidence events
with stable knowledge:// references and inherited access scope.

See [POI evidence bridge](poi-integration.md).

### Source disagreement cases (designed; not yet implemented)

The [RKB disagreement-journal design](design/rkb-disagreement-journal-design-20261018.md)
introduces a separate research process for disagreements in sources, facts and
interpretations. RKB owns those cases, exact evidence, attributed contributions
and revisions of preliminary conclusions. Story Registry consumes a case result
by stable ID and conclusion revision for stories and publication materials; it
does not own the research case.

A Street Story POI conflict may reference an RKB case and a particular conclusion
revision, and the RKB case may retain the external conflict reference. Applying a
research conclusion to canonical POI data remains an explicit Street Story
decision. Existing POI conflicts do not need to be migrated into RKB, and creating
an RKB case does not automatically change POI claims or identity.

## Projects Hub

Projects Hub is the natural user-facing orchestration surface:
- one Live assistant;
- project selection/routing;
- Knowledge Base read capability alongside other project tools.

It consumes the Knowledge Base Live profile (`knowledge_search`, a compact evidence pack in one call) through `live-interaction`. The full profile separately exposes `search` and `fetch`. For private knowledge, Projects Hub holds a user-approved, resource-bound Knowledge OAuth delegation; it never forwards its own bearer token. Projects Hub does not become the identity provider.

For the planned disagreement workflow, Mira, personal MCP connections and
registered Slack threads provide authorized contributions to RKB-owned cases.
A source/research need remains in the RKB case; an optional Projects Hub task owns
assignment, deadline and execution tracking. A completed task does not establish
that the historical question is resolved. The research-specific task bridge is
a planned narrow contract, not implied by the existence of generic tasks.

### Planned Kaliningrad Base Slack expert interface

The end-user Slack app **Kaliningrad Base** is not dot's public alias. Users interact only with the dedicated app. It receives trusted Slack events, stores an identity-bound request with the RKB backend, wakes dot via an MCP Events-style contract, and delivers correlated replies as Kaliningrad Base. The previously accepted Projects Hub MCP Events flow is a reference for the protocol only: its owner_actor_id and dot-as-Slack-sender are not reusable for multiple experts. No additional IAM or mandatory Projects Hub intermediary is introduced.

RKB remains the authorization authority for delegated actor, requested action, source/case ACL, original-scan proof and outgoing projections. Import remains **owner-only through owner-specific ChatGPT/MCP/dot paths**, never through the Slack expert app, including when the owner sends the message. The bot's service profile cannot assume the owner's privileges. Slack actor, model executor and source-comment author remain separate. The client stores no case copy; Projects Hub/Mira/mobile will later use the same RKB contract with their platform actor and resource-specific delegation.

See [collaboration design](design/rkb-kaliningrad-base-collaboration-20261018.md) and [product acceptance methods](testing/rkb-disagreement-pmi-20261018.md). These describe planned capabilities; no new bot or multi-user wake-up acceptance is claimed.

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

For illustration mirroring, use the illustration reference and a stable request
key based on owner/document/revision/illustration identity. Do not compare source
and delivered image SHA values or require image-similarity verification before
accepting the mirror. Compression or cropping must not become a delivery gate.
Optional crop SHA is technical provenance only. If visual deduplication is needed
later, reuse the existing perceptual-image tools from other projects; exact-byte
SHA does not establish visual sameness and ordinary pHash alone does not cover
partial crops. Exact PDF source deduplication remains a separate SHA-based rule.

The existing indexing owner also reconciles active private illustrations through
VibePublish binary asset ingress and media-store put/status/search/get. It persists
native operation IDs and verified entry references; failed provider/grant reads
leave missing mirrors recoverable without holding activation or vector indexing.
Only the RKB owner named in the private resource-specific OAuth grant is eligible.
Destination alias/topic stay in that runtime grant, and token rotation is written
atomically. RKB owns no Telegram session or second provider limiter.

### Planned VK case discussions

VibePublish is the intended owner of VK post publication and the required
extension for reading, replying to and moderating comments under explicitly
registered case-discussion posts. RKB owns the case, participant identities and
weights, allowed participation policy, observed contributions and conclusions.
These comment capabilities are planned; generic thread or reply commands must not
be treated as proof of VK support.

VK case posts are planned as **one ordinary new case-discussion post per Europe/Kaliningrad calendar day per community+RKB series**. Urgent posts are a separate bounded editor-approved lane (initial proposal: at most two additional posts/day), not a bypass for expert requests. RKB owns priority, slot reservation, public projection and case→VK binding; VibePublish owns provider-native scheduling, actual send/edit/cancel/comment operations and receipts. A reserved/scheduled item is not a published URL; a lost provider response is reconciled before any retry. Native VK scheduling and comment rights require live acceptance before launch.

Transport uses the existing local, target-scoped service connection. Provider
author identity, message revisions and native delivery receipts remain distinct
from model-authored interpretation. Public discussion exports inherit source
rights and require a separately allowed public projection. Whitelist moderation
deletes disallowed comments after publication; it is not a platform guarantee
that outsiders cannot post.

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

The background discovery adapter reads Street Story's local canonical identity
projection (`RKB_STREET_STORY_POI_DB`), reading only POI identity/name aliases.
No end-user bearer, book text or private claim is sent to another MCP. The adapter
never creates POIs and never copies claims/location/lifecycle records. Alias-set
versions on already referenced canonical POIs schedule bounded reverse discovery.
Separately, the implemented `poi_registry(search|get|select|create)` bridge calls
the configured Street Story owner service with its dedicated service credential,
never an end-user MCP bearer. The calling model explicitly selects/creates a
physical POI from an evidence-backed RKB entity; the owner receipt links the
same entity to its canonical POI. `poi_context` provides authorized evidence
for an existing POI. This optional integration is not an activation prerequisite;
a missing configuration or owner service is an explicit integration blocker,
not a reason to invent an identity. See [model-selected POI linking](poi-model-link.md).
