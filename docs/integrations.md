# Cross-project integration contracts

Updated2026-10-20. The [single handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) is the target; prior release reports are scoped historical evidence. Reuse stable references and narrow owner APIs, without copying product databases or adding a new integration service.

| Product | Owns | RKB relationship |
|---|---|---|
| Regional Knowledge Base | Source provenance, accepted knowledge, historical entities, actual stories/variants, independent disagreement cases, source needs. | Typed evidence/ref API and durable request/receipt lifecycle. |
| Street Story | Physical POI identity, aliases, geometry/lifecycle decisions and its own POI arbitration. | Source evidence events, model-selected POI links, poi_context; RKB independent case may link POI but is not owned by POI arbitration. |
| Regional Cartography | Historical map/layer interpretation, georeferencing/geometry and calibration. | Versioned layer refs/temporal place context, unresolved/outside_coverage; do not fabricate coordinates. |
| Projects Hub / Mira | Project routing, tasks, one live voice runtime and user-facing screen/voice. | Existing live-interaction + expanded typed RKB provider; RKB owns research need, PH owns task execution/deadline. |
| VibePublish | Native provider operations, uploads/media-store entries, Telegram/VK credentials, rate limits and receipts. | One existing uploader, binary ingress, exact operation refs/readback; RKB owns source/figure semantics and publication intent. |
| Wonderful Lections | Lecture/editorial product. | Sourced research, historical illustrations, page provenance and existing Vibe asset bridge; no coupling to its business DB. |

## Clients and request completion

Kaliningrad Base is the single RKB Slack bot; Dot is a replaceable executor. Personal MCP and Mira need no mandatory Slack intermediary. All routes share verified person, per-dialog order, equal-person fairness and resource permits. Real user request accepted, domain Saved and provider delivered/ACK are separate durable facts. Unknown delivery reconciles before resend; another dialog continues.

Expand PH's current search-only adapter with resource-bound transport/identity, versioned pending/result/artifact envelopes, allowed research operations and receipt continuation after voice reconnect. Keep the Live tool surface small (`knowledge_search`); progressive research capability belongs to the typed owning-product adapter, not an indiscriminate dump of admin tools into voice. No second voice app/session/IAM/queue. A spoken story reads the saved body or saved adaptation, not praise/summary invented during delivery. Evidence and bibliography appear on screen; proof follows its audience policy.

## Evidence and media contracts

Use stable opaque `knowledge://documents/...`, `knowledge://illustrations/...`, POI/layer refs and native provider entry refs. Identity is not authority. No permanent signed URLs or bearer tokens inside refs. Source activation and outbox event commit together; downstream map/provider downtime does not roll back a valid book. Exact source revision/payload/operation keys and native receipts make retry idempotent.

Source facts, people, dates, observations, GEO, stories and disagreement proposals share one bounded reading/research frontier. Reconcile semantic identity before linking entities/POIs; nearby coordinates/name similarity does not suffice. Preserve graph_fetch/related/stage, poi_registry search/get/select/create and poi_context without whole graph dumps. Accepted source-side places can remain unmapped.

Illustrations are public; stripe quote/page proof requires owner/demo/expert/local_historian capability and suitable delivery route. Full originals and private drafts are separate. A mirror's private storage location does not determine the public class of a returned illustration. Keep source, display, provider rendition and delivery SHA separately; exact originals require exact SHA, transformed media need their own native rendition readback. Do not require equality of different renditions or pHash as proof. Missing provider digest fails closed.

VibePublish remains uploader and provider limiter. Owner-provided `TELEGRAM_RKB1`/`TELEGRAM_RKB2` are the initial independent session pool for direct Kaggle reads, subject to stage-0 authorization/archive-access checks (the second is currently empty). One exclusive session lease per run limits simultaneous runs together with Kaggle capacity; no cloned auth keys, second uploader or unbounded PDF in model JSON. Topic2 originals/topic4 images are configuration verified during implementation, not credentials copied here.

Same-host RKB↔Street Story/Vibe calls use dedicated target-scoped identity on loopback/Unix socket. Do not use owner browser OAuth, public detours or another proxy. Kaggle must reach the owner via authenticated approved transport; its localhost is not DevCoveer.

## Public discussion and tasks

Preserve VK editorial queue/scheduling/cancellation, registered-post comment ingestion/moderation and subscriptions through VibePublish. Actual provider allowlist/token permissions must be verified; generic read.thread is not VK comments support. RKB retains case question/arguments/conclusions and history; PH task_done does not resolve a research need automatically.

Astro static pages use public selected projection, permanent opaque slugs, no login/backend dependency, current and last-supported conclusion, exact published revision and last-known-good output. Real URL exists only after first successful readback. Late build/revocation is generation-fenced and purges/rebuilds HTML/JSON/OG/assets. Restricted proof/private drafts never enter the export.

Do not import old non-GEO content while preserving these functions. Reuse PR151 code only after current-diff review; branch-level tests are not production PASS. Verify at least one real non-owner across personal MCP/Slack/Mira, authoritative POI/map receipt, actual provider result/hash ACK and safe public page. [Rights](rights.md), [POI](poi-integration.md), [case collaboration](design/rkb-kaliningrad-base-collaboration-20261018.md).
