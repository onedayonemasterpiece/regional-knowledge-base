# Organizations in the regional evidence graph

**Target contract, 20 October 2026; documentation is not deployment acceptance.** See the [implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [architecture](designs/rkb-scalable-kaggle-architecture-20261019.md) and [Story/facts contract](story-registry.md).

## Current code, target ownership and migration boundary

The [a8cf41b baseline](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/a8cf41b50b8ccb62409d046d009d53c09d74b1d7) contains bounded GraphBundle ingestion, entities, mentions, aliases, directed relations and source/page/region evidence. Historical SQLite implementation and sql/024_organization_graph.sql are reusable code/schema evidence, not the target deployment.

The target authoritative corpus is one PostgreSQL/pgvector database on Kaggle with one fenced writer, BGE-only and the existing request/controller/BgeQueue path. The direct outbound notebook WSS uses the existing DevCoveer ASGI broker; its small durable control journal does not duplicate the graph authority. No local encoder, E5, Supabase target or separate graph service is introduced.

**Only historical/cartographic GEO layers are migrated.** Old books, Stories, personal/manual drafts, cases, contributions and old research jobs are not migrated. Fresh MCP imports rebuild source facts, entities, cases and sufficient narratives. Provenance needed for GEO must remain honest about its archived origin; it must not masquerade as a newly accepted book revision.

## Model semantics

The calling model submits a bounded GraphBundle in the existing book_ingest(stage) workflow or an authorized graph_stage enrichment against an active accepted source. GraphService validates exact quote membership, source/revision, actor and permitted operation. There is no hidden server-side historical inference/LLM. Graph facts share the same research checkpoint as dates, observations, Stories and cases; they do not require a second whole-book pass.

Canonical POI identity remains with Street Story: an organization is not the building in which it operated. Historical source people and authors are not rkb_users participants. Identical names do not merge people, institutions or places. Author, editor, translator and quoted narrator remain distinguishable.

Supported GraphEntity.kind:

- person;
- organization;
- event;
- historical_thread;
- poi_ref.

Organizations have stable explicit IDs/keys, current/historical/former aliases, language, valid-time/time scope and exact source evidence. Two institutions with the same written name remain separate candidates. A name change becomes an alias only after source review. A predecessor is a directed relation, not identity. An institution's move does not change building identity. Roles, affiliation intervals and geographic scope retain evidence and uncertainty.

## Controlled directed relations

| Relation | Source kind | Target kind | Meaning |
|---|---|---|---|
| affiliated_with | person | organization | Sourced affiliation; not automatically employment |
| participated_in | person / organization | event | Participation, not causality |
| member_of | person / organization / event / poi_ref | historical_thread | Editorial thread grouping |
| founded_by | organization | person | A source's founding claim |
| operated_at | organization | poi_ref | Location of activity, not building ownership |
| predecessor_of | organization | organization | Institutional succession, not entity merge |
| occurred_at | event | poi_ref | Event location, not proximity |

Every edge carries 1–8 exact source refs, optional time_scope, state and review_note. Same-entity edges and reversed types are rejected before persistence; the authoritative writer repeats endpoint validation. Model annotations do not bypass validation. Existing accepted relations in the new corpus are not silently relabelled/remapped. Correction/retraction retains previous evidence and versions.

## Reading and derived uses

Use existing graph_fetch(entity_id, limit<=20) for a permitted entity and one-hop neighbors/aliases/mentions with source refs; graph_related finds candidate evidence. These calls do not promise full graph traversal or global cross-source identity normalization. Entity_list(kinds=["organization"]) lists accepted source mentions, not all institutions in the region.

Searchable text and bibliography of all active accepted books have no per-book owner filter. Illustrations are public. Source withdrawal/revision, private drafts and nonpublic participant data still constrain disclosure. Page/stripe proof requires existing scan_proof_read for owner/demo/expert/local_historian with current grant/expiry; full PDF remains private. Verified owner_attested/independently_verified demo/expert/local_historian categories atomically materialize the existing scan_proof_read grant with audit/source category revision and demo expiry; claimed does not grant. Reads check current capability/scope, never a category bypass or automatic regrant after revoke. Removing a category revokes only its policy-derived grant basis; independent explicit grants retain their provenance. Recheck cached and delayed results. Graph visibility must not be inferred from a requester's professional weight.

Facts, dates and typed numeric observations are preserved without forcing a Story. Exact dates retain calendar, precision, date_role and valid-time; measurements retain decimal/unit/subject/period/method. A sufficient coherent episode or explanation automatically receives a saved basic narrative in existing Variant.body, exact default_variant_ref and readback. Facts-only material remains facts/candidates without a fake body. A brief narrative need not repeat every canonical fact; graph and evidence retain them. Story get/search/voice/export use the actual saved text, not a summary of its potential.

POI facts use the existing evidence/outbox and Street Story fact intake; identity linked is not evidence delivered/accepted. Source revision changes require reanchor/review; withdrawal removes affected current projections without erasing other independent evidence. RKB case conclusions do not automatically change Street Story's canonical POI.

## Acceptance and remaining implementation

A useful check includes two different same-named institutions, a source person distinct from a same-named user, event and unresolved canonical POI; exact evidence; directed roles; alias/affiliation period; idempotent stage; wrong-direction rejection; source withdrawal and access readback. Calendar/measurement tests must also work without a Story. Narratives need persistence/readback, not merely a high-interest graph card.

Baseline tests are not proof of deployed MCP schemas, extraction coverage or latency. Typed organization roles/intervals, source-grounded event/entity dates, numeric observations, edge retraction/history and authorized depth/cursor traversal require the concrete gates in handoff. No extra registry or global entity-resolution platform is a prerequisite. Street Story remains the owner of geographic identity. Protected quality/capacity/latency requirements are unchanged.
