# Location-first source ingestion

Updated 2026-10-20. The [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) defines the PostgreSQL/pgvector+BGE-only target on Kaggle, the existing DevCoveer controller and one WSS connection. This document preserves the source-side geographical model; it does not certify deployment.

The ordinary import path remains model-selected location identity without mandatory coordinates, cartographic completion or a separate approval queue. The source place graph, Street Story's physical POI registry and Cartography's geometry keep distinct owners.

## GEO-only bootstrap

Migrate only geographic/cartographic identities and data: layers/places/toponyms, geometry/topology, CRS/GCP/transforms, temporal validity, uncertainty, source-specific geographic observations and minimal provenance/author attribution. Do not migrate old book text/chunks/vectors, stories/variants, personal drafts, general facts/people/events, expert contributions or disagreement cases merely because they mention a location. All other content is produced through fresh MCP import.

Before production fresh imports, establish the minimal GEO identity manifest with stable layer/place IDs, aliases, owner refs and CRS. Bulk rasters/geometry may arrive later through the existing GEO queue with explicit unresolved availability; they enrich the same IDs without book reimport. Fresh A+B tests before the manifest run only in an isolated fixture. Preserve stable GEO and external owner IDs when fresh target books link to them. An old book/quote link survives only as the minimal `legacy_unanchored` source/edition/SHA/page/locator needed by the GEO object, not an accepted target mention or searchable old corpus. Exact edition/source SHA plus validated evidence mapping is required to reanchor. Keep an unavailable/different-source mapping explicitly unresolved. See [GEO cutover](migrations/rkb-layers-only-cutover-20261019.md).

Infrastructure identities/configuration are preserved separately. There is no mandatory export or active migration of old non-GEO manual work. A bounded rollback backup follows the handoff's retention and does not become a second active corpus.

## Normal path

Read the source once. During the same model review, extract meaningful people, organizations, episodes and places. Search the existing registry through `entity_list`; use `graph_fetch` to compare aliases, context, source evidence and neighboring places. The MCP-calling model decides whether an existing entity is the same place. Reuse its `entity_id`, or create a new location when the context really differs. The backend validates ownership, exact evidence and reference integrity; it does not decide historical identity from words.

Use `book_ingest(stage).entity_candidates` alongside ordinary source chunks, factual/date/observation records and any justified `story_candidates`. A source-bound fact, date or measurement requires no artificial Story parent. No second model invocation or separate location-import campaign is required. For already accepted books, use `graph_stage` on the active revision; do not reimport, re-chunk or re-embed unchanged source material merely to enrich locations. Within a still-pending book, keep the same new-node key and canonical label across batches; after materialization, reuse the returned ID across books.

A geographical attachment is not necessarily a coordinate. “On the island”, “at the market square”, “in Königsberg”, an address, or a now-lost building are valid levels of source description. Preserve that level. Do not invent a point or assign an entire passage to the nearest modern house. Several places may be mentioned in one chunk; a mention is not automatically the setting of every claim in that chunk.

## Physical POI integration

A source place which identifies a standing building, ruins, memorial, plaque or known lost-building site can also be a Street Story POI. The importing model uses `poi_registry(search)` and explicitly selects/creates the canonical owner POI with the existing RKB entity ID; it does **not** create a second owner catalog. Street Story then finds source stories, people and original evidence through `poi_context` and its read-only `/v1/pois/{id}/knowledge` bridge. See [model-selected POI linking](poi-model-link.md). Ordinary book activation never waits for this optional owner network call.

## Reuse the existing graph

There is no new database or parallel location catalog. Source-side locations use the existing `rkb_entities` table and legacy wire kind `poi_ref`. `entity_list(kinds=["place"])` also finds these nodes. An RKB entity UUID identifies the source-side place; it exists independently of a Street Story POI or a map feature.

Each location has a label, exact source spelling and exact chunk/page/region/quote provenance. Small optional fields are:

- `place_kind`: a descriptive type such as city, settlement, district, island, street, square, building, bridge, watercourse, courtyard or area. It is not a closed ontology.
- `place_context`: a short disambiguation: containing area, historical setting, building generation or distinction from a namesake.
- `aliases`: historical/current/variant names with their own source evidence and optional time scope.
- `research_sources`: URL and note for context consulted by the model. On an alias this marks external research explicitly; its book evidence remains the original mention and does not purport to contain the modern name.
- `map_refs`: model-selected `cartography://...` references added later to the same entity. A reference does not certify geometrical accuracy or permission to publish a map.
- `canonical_poi_ref`: an explicitly model-selected existing `streetstory://poi/...` key. The server checks that the owner key exists; a name match does not select it.

`located_in` is a directed, source-backed place-to-place relation, for example building → square → island → city. It may have a `time_scope`; membership need not be historically permanent. Do not infer containment merely from co-occurrence, and do not fabricate a parent to fill every branch. An uncertain parent can remain absent.

A bundle can use `entity_refs: {"parent": "existing UUID"}` for an existing endpoint. This avoids inventing another mention of the parent in the current passage. The relation itself still needs actual source evidence. Self-relations, unresolved endpoint substitutions and unauthorized mutations are rejected. Public source-graph read access does not grant write access to another actor or canonical owner. People, organizations and events retain their existing relation types; a building is not the organization occupying it.

## Avoid duplicates through model reuse

`entity_list` searches labels, authorized active aliases and original mention spellings, with Unicode case folding. It returns IDs, aliases and concise location context. Search is a shortlist, not an identity oracle. Inspect candidates rather than choosing the first match. Homonymous streets in different towns, a demolished building and its replacement, an administrative town and a physical island may be distinct entities even if names overlap.

Repeated writes with the same source keys and evidence are idempotent. Cross-book sameness uses an explicit `entity_id`; a different spelling is appended as an alias/mention. Later maps and canonical POI references enrich that ID, preserving mentions and relations. This prevents duplicates on the intended workflow; it is not a claim that a model can never make a mistaken identity decision. There is no automatic name-based merge or destructive history rewrite.

Reverse discovery stores possible passages on its existing discovery job, not as source mentions. `graph_fetch(discovery_job_id=...)` exposes those suggestions for model review. Legacy machine-only mention candidates are excluded from the normal location card and alias search. Alias synchronization may update an already selected external ID, but never selects an ID for an unbound entity.

The background geo worker may discover catalog candidates. It leaves `awaiting_agent` with `identity_selected=false`; it never calls semantic stage/apply on behalf of the model. The older multi-call geo interface remains an optional downstream adapter, not the routine import path. Ordinary ingestion with no selected external ID makes no owner-catalog call and does not depend on Retromap, OSM or Street Story availability.

## Chunks, stories and later maps

`rkb_entity_mentions` attaches exact source passages to one or more locations. `graph_fetch` exposes those references and neighboring places. Story Registry links use its existing `link_entity` operation with the same RKB UUID and `kind="place"` (or another applicable entity kind). The model links the episode, not every unrelated place name found in the surrounding retrieval chunk. Existing source and story search continue to serve their distinct roles; no vector is created for each map primitive.

Cartography independently creates source-specific observations, dates and geometry. A later model compares them with existing place identities and adds references. A city or island can be useful to content generation, guides and historical research before any outline exists. No cartographic coverage or acceptance state is a gate for source publication. A map reference, a source assertion, and an accepted physical POI remain different things.

The same registry applies to lectures, transcripts and other attributed materials once they are registered as sources. This change does not claim to implement audio capture/transcription or a new arbitrary-file intake adapter. Preserve author/speaker, source type and available page or time provenance; a recollection does not become a verified book fact merely by importing it.

## Scope and checks

Focused regression coverage checks map-free creation, source quotes, explicit cross-book reuse, alias search, hierarchy with existing parents, later map attachment without a second identity, current resource authorization and absence of automatic name-based binding. Existing retrieval vectors, chunking, private source bytes and source revisions are unchanged by location enrichment. Broad mass-ingestion and retrieval SLA are separate existing product requirements; a location test does not certify them.

Historical illustrative context (not new acceptance): Kneiphof was a practical initial case. The official Cathedral site identifies today's Kant Island as former Kneiphof: https://sobor39.ru/about/life/ . The Cathedral's own history locates it on that island: https://sobor39.ru/about/ . These are web context sources, not invented statements inside Gause or Brunneck. Historical municipal Kneiphof and the geographical island must still be distinguished when an account concerns administration rather than location.
