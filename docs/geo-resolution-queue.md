# Geography enrichment: optional downstream queue

Updated 2026-10-20. Normative target: [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md). Use the same PostgreSQL/pgvector corpus on Kaggle, existing DevCoveer controller and one outbound WSS connection. The source graph and optional geographic enrichment share that workflow; no new queue service or canonical location database is introduced.

## Ordinary ingestion needs no geo queue

Use [location-first ingestion](location-registry.md). The calling model reads the source, searches existing locations, chooses an ID or creates a distinct place, and records evidence and geographical relationships. A map, coordinates, Street Story identity or queue completion is not required. Existing `poi_ref` graph nodes are source-side locations; `unresolved` means the external POI reference is absent, not that the source location is unusable.

Use `book_ingest(stage).entity_candidates` during fresh import and `graph_stage` on accepted target material. `entity_list` searches labels, aliases and literal mentions; `graph_fetch` supplies bounded context. The model may add a source-backed `located_in` relation for a building/square/street/island/city. Web clarification stays separate in `research_sources`. Facts, dates and observations may exist without Stories.

## Purpose and decision boundary

The existing downstream queue is a bounded adapter for changed owner-catalog/cartography dependencies. Its durable states make interrupted work resumable; they are not semantic acceptance or extra user approval steps.

A worker may read the owner catalog and save `candidate_discovery`. It leaves `awaiting_agent` with `identity_selected=false`; it never calls semantic stage/apply automatically. A unique name, external-ID hint or proximity is not a model identity decision. Lookup failure remains a bounded dependency state, not proof no place exists.

The model can directly enrich an existing entity through `graph_stage` with its `entity_id` and explicitly selected existing `canonical_poi_ref` or `map_refs`. Check the current producer authority and exact key. Preserve source identity/mentions. Today's coordinates do not prove a historical footprint; a map URI does not certify accepted geometry, site/generation identity or publication rights.

## Existing operations and bounded scheduling

`geo_status`/`geo_get` inspect work. `geo_claim`, `geo_stage`, `geo_apply` support explicit external processing without becoming ordinary book-import steps. Keep current actor authorization, exact evidence scope, dependency versions, lease fencing and idempotent accepted receipts. A model-selected existing owner ID is not rejected merely because its present name differs from the source's old spelling.

`geo_recheck` opens a new relevant attempt only for a meaningful dependency change. Target-source mentions may populate intents without reimport/re-embedding. Separate candidate discovery from source activation/indexing; do not poll identical dependencies repeatedly. A stale owner/cartography context requires recheck, not reuse of its old decision. The queue is outside the Live query path and cannot starve interactive or accepted source work.

## GEO cutover and state retention

Only whitelisted GEO data and minimal provenance migrate; [cutover details](migrations/rkb-layers-only-cutover-20261019.md). Old books, stories, drafts, contributions, cases and their pending delivery/discovery jobs are not target bootstrap content. Do not replay an old source command merely because a queue row survived.

Production fresh imports require the minimal layer/place identity manifest, aliases, owner refs and CRS first; pre-manifest A+B imports are isolated fixtures only. Bulk rasters/geometry can follow through this existing queue with explicit unresolved availability and no reimport of accepted books. Any retained/reconstructed GEO intent must refer to transferred GEO identities and verified current dependencies; a legacy book evidence reference remains `legacy_unanchored` until exact SHA/edition/page mapping. Keep infrastructure identity/configuration and live control-plane receipts according to the bounded recovery contract. Byte-cache/journal GC must not discard the sole recovery record for an accepted nonterminal target operation; historical jobs do not justify unbounded hot-log retention.

## Ownership and visibility

RKB owns source evidence and source-side places; Street Story owns canonical physical POI identity; Cartography owns map observations and geometric interpretation. None writes replacement IDs or canonical state in another owner's store. No copied modern-map corpus or embeddings for every geographic primitive are required.

Accepted active book text/bibliography and accepted illustration renditions use the public projection. Proof scans require current owner/demo/expert/local_historian `scan_proof_read` authorization materialized by verified-category changes under [rights.md](rights.md), with no category-name fallback or recreation after manual revoke; full originals/private drafts remain separate. Source withdrawal/current revision/audience are rechecked on reads and delivery. A public POI is not a proof-pixel grant. Updating a place does not mutate source text or trigger a full BGE reindex.

Acceptance preserves map-free ingestion, exact source identity, explicit model selection, stale-dependency fencing, current access, unchanged owner keys, idempotent retry and honest unresolved output. Old queue implementation evidence does not establish target deployment.
