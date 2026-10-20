# Model-selected Street Story POI from a regional source

Updated 2026-10-20. Normative target: [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md). The same model-selected owner operation and source graph move to PostgreSQL/pgvector on Kaggle with BGE-only retrieval. DevCoveer retains the existing controller and same-host Street Story bridge; the notebook uses one outbound WSS. This document is not evidence of target deployment.

## Migration and access boundary

Only GEO/place/layer identities, temporal/geometric data and minimal provenance transfer. Production fresh imports start after the minimal GEO identity manifest (layer/place IDs, aliases, owner refs, CRS); bulk raster/geometry arrival can remain unresolved in the existing GEO queue without later book reimport. A+B imports before that manifest are isolated fixtures only. Preserve the exact existing external POI key (`UUID` or `poi_ss_<24 lowercase hex>`), source-side place ID and owner. Old books/stories/manual drafts/expert contributions/cases and their evidence bodies do not migrate. Fresh MCP import creates new source facts, Stories and cases; exact edition/source SHA plus verified page/region mapping reanchors any retained GEO `legacy_unanchored` link. Infrastructure identities/configuration are preserved separately. See [GEO cutover](migrations/rkb-layers-only-cutover-20261019.md).

Accepted illustration renditions are public for everyone. Quote stripe/page/crop scans use the existing owner/demo/expert/local_historian `scan_proof_read` capability; full originals remain independently private. Its participant grants are atomically materialized from verified categories under [rights.md](rights.md); current grant/scope, expiry/removal and manual revocation govern reads, without a category-name bypass or grant recreation on read. Public POI identity is neither a proof-pixel grant nor permission to expose surrounding source-page text. Current source withdrawal/policy and audience apply on every delivery, including cache and late callbacks. Shared Slack remains `public_only`. See [rights](rights.md).

## Model-selected owner operation

**Rule:** A source location is not automatically a POI. During the same book-review the model identifies physical places worth visiting or historically locating (standing building, ruins, plaque, memorial, lost building site). Broad geographic references stay RKB locations.

After the normal `book_ingest(stage)` and source activation, the agent calls `poi_registry(action="search",query,external_ids)`; the shortlist contains owner POI IDs and aliases but selects nothing. The model compares site context and either calls `poi_registry(action="select",entity_id,poi_id,model_reason)` or `poi_registry(action="create",entity_id,site_state,site_context,model_reason)`.

Creation occurs ONLY in Street Story's existing canonical `pois` table with status `candidate`; a second POI register in RKB is forbidden. The operation attaches the returned `streetstory://poi/...` ref to the SAME RKB entity, keeping all historical names, exact evidence and story links. The Street Story owner stores the reverse RKB entity reference and source locator, not full passage text. A later source/book reuses both IDs. RKB source activation is independent of owner reachability; replay after network loss is idempotent.

Name and even proximity are candidate discovery signals, not an identity merge. Existing external identifiers or conflicting names must be checked by the model. Missing map geometry does not prevent a lost-building site POI; absence of modern coordinates must not generate a fake public pin.

Use `poi_context(streetstory://poi/<id>)` to obtain eligible RKB source mentions, linked people, events, places, source-bound facts/dates/observations and editorial stories, then fetch exact excerpts using ordinary evidence tools. A fact does not require a Story parent. Accepted active book text/bibliography are public without a per-book owner read filter; genuinely private records keep their own scopes. Street Story's authenticated `GET /v1/pois/{id}/knowledge` returns the same bounded context under the owner's separate loopback service grant. No user OAuth bearer or full PDF moves through this context bridge. The service credential does not impersonate a user.

The bridge is restricted to the established RKB↔Street Story installation service identity and purpose; it is not an owner-only read filter on public book text or illustrations. A dedicated secret must be provisioned in a protected systemd EnvironmentFile. The Android `STREET_STORY_DEVICE_TOKEN` is NOT that secret. The bridge requires its established protected service configuration; missing or invalid configuration fails closed. Test source-specific private-resource authorization and rejection of invalid service tokens; public projection must not inherit an owner-only content filter.

## Observed production acceptance — 2026-10-10

RKB runtime: `7be295732d4c2ce07bead5bed3c4947882138f41` (PR #110).
Street Story runtime: `93a0878a80f0f7c6b0cef286e4b691f2462c0206` (PR #255).
Both reported their exact expected source release and healthy HTTP listeners.

A ChatGPT MCP model reviewed already accepted Gause evidence for the **stone
Cathedral on Kneiphof**, not the former wooden cathedral at Altstadt.
It searched the Street Story owner registry by multiple current/historical
names and Wikidata Q225459. No existing matching owner identity was returned.
A model-authored `poi_registry(create)` created ONE owner-side candidate site,
without fabricated coordinates. RKB retained its original, source-backed
entity UUID and added the canonical Street Story reference; no book reimport,
re-chunking or vector reindex took place.

The model added a separately web-attributed historical alias
`Königsberger Dom` to the existing RKB location, and a source-backed
`located_in` relation to the already accepted Kneiphof island entity.
Existing editorial story about construction piles was explicitly linked to
the same cathedral entity, without approving its underlying historical claim.

**Readbacks actually passed:** owner catalog search by Wikidata Q225459 returned
one candidate; owner `get` returned the canonical ID and inverse RKB reference;
the same `create` command replayed with `replayed_owner_receipt=true`;
explicit `select` reused that owner ID; current RKB alias search returned
exactly one location. The authorized RKB MCP `poi_context` returned one source
location, one editorial story and original evidence. Street Story's real
device-authenticated `GET /v1/pois/{id}/knowledge` returned HTTP 200 and a
payload equal to the dedicated RKB service read, while unauthenticated
interservice requests returned HTTP 401. Neither application copied full
book bytes to the other's database. The private owner service credential is
stored under a 0600 EnvironmentFile and was not printed.

Historical probe (run only against the applicable registered release/environment): reproduce the narrow HTTP readback from the registered DevCoveer checkout,
without including user bearer tokens in arguments:
`python scripts/production/probe_poi_bridge.py --poi-id <existing-poi_ss-id>`.
It reads established local private tokens and emits only bounded IDs/status
and aggregate evidence presence, not source quotations.

**Limits:** This is a real one-site integration acceptance, not exhaustive
mass-import extraction, not a claim of independent verification of the physical
POI candidate, and not a historical-map geometrical acceptance. Selecting
an existing POI for a new different source and semantic recall over all people
in all books remain separately testable as additional material is ingested.

Source location graph semantics remain in [location-registry.md](location-registry.md).
Physical POI identity and candidate lifecycle remain Street Story authority.

## One-hop geographic projection available to Street Story

The authorized `poi_context` / `GET /v1/pois/{id}/knowledge` pair
returns the current eligible public/private projection and also returns each linked RKB source location's `place_kind`,
`place_context`, and any model-selected `map_refs`. Its relations now carry
the neighbor's selected Street Story POI ref (if any), the **source relation**
review state and its source time scope. This reuses RKB's existing source
graph — it does **not** create, merge or rename any owner-side POI, invent
coordinates, transmit private passages to Cartography, or add a second
location register.

**Historical evidence separation is explicit:** every projected item has
`map_ref_verification=not_verified` and
`historical_geometry=not_verified`; the response itself sets
`cartography_acceptance_claimed=false`. A Cartography place/group URI is
a navigation/reference hint chosen by the model, **not** proof of accepted
footprint, surveyed location, physical-building generation, source rights,
or a live map ready for distribution. The source context and relation time
scope do not imply physical validity dates. Missing map refs are returned
as `[]`, not generated from a name or today's OSM location.

The bounded response queries one extra *authorized* RKB location to set
`has_more` correctly. Exactly `limit` accessible locations means
`has_more=false`; an actual `limit+1`-th source location means
`has_more=true`. Public eligible source locations are not filtered by book owner; pagination of genuinely private context remains actor-scoped and must not leak inaccessible inventory.

**Before a new map ref is staged**, the importing model must verify
Cartography's current context at its producer authority, confirm the
precise cartographic **site vs physical part/generation vs street corridor**
semantics, and save the same source RKB entity UUID. When the producer
returns `cartographic context is stale; review changed source observations`,
do **not** attach the previous link. Cartography's source observations,
independent semantic QA and owner POI binding remain separate acceptance
gates; attaching a candidate link in RKB cannot waive any of them.
