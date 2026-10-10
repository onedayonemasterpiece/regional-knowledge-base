# Model-selected Street Story POI from a regional source

**Rule:** A source location is not automatically a POI. During the same book-review the model identifies physical places worth visiting or historically locating (standing building, ruins, plaque, memorial, lost building site). Broad geographic references stay RKB locations.

After the normal `book_ingest(stage)` and source activation, the agent calls `poi_registry(action="search",query,external_ids)`; the shortlist contains owner POI IDs and aliases but selects nothing. The model compares site context and either calls `poi_registry(action="select",entity_id,poi_id,model_reason)` or `poi_registry(action="create",entity_id,site_state,site_context,model_reason)`.

Creation occurs ONLY in Street Story's existing canonical `pois` table with status `candidate`; a second POI register in RKB is forbidden. The operation attaches the returned `streetstory://poi/...` ref to the SAME RKB entity, keeping all historical names, exact evidence and story links. The Street Story owner stores the reverse RKB entity reference and source locator, not full passage text. A later source/book reuses both IDs. RKB source activation is independent of owner reachability; replay after network loss is idempotent.

Name and even proximity are candidate discovery signals, not an identity merge. Existing external identifiers or conflicting names must be checked by the model. Missing map geometry does not prevent a lost-building site POI; absence of modern coordinates must not generate a fake public pin.

Use `poi_context(streetstory://poi/<id>)` to obtain actor-authorized RKB source mentions, linked people, events, places and editorial stories, then fetch exact excerpts using ordinary evidence tools. Street Story's authenticated `GET /v1/pois/{id}/knowledge` returns the same bounded context under the owner's separate loopback service grant. No user OAuth bearer or PDF moves between services.

Only the RKB/Street Story installation owner is granted this loopback bridge. A dedicated secret must be provisioned in a protected systemd EnvironmentFile. The Android `STREET_STORY_DEVICE_TOKEN` is NOT that secret. The bridge is disabled without explicit private provisioning. Test both owner ACL and service token rejection.

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

Reproduce the narrow HTTP readback from the registered DevCoveer checkout,
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
