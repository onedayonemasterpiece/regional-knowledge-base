# Model-selected Street Story POI from a regional source

**Rule:** A source location is not automatically a POI. During the same book-review the model identifies physical places worth visiting or historically locating (standing building, ruins, plaque, memorial, lost building site). Broad geographic references stay RKB locations.

After the normal `book_ingest(stage)` and source activation, the agent calls `poi_registry(action="search",query,external_ids)`; the shortlist contains owner POI IDs and aliases but selects nothing. The model compares site context and either calls `poi_registry(action="select",entity_id,poi_id,model_reason)` or `poi_registry(action="create",entity_id,site_state,site_context,model_reason)`.

Creation occurs ONLY in Street Story's existing canonical `pois` table with status `candidate`; a second POI register in RKB is forbidden. The operation attaches the returned `streetstory://poi/...` ref to the SAME RKB entity, keeping all historical names, exact evidence and story links. The Street Story owner stores the reverse RKB entity reference and source locator, not full passage text. A later source/book reuses both IDs. RKB source activation is independent of owner reachability; replay after network loss is idempotent.

Name and even proximity are candidate discovery signals, not an identity merge. Existing external identifiers or conflicting names must be checked by the model. Missing map geometry does not prevent a lost-building site POI; absence of modern coordinates must not generate a fake public pin.

Use `poi_context(streetstory://poi/<id>)` to obtain actor-authorized RKB source mentions, linked people, events, places and editorial stories, then fetch exact excerpts using ordinary evidence tools. Street Story's authenticated `GET /v1/pois/{id}/knowledge` returns the same bounded context under the owner's separate loopback service grant. No user OAuth bearer or PDF moves between services.

Only the RKB/Street Story installation owner is granted this loopback bridge. A dedicated secret must be provisioned in a protected systemd EnvironmentFile. The Android `STREET_STORY_DEVICE_TOKEN` is NOT that secret. The bridge is disabled without explicit private provisioning. Test both owner ACL and service token rejection.

Source location graph semantics remain in [location-registry.md](location-registry.md). Physical POI identity and candidate lifecycle remain Street Story authority.
