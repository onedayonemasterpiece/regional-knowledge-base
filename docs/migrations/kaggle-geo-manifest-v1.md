# Minimal GEO manifest, version1

This implements the whitelist contract from
[the GEO-only cutover plan](rkb-layers-only-cutover-20261019.md). It performs no
live export, upload, accepted apply, routing switch or access-grant mutation.
SQL011 is reserved after editorial009 and Cases010; it is not implicitly added
to an existing runtime or accepted-command replay context.

The controller supplied a read-only inventory summary of409 `poi_ref` identity
candidates,540 geographic intents and3 entities with accepted `geo_queue`
metadata. The retained count inventory is
`/home/dev/artifacts/regional-knowledge-base/20261020T110555Z-kaggle-implementation-20261020/core/geo-migration-readonly-inventory.json`.
That file says `source_rows_read=false`; counts are not a completed whitelist
or proof of target migration. No source rows were exported by this change.

`build_geo_whitelist` takes bounded, already authorized snapshot projections.
It has no database/file/provider connection or credential. Independent expected
entity/place/mention/alias/relation/intent/resolution counts must match the
supplied rows exactly, preventing an accidentally shortened input from becoming
a successful whitelist. Source, page, region and chunk inputs are metadata for
referenced locators; none of their bodies enters the result.

| Included | Treatment |
| --- | --- |
| Existing `poi_ref` UUIDs and literal source names | Preserved exactly; private to the same canonical person. No name-based merge or replacement POI ID. |
| Geographic aliases and `located_in` between two retained places | Keep IDs, source spellings, language/time scope and minimal exact locator. Other entity/edge kinds produce explicit exclusions. |
| Source geographic intents | Keep the original intent/mention/entity IDs, names, declared geographic time scope and unverified locator hints. Verify the original mention hash and source SHA against supplied metadata. Old leases, claims, attempts, deliveries and executable queue state are excluded. |
| Existing Street Story / Cartography links | Exact owner references are `left_with_owner`; canonical databases, geometry and layers are not copied. A recorded resolution candidate is not an owner geometry receipt. |
| Minimal legacy provenance | Preserve the legacy document plus recorded SHA, edition metadata, source revision, physical/printed page, region/chunk locator IDs and evidence hash. Never copy the old quote, page image, book/chunk text, vectors, Story, case or draft. |

The recorded legacy source SHA is labelled `legacy_recorded_not_reverified`.
An edition binding is explicitly the old document plus that recorded SHA and
literal edition metadata, not a fabricated canonical edition ID. Locators are
`legacy_unanchored`, with `target_source_id=null`; they grant no original/scan
access and never become accepted target books. Reanchoring later requires exact
edition/SHA and independently checked page/region mapping, not a title, printed
page number, similar wording or nearest name.

Every item records its source owner/store/revision, original identity, private
scope, status, complete dependency keys and SHA256 of its normalized minimal
payload. The complete canonical manifest has a separate SHA256 and a4MiB bound.
`from_wire` checks individual hashes, exact schemas, deterministic unique order,
dependency closure and owner consistency. Missing required metadata remains
`blocked_missing_dependency`; `require_ready` refuses the whole installable
manifest. It never omits those records or substitutes zero/unknown hashes.

The preview whitelist supports geographic identity readiness while geometry is
explicitly `unresolved_with_owner`. Coordinates retained from old source
locators remain `source_locator_unverified`, with no inferred topology, CRS,
coverage or historical accuracy. Geography and public sharing are separate:
this initial manifest preserves private owner-only scope, and introduces no
new grant or public geometry claim.

Native serving must use the configured queue/admission and current verified
person. These old source locators cannot be passed to `SourceReadGate` as
accepted `SourceDependency` values: the native GEO read instead needs an exact
accepted manifest/receipt dependency and the current private owner predicate.
Absent that typed proof, the capability stays closed. An owner reference read
does not query or duplicate Street Story's canonical database.

Validation uses synthetic snapshots only: deterministic hashes, stable opaque
POI and UUID keys, old-text/lease exclusion, geographic edge whitelist, missing
SHA/page/mention closure, source/intent hash mismatch, private owner separation,
complete inventory counts and refusal of injected old text or omitted declared
dependencies. Native install/read, remote durability, actual owner readback,
real GEO manifest generation and cutover remain separate verification steps.
