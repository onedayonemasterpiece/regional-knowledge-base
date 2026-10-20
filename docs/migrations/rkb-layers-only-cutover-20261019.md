# GEO-only cutover; all other content is freshly imported

Updated 2026-10-20. Status: implementation plan; this documentation update moves no data. The owner has resolved the scope: **migrate only GEO**. The [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) is the coding-agent entry point.

## 1. Explicit migration boundary

| Transfer | Do not transfer as target content |
| --- | --- |
| Historical/cartographic layers; geographic place/layer stable IDs; rasters/vector geometry and topology; CRS, axes, units, GCPs and transforms; temporal IDs/validity and uncertainty | Old books/PDF corpus, accepted transcriptions, chunks, vectors, search projections and import receipts |
| Toponyms, sourced aliases, historical addresses and geographic relations; source-specific cartographic observations; existing Cartography/Street Story owner references | Old stories/variants, manual drafts/history, expert contributions, disagreement register/cases and general factual/person/event records merely mentioning a place |
| Minimal geographic provenance necessary to interpret the retained geometry/identity: source/edition/SHA/page/locator, dating evidence and attribution of the geographic interpretation | Transitive copies of entire chapters, story dossiers, personal work or old delivery queues under the name of a provenance dependency |

A geographic measurement/date belongs in the transfer when it describes the retained geographic observation, transform or layer validity. General statistics, a biographical date or an event's location does not make its full non-GEO record transferable. Preserve the geographic part and minimal attribution/reference without importing the surrounding story or contribution history.

Infrastructure identity, authorization configuration, service registrations, secrets and routing settings are not book content migration. Preserve their established control-plane lifecycle; do not reset or delete them to obtain a clean corpus. External canonical stores stay with their owners. This scope does not authorize copying Street Story/Cartography databases or minting replacement IDs.

There is no unresolved owner choice about migrating manual stories/drafts/contributions and no mandatory export of all non-GEO assets. An already established bounded rollback backup may remain for its agreed retention; it is not a permanent second corpus or an active source for target search. The bounded retention/restore policy in the handoff applies to that backup and later Dataset generations.

## 2. Manifest and provenance

Produce a read-only inventory and deterministic GEO whitelist before export. Each manifest item records source owner/store, category, stable ID, count/hash/revision, geometry/CRS/axis/units/transform/time, uncertainty, minimal provenance, dependencies, target owner/store and transfer status. Separate `migrated`, `legacy_unanchored`, `left_with_owner` and explicit `excluded_non_geo`; a missing dependency must not silently become a successful item.

For references into old books, preserve only the bibliography/edition/source SHA and exact physical/printed page or locator needed by the GEO object. Mark these as `legacy_unanchored`. They do not become an accepted target book, searchable old text, copied quote scan or a proof of the new import. A legacy locator retains its access class and current source policy; it grants no access to an old private original.

Fresh book import starts from verified original bytes through the normal MCP workflow, with a new accepted semantic graph and disagreement processing. Reanchor a GEO reference only to the **exact edition/source SHA plus checked page/region mapping**. Same title, printed page number, similar quote, name or vector proximity is insufficient. For a different scan/edition or unavailable original, keep the old reference visibly unresolved until an explicit mapping is verified. Record the mapping and its provenance; do not rewrite the historical interpreter's statement as if it originally used the new revision.

## 3. Cutover sequence

1. Prepare and verify the minimal GEO identity manifest: stable layer/place IDs, aliases, owner refs, CRS/axis/units and required provenance/version bindings. Restore/check those identities before production fresh imports. Full raster/geometry payloads may follow through the existing bounded GEO queue; manifest readiness does not falsely claim their availability.
2. Fresh A+B book/import/research/story/case tests before that manifest are permitted only in an isolated fixture. They are not production bootstrap and do not import old non-GEO content.
3. Production fresh books reuse the established manifest identities. Pending bulk rasters/geometry remain explicitly unresolved/unavailable until verified; their later arrival enriches the same IDs without reimporting accepted target books or duplicating places. Verify full layer geometry/topology, temporal validity, uncertainty and attribution when those payloads arrive.
4. Bound the final source write window with a short pause of applicable GEO writes or a proven delta watermark. Do not promise delta capture where the owner source has none. Reconcile external references through their canonical owners without copying those stores.
5. Check the source/target manifest and replay state, then switch routing and readiness. Maintain one live writer authority. An old rollback snapshot is read-only. The handoff's default is 7 days after confirmed cutover, extendable only for a concrete unresolved rollback incident within the measured byte budget; no independent longer retention is introduced here.
6. After any new target acceptance, rollback preserves that exact accepted target tail, domain identities and delivery receipts. Restoring an old whole database over fresh imports or replacing the target with an empty database is prohibited. A compatible target restore/replay or explicitly degraded read path is required.
7. Expire obsolete staging/backup copies only through the verified retention/GC policy. The design does not require permanent preservation or transfer of excluded non-GEO content.

## 4. Required readback and recovery

Exercise a real historical layer/overlay, stable place lookup, historical alias by time, uncertain/unresolved place, topology/CRS conversion, retained author/provenance, and an unchanged external POI key. Verify that an old book reference is visibly unanchored and that exact reanchor succeeds only with the verified source mapping. Confirm old non-GEO search results, story/draft/case bodies and pending delivery events are absent from the target bootstrap.

Interrupt export/restore and final write cutover; reject incomplete or mismatching manifests. Test concurrent GEO writes and rollback after a new target book/accepted contribution. Count omissions explicitly rather than accepting a lower row count as cleanup.

The DevCoveer journal is a bounded hot recovery tail, not a permanent archive. An older Dataset/backup may be designated a historical recovery point with a declared older RPO; it cannot require unbounded local journal retention. Before truncating accepted changes needed by a supported replay base, verify a replacement durable checkpoint/tail. Only a few hours of local journal plus a daily remote backup cannot prove a few-hour RPO after loss of DevCoveer; the independent durable checkpoint/tail cadence or the honest recovery point must be stated in acceptance. No additional control plane is required.
