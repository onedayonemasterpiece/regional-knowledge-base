# RKB ↔ Street Story — реальная геопривязка открытия зоопарка, 9 октября 2026

## Итог приёмки

**Работоспособный реальный E2E-сценарий**: существовавшая история RKB об открытии Кёнигсбергского зоопарка 21.05.1896 → историческое событие из source-backed графа → `occurred_at` с тем же источником → прежний graph `poi_ref` → существующий в Street Story канонический каталог, получивший нового owner-side кандидата `streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a`.

- RKB production: `be677f573787c512c55d2a8e534096326fa017ba` ([PR #98](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/98), merged, CI Python 3.12/3.13 PASS).
- Street Story production backend: `db0c0d08024441536a2e59f5c3c433b12c7f4189`; `/healthz` PASS, candidate registered through the **existing** Street Story `ensure_poi_identity` owner method and committed atomically.
- Supabase BGE required: **2434 / 2434 active chunks ready** after release. No vector re-embedding, source re-import, story duplicates or POI storage move.
- SQLite shadow integrity/FK checks PASS before RKB cutover. Consistent online backup before restart verified; main MCP/indexing worker on same exact RKB release with public health PASS.
- Actual authenticated connected RKB MCP `story_get`, `graph_fetch`, `graph_stage`: graph stage resolves **`unresolved_pois=0`** and returns the pre-existing graph entity ID; readback confirms `external_ref` and `candidate` identity state from Street Story. The `occurred_at` edge has the existing source quotation and time scope; no new edge or story ID was created. Story remained revision 7 and its 2 source excerpts persisted.

## Why the original link remained unresolved

Street Story's real POI SQLite authority had **12 POIs and 55 aliases**, but lacked the zoo entirely. The previous read-only RKB resolver required all owner IDs to be UUID even though the live Street Story canonical table contains `poi_ss_<24 lowercase hex>` IDs (and legacy UUIDs). There were therefore **two different blockers**:

1. Missing owner-side zoo candidate: RKB correctly refused to invent a canonical place.
2. Wrong consumer ID validator and missing late-identity refresh: even an owner-side candidate could not be read through `version` / `discover_poi`, and repeated same-document `graph_stage` did not promote an existing unresolved entity.

PR #98 fixes those actual integration defects without a second POI authority. Only precise allowed canonical keys pass; an explicit external ID that does not match the owner's current aliases is not replaced by a name-only guess. Late-resolution same source/revision updates the existing RKB `poi_ref` once; an attempted reassignment from one non-null canonical ID to another fails pending editorial review.

## Public candidate and source custody

Owner-side registered **one** Street Story POI candidate, with nine public aliases (current Russian name, historical German/Russian variants, a modern address hint and existing external registry identifiers).

Public anchors:
- [Wikidata Q1193386](https://www.wikidata.org/wiki/Q1193386), canonical modern zoo identity / representative coordinates.
- [Kaliningrad Zoo — official history](https://kldzoo.ru/about/history/), historical site continuity / opening context.
- [Wikipedia — Калининградский зоопарк](https://ru.wikipedia.org/wiki/Калининградский_зоопарк), auxiliary discoverability.

A representative position was retained **as a modern approximate point**, NOT the precise visitor entrance, NOT 1896 building geometry or a surveyed polygon. Street Story row is explicitly `candidate`, RKB graph reference is `candidate`, and resolver advertises `historical_geometry=not_verified`. This is evidence-backed **place-candidate continuity**, not proof every surviving physical structure is identical to a nineteenth-century building. No source-licensing claims were inferred from the owner's copy of a book.

Private RKB book excerpts, internal source IDs, object keys, OAuth tokens and user ownership data **were not sent** to Street Story. Historical evidence remains under its existing RKB ACL. This workflow's canonical POI registration used a trusted owner-local deterministic operator, not a newly verified external Street Story API bearer or OAuth mutation.

## What is NOT completed by this pilot

- Historical GIS: two licensed urban-map sheets, independent georeferencing check points, transform/GCP/CRS/accuracy, validity by year, demolished physical building distinct from surviving/current building.
- Correct full `name + coordinates + address + time` querying against actual historical areas, building polygons and periods, an accessible visitor route/entrance and exact spatial predicates.
- Universal late-identity retry queue for every RKB graph entity and entire source corpus; this pilot proves retry on one real source-backed node.
- Independent historical claim corroboration, consented private POI evidence events into Street Story, and complete cross-MCP Projects Hub/Mira consumer interoperability.
- Existing protected large-scale requirements: 100+ book-equivalent sources / 100,000 active chunks, measured Supabase steady/peak, dense retrieval multilingual held-out quality and p95 live search.

These remaining items are a separate Stage E full-georeference and Stage B mass-retrieval acceptance, not implicit claims made by successful health or this one-place integration.

## Repeatable product check

1. Street Story owner `pois` and `poi_aliases` resolve Wikidata `Q1193386` to `streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a`; owner state remains `candidate`.
2. RKB book graph has the same source-backed `poi_ref` before/after promotion; `graph_stage` with exact existing chunk/page/region excerpt and explicitly sourced Wikidata locator returns `unresolved_pois:0`, same RKB entity key, repeat is idempotent.
3. Connected RKB `graph_fetch` for place shows `external_ref`, `external_identity_state: candidate`, same inbound `occurred_at` relation with evidence; `graph_fetch` for event shows reverse neighbor with matching canonical external ref.
4. Connected RKB `story_get(view=evidence)` still has the original linked event/place entities, revision 7 and both quoted proofs. `indexing_status` BGE 2434/2434.
5. Negative gates: changing canonical owner reference without explicit review fails; owner ID not yet in Street Story remains unresolved; `poi_ss_` malformed keys are refused; owner name matches do not automatically merge candidates.

Source changes: [PR #98](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/98). Earlier v11 date/metric [acceptance report](rkb-graph-calendar-measurements-production-20261009.md).
