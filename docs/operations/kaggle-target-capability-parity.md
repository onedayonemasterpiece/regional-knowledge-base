# Target capability parity, 2026-10-20

The explicit `kaggle_source` profile is a partial product vertical. It is not a
full replacement for the legacy public profile. Closing unsupported tools is a
required safety boundary, not proof of feature parity or cutover readiness.

The protected contract remains `.devcoveer/requirements.json`, the v4 handoff,
and the GEO-only cutover plan. Existing non-GEO content is not migrated merely
to make a target route appear populated. New source facts must remain usable
without a Story, and all target reads use the configured shared admission and
current source-history gate.

| Existing surface | Target capability / remaining blocker |
| --- | --- |
| `search`, `fetch`, live `knowledge_search` | Bounded native source search and exact passage continuation; current source/history/output fences required. |
| `book_ingest`, `book_pages` | Explicit pinned PDF parser/intake and independent HF counter required; direct original archive receipt, complete page/block continuation. DjVu, multipart and later reprocess paths remain unsupported. |
| `story_search`, `story_get`, `story_history`, variants | Typed native Story reads are being integrated separately; exact saved body/default reference and accepted origin proof required. |
| `story_calendar`, `story_observation_search` | Fresh schema8 adds indexed source facts independent of Story. Exact event-day/calendar semantics and decimal/unit preservation are required. No schema7 adoption is implied. |
| `story_observation_compare` | Not enabled by the initial facts read path; source subject identity/method review remains necessary, with no automatic arithmetic. |
| `entity_list`, graph stage/fetch/related, POI context/registry | Stable native people/organization/place identity, aliases, directional relations and exact source evidence are not implemented. Street Story retains physical POI ownership. |
| GEO status/receipt/lookup/request/claim/stage/apply/recheck | GEO whitelist, stable retained identities and native bounded resolution state are not implemented. Do not copy non-GEO corpus or create another POI authority. |
| Cases, contributions, conclusions, personal drafts and public pages | Native versioned lifecycle, scoped access, accepted recovery and public projection are not implemented. Cases must be independent of a Story. |
| Story create/edit/archive/extract/reconcile/transition/merge/access/export/publication, extraction jobs | Editorial and research mutation/recovery parity is not established by source import or read support. Private versions and publication approval remain distinct. |
| `catalog`, `book_find`, `corpus_read`, illustrations, `source_proof`, `document_access`, `indexing_status` | Each needs an explicit native adapter, proper artifact/scan/original policy and same-generation read/output fences. None may silently read a legacy corpus in target mode. |
| PH/Mira and Slack | Real delegated authorities, generation/deadline/result recovery and delivery fences require separately verified integration; naming a compatible client is not acceptance. |

Schema8 is selected explicitly for a new empty generation; SQL001–007 bytes and
accepted schema7 commands remain unchanged. The added projections are populated
atomically while applying exact resolved source commands, never by a periodic
whole-corpus scan. A new catalog/ACL pin and native metadata count must be
measured before accepting schema8. An already initialized schema7 generation
requires a supported ordered semantic transition; appending SQL008 is refused.
Schema8 resolves new imports only as `source.import.v3` under
`rkb-v4-source-story-facts-v3`; the full existing source/story payload codec is
retained. New exact selected-evidence literal/index preconditions belong to v3.
Existing v2 accepted commands keep their exact schema7 handler and bytes.

The native facts read contract has fixed actions and keyset pagination. Its
cursor binds the verified actor, exact filters, database generation and observed
data/projection/visibility frontier. Calendar selection includes only exact
`date_role=event`, `precision=day` in the requested stated calendar; it never
converts Julian/Gregorian dates or substitutes publication/approximate dates.
Quantities retain source wording, exact decimal strings, units, method and
period. Source labels are not canonical subject identity, and no automatic
conversion, difference, ratio or aggregation is performed. Every returned item
retains source revision/eligibility dependencies and selected exact evidence.

Unit tests and authored SQL do not establish native fixture, live deployment,
source fidelity, capacity, latency, dense quality or full cutover acceptance.

Root's isolated exact Noble PG17.11 fixture passed7Story+3facts tests and measured
fresh8 catalog545records/218231bytes. Fresh8 runtime/history/physical-restore
observers therefore use576records; schema6/7 retain512. The1MiB byte cap and
64KiB per-definition cap remain unchanged. Fixture evidence is retained at
`/home/dev/artifacts/regional-knowledge-base/20261020T181101Z-native-facts-pg1711-fixture-20261020`.
This is native fixture evidence, not a target deployment or scale gate.
