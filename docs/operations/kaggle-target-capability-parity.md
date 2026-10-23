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
| `story_search`, `story_get`, `story_history`, variants | Native saved-body, default-variant, editorial revision/history and bounded variant reads are prepared behind the shared source/output fence. Production-role PG fixture passes; live MCP acceptance remains open. |
| `story_calendar`, `story_observation_search` | Fresh schema8 adds indexed source facts independent of Story. Exact event-day/calendar semantics and decimal/unit preservation are required. No schema7 adoption is implied. |
| `story_observation_compare` | Fixed native exact-pair read prepared through the existing FACT_READ slot and source/output gate. The unchanged Story/left/right selectors require both observations in that current Story. Returns exact decimals, source proofs and conservative compatibility reasons; identity/method review and live acceptance remain open, with no arithmetic. |
| `entity_list`, graph stage/fetch/related, POI context/registry | Fresh11 source graph writes retain five entity kinds, explicit identities, aliases, directional relations and exact passage/page/region/Unicode evidence. Native atomicity, duplicate, withdrawal and corruption tests pass; typed public graph reads are still being integrated. Street Story retains physical POI ownership. |
| GEO status/receipt/lookup/request/claim/stage/apply/recheck | Bounded whitelist manifest staging/activation, retained identities, indexed native lookup/context and shared MCP reads are prepared. Native and production-role fixtures pass. Actual GEO-only migration and full operation parity are not accepted. |
| Cases, contributions, conclusions, personal drafts and public pages | Native versioned lifecycle, explicit publication selection, scoped access, exact accepted origins and production-role reads/writes pass synthetic PG tests. Four public MCP operation groups are being integrated. Real non-owner and recovery acceptance remain open. Cases remain independent of a Story. |
| Story create/edit/archive/extract/reconcile/transition/merge/access/export/publication, extraction jobs | Editorial and research mutation/recovery parity is not established by source import or read support. Private versions and publication approval remain distinct. |
| `catalog`, `book_find`, `corpus_read`, illustrations, `source_proof`, `document_access`, `indexing_status` | Indexed `book_find` and exact passage fetch with structured page/region/span provenance are prepared and production-role tested. Full catalogue browse/corrections, corpus utilities and artifact/proof access parity remain incomplete. None may silently read a legacy corpus in target mode. |
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

Current integration uses an explicitly selected fresh11 generation. Frozen
historical import validators and accepted commands retain their original
versions. The new source graph belongs only to fresh11 `source.import.v4`,
whose new native handler preserves the complete accepted payload hash. Its
tables must join the exact catalog, grants and history inventory before startup.

The joined source/editorial/Case/GEO PG17.11 fixture passed ten tests. A separate
fixture using the actual production `rkb_writer` and `rkb_reader` setup passed
source import, Case/GEO effects, all four read families, hybrid SQL, exact fetch
and rejection of raw original-receipt access. Its vector was synthetic: it did
not run BGE or prove live search quality. Retained evidence:
`/home/dev/artifacts/regional-knowledge-base/20261020T190433Z-native-reader-proof-pg1711-20261020`.

After adding the source graph, thirteen focused tests passed, including native
atomic rollback and exact origin corruption refusal. The unconfigured fresh11
catalog measured1329 records/606416 bytes. The previous1152-row native metadata
budget therefore cannot admit that union; measure and explicitly bind the new
budget before activation. These measurements are not release pins. Evidence:
`/home/dev/artifacts/regional-knowledge-base/20261020T191731Z-native-source-graph-verified-pg1711-20261020`.

Root's isolated exact Noble PG17.11 fixture passed7Story+3facts tests and measured
fresh8 catalog545records/218231bytes. Fresh8 runtime/history/physical-restore
observers therefore use576records; schema6/7 retain512. The1MiB byte cap and
64KiB per-definition cap remain unchanged. Fixture evidence is retained at
`/home/dev/artifacts/regional-knowledge-base/20261020T181101Z-native-facts-pg1711-fixture-20261020`.
This is native fixture evidence, not a target deployment or scale gate.

The native observation comparison adds no SQL artifact, stored function, accepted
command or schema revision. In one repeatable-read transaction it reads the exact
current Story and two observation primary keys. Missing membership or either
missing/currently unavailable observation closes the whole pair. Output refresh
reconstructs both sides from the currently gated result; it never retains a
previous comparison after withdrawal or authority loss. Source-only observation
search remains available without a Story; this existing comparison operation
continues to require its Story argument. Method, subject and period wording stay
source-attributed, with no calendar/unit conversion, difference or ratio. The
explicit route `source.observation_compare` joins both fixed history allowlists,
so the native observer fingerprint must be reviewed with the new worker bundle.
