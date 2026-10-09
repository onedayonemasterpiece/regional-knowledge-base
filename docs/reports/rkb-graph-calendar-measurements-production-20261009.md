# RKB graph/calendar/numeric production acceptance — 2026-10-09

## Scope and observed release

- **Live release SHA:** `5d6c421a88ea62069c69ed97070c9f5a53046a86`, merged from [PR #96](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/96). The preceding calendar release was `b9631bb69f3699f757b7632791f9ac85b2d7988e`, [PR #95](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/95).
- Both real services, `regional-knowledge-base.service` (MCP) and `regional-knowledge-indexing.service`, were verified on the same release. The deployment returned `public_health=ok`. Separate post-deploy readback of five existing stories and the current locally generated MCP tool declarations passed.
- All code CI on Python 3.12 and 3.13 plus Story Registry synthetic load CI passed on the merged PRs. A real-authority **temporary** SQLite Online Backup migrated from schema v10 to v11, `integrity_check=ok`, `foreign_key_check=0`; second migration replay unchanged. No permanent corpus migration outside the release's normal additive SQLite initialization.
- Before v11 application, the original populated authority contained 68 stories, 174 revisions and 81 evidence entries. After two number commits: **68 stories, 176 revisions, 81 evidence entries**; SQLite migrations [1..11]. The added evidence row was from the separately reviewed preceding edit. Existing document identities and original media were not reimported.

## Actual-source calendar acceptance (no fixture)

Two **existing** zoo brochure editorial dossiers received exact event-date claims and were read back through production SQLite:
- 21 May 1896 — Königsberg zoo opened.
- 26 September 1920 — first German Eastern Fair opened at the zoo.

Both citations contained the literal printed date and retained source revision/page/region; resulting `story_calendar` exact-day queries returned the stored stories. The local editor/agent explicitly interpreted 19th–20th-century German civic dates as Gregorian; this choice is not presented as a calendar field supplied verbatim by the author. No Gregorian/Julian inference occurs inside the backend.

## Numeric-source repair and acceptance (no fixture)

A **preflight failed safely** when attempting to save 893 and 262 from the zoo story: the existing `story_evidence` excerpt was **36 characters** and proved only the date, not the numerical quantities. The Story Registry did not allow manufacturing a numeric proof from the story summary.

The operator located the original accepted source region/chunk on the same book revision and verified a **128-character exact source excerpt** containing date + both counts against both the authoritative region text and the full local chunk text. A second immutable `reports` evidence row was attached to the **existing assertion**. The previous citation, source ID, story ID, document revision and authorship were kept; this was the same source root, **not** independent corroboration.

Only after that did the model-authorized, bounded local operator save two individually sourced `RecordObservation` revisions, both for 1896:

| Original claim | Normalized exact value | Unit code | Metric key |
| --- | ---: | --- | --- |
| Zoo collection individuals | 893 | `individuals` | `zoo_animal_individuals` |
| Species in that collection | 262 | `species` | `zoo_species_diversity` |

`story_get(view=observations_page)` returned both proof-bearing rows, `story_observation_search` found each only under its own explicit metric/unit and `story_observation_compare` returned `different_metrics`, `comparable=false`, `difference=null`. No false historical trend was computed. The original zoo story now has editorial revision 6 and two exact source evidence excerpts. The existing connected RKB ChatGPT plugin's older `story_get(view=evidence)` returned revision 6 and evidence count 2, verifying the **actual connected OAuth reading path**, but that plugin schema did not yet expose the newer `story_calendar` / observation tools.

## Preserved unrelated retrieval and bounds

After these changes, live `indexing_status` confirmed **2,434 BGE-ready / 2,434 active chunks**, BGE required, E5 optional (553 ready). No embedding, vector table, HNSW or search policy was changed by PR #95/#96. 100,000-chunk capacity, halfvec recall, held-out multilingual fact families, and public MCP p95 SLO are **not** accepted by this report.

This acceptance was performed through a **trusted local configured-owner operator** with distinct client IDs, explicit release guard and no raw source text/IDs in stdout. That is **not** an externally authenticated OAuth mutation test. Source-book assertions are reports attributed to their authors; dates and numbers do **not** assert independently verified historical truth. No external social publication, POI creation, or independent medical/financial/statistical inference occurred.

## Remaining product work

1. Refresh the connected ChatGPT/Projects Hub/Mira MCP schema and verify genuine OAuth `story_calendar`, observation read/search and authorized write; existing current-chat tool declarations lag behind server functionality.
2. Complete general source graph and media intake beyond books, durable cross-corpus watermark/late indexing overflow, and genuinely semantic story retrieval.
3. Add stable, reviewed entity/POI subject identity before automatic series comparison. Continue typed event-entity-date linking, time uncertainty, and historical geo through **Street Story's canonical** POI owner, not a second POI registry.
4. Run the protected mass-ingestion/capacity/latency/retrieval gates (100 book-equivalent sources/100,000 active chunks, BGE-only held-out recall, 80%/90% storage, p95 latency). See [measured capacity report](rkb-vector-storage-compact-capacity-20261009.md).
