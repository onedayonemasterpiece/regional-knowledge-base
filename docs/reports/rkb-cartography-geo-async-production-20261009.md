# RKB × Historical Cartography — async geo queue production acceptance

**Date:** 2026-10-09 (post-release acceptance). **Scope:** owner-only RKB
source-backed geo-intent producer/consumer and Street Story read-only POI
bridge. This is **not** an accepted historical-map/vector-layer release.

## Exact code / ownership

| Surface | Actual status and immutable checkpoint |
| --- | --- |
| RKB PR [#100](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/100) | Merged `eaa48b45562a0e6b1795396758ca98847f003eb4`; same-transaction geo mention intents, actor queue, lease/fence, receipt, owner POI bridge |
| RKB PR [#101](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/101) | Merged `c1771d02bf351e93ae66462bbaae6b8dacc3bbfe`; bounded owner outages/retries, multi-book fairness, actor lag, **internal** `rkb.geo_claim.v1` |
| RKB PR [#102](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/102) | Merged and **production deployed** `4b7e39f4ce8d2eaa6caa6878459f94a5e94a52cf`; source-exact partial-index geo_request pagination, including cross-book same-owner evidence |
| Cartography [PR #2](https://github.com/onedayonemasterpiece/regional-cartography/pull/2) | Merged `252bce15d3e293d2c8ee4914f924e5689300ba11`: four canonical v1 JSON Schemas, five synthetic valid and four negative fixtures, independent validator |
| Cartography [PR #1](https://github.com/onedayonemasterpiece/regional-cartography/pull/1) | Separate historical-map producer work ongoing. Do not treat producer code as an accepted Kneiphof map |
| Street Story | Unchanged canonical POI authority and owner SQLite: RKB does **not** upsert a physical POI; no duplicate canonical ID |

Original execution source: [IdeaHub implementation handoff](https://github.com/onedayonemasterpiece/idea-hub/blob/main/prompts/implementation/regional-cartography-rkb-async-integration-20261009.md).

## Verified production data and restart

The guarded deployment produced a consistent 80,605,184-byte SQLite Online
Backup, SHA-256
`b426ca0d06ba44c8e745706db09a4ee78298355ef03bb2e0f9151b4c938e4eaa`,
and both `PRAGMA integrity_check` and `foreign_key_check` passed.
Preflight performed a migration on a temporary backup and verified the
preservation of story records. User services were restarted with **exact
immutable release source SHA** (verified by process environment):

- `regional-knowledge-base.service` (MCP);
- `regional-knowledge-indexing.service` (source activation worker);
- `regional-knowledge-graph-discovery.service` (same existing graph and
  bounded geo worker; a second worker was not created).

Public MCP health was `ok`; five old story records sampled, all story schema
versions 1–11 retained. Git main checkout with unrelated dirty BGE work was
not switched or modified.

| On real RKB SQLite, after geo consumer run | Count |
| --- | ---: |
| Persisted geographic intents | 99 |
| Geo attempts | 99 |
| Durable command receipts | 198 |
| `linked_candidate` outcomes | 86 |
| `ambiguous` | 7 |
| `unresolved` | 6 |
| Editorial story records | 68 |
| Editorial story revisions | 177 |
| Exact story evidence links | 81 |
| Active BGE vectors / active chunks | 2434 / 2434 |

The connected `indexing_status` readback confirmed BGE ready 2434/2434;
E5 remains optional 553/2434. The connected `graph_fetch` readback
confirmed the same zoo historical-place entity
`74294189-1577-5105-a114-46feb55eb010` links to
`streetstory://poi/poi_ss_2d0ab75849ea3099ea17193a`, and the
Street Story owner status is still `candidate`. Its retained historical
event/story/links were **not** recreated.

The original zoological opening story
`347e33b2-170d-4c9c-aad0-c29bd6a5df8d` remains at revision 7 with
two exact source citations. The historic 1896 boundary, building outline,
and visitor entrance remain `not_verified`: the owner representative
modern point cannot provide them.

## Reproduction of a real ingestion regression

A live RKB graph node had **81 graph mentions**, but only **two** carried a
nonempty, proof-backed `exact_source_spelling`; the others were discovery
candidates. The previous `geo_request` examined the first 20 unfiltered
mentions, returned zero, and thus falsely suggested there were no
source-backed mentions. This did **not** delete the original two intents.
PR #102 now indexes and paginates exactly original source spellings and
revalidates their current document owner, revision and quoted region/chunk.

Against a temporary Online Backup of the **real** populated SQLite:
- 81 actual graph mentions, 2 original-source names read back;
- `geo_request` returned **both** existing intent IDs;
- 24 consecutive same-actor replays preserved all **99 intents**, all
  **198 receipts**, all 68 stories and 81 story proofs;
- the SQLite optimizer used `geo_source_mention_entity` (query plan),
  with paginated cursor and no duplicate intent/attempt;
- restored copy `integrity_check` and foreign keys: **ok**;
- durable receipt recovered exactly; owner-only status and `rkb.geo_claim.v1`
  verified; copy discarded without mutating live data.

On the copied data, `geo_request` (24 calls) p95 was **2.29 ms**
(max 192.61 ms cold/first-call). On the live host,
`geo_lookup` (32 calls) p50 was **1.79 ms**, p95 **2.00 ms**
(max 155.47 ms, cold). These are **local indexed SQLite/actor checks**,
not the full OAuth/network voice/MCP p95. An actual connected source
`search` call returned eight BGE results with a recorded
`latency_ms=1748.9`; that is one observation, **not** proof of
p95<=1500 ms or a hard 2000 ms gate across concurrency.

## Served MCP capabilities: source and advertised client are distinct

The immutable production release's local `build_server.list_tools` was
verified on a temporary empty corpus (without OAuth):

| MCP profile | Geo methods actually registered in release code |
| --- | --- |
| `full`, `story_contributor`, `story_editor` | `geo_request`, `geo_status`, `geo_lookup`, `geo_claim`, `geo_stage`, `geo_apply`, `geo_receipt`, `geo_recheck` |
| `story_reader` | `geo_status`, `geo_lookup`, `geo_receipt` |
| `live` | None: deliberate bounded, read-only Live search surface retained |

`geo_request` has `entity_id,cursor,limit` and `geo_stage` has a
typed `GeoProposal` input in the served schema. This smoke test
**does not** prove a new remote OAuth-authenticated tool call. The existing
ChatGPT Kalinigrad Base connector session exposed the older fixed geo-less
tool definitions even after deployment; client-side schema refresh/reconnect
and connected-method invocation remain a separate acceptance gate.

## Cross-project contract conformance and limits

Canonical schemas and fixtures are in
[regional-cartography/contracts](https://github.com/onedayonemasterpiece/regional-cartography/tree/main/contracts)
and [interop test guidance](https://github.com/onedayonemasterpiece/regional-cartography/blob/main/docs/rkb-interop-v1.md).
The isolated Python Draft202012Validator read from the exact GitHub PR #2
files **passed 5 valid + 4 negative** fixtures. GitHub Actions for the
new private Cartography repository report failed jobs **without available
executed steps/log blob** on PR and main; the runner/billing/CI-policy root
cause has not been established, so **Cartography GitHub CI is NOT green**.

These contracts are not yet an authenticated interservice mutation gateway.
JSON fields cannot authenticate issuer, audience, resource grant or owner
receipt by themselves. No impersonation/token forwarding was used. The RKB
consumer uses the proven **owner read-only POI projection** until a real
Street Story owner-side geographic mutation API and accepted map revision
exist. Its linked result is an **identity candidate**, not
`identity_verified`.

An accepted Kneiphof/Retromap vector `layer_revision` with source rights,
coverage, coordinates, historical period, independent QA and uncertainty is
**pending**. Tests do not fabricate a polygon or mark a draft vector layer as
accepted. The downstream real map projection/owner-binding E2E is thus
**not accepted**.

## Exact outstanding gates

1. Cartography owner: finish the five historical map layers in the existing
   producer PR #1, provider/edition provenance and independent QA, apply
   PostGIS in its own RC Supabase project, verify immutable vector master
   readback and announce actual accepted `layer_revision`. Do not make
   RKB a second Cartography/Street Story authority.
2. Interservice transport: authenticate `cartography.resolve.v1` and
   `projection.v1` using real existing service identity and safe
   private-resource grants; receive Street Story's true owner-signed/verified
   `cartography.decision.v1` and its exact expected revision/receipt.
   No new IAM or bearer delegation.
3. Connected consumer: refresh ChatGPT/MCP geo method definitions and exercise
   actual OAuth caller schema, ACL, stage/apply/readback. Local schema presence
   alone is not the connected method acceptance.
4. Real historical time/address/geometry search: explicit year, valid vs
   edition time, address numbering schemes, `within` vs `nearby`,
   source uncertainty and real layer revision. Current unknown time remains
   unknown rather than silently mapped to the modern city.
5. Measure warm **full public** geo lookup p95<=800ms, existing MCP public
   ranked-search p95<=1500ms/hard 2000ms on representative concurrency;
   verify revoked-access/reconciliation and full source-archive restore.
   The 100,000-active-chunk Supabase capacity gate remains separate.
6. Cartography repo GitHub Actions: investigate runner startup failure and
   re-run exact CI on the provider PR; local schema fixture checks are not
   a replacement for a green canonical workflow.

**Acceptance verdict:** RKB durable owner-only geo queue **production
accepted** with actual backing state, receipts, crash recovery on copy and
existing POI late-binding. Authenticated network Cartography→SS→RKB
historical-geometry E2E **pending**.
