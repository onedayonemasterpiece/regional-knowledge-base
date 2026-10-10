# RKB ↔ Street Story ↔ Regional Cartography: live integration acceptance

**Date:** 2026-10-10. **Production RKB SHA:** [`2d63935aa58f0c4a9da3577cfcf4404b98e7ee0f`](https://github.com/onedayonemasterpiece/regional-knowledge-base/commit/2d63935aa58f0c4a9da3577cfcf4404b98e7ee0f), from [PR #112](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/112). **Decision:** real source-backed RKB place ↔ Street Story canonical POI ↔ source/evidence/story readback **accepted**; complete historical Cartography POI/map connection **blocked by producer state**.

## Actor-owned places and POI identity

Connected live RKB graph kept the original source-backed place IDs from Gause (unchanged source revision, citations, no book reimport):
- Cathedral on Kneiphof: `abf454f2-8714-5f1b-af78-e3e516600947`, **selected Street Story candidate** `streetstory://poi/poi_ss_54640a1688c2ac3437e71201`;
- Geographic Kneiphof/Kant Island `4a2f330f-76ce-5347-bcad-d5edeb959c88`, not historical municipality;
- Kneiphöfische Langgasse `8a86e07b-c6ce-559a-8ae2-cbc623ac8332`, not Altstadt Langgasse;
- Market square near Brodbänkenstrasse `dc3e3d77-a83a-52c1-a649-694d75218f92`, not Domplatz.

Each has exactly source-grounded chunk/page/region evidence and relevant `located_in` relationship. All inspected source `map_refs` are **empty**, correctly, as current Cartography place context is not accepted. Candidate POI is **not** accepted historic physical footprint or location.

## Live owner bridge acceptance: passed

Existing service-to-service endpoint `POST /v1/internal/rkb-poi` is **Street Story owner only**, no source-text copy. Real health responses from RKB and Street Story were HTTP **200**. Owner candidate lookup without independent private grant **401**, with grant **200** and one Cathedral candidate. RKB protected internal context without service grant **401**, with grant **200**. Street Story device-authorized `GET /v1/pois/poi_ss_54640a1688c2ac3437e71201/knowledge` **200** and JSON **byte-semantic equal** to RKB internal `GET /internal/street-story/poi-context`, giving one source location, one exact evidence pointer, one one-hop historic relation, one editorial story.

The new [RKB PR #112](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/112) response projects `place_kind`, `place_context`, bounded model-selected `map_refs`, and source graph relation `source_relation_state`, `source_time_scope`, `neighbor_poi_ref`. The payload explicitly states `map_ref_verification=not_verified`, `historical_geometry=not_verified`, `cartography_acceptance_claimed=false`; it does not assign owner POI identity, create historic coordinates or replace the source RKB entity. `has_more` now probes `limit+1` authorized locations, removing its false positive at exact limit. Python 3.12/3.13 CI green for PR #112; actor ACL, no false geom acceptance, and real second-page regression covered on SQLite tests.

To reproduce on an authorized DevCoveer host, without copying or printing local private tokens:

```sh
python scripts/production/probe_poi_bridge.py \
  --poi-id poi_ss_54640a1688c2ac3437e71201 --assert-projection
```

The assertion is **opt-in** for the geographic extension. It validates response equality and source metadata; neither this check nor a Street Story candidate certifies historical source map geometry. The source revision and generated private SQLite backup passed pre-deploy integrity/foreign-key readback. The three RKB existing units (MCP, indexing, graph discovery) were restarted with immutable PR #112 SHA, no second consumer. Postdeploy Story Registry **68 records/81 evidence/179 revisions** preserved, BGE **2434/2434** ready; optional E5 553.

## Cartography actual canonical producer: partially ready, context blocked

The merged [Cartography PR #1](https://github.com/onedayonemasterpiece/regional-cartography/pull/1) was followed by newer changes. *Current read-only live data*, not the October 9 five-layer checkpoint:
- **7 canonical historical layers**; **830** current semantic observations: `1418851:68`, `1419283:47`, `1419312:56`, `14193416:60`, `1419385:505`, `14194114:45`, `141944:49`.
- `scripts/verify_canonical_v1.py`: **0** canonical reference errors, **0** membership/view mismatches, **0** invalid active geometry; **17** private Storage objects, **0** orphaned/missing and **0** raster objects.
- Yet **0 historically accepted semantic layers**, **0 owner identity decisions**, **1240** outstanding previous five-source review jobs. Two newly added editions lack the earlier source calibration.
- The *older* authored five-edition review documented **14** cartographic groups, **68** members and **30** source-depicted confirmed network edges, not a valid claim of current seven-layer interop.

**Blocking reproduction:** both `scripts/cartographic_context.py --region kneiphof --place cathedral_site` and `--place langgasse_corridor` fail: `ValueError: cartographic context is stale; review changed source observations`. Even a **read-only** preview `--review data/kneiphof/context-review.json` now fails with `ValueError: unknown/duplicate reviewed relation`. Do **not** apply the old review or attach old Cart place IDs to RKB. Producer-owned repair and acceptance criteria: [Cartography issue #3](https://github.com/onedayonemasterpiece/regional-cartography/issues/3). Do not reset the currently active Cartography development tree or discard the seven verified canonical masters.

## Gates not closed

1. Cartography producer must reconcile its authored review against the present observation/relation digests, regenerate coherent `current_context` and show successful live place lookups; later complete independent semantic/historic QA (0/7 today). Historic site/corridor matches are **not** automatically equivalent to an owner-accepted physical-building/bridge generation.
2. An **explicit model-reviewed** RKB entity ↔ current `cartography://place/...` reference can then be saved on the **same** source-side UUID; Street Story alone issues POI owner identity decisions. Do not conflate `same_site`, `same_crossing_function`, modern OSM candidates or historical footprints.
3. The **connected ChatGPT Kalinigrad Base MCP tool schema is still stale** (lacks the newly registered `poi_registry`, `poi_context`, `geo_*` despite full server profile). Reconnect/refresh tool declarations and exercise real owner OAuth MCP, not only private on-host loopback grant. This is a client binding blocker, not evidence the server lacks the implementation.
4. Public-network geo/search p95 and unseen bulk source-place extraction recall, full Cartography restore/rights and independent full-map QA remain separate unpassed acceptance criteria. The private Cartography GitHub Actions runner reports failed workflows, independent of previous successful host tests.

**Safety boundary:** source RKB location, Cartography map place/site/geometry, Street Story canonical physical POI and historically accepted geometry are independent authorities. This report does not turn an owner candidate into a surveyed historical footprint.
