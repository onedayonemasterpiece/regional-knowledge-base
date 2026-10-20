# Regional Knowledge → Street Story POI integration

Updated 2026-10-20. Normative target: [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md). One PostgreSQL/pgvector corpus on Kaggle and BGE-only retrieval replace the old corpus placement. DevCoveer retains the existing authorization/controller and durable request/change/delivery state; one outbound notebook WSS connects them. This document is not a target deployment claim.

## Ownership and functional boundary

| Authority | Owns |
| --- | --- |
| RKB | Exact source/publication/page/region evidence; contributor metadata; source-bound facts, dates and observations; source-side historical entities; independent disagreement register/cases; source verification-score components |
| Street Story | Canonical physical POI IDs, aliases/external identities and lifecycle; its atomic POI claims/evidence sets; arbitration of its canonical POI state |
| Cartography | Source-specific map observations, geometry/topology, georeferencing and accepted geographic interpretation |
| Projects Hub / Mira | Authorized user/expert interaction through typed adapters; no second facts/case/POI authority |

An RKB disagreement case can concern a question with no Story or mapped POI. Link it to a Street Story case when relevant; do not duplicate every RKB case or let either system silently settle the other's canonical state. A source assertion, a model synthesis, accepted geometry and verified physical POI identity are separate claims.

Facts/dates/quantities can be source-bound without an artificial Story parent. Preserve event/calendar precision, people/roles, uncertainty, geographic time scope and quantities with units/period/method. Sparse chronology/statistics can yield no new episode. One source reading pass and comparison receipts supply facts, stories and disagreement candidates. See [Story Registry](story-registry.md).

## GEO-only cutover

Only geographic/cartographic identities, layers/toponyms/geometry, temporal interpretation and minimal provenance transfer. Old books/chunks/vectors, stories/manual drafts, expert contributions, cases and general facts are freshly imported/recreated; they are not copied because they have a POI link. Do not replay their old outbox commands into the new corpus.

Before production fresh imports, establish the minimal GEO identity manifest (layer/place IDs, aliases, owner refs, CRS). Bulk rasters/geometry may arrive through the existing GEO queue later, explicitly unresolved until ready, without book reimport. Earlier A+B imports are isolated fixtures only. Preserve exact existing Street Story/Cartography keys and source-side GEO IDs. Legacy book references remain minimal `legacy_unanchored` edition/SHA/page/locator provenance until the fresh import's exact mapping is verified. Infrastructure identities/service registrations/configuration are preserved separately. No mandatory export/permanent copy of old non-GEO content is required. Follow the [GEO cutover](migrations/rkb-layers-only-cutover-20261019.md).

## Canonical POI identity and source locations

Street Story owns opaque canonical POI keys. Existing accepted encodings include UUIDs and `poi_ss_<24 lowercase hex>`; preserve the exact owner key under `streetstory://poi/<key>`. Do not convert one format to another or mint an RKB replacement. Aliases may include Wikipedia/Wikidata/OSM IDs, old candidate IDs and historical names, but an alias is not a new canonical object.

A producer sends a `poi_locator`, not a self-declared canonical identity. A shortlist/name/proximity/external-ID hint is discovery evidence. The importing model compares physical site/generation/context and explicitly selects an existing owner ID or uses the existing owner create operation for a real candidate site. Name-only ambiguity stays unresolved; no automatic merge/duplicate place is permitted. A canonical `candidate` remains distinct from an independently verified identity.

A source location can be useful without coordinates or a physical POI. The normal [location-first workflow](location-registry.md) and [model-selected POI link](poi-model-link.md) do not block source activation on owner reachability. A modern representative point is not a historical entrance, footprint or surveyed polygon. Late map references enrich the same source-side identity without replacing it.

## Fact-evidence event contract

Reuse logical contract `poi.fact_evidence.v1` and its stable field semantics. Example for the approved public book-text projection:

```json
{
  "contract_version": "poi.fact_evidence.v1",
  "event_id": "uuid",
  "idempotency_key": "knowledge:<document>:<revision>:<candidate>",
  "producer": "regional_knowledge",
  "scope": {"visibility": "public", "owner_sub": "source-owner-uuid", "workspace_id": null},
  "source": {"document_ref": "knowledge://documents/<uuid>", "revision": 1, "title": "...", "publication_year": 1936},
  "poi_locator": {
    "names": ["Königstor", "Королевские ворота"],
    "external_ids": {"wikidata": "Q...", "wikipedia": "...", "osm": "way/..."},
    "latitude": null, "longitude": null
  },
  "claim": {
    "candidate_id": "uuid", "semantic_key": "construction:1843-1850",
    "kind": "construction", "text": "Построены в 1843–1850 годах.", "time_scope": "1843/1850"
  },
  "evidence": {
    "evidence_ref": "knowledge://evidence/<uuid>", "page_ids": ["..."], "region_ids": ["..."],
    "source_family_id": "book:<edition-or-lineage>", "author_profile_refs": ["knowledge://authors/<uuid>"],
    "author_subject_authority": 88, "publication_method_score": 82,
    "provenance_precision_score": 100, "evidence_verification_score": 88,
    "score_policy_version": "book-evidence-v1"
  }
}
```

The source owner is provenance, not a per-book reader filter. Preserve `private`/workspace scopes for genuinely restricted resources/contributions; do not place private draft text, proof pixels or archive locators in a public event. Never expose object-store keys, signed original URLs or Telegram credentials in the envelope. Source activation, validated document/revision/page/region provenance and domain outbox insertion are atomic at the domain boundary, with durable acceptance/replay as specified in the handoff.

## Author, contributor and evidence strength

Use versioned profiles with canonical name/aliases, roles/affiliations, geography/subject/period expertise, evidence refs, authority components/policy and actual review provenance. Context-specific authority must not be an ungrounded model reputation guess. Unknown authority is `null`, not an invented midpoint.

Prefer the chapter/article contributor, then named monograph author or co-author group. Attribute an editor/translator only to material actually theirs. Do not make the issue editor author of every article or infer a principal author solely from list position.

The existing `book-evidence-v1` formula, when author authority is known, is:

```text
A = author_subject_authority       (0..100)
M = publication_method_score       (0..100)
P = provenance_precision_score     (0..100)
score = round(0.55*A + 0.25*M + 0.20*P)
```

This is evidence strength, not truth probability or an admission gate. Versioned provenance examples are exact page+region 100, exact page/approximate passage 90, chapter 70, book 50. Publication-method inputs follow the explicit policy, not marketing language. Unknown inputs remain explicit; do not fabricate a score to populate the field.

Street Story may combine the strongest score with the existing bounded corroboration policy (+5 per additional independent family, at most +10), but copied upstream accounts/same lineage are not independent sources. Preserve `source_family_id`; unresolved lineage stays conservatively unknown. Counts and scores never choose the winner of a contradiction. Different dates can mean different phases or scopes.

## POI-linked historical media and proof access

Reuse separate `poi.media_evidence.v1`: `knowledge://illustrations/<id>`, `poi_locator`, explicit `depicts|illustrates|map_of|detail_of`, page/figure/caption provenance, exact rendition hash, caption/time scope and current policy/rights metadata. A figure can be useful without a new atomic fact. The model must explicitly identify its POI relationship; mere page co-location is insufficient.

Accepted illustration renditions are public for everyone under the owner's policy. Street Story stores their relationships/provenance and requests bytes from RKB; it does not become a second canonical image store. Publication/read paths recheck source withdrawal and current rendition policy. Unknown legal-status metadata is not converted into verified public domain and is not a new per-figure editorial gate.

Quote/page/crop scans and locator previews are a distinct class for owner/demo/expert/local_historian via current `scan_proof_read`; originals retain independent private access. Verified category assignment atomically materializes the audited grant under [rights.md](rights.md); claimed status gives none, and rendering cannot bypass expiry/removal/manual revocation through a category label or recreate grants on read. A public POI or public illustration does not grant either. Validate actual figure geometry so the public image contains no surrounding restricted source page. See [rights](rights.md) and [illustration pipeline](operations/illustration-pipeline.md).

## Durable delivery and same-host integration

Source finalization cannot depend on Street Story availability. After atomic activation/outbox insertion, the existing delivery path sends one stable event; Street Story accepts idempotently by event ID/key and returns its durable receipt. Lost response reuses that key. RKB records the exact accepted receipt/readback, never infers delivery from a request log.

The owner intake/bridge is on DevCoveer loopback, with the existing dedicated service identity. Kaggle localhost is not that host; route through the existing controller/delivery bridge. Do not forward user bearer/OAuth tokens or introduce another uploader/gateway. The service identity is limited to the agreed integration and never impersonates a person.

Public accepted-text events use the public projection. Genuinely private resource delivery requires its existing scoped delegation. Missing delegation yields `pending_authorization`; missing owner/dependency yields bounded pending/retry. Neither invalidates source activation or permits copying private evidence. Recheck source/revision, withdrawal, authorization and audience before delayed dispatch/readback. Persist retry-after and current attempt; permanent/ambiguous errors remain visible and reviewable instead of creating fresh effects.

Street Story preserves the producer evidence snapshot, resolves the exact owner POI, normalizes its atomic claim, keeps conflicting evidence separately and returns resolved/unresolved identity and case state. A receipt that accepted an unresolved name is not proof of a verified POI or populated fact graph. Missing identities can be completed later through model-selected owner operations without reimporting the source.

## Disagreement and expert work

Preserve relations `contradiction`, `scope_difference`, `temporal_sequence`, `source_disagreement`, `uncertain` and review choices `prefer_left`, `prefer_right`, `both_valid_scope`, `both_valid_temporal`, `unresolved`, `needs_more_sources`, `wrong_poi_link`. Decisions are append-only with provenance; resolved history is not deleted and new evidence may reopen a question. Contested status is independent of whether a useful story/factual projection is ready.

Projects Hub/Mira receives an authorized projection: owner case ID, POI/context, claims and evidence, score components, contributor provenance, detector suggestion and applicable expertise. Identity is the shared issuer+sub with existing participant capabilities, not a model-invented role. Keep the existing expert profile dimensions: geography, historical period, subject/domain, languages and optional institutional role; routing uses current authorization and the case requirements. Route decisions to the actual case owner. Ordinary work does not acquire a universal two-expert approval gate.

Extend Mira's existing RKB typed adapter with v2 explicit status/freshness/min-cursor/detail continuation, evidence/proof, Stories and cases; the prior search/evidence-only bridge is not complete coverage. Dot→tools and Mira→tools are independent smart routes using the same canonical authority. PH provider uses existing Dev auth/admission and the notebook-initiated WSS, without an extra Dot turn. Ordinary read_query takes a RAM permit and has no durable intake/ACK, fsync/seal, RESUME or resultSaved; a nested read does not create another human question. Accepted durable questions and mutations/provider effects retain exact intake/outcome, dialog order and native result-hash ACK. Shared-channel output stays `public_only`; a user's private grant cannot authorize the group. Announce a resolution only after owner durable receipt/readback. Preserve personal drafts and their version history separately from shared Story variants/case arguments for newly created target work.

Read-query output carries generation/deadline and current freshness; if the preceding write of the same dialog is UNKNOWN, return prior_write_pending with known freshness rather than latest/not-found. Apply current source/grant eligibility before ranking and at delivery, including cache/late results. V1 evidence/mode success is allowed only when the v2 meaning is fully representable; otherwise use the existing unavailable/error boundary. Read cancellation does not cancel or repeat an accepted POI/case/provider effect.

## Required acceptance

Verify unchanged owner keys (both supported formats); ambiguous/homonymous and lost-building sites; exact source/entity reuse across fresh books; no coordinates invented; source-bound facts/calendar/observations without Story; null author authority and lineage-dependent corroboration; contested high-score evidence; idempotent duplicate/lost-response delivery; owner outage without activation failure; current grant/withdrawal after queuing; public illustration versus restricted proof/full-file bytes; and a real expert decision readback through Mira/Slack without a competing case store.

After actual PH provider wiring, [handoff §14.4](prompts/rkb-scalable-kaggle-implementation-20261019.md) compares Mira/Dot using identical text queries, corpus/revisions, rights and a common tool profile/budget, then separately measures real ASR/voice/Slack routes. Report quality/completeness/source grounding and time to first useful/full answer without attributing route/cache differences to the model. Future explicit Dot delegation from Mira is not implemented in the base release.

The optional [geo queue](geo-resolution-queue.md) is distinct from BGE/FTS source-mention discovery. One discovers possible source mentions; the other tracks geographic owner/dependency enrichment. Neither automatically selects a physical identity, proves historical geometry or gates ordinary source import.

## Historical implementation evidence

The [immutable 19 October integration document](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/poi-integration.md) retains the 2/4/9/19 October producer/queue/delivery checkpoints, exact releases, endpoint/config descriptions and measured counts. These were observations of the prior runtime; they are not target migration instructions or proof of present deployment. In particular, the 19 October snapshot had 19 finalized private books, 409 `poi_ref` entities and one delivered outbox event, answered as `unresolved_identity` for a name-only locator. Delivery wiring did not certify extraction coverage or resolved identities.

The [zoo pilot acceptance](reports/rkb-street-story-zoo-geo-acceptance-20261009.md) preserves the 9 October real source-event-place-owner-ID readback and its `candidate`, not verified-geography, limitation. Do not reinterpret those results as a new all-book/PG/Kaggle acceptance.
