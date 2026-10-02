# Regional Knowledge → Street Story POI integration

## Ownership boundary

Regional Knowledge Base does **not own the canonical POI database**.

Street Story owns:
- stable regional poi_id;
- aliases and external identities;
- canonical atomic POI claims;
- evidence sets;
- contradiction ledger;
- arbitration state.

Regional Knowledge Base owns:
- books/journals and their provenance;
- page/region graph;
- author/contributor metadata;
- extraction of evidence-backed POI fact candidates;
- exact knowledge:// evidence references;
- author/source verification-score calculation.

Projects Hub owns neither facts nor conflicts. It is the user/expert work surface.

## Why the current Street Story candidate_id is not enough

Street Story currently has stable accepted object candidates such as Wikipedia/OSM
identities. Cross-service ingestion adds books that may name the same physical
object without one of those identifiers.

Therefore Street Story should evolve to a service-owned opaque UUID:

~~~text
poi_id
  ├── external alias: wikipedia:...
  ├── external alias: wikidata:Q...
  ├── external alias: osm:way/...
  ├── legacy Street Story candidate_id
  ├── historical names
  └── normalized geo/name aliases
~~~

A Knowledge producer sends a poi_locator, not a self-declared canonical POI.
Street Story resolves it.

Ambiguous identity creates an unresolved POI-link review case; it must not
silently create a duplicate POI or merge two places.

## Evidence event contract

Logical version: poi.fact_evidence.v1.

Example:

~~~json
{
  "contract_version": "poi.fact_evidence.v1",
  "event_id": "uuid",
  "idempotency_key": "knowledge:<document>:<revision>:<candidate>",
  "producer": "regional_knowledge",
  "scope": {
    "visibility": "private",
    "owner_sub": "uuid",
    "workspace_id": null
  },
  "source": {
    "document_ref": "knowledge://documents/<uuid>",
    "revision": 1,
    "title": "...",
    "publication_year": 1936
  },
  "poi_locator": {
    "names": ["Königstor", "Королевские ворота"],
    "external_ids": {
      "wikidata": "Q...",
      "wikipedia": "...",
      "osm": "way/..."
    },
    "latitude": null,
    "longitude": null
  },
  "claim": {
    "candidate_id": "uuid",
    "semantic_key": "construction:1843-1850",
    "kind": "construction",
    "text": "Построены в 1843–1850 годах.",
    "time_scope": "1843/1850"
  },
  "evidence": {
    "evidence_ref": "knowledge://evidence/<uuid>",
    "page_ids": ["..."],
    "region_ids": ["..."],
    "source_family_id": "book:<edition-or-lineage>",
    "author_profile_refs": ["knowledge://authors/<uuid>"],
    "author_subject_authority": 88,
    "publication_method_score": 82,
    "provenance_precision_score": 100,
    "evidence_verification_score": 88,
    "score_policy_version": "book-evidence-v1"
  }
}
~~~

Never expose object-store keys or signed URLs in this envelope.

## Privacy inheritance

A fact extracted from a private book may enter the POI graph only with its access
scope preserved.

~~~text
private book
  -> private claim/evidence branch
  -> not visible in public POI retrieval
  -> not assigned to an expert without a matching grant
~~~

Public canonical POI facts require evidence whose policy permits that visibility,
or an explicit expert resolution based only on evidence the target visibility may
use.

Promoting normalized content does not publish the user's exact PDF.

## Author authority and book verification scores

Do not turn “author reputation” into an opaque model opinion.

Knowledge Base maintains versioned author profiles:

~~~text
author_id
canonical_name
aliases
roles/affiliations
expertise:
  geography[]
  subject[]
  period[]
authority_components
evidence_refs[]
authority_policy_version
verified_by
verified_at
~~~

Authority is **context-specific**. A recognized specialist in East Prussian
fortifications may have a high score for fortification architecture and no score
for unrelated subjects.

Unknown author authority is null, not 50.

### Contributor attribution

Prefer the most specific contributor:
1. chapter/article author;
2. named primary author of the monograph;
3. co-author group;
4. scientific editor/translator only for material explicitly attributable to them.

Do not automatically treat an editor or translator as the author of every claim.

### Evidence score v1

For book evidence with known author authority:

~~~text
A = author_subject_authority        0..100
M = publication_method_score        0..100
P = provenance_precision_score      0..100

evidence_verification_score =
  round(0.55*A + 0.25*M + 0.20*P)
~~~

This is an **evidence-strength score**, not a probability that the claim is true.

Suggested provenance precision:
- exact page + exact source regions: 100;
- exact page, approximate passage: 90;
- chapter-level attribution: 70;
- book-level attribution only: 50.

Publication-method score is a deterministic policy input: academic/source edition,
scholarly monograph, institutional catalogue, general history, memoir etc. It is
not inferred from marketing language.

### Claim-level corroboration

Street Story may derive a claim verification score from independent evidence
families:

~~~text
base = strongest evidence_verification_score
+ bounded corroboration bonus for independent source families
~~~

Recommended initial bonus: +5 for each additional independent family, max +10.

Several books/articles repeating the same upstream source or the same author are
not independent corroboration and share source_family_id.

An unresolved contradiction overrides any score: no “92 beats 88” automatic truth
decision.

## Extraction during book ingestion

While ChatGPT parses pages it may identify:
- POI mentions;
- historical/alternative names;
- external identifiers explicitly present or confidently resolved;
- atomic POI facts;
- exact supporting page/region refs;
- contributor attribution.

This material should be staged in the same immutable document graph as the book.
It is not delivered to Street Story until the book revision successfully finalizes.

## POI-linked historical media

Book ingestion may also detect that an illustration itself depicts or documents a
POI. This relationship is independent from atomic fact extraction.

Use a separate versioned event:

~~~text
poi.media_evidence.v1
~~~

A media event carries:

- the same `poi_locator` rules as fact evidence;
- `knowledge://illustrations/<id>` instead of object-storage locators;
- relation: `depicts | illustrates | map_of | detail_of`;
- source page/figure/caption provenance;
- exact crop SHA-256;
- caption and time scope when present;
- illustration visibility and rights status.

The link is created only when the model explicitly identifies the image as being
about that POI. Mere co-location on the same page is not enough.

Object Storage remains the binary source of truth. Street Story stores the POI ↔
illustration relationship and provenance reference, not the image bytes.

This makes future Street Story workflows possible:

~~~text
POI
  -> historical media search
  -> filter by date / media kind / rights / visibility
  -> fetch permitted Knowledge illustration
  -> optionally use VibePublish media mirror
  -> publication/editorial workflow
~~~

A private or rights-restricted illustration stays non-publishable even if the POI
itself is public. Street Story must therefore treat media visibility/rights as a
hard retrieval/publication filter, not as descriptive metadata.

Fact evidence and media evidence remain separate so an old photograph can be
useful for a POI even when it introduces no new factual claim.

## Delivery topology

Book finalization must not depend on Street Story availability.

~~~text
book finalize
  -> activate Knowledge revision
  -> append POI evidence event to durable outbox
  -> asynchronous delivery
  -> Street Story idempotent ingest
~~~

For public/system corpus maintenance a narrowly scoped service identity is
acceptable.

For private user corpus delivery requires a user-approved Street Story resource
grant/delegation under the shared platform identity. A global service credential
must not impersonate a user.

If no grant exists:
- Knowledge finalize succeeds;
- outbox item becomes pending_authorization;
- nothing private is copied to Street Story.

## Street Story ingest behavior

Street Story must:
1. idempotently accept the evidence event;
2. resolve poi_locator to a canonical poi_id;
3. preserve the producer evidence snapshot;
4. normalize the atomic claim into its semantic event key;
5. merge evidence without majority voting;
6. run deterministic conflict candidate selection;
7. keep conflicting claims separately;
8. create/update a contradiction review case where needed.

No source count alone increases certainty.

## Contradiction lifecycle

Existing Street Story relations remain:

~~~text
contradiction
scope_difference
temporal_sequence
source_disagreement
uncertain
~~~

A contradiction record is never deleted when resolved. Resolution is append-only
history.

Canonical review choices:
- prefer_left;
- prefer_right;
- both_valid_scope;
- both_valid_temporal;
- unresolved;
- needs_more_sources;
- wrong_poi_link.

## Expert review through Projects Hub

Street Story owns the review case and final POI state.

Projects Hub receives an authorized projection:

~~~text
review_case_id
poi identity/name
claim A + evidence
claim B + evidence
verification-score components
source/author provenance
model/detector suggestion
required expertise
priority/deadline
~~~

The expert sees exact evidence links, including Knowledge page/region evidence
when authorized.

Projects Hub submits a typed resolution back to Street Story. It does not create
its own truth copy.

Expert identity uses the shared platform issuer + sub.

Expert profile contains scoped expertise:
- geography;
- historical period;
- subject/domain;
- languages;
- optional institutional role.

Routing is deterministic from case requirements to authorized expert profiles;
the Live agent may explain and help, but does not invent expert permissions.

High-impact or low-confidence cases may require two independent expert decisions.
Ordinary cases may accept one authorized expert decision.

## Review UI principles for Projects Hub

This fits Projects Hub's existing voice-first model:
- a compact “needs review” work card;
- voice explanation from the central Live agent;
- button choices for common resolutions;
- voice rationale;
- “need more sources” as a first-class action;
- no hidden automatic canonicalization.

Resolution is announced only after Street Story returns a durable receipt/readback.

## Implemented producer checkpoint — 2026-10-02

Regional Knowledge now has a local implementation slice for the producer side:

- `book_ingest(stage)` accepts typed POI fact candidates tied to exact staged
  page/region refs;
- candidate/event identities are deterministic per document revision;
- contributor names prefer chapter/article attribution over generic book authors
  when the model can identify it;
- verified author authority is stored in curated, versioned profiles and looked up
  by geography + subject; ambiguous same-name profiles fail closed;
- unknown author authority remains `null`;
- publication method and exact-region provenance feed the versioned
  `book-evidence-v1` evidence-strength score;
- finalize builds `poi.fact_evidence.v1` envelopes only from the validated staged
  graph;
- DB activation validates document/revision/scope/page/region provenance before
  writing events;
- unresolved source lineage is normalized to the conservative family `unknown`;
- public events enter `pending_delivery`; private/workspace events enter
  `pending_authorization`;
- outbox insertion and `active_revision` activation share one DB transaction.

This checkpoint deliberately stops **before** network delivery. A Street Story
ingest endpoint, authorization/grant resolver and retry worker remain separate
acceptance gates. Their absence cannot make book finalization fail after the DB
transaction succeeds.

## Acceptance gates

Before enabling automatic book→POI delivery:

1. same POI from Wikipedia, OSM and a book resolves to one poi_id;
2. ambiguous names fail to an unresolved link, not a silent merge;
3. private book evidence stays private end to end;
4. unknown author authority remains null;
5. contextual author score changes by domain without changing author identity;
6. same upstream source repeated in several publications is one source family;
7. high-score conflicting evidence remains contested;
8. duplicate outbox delivery is idempotent;
9. Street Story outage does not fail Knowledge finalize;
10. expert resolution is visible in Street Story and Projects Hub readback;
11. revoking an expert/user grant immediately prevents further private evidence reads.
