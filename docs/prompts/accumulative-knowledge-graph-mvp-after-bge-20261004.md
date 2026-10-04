# Regional Knowledge — minimal accumulative knowledge graph MVP

Date: 2026-10-04

Run this **only after**:

1. the DevCoveer E5 fast tier is accepted in production;
2. the BGE-M3 Kaggle multilingual retrieval acceptance is complete.

Read first:

- docs/reports/accumulative-knowledge-graph-decision-20261004.md
- docs/reports/multilingual-hybrid-poi-retrieval-decision-20261004.md
- docs/poi-integration.md
- the final E5 production acceptance report
- the final BGE/Kaggle multilingual acceptance report

This is an implementation task, not a new graph-platform design exercise.

## Product goal

Add a small accumulating evidence-backed graph for:

- people;
- historical events;
- reusable historical/story threads;
- stable references to canonical Street Story POIs.

The graph must help discover related evidence across multiple books and support
future story/route/lecture assembly.

## Occam constraints

Do NOT add:

- Neo4j or another graph database;
- RDF/OWL/SPARQL;
- a generic ontology engine;
- GraphQL;
- embeddings for graph edges;
- a second POI catalogue;
- server-side LLM extraction;
- an automatic story-writing service.

Use the existing Regional Knowledge Postgres/Supabase data plane.

## Ownership

Regional Knowledge owns:

- people/event/thread nodes derived from evidence;
- their aliases with provenance;
- mentions in documents;
- evidence-backed structural relations;
- stable POI references.

Street Story owns:

- poi_id;
- POI aliases/current/historical names;
- POI lifecycle and location identity;
- POI claims/conflicts;
- canonical POI-to-POI relations.

Projects Hub remains a review/work surface.

## Minimal schema

Implement the smallest relational schema that satisfies the product behavior.

Expected shape:

### rkb_entities

- id
- kind: person | event | historical_thread | poi_ref
- canonical_label
- external_ref nullable, required for poi_ref
- compact metadata
- review/merge state as needed

### rkb_entity_aliases

- entity_id
- value
- normalized_value
- language nullable
- alias_type: current | historical | former | transliteration | spelling_variant
- optional time scope
- evidence locator/ref
- uniqueness/idempotency key

### rkb_entity_mentions

- entity_id
- document_id
- revision
- chunk/page/region evidence
- exact source spelling
- candidate/review state

### rkb_entity_relations

Initial relation kinds only:

- participated_in: person -> event
- occurred_at: event -> poi_ref
- member_of: person/event/poi_ref -> historical_thread

Every relation has evidence refs and optional time scope.

Do not add more relation types unless a real acceptance fixture cannot be
represented without them.

## Privacy and ACL

Graph retrieval is evidence-scoped.

A public POI must not expose the existence of another user's private book.

A node/edge projection is user-visible only through authorized evidence.
System/public enrichment sees only public/system-authorized evidence unless a
specific delegated user/workspace grant exists.

Test this explicitly.

## ChatGPT-led import

Do not make deterministic backend code infer people/events/relations.

Extend the ingestion staging contract so ChatGPT can submit bounded typed graph
candidates with exact page/region evidence:

- person;
- person alias;
- event;
- relation;
- historical-thread membership;
- poi_ref / poi_locator.

The backend validates references, ACL, idempotency, relation shape and persistence.

Ambiguous entity identity must remain unresolved/candidate rather than silently
merging.

Graph materialization must not make a valid book finalize fail merely because an
external Street Story POI resolver is temporarily unavailable.

## POI reference resolution

Reuse the existing Street Story POI contract.

If a staged POI locator resolves uniquely, store/reference the canonical poi_id.

If ambiguous, keep a reviewable unresolved link.

Do not copy Street Story's full POI record into Regional Knowledge.

Historical/demolished POIs are valid canonical POIs when Street Story knows them.

## Reverse discovery

Implement one bounded durable discovery mechanism.

Trigger when:

- a canonical POI gets a meaningful new alias/version;
- a Regional Knowledge person/event gets a new alias;
- a new document revision finalizes.

Use the already accepted retrieval stack:

- exact alias candidates;
- lexical;
- E5;
- BGE when ready.

Do not synchronously rescan the whole corpus for every update.

A discovery result is a candidate mention/evidence link. It does not automatically
merge entity identity or create an atomic fact.

Make jobs idempotent, keyed by entity/alias version plus corpus/revision boundary
as appropriate.

## Query/navigation surface

Add only narrow read capabilities needed by products/models, for example:

- fetch one graph entity and authorized aliases/evidence;
- get one-hop neighbors with evidence;
- search related evidence around one entity/thread.

Do not expose unrestricted graph dumping.

Limit one-hop expansion initially. No recursive graph traversal until usage proves
it valuable.

## Historical thread

A historical_thread is a lightweight durable grouping, not generated prose.

It may group people, events and POIs around a reusable subject such as a building,
district, institution or historical episode.

Street Story publication sessions remain a separate product concept.

## Acceptance fixture

Use real Regional Knowledge evidence.

At minimum prove:

1. one person is linked to multiple events/threads;
2. one event has multiple people and one POI;
3. a source-only historical German POI name can resolve to the current canonical
   POI when evidence supports it;
4. a demolished/historical POI participates in the graph;
5. German source -> Russian current-name query finds the required evidence;
6. adding a POI alias discovers an older already-imported book mention;
7. adding a person alias discovers an older mention;
8. same-name ambiguous person/POI does not silently merge;
9. private graph evidence is invisible without authorization;
10. every returned relation has exact source evidence.

Measure candidate recall and false/ambiguous-link behavior. Do not optimize only
for recall.

## Story-building usefulness test

Add one bounded acceptance scenario:

- choose one historical_thread;
- retrieve its people/events/POIs and source evidence;
- demonstrate that a model/client can obtain enough structured evidence to build
  a coherent historical outline without a corpus-wide ad-hoc search.

Do not generate/persist final editorial prose as part of this backend acceptance.

## Tests

Cover:

- graph node/relation schema validation;
- evidence-reference validation;
- alias normalization without semantic merge;
- ACL filtering;
- idempotent stage/finalize/discovery;
- ambiguous identity;
- external POI ref;
- one-hop bounds;
- reverse discovery on alias change;
- no external paid inference fallback.

Run full project tests.

## Documentation

Update:

- docs/architecture.md
- docs/integrations.md
- docs/poi-integration.md

Create:

docs/reports/accumulative-knowledge-graph-mvp-acceptance-20261004.md

Record actual schema, graph counts, acceptance fixtures, privacy checks, and
measured retrieval/discovery behavior.

## Definition of Done

Done only when:

1. the minimal relational graph schema is deployed;
2. no separate graph database exists;
3. ChatGPT can stage evidence-backed person/event/thread graph candidates;
4. canonical POIs remain Street Story-owned references;
5. reverse discovery works on new aliases/new documents;
6. multilingual accepted retrieval is reused rather than duplicated;
7. ambiguous identity fails to review/candidate state;
8. private evidence does not leak;
9. one-hop graph reads return exact evidence;
10. the real historical-thread usefulness scenario works;
11. full tests are green;
12. production deployment/readback is verified;
13. final acceptance report is committed.

At the end return only the implemented status, acceptance results, exact commit/PR
and remaining product gaps. Do not propose a larger graph platform unless the
measured MVP demonstrates a concrete missing capability.
