# Accumulative regional knowledge graph — decision 2026-10-04

Source idea: IdeaHub voice intake
`voice-20261004-082140-494ba64c` ("Формирование концепции накопительного графа
знаний региона").

## Decision

Regional Knowledge Base should accumulate a small evidence-backed semantic graph
in addition to the document/page graph and retrieval indexes.

The purpose is to connect:

- people;
- historical events;
- historical/story threads;
- canonical POI references owned by Street Story.

This graph is for accumulation, traversal, query expansion and story assembly.
It is **not** a new source of truth that can replace book/page evidence.

## Keep it small

Do not introduce Neo4j, RDF/OWL, SPARQL, a new graph service or a generic ontology
for the MVP.

Use the existing Postgres/Supabase data plane with a small relational projection.
The current document graph (Document -> Page -> Region -> relations) remains
unchanged and separate.

The first semantic graph needs only four node kinds:

- `person`
- `event`
- `historical_thread`
- `poi_ref`

Additional entity kinds are added only when real sources require them.

## Ownership boundary

### Regional Knowledge owns

- source documents and provenance;
- person identity candidates/aliases accumulated from sources;
- historical event nodes;
- lightweight historical/story threads;
- evidence-backed structural relations between those nodes;
- references from events/threads to canonical POIs;
- exact page/region/chunk evidence for mentions and relations.

### Street Story owns

- canonical `poi_id`;
- POI current/historical aliases and external identities;
- POI location/lifecycle identity;
- POI claims/evidence/conflicts;
- relations whose semantics are fundamentally between POIs, such as a historical
  object and its modern successor/site relation.

Regional Knowledge stores only a stable `poi_ref`, for example
`streetstory://poi/<uuid>`, and source evidence that may suggest a POI relation.

### Projects Hub

Projects Hub is a review/work surface, not another graph owner.

## Minimal graph storage

A practical first schema can be only four compact relation/index tables:

1. `rkb_entities`
   - id
   - kind: person/event/historical_thread/poi_ref
   - canonical_label
   - external_ref for poi_ref
   - compact metadata/review state

2. `rkb_entity_aliases`
   - entity_id
   - value / normalized value
   - language
   - alias_type: current/historical/former/transliteration/spelling_variant
   - optional time scope
   - evidence reference

3. `rkb_entity_mentions`
   - entity_id
   - document/revision/chunk/page/region
   - exact source spelling
   - evidence/access scope
   - reviewed/candidate state

4. `rkb_entity_relations`
   - source entity
   - kind
   - target entity
   - evidence refs
   - optional time scope
   - reviewed/candidate state

Initial relation kinds are intentionally structural:

- `participated_in`: person -> event
- `occurred_at`: event -> poi_ref
- `member_of`: person/event/poi_ref -> historical_thread

Do not turn every atomic historical claim into a graph edge type. Detailed facts
remain evidence-backed claims. The graph should make related evidence discoverable,
not become an ontology project.

## Story/thread semantics

A `historical_thread` is a durable grouping of entities/evidence that can be
reused to assemble narratives, routes or lectures.

It is not:
- a duplicate copy of all facts;
- a generated publication draft;
- a Street Story user publication session.

Example threads could be "Альбертина", "Амалиенау", or the history of one
building across several source documents.

## Import behavior

ChatGPT remains the semantic parser.

During book import ChatGPT may stage:

- people and aliases exactly as printed;
- events;
- POI locators/historical names;
- person <-> event relations;
- event <-> POI references;
- historical-thread membership;
- exact source evidence.

The MCP/backend validates structure, ACL, evidence references, idempotency and
persistence. It must not invent entities or semantic relations itself.

Ambiguous identity is a candidate/review state, not an automatic merge.

## Reverse discovery

Accumulation must work in both directions.

When a new canonical entity or a materially new alias appears, schedule a bounded
discovery job against the already indexed authorized corpus:

- exact alias search;
- lexical search;
- multilingual E5;
- BGE when available.

This applies to POIs and later to persons/events.

The result is candidate mentions/evidence. A vector hit does not automatically
create an identity link or fact.

When a new document finalizes, only its new chunks need entity profiling; do not
rescan the whole corpus synchronously.

## Multilingual and historical names

The graph and retrieval stack are complementary.

Explicit aliases solve historical identity:
- current Russian name;
- historical German name(s);
- spelling variants;
- transliterations;
- time/jurisdiction metadata when known.

Multilingual vector retrieval solves semantic cross-language discovery:
- German source -> German question;
- German source -> Russian question;
- current Russian name/question -> German historical source evidence.

The exact source-language passage remains the evidence target.

## Query flow after the graph exists

The graph should improve retrieval without replacing it:

~~~text
user query
  -> detect already-known entity/POI candidates
  -> collect authorized aliases / linked context
  -> lexical alias branch
  -> E5 branch
  -> BGE branch when warm
  -> rank fusion
  -> evidence fetch
  -> optional graph expansion to related evidence
~~~

Do not recursively traverse the graph without bounds. Start with one-hop expansion
and measure whether it helps.

## Privacy

A public POI or person identity must not reveal another user's private corpus.

Node/edge APIs return data only when the requester has at least one authorized
evidence path for that projection. Private mentions/relations remain private.

System/public enrichment uses only public/system-authorized evidence unless an
explicit delegated user/workspace grant exists.

## Historical/demolished POIs

A no-longer-existing object is still a valid POI if it has historical identity and
meaningful geography.

Canonical lifecycle and POI-to-POI spatial/historical relations stay in Street
Story. Regional Knowledge contributes source evidence and can discover candidates.

## Sequencing

1. Finish the current always-ready E5 production acceptance.
2. Implement BGE-M3 Kaggle CPU and measure multilingual E5/BGE/lexical fusion.
3. Add this minimal relational knowledge graph and bidirectional discovery,
   reusing the accepted retrieval stack.
4. Reimport the incomplete Gause book through the corrected ChatGPT-led source
   review and use it as one graph fixture.

Do not mix graph implementation into the current E5 task.

## Acceptance for the graph MVP

Use a small real-source fixture, not a synthetic ontology.

At minimum prove:

- one person linked to multiple events/threads;
- one event linked to multiple people and a POI;
- historical POI name resolves to the same canonical POI reference as the current
  Russian name when evidence supports that identity;
- a demolished historical POI can participate in the graph;
- German source evidence can be found from a Russian current-name query;
- adding a new POI/alias finds old book mentions without rescanning everything
  synchronously;
- ambiguous same-name person/POI does not silently merge;
- private-source graph evidence does not leak into public discovery;
- every graph relation returned to a user has source evidence.

The graph MVP succeeds when it improves evidence discovery and story assembly with
a small schema. It does not need a general graph platform.
