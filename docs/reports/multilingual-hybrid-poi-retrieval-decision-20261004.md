# Multilingual hybrid retrieval and POI discovery decision — 2026-10-04

## Product requirements

Regional Knowledge retrieval is language-independent with respect to the source.

A source may be a pre-war German book while the user asks in Russian, German, or
another supported language. Retrieval must not depend on translating the book into
the query language first. Original source text and exact provenance remain the
evidence of record.

The system also needs bidirectional POI discovery:

1. book -> POI: while ChatGPT imports a source, identify POI mentions,
   historical/alternative names and new evidence-backed facts;
2. POI -> corpus: when a POI is created or its alias set changes, search the
   authorized Regional Knowledge corpus for existing mentions/evidence that may
   belong to that POI.

Historical names are first-class identity evidence. Kaliningrad-region objects may
have several German names/spellings, later Russian names, transliterations, and
names valid under different jurisdictions or periods.

## What already exists

### Source/user ownership

Regional Knowledge already stores the authenticated owner on both the document and
the ingestion job and enforces document/workspace access through RLS/grants.

Current semantics therefore already preserve who uploaded/owns a source:

- document owner: rkb_documents.owner_user_id;
- ingestion actor/owner: rkb_ingestion_jobs.owner_user_id;
- private/workspace/public visibility and grants remain independent from source
  bibliographic metadata.

Do not expose another user's private source merely because a POI is public.

If product requirements later introduce ownership transfer, add an immutable
created_by/uploader provenance field then. Do not add it now without that feature;
current ownership is not transferable to an arbitrary other user.

### Existing POI boundary

Street Story remains canonical owner of:

- stable regional poi_id;
- aliases/external identities;
- canonical atomic POI claims;
- evidence sets;
- contradiction/arbitration state.

Regional Knowledge remains canonical owner of:

- books/journals/source provenance;
- page/region/illustration evidence;
- extracted evidence-backed POI fact candidates;
- exact knowledge:// evidence references.

Regional Knowledge already stages poi_facts and poi_media_links and can emit
versioned evidence events after successful finalize. Preserve this boundary.

## Multilingual retrieval

Fast tier:

- multilingual E5-small INT8 on DevCoveer;
- explicit 384-dimensional E5 space.

Main tier:

- BGE-M3 on Kaggle CPU;
- explicit 1024-dimensional BGE space.

Never compare raw vectors from different encoders. Same dimensionality would not
make two spaces compatible.

### Warm-path hybrid

When BGE is ready, test and permit a three-branch retrieval:

~~~text
query
  -> E5 ranking
  -> BGE-M3 ranking
  -> lexical / alias ranking
  -> rank fusion
  -> evidence hydration
~~~

Use rank-based fusion, initially RRF, not addition of raw cosine scores from E5
and BGE.

Do not assume E5+BGE is better merely because two encoders are available.
Acceptance must compare:

- E5 only;
- BGE only;
- E5 + lexical;
- BGE + lexical;
- E5 + BGE;
- E5 + BGE + lexical.

Enable the extra E5 branch on the warm BGE path only if multilingual retrieval
metrics improve enough to justify its CPU use.

When BGE is cold or unavailable, E5 + lexical provides immediate fast evidence
while a single Kaggle worker starts.

## Multilingual acceptance

The next retrieval benchmark must include source/query language crossing, not only
Russian questions over Russian text.

At minimum create source-verified tests for:

1. German source -> German query;
2. German source -> Russian query;
3. German source -> query containing a historical German POI name;
4. German source -> query containing the current Russian POI name;
5. historical spelling/orthographic variants;
6. transliteration variants where users plausibly use them;
7. ambiguous names shared by more than one POI;
8. query that uses a current name when the source only contains an older name.

Do not translate evidence passages for scoring. The relevant source passage in its
original language is the target.

For paired German/Russian questions that express the same information need,
measure whether both retrieve the same required evidence set.

Required metrics include Recall@1/5/10, MRR, multi-evidence coverage, and
per-language slices. Report cross-language degradation explicitly.

## POI alias model

A flat string alias list is sufficient for an MVP lookup but not enough for
historical identity work.

Street Story should evolve the canonical POI alias representation so an alias may
carry, where known:

- name;
- normalized form;
- language;
- script/transliteration;
- alias type: canonical/current/historical/former/transliteration/spelling variant;
- validity period or approximate time scope;
- jurisdiction/context;
- source/evidence reference;
- confidence/review state.

Do not invent dates or alias equivalence. Unknown metadata remains unknown.

Aliases are identity candidates, not proof that two differently named objects are
the same. Ambiguous resolution creates a review case rather than an automatic
merge.

## Book -> POI profiling during import

ChatGPT remains responsible for understanding the book.

While parsing a book, ChatGPT should identify and stage:

- POI mentions;
- the exact name as printed in the source;
- historical/alternative names indicated by the source;
- candidate canonical/current identities when supported;
- atomic new facts about a POI;
- exact page/region evidence;
- contributor/source attribution;
- POI-linked historical illustrations when applicable.

For each mention/fact, Regional Knowledge sends a narrow poi_locator containing
the observed/known names and available external IDs/coordinates. Street Story
resolves it against its canonical POI/alias graph.

Do not ship the whole book to Street Story and do not make Regional Knowledge own
a duplicate POI database.

If a source contains a potentially new fact about a known POI, finalize produces
the existing typed evidence event. Street Story decides whether it is new,
corroborating, conflicting, temporally scoped, or incorrectly linked.

## POI -> corpus reverse discovery

When a POI is created, accepted, or gains meaningful aliases, Street Story should
emit a narrow idempotent discovery request/event containing:

- poi_id;
- canonical/current name;
- all authorized historical/alternative names;
- external IDs when available;
- optional coarse geography;
- alias-set version.

Regional Knowledge then searches the corpus visible to the relevant scope.

Discovery uses several independent signals:

1. exact/normalized alias lexical matches;
2. historical spelling/transliteration variants supplied by Street Story;
3. E5 multilingual retrieval;
4. BGE multilingual retrieval when available;
5. optional geography/date context for disambiguation.

The result is a set of candidate evidence links, not automatic truth.

Every candidate retains document/revision/chunk/page/region, matched source
spelling, retrieval signals/ranks, access scope, internal source owner, and exact
evidence reference.

ChatGPT or expert review may then extract an atomic fact or reject the POI link.

### Privacy

Reverse discovery must not turn a public POI into a capability to enumerate other
users' private books.

For a user-facing search, query only documents that the requester can read.

For system/public POI enrichment, scan only public/system-authorized corpus unless
there is a specific delegated user/workspace grant.

Private evidence events retain their private scope end-to-end.

## Efficient reverse lookup

Do not synchronously rescan every chunk whenever a POI changes.

Use the existing retrieval indexes and a durable discovery job:

~~~text
POI created/aliases changed
  -> one idempotent discovery job keyed by poi_id + alias_version
  -> lexical alias candidates + vector retrieval
  -> bounded candidate pool
  -> evidence review/extraction
  -> typed POI evidence events
~~~

Re-run only when the POI alias/version materially changes or a newly finalized
document/revision may contain additional evidence.

## Import-triggered profiling

After a book revision finalizes:

1. preserve the staged POI facts already extracted by ChatGPT;
2. publish their evidence events asynchronously;
3. optionally schedule a bounded resolver pass for other POI mentions detected in
   the new chunks;
4. do not block book finalization on Street Story availability.

A new book should be discoverable for future POI reverse searches through the
normal chunk indexes; it does not require scanning all existing POIs synchronously
at finalize.

## Evaluation dataset for POI/history

Build a small source-verified benchmark with real regional examples covering:

- present Russian name vs pre-war German name;
- several German historical names for one object;
- spelling variant;
- renamed settlement/street/building;
- same/similar name referring to different objects;
- source passage that mentions only the historical name;
- new fact that already exists in Street Story;
- genuinely new fact;
- conflicting fact;
- private book that must not leak into public discovery.

Quality acceptance must measure both POI-link candidate recall and false/ambiguous
link rate requiring review. Do not optimize only for Recall if it produces silent
wrong-POI merges.

## Sequencing

1. Complete and accept the current DevCoveer E5 fast-tier implementation.
2. Implement BGE-M3 Kaggle CPU + multilingual dual-encoder retrieval and measure
   whether E5+BGE fusion improves the warm path.
3. Implement/accept the bidirectional POI discovery hooks and historical alias
   behavior, reusing the retrieval stack rather than creating another search
   system.
4. Reimport the incomplete Gause book only through the corrected ChatGPT-led
   source-review flow; then use it as one multilingual/POI evidence fixture.

This sequence keeps the system small: one Knowledge corpus, two intentional vector
spaces, one canonical POI graph in Street Story, and rank fusion rather than a new
semantic platform.


## Relation to the accumulative knowledge graph

The new IdeaHub voice requirement generalizes this POI work into a small
evidence-backed regional graph of people, events, historical threads and stable
POI references.

This does **not** change the embedding rollout order. The accepted multilingual
retrieval stack is reused by graph reverse-discovery instead of creating another
search engine.

Before graph implementation, the BGE acceptance should therefore include a few
entity-oriented cross-language cases in addition to POI names:

- German source person name -> Russian question;
- Russian/current POI name -> German historical passage;
- one event described with several people;
- one person appearing in more than one event/thread.

The graph design itself remains a later, separate implementation task.

See
[accumulative knowledge graph decision](accumulative-knowledge-graph-decision-20261004.md).
