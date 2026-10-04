# Accumulative evidence graph MVP — production acceptance

2026-10-04. Implementation and real-source acceptance: [PR #31](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/31).
Prerequisites at `db309a0ad2e01e5143ac1c20735bb214c30dc9ef` were reused;
no reconciliation, historical-book reimport or new model research was repeated.

## Implemented behavior

Migration 012 adds `rkb_entities`, `rkb_entity_aliases`,
`rkb_entity_mentions`, `rkb_entity_relations` and one durable discovery job table.
Kinds are person/event/historical_thread/poi_ref. Relations are only
participated_in, occurred_at and member_of. PostgreSQL remains the data plane.

ChatGPT-authored bounded candidates can be staged with a book or onto an owned
active revision. The server validates exact chunk/page/region/quote references,
source bytes/hashes, relation shapes, actor ACLs and deterministic replay keys.
No backend extraction or story-writing inference is introduced. Replacement
seeds advance atomically with activation; a failed finalize does not retire the
previous visible graph. Reviewed state requires an explicit model review note;
it does not establish canonical POI lifecycle approval.

Regular MCP has graph_stage, graph_fetch and graph_related. Fetch is one entity
and at most 20 one-hop neighbors. Related search uses the existing accepted
exact alias/lexical/E5/BGE stack. The Live surface is unchanged. Public canonical
POIs can request discovery before any Regional Knowledge graph seed exists;
job results remain actor-private, bounded and source-authorized.

One worker consumes durable, leased, claim-fenced jobs. Alias/content-version
and corpus/revision boundaries form idempotency keys. New document activation
pages through owned entities in batches of 32 and scopes children to that new
revision. Native POI alias polling covers 16 references per tick. Search reuses
the same durable BGE encoding job across polls; it does not repeat fast queries
while waiting. Retries are bounded. Discoveries remain candidate mentions with
ranking signals and unresolved identity; they never create relations or merge
people automatically.

Street Story owns canonical IDs/names/lifecycle. Its native read-only identity
adapter resolves unique names or external references; ambiguous or unavailable
resolution remains unresolved. Two identity-only canonical acceptance fixtures
and one historical alias were registered through Street Story's existing native
resolver. They are **candidate identities**, not verified lifecycle decisions.
No private book passage, claim, user bearer or full POI record was sent there.

## Actual real-source fixture and public OAuth checks

The private fixture uses two already-imported books: a German medical source
and a Russian historical source. Selected original pages were rendered and
visually reviewed; original chunk and region hashes and exact quotations were
checked. Publication promises are represented as announced events, not proof
that those events occurred. Reusable threads are subject groupings.
Private titles, names, passages, IDs and scans are excluded from this report.

| Acceptance | Actual result |
| --- | --- |
| One person with multiple events/threads | 3 events, 2 threads |
| One event with multiple people and POI | 3 people, 1 POI |
| Historical German name → current canonical POI | Same external identity reference |
| Historical/demolished object | Source-backed POI reference participates |
| Russian current-name query → German gold evidence | 1.0 known-gold recall |
| New native POI alias → old imported mention | 1/1 held-out passage found |
| New person alias → old imported mention | 1/1 held-out passage found, on a different page from its seed |
| Same-name person | Distinct candidate entity; no silent merge |
| Conflicting POI locator | Unresolved, no canonical reference |
| Foreign actor | Private graph read/write denied; private job denied |
| Public POI with foreign empty corpus | Job completed with zero private candidates |
| Returned relations | Every edge carries exact chunk/page/region/quote evidence |
| Bounded one-hop | Limit 1 truncation exercised; maximum 20 |
| Stage/alias/job replay | Identical IDs, no duplicate semantic effects |
| Thread usefulness | People/events/POIs plus 4 source fetches across two books; enough material for a source-grounded outline without corpus-wide search |

No final editorial prose was generated or persisted.

Initial uncached production alias acceptance took **288.894 s**, including cold
BGE startup and serial queue backlog. The strengthened held-out readback observed
already-completed discovery in **0.320 s**; this is a cached check, **not** a new
uncached latency claim. No interactive discovery latency SLA is established.

## New document and bidirectional discovery acceptance

A separate one-page **synthetic transport control** explicitly marked as not
historical evidence exercised the real production S3/database pipeline.
Operator-local attachment intake was used; this does not retest external chat
attachment URL download. Public OAuth performed stage, validate, asynchronous
finalize, status and finalize replay. Activation scheduled durable revision jobs;
existing person and POI aliases discovered the new chunk in **43.128 s**.

Existing backfill commands each encoded only that new chunk: **747 skipped,
1 written** for E5 and BGE. Neither accepted index/model was reconstructed.
The owned synthetic controls were then reversibly archived, preserving their
private audit trail and restoring the **747-chunk active historical corpus**.
There are 909 stored chunks including two inactive transport controls, 748 stored
vectors per accepted space, and 747 valid active vectors per space. Legacy
historical vectors, source hashes and active revision boundaries are retained.

This exercise found a pre-existing async-finalize incompatibility: the HTTP
worker records processing/cursor=finalize, while three SQL functions accepted
only ready. Migration 013 narrowly admits that exact validated async state and
preserves ownership, revision, coverage and provenance guards. NULL/parse/stage
cursors remain rejected. Structured finalize success/failure events now retain
correlation and error type/SQLSTATE without credentials or source payloads.

## Candidate quality and ambiguity measurement

After the acceptance queue drained, the owned active graph had **15 entities**:
5 people (4 reviewed, 1 same-name candidate), 4 events, 2 threads and 4 POI
references (3 reviewed source links, 1 unresolved locator). It had **20 relations**:
13 member_of, 5 participated_in, 2 occurred_at. Snapshot: 15 reviewed mentions
and 415 candidate mentions; 97 exact-alias discovery rows and 317 vector-only
rows, plus the explicitly staged same-name candidate. These are persisted rows
across versioned jobs, **not distinct people or deduplicated precision examples**.
All 58 owner-visible jobs were done at this snapshot.

The held-out gold lists were model-authored and narrow: 1/1 person, 1/1 POI.
To inspect false behavior rather than report recall alone, six distinct chunks
for the additional person were reviewed: the exact-alias held-out passage was
supported; five sampled vector-only excerpts (unrelated headings/body fragments)
were rejected as person-identity evidence. Thus this deliberately stratified
sample contained **1 supported and 5 false candidates**, with **0 automatically
accepted facts/edges/merges**. It is not a random sample or a corpus-wide precision
estimate. Other candidates remain unjudged. Vector-only ranking is noisy and
requires review; neither similarity nor exact-name match proves identity.

## Verification, deployment and receipts

- Full project suite: **114 passed**, including actual PostgreSQL/RLS, ordinary
  async finalization, replay, resolver outage, stale projections, ACL revocation,
  forged locators, claim fencing and bounded reads. CI runs Python 3.12/3.13 with
  an isolated pgvector/PostgreSQL service.
- Migrations 012 and 013 were applied twice in the isolated fixture before
  additive production deployment. Migration application preserved the existing
  corpus/vector digests before/after that transaction.
- Historical real-source public acceptance ran on release
  `58ef3a3e096f506207354dd964e8b6bb0d24c09e`; async guard regression and structured
  logging landed in `0094946`. The final exact-main SHA/CI/runtime receipt is
  attached to PR #31 and retained as `final-readback.json` after merge. A report
  cannot embed the SHA of its own final commit.
- MCP, E5, BGE controller and graph worker use immutable tracked-source releases.
  Existing CPU-only native E5 and remote BGE transports are reused. No paid
  embedding configuration or graph inference fallback is selected.

Private evidence is retained, without expiry, under
`/home/dev/artifacts/regional-knowledge-base/20261004T090738Z-accumulative-graph-mvp-20261004`:
real-source candidates/page render proofs, OAuth readbacks, synthetic control
readback, candidate judgment inputs, migration proofs, test logs and final
runtime receipts. It contains private source passages/images; access is local,
outputs are restricted, and OAuth test families are revoked. Public reusable
runners are `scripts/production/accept_entity_graph.py` and
`scripts/production/accept_graph_new_document.py`; they contain no private fixture.

## Remaining product limits

The old active Gause projection still has clipped text and image-only pages
without chunks. This graph uses bounded source-verified passages and does not
claim that book or corpus is complete. Future review/reimport remains separate.

The canonical identity adapter currently uses the same-host Street Story native
store. Remote/delegated enrichment transport and a product review UI for noisy
candidate mentions are not provided. Native acceptance identities remain
candidate lifecycle records. Explicit identity review is necessary before story
publication. Traversal is one hop and discovery is asynchronous with bounded
serial throughput; this MVP does not promise full-corpus precision or completeness.
