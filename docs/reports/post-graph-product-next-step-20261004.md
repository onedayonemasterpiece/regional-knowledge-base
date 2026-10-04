# Post-graph product next step — automatic indexing before real book reimport

Date: 2026-10-04

## Current accepted state

The accumulative graph MVP is merged and production-accepted in PR #31.

The product now has:

- source-grounded person/event/thread/POI-reference graph;
- bounded one-hop graph reads;
- bidirectional alias/document discovery;
- actor/ACL isolation;
- local E5 fast retrieval;
- Kaggle CPU BGE main retrieval;
- source-completeness transport for a correct future ChatGPT-led reimport.

The next step should **not** be a larger graph platform or a review UI.

## One product gap before the next real book import

The graph new-document acceptance proved that a newly activated revision can
schedule discovery, but E5/BGE indexing still required explicit operator
backfill commands. The control wrote only the one missing vector while skipping
the existing 747, which proves incremental/idempotent backfill but not automatic
user-facing indexing.

A normal user import must not require an operator to run:

- `scripts/production/backfill_e5.py`;
- `scripts/production/backfill_bge.py`.

Therefore the immediate implementation goal is:

**after a document revision becomes active, automatically reconcile missing E5
and BGE vectors for that active revision.**

## Occam design

Do not create a generic orchestration system.

Reuse the existing facts as durable state:

- active revisions/chunks are already in PostgreSQL;
- E5 vectors have their own explicit 384-d space;
- BGE vectors have their own explicit 1024-d space;
- the BGE durable queue already exists;
- E5 encoder sidecar is already always ready;
- BGE controller already provides single-start, lease and recovery.

The smallest acceptable implementation may be a bounded periodic/index-maintenance
loop in an existing background service, or one very small worker/timer if no
existing loop is a clean fit.

It should repeatedly reconcile:

~~~text
active chunks missing E5
  -> local E5 sidecar
  -> upsert matching E5 vector

active chunks missing BGE
  -> enqueue existing durable BGE document jobs
  -> BGE worker writes matching BGE vector
~~~

Missing-vector state plus existing BGE jobs should remain the primary durable
recovery mechanism. Do not introduce another queue/table unless the current data
model cannot make the operation idempotent.

## Activation and search behavior

Source activation must remain independent of Kaggle availability.

After activation:

- source/evidence is immediately authoritative;
- graph discovery may proceed;
- E5 indexing starts automatically;
- BGE document jobs start/warm Kaggle automatically;
- import/finalize does not wait for BGE completion.

Retrieval degrades by actual readiness:

~~~text
BGE ready for the active corpus -> BGE + lexical
BGE incomplete/unavailable but E5 ready -> E5 + lexical
E5 incomplete/unavailable -> lexical
~~~

Never use stale vectors from an inactive revision as if they belonged to the new
active revision.

## Status

Expose enough status for the client/model to know:

- active chunks;
- E5 vectors ready / missing;
- BGE vectors ready / missing;
- BGE worker state;
- current retrieval mode;
- indexing pending/running/ready/degraded.

Do not expose private text, source titles or another actor's corpus counts.

## Priority

Interactive BGE query jobs keep priority over book/document embedding jobs.
A large import must not make live retrieval unusable.

E5 stays bounded to the already accepted one-CPU/one-GiB sidecar constraints.

## Failure/recovery

Prove:

- service restart during E5 indexing resumes from missing vectors;
- BGE worker loss leaves durable document jobs recoverable;
- duplicate activation/finalize replay does not duplicate vector rows/jobs;
- archived/inactive synthetic controls are not treated as active search corpus;
- actor/ACL scope does not leak via indexing/status;
- no paid/external inference fallback.

## What follows this task

Once automatic post-finalize indexing is production-accepted, the next product
step is **not another Codex infrastructure task**.

ChatGPT should create a corrected new revision of the Gause book using the
already-deployed source continuation and visual review flow:

1. read every source page;
2. follow all clipped native-block continuations;
3. visually inspect image-only/materially visual pages;
4. build complete chunks/evidence;
5. stage people/events/historical threads and POI locators while understanding
   the book;
6. preserve exact historical German/current Russian aliases where supported;
7. finalize the corrected revision;
8. observe automatic E5/BGE indexing and graph discovery;
9. run multilingual German/Russian retrieval and graph checks against the new
   revision.

Do not ask Codex or the backend to semantically read the book.

## Deferred until real corpus evidence says otherwise

Do not build these before the corrected Gause reimport:

- a full graph review UI;
- a graph database;
- generalized ontology;
- automatic candidate acceptance;
- remote/delegated Street Story transport if same-host integration remains valid
  for the deployed product topology.

The graph acceptance intentionally showed vector-only candidate noise. A real
reimport will provide the evidence needed to design the smallest useful review
workflow instead of guessing it now.
