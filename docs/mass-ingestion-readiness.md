# Mass-ingestion readiness

This is the human-readable companion to the protected requirements contract in
`.devcoveer/requirements.json`.

## Product outcome

Regional Knowledge Base is being built so the owner can move from one-off book
experiments to routine **mass ingestion of books**. A technically successful
import is not enough. The system is ready for mass filling only when all of the
following remain true together:

1. the vector/search plane fits the applicable Supabase quota with headroom;
2. dense retrieval finds source-backed evidence with high quality without relying
   on lexical search to rescue it;
3. search latency is low and predictable enough for repeated agent tool calls;
4. each imported book preserves source/provenance and uses retrieval-oriented
   semantic chunks;
5. batch import/indexing is resumable, bounded and operationally boring.

These are release gates, not aspirational telemetry.

## Critical acceptance gates

| Gate | Required acceptance |
| --- | --- |
| Scale | Minimum engineering benchmark: >=100 book-equivalent sources and >=100,000 active chunks, or a larger real corpus when available. This is not a product limit. |
| Supabase steady-state | <=80% of the current applicable database quota, including vectors, vector indexes, minimal anchors/scope, ordinary indexes, TOAST and baseline/system overhead. |
| Largest-source reimport peak | <=90% of the current applicable database quota. |
| Dense retrieval | BGE-only held-out known-evidence Hit@5 >=85% and Hit@10 >=90%. |
| Critical language slice | Any product-critical language slice, including RU query -> DE source while German books are in scope, Hit@10 >=85%. |
| Retrieval benchmark | >=30 held-out source-grounded fact families; BGE-only, E5-only, lexical-only and configured fusion reported separately. |
| Warm backend retrieval | p95 <=1000 ms on production-like corpus/concurrency. |
| Public MCP search | p95 <=1500 ms, with a 2000 ms hard evidence-search interaction deadline. |
| Chunk safety | Semantic target around 256 encoder tokens; exact final E5 and BGE input must remain <=512 tokens. |
| Per-book acceptance | A newly imported/reprocessed active revision must pass source-grounded natural-query extraction/retrieval checks; paraphrases and relevant cross-language cases are included. |

## Storage boundary

Supabase is the vector/search data plane, not the full corpus archive. Full source
text, scans/page images and detailed geometry remain outside Supabase. Capacity is
measured from the actual database, including index and system overhead; raw vector
math or row counts alone are not sufficient evidence.

If the capacity gate fails, optimize placement/index representation and measure
again. Do not silently shrink the corpus, discard evidence or weaken retrieval
quality merely to fit quota.

## Retrieval policy

Dense retrieval must stand on its own. Lexical/FTS search is a useful bounded
complement for names, exact terminology and other selective queries, but it is not
allowed to hide a weak vector path in acceptance.

Quality acceptance is source-grounded retrieval acceptance, not generated-answer
accuracy. A green vector-count/readiness check is not a quality test.

Any change to chunking, embedding model/space, vector index/search, fusion,
lexical query formulation, authorization scope or storage placement must rerun the
relevant capacity, retrieval-quality and latency gates before production rollout.

## Per-book import contract

A book is not accepted merely because finalization or indexing completed.

The accepted active revision must:

- exclude scanner/service material that is not part of the book;
- preserve document -> page -> region evidence provenance;
- use coherent semantic passages rather than page-sized embeddings;
- pass exact E5/BGE token-budget validation with no silent truncation;
- complete the configured required vector spaces; the measured production default is BGE-only, while E5 remains an optional diagnostic/fusion space;
- pass natural-query retrieval checks against **that exact active revision**.

The page review/provenance layer may be reused for a byte-identical archived
source, but new chunking/retrieval output is evaluated again.

## Operational contract

Mass ingestion must be resumable and idempotent. One failed or superseded
revision must not block unrelated books. Queues and concurrency are bounded;
backpressure is explicit; ChatGPT does not need to keep a turn open while indexing
finishes; only a fully indexed/accepted revision becomes active. A pending book
must also be observably making progress: five minutes without required-vector
publication progress is a degraded/stalled condition, not a normal steady state.
Deployment must keep the MCP, indexer and BGE controller on one exact release SHA;
mixed runtime releases are a failed deployment, not an acceptable compatibility
mode.

## Current evidence and remaining proof

The two-book dense-only stress work now strengthens the small-passage BGE decision.
On the frozen held-out original questions, t256+BGE reaches Hit@5 **87.5%** and
Hit@10 **90.625%**. The held-out Russian-query -> German-source slice reaches
Hit@5/Hit@10 **91.67% / 91.67%**. E5 is materially weaker on the same corpus and
is therefore no longer a default publication requirement.

The same stress deliberately exposes remaining weaknesses rather than hiding
them. Verbose instruction wrappers reduce retrieval, and adding an unrelated
other-book title can poison the query embedding badly. A single cosine threshold
also does not reliably separate answerable from out-of-corpus questions.
Retrieval clients should therefore send the semantic question cleanly, use
document scope separately from query text, and verify returned evidence.

Compact pgvector storage has also been measured, not merely estimated. At
100,000 synthetic rows backed by real BGE vectors, halfvec(1024) plus a
binary-quantized HNSW candidate index and halfvec rerank projects the migrated
database to about **345 MB** steady state and **352 MB** with a 2,000-row pending
revision, against the current 500 MB planning quota. Candidate+rerank p95 is
about **35-42 ms** in that temporary-table stress. This passes the storage
headroom gate for the benchmark shape, but still requires production migration
acceptance.

That does **not** yet mean the project is mass-ingestion-ready. The remaining
critical proof includes:

- production migration/acceptance of the compact BGE vector plane;
- end-to-end p95 MCP/backend latency, including query embedding, at production-like
  scale and concurrency;
- repeatable per-book active-revision retrieval acceptance on real imports,
  including the current Brünneck reprocess;
- continued quality after corpus growth beyond the current two-book design corpus.

Until these gates pass together, treat the system as retrieval-hardening /
mass-ingestion-preparation rather than approved for unattended bulk filling.