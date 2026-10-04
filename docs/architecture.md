# Architecture

## Invariants

Regional Knowledge Base is deliberately split into a cheap online retrieval plane and a resumable ingestion plane.

```text
ONLINE / latency-sensitive
model -> MCP -> E5/BGE + Supabase pgvector/FTS -> authorized evidence

INGESTION / throughput-insensitive
PDF/DjVu source -> thin format adapter -> page image/native hints -> ChatGPT vision/reading
               -> typed staged graph -> validate/finalize -> Supabase text/index state
               -> original source archive through VibePublish Telegram /2
               -> illustration archive through VibePublish Telegram /4
```

The runtime server is not the vector engine, full-text engine or permanent binary store.

## Data ownership

| Layer | Owns |
|---|---|
| VibePublish / Telegram via dedicated `TELEGRAM_KNOWLEDGE_BASE` connection | durable original source DOCUMENTs in topic /2 and extracted illustration DOCUMENTs in topic /4 |
| Supabase Postgres | catalog, ACLs, rights state, revisions, source refs/hashes/formats, chunk source text/search material, page/region/graph metadata, embeddings, FTS, ingestion/index state |
| S3-compatible object storage | bounded temporary ingestion/cache objects only; not the corpus archive |
| GitHub | source code, schemas, migrations, tests, public documentation |
| local disk | bounded disposable cache/work files only |

Telegram/provider IDs and object-store keys are never authorization. “Public
document” is an application authorization state enforced by Regional Knowledge.

Parsed chunk text is compact enough for the current corpus scale and belongs in
Supabase alongside FTS/pgvector. The 500 MiB database budget is protected by
keeping large original binaries out of Postgres, not by pushing sub-megabyte book
text into a permanent S3 corpus.

## Canonical document graph

```text
Document
  -> Page
      -> Region(kind, bbox, reading_order)
          -> Relation(caption_of, footnote_of, continues_to, illustrates, refers_to)
      -> Illustration(source_region, crop, caption relations)
```

Chunks reference region/page IDs. They are derived, can be rebuilt, and do not own page geometry.

## Availability

Search degrades explicitly:
- vector + lexical available: hybrid result;
- embeddings unavailable: lexical-only result with degraded-mode marker;
- object storage unavailable: search may still return metadata/snippets already stored in the index, while fetch reports source unavailable;
- Supabase unavailable: private retrieval fails closed; no fallback scans private objects.

Ingestion never competes with Live search for mandatory CPU. Concurrency is bounded separately.

## Deployment shape

Initial target: one lightweight MCP/gateway process on DevCoveer plus managed
Supabase, VibePublish/Telegram binary archival and a bounded S3-compatible staging
cache. The MCP Python SDK 2.x stateless HTTP mode is preferred so no user session
is pinned to one worker.

The MCP is deliberately a **thin document orchestrator**, not a recognition
service. It may decode/render supported containers (PDF, DjVu), hash bytes, expose
native text already present in the container, persist typed results and route
binary assets. ChatGPT performs transcription/recognition, semantic layout,
caption/footnote reasoning, chunking and entity extraction.

Do not add Redis, Kafka, a local Postgres, a local vector database, OCR engine,
VLM or server-side LLM parser until measured product need justifies one.

## Accumulative semantic graph

The document graph and the regional knowledge graph are different layers.

The document graph preserves source structure:

`Document -> Page -> Region -> layout/evidence relations`.

The accumulative semantic graph is a small evidence-backed projection over sources:

`person <-> event <-> poi_ref`, grouped where useful into `historical_thread`.

MVP uses the existing Postgres data plane, not a separate graph database. Every
semantic node/edge returned to a user must remain traceable to authorized
page/region/chunk evidence.

Street Story remains canonical owner of POI identity, aliases, lifecycle and
POI-to-POI relations. Regional Knowledge references stable POI IDs rather than
creating a second POI catalogue.

See
[accumulative knowledge graph decision](reports/accumulative-knowledge-graph-decision-20261004.md).

## Evidence-backed entity graph MVP

Migration 012 adds owner-scoped people, events, historical threads and canonical
Street Story POI references. Mentions, aliases and three structural edge kinds
retain exact chunk/page/region locators and source quotes. Reads are evidence-RLS
filtered, active-revision filtered and limited to one hop/20 edges. Identity keys
are explicit; Unicode alias normalization never merges people or places.

ChatGPT submits bounded typed `entity_candidates` during staging, or
`graph_stage` on an owned active revision. The backend validates source bytes,
region attribution, shape and idempotency; it performs no semantic extraction.
Unresolved POI locators remain reviewable and do not block book activation.

A separate small Postgres discovery queue drives one bounded background worker.
It reuses accepted E5/BGE/lexical retrieval and exact alias branches. Candidates
retain retrieval signals and never become facts or identity merges automatically.
New revisions enqueue one paging job; alias versions enqueue idempotent jobs.
Existing incomplete source projections remain incomplete.

Entity seeds prepared for a replacement revision become visible only with the
same successful document activation transaction. The current active graph remains
readable during retryable finalization. Source-owner staging rows may be inspected
under owner RLS; public graph APIs always filter the active source revision.

Automatic post-activation indexing uses the small `indexing` process: payload-free
Postgres activation wakeup plus missing-vector reconciliation, local E5 batch4
and existing priority BGE document jobs. It introduces no durable scheduler/table.
Readiness and SQL snapshot guards use actor-authorized active source/hash/revision
coverage; incomplete spaces safely degrade. See [automatic indexing](operations/automatic-indexing.md).
