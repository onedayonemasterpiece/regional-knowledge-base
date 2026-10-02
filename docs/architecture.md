# Architecture

## Invariants

Regional Knowledge Base is deliberately split into a cheap online retrieval plane and a resumable ingestion plane.

```text
ONLINE / latency-sensitive
model -> MCP -> embedding provider -> Supabase hybrid RPC -> object fragment fetch -> evidence

INGESTION / throughput-insensitive
uploaded PDF -> object storage -> page renderer -> ChatGPT vision/layout -> staged graph
             -> crops/media -> validation -> index revision -> activate
```

The runtime server is not the vector engine, full-text engine or permanent binary store.

## Data ownership

| Layer | Owns |
|---|---|
| S3-compatible object storage | exact original bytes, page renders, image crops, canonical document graph snapshots, normalized UTF-8 text projections |
| Supabase Postgres | catalog, ACLs, rights state, page/region metadata, chunks, embeddings, FTS, ingestion state |
| VibePublish MediaBank | optional Telegram-backed mirror/catalog of selected reusable illustrations |
| GitHub | source code, schemas, migrations, tests, public documentation |
| local disk | bounded disposable cache/work files only |

Object storage is private. “Public document” is an application authorization state, not a public bucket ACL.

Corpus body text is also kept out of Postgres: chunks carry a private text-object
ID, byte range and hash while Supabase holds only retrieval indexes and compact
metadata. This preserves the 500 MiB database budget.

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

Initial target: one lightweight MCP/gateway process on DevCoveer plus managed Supabase and S3-compatible object storage. The MCP Python SDK 2.x stateless HTTP mode is preferred so no user session is pinned to one worker.

Do not add Redis, Kafka, a local Postgres, a local vector database or a second OCR service until measurements justify them.