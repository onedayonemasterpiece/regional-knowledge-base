# Regional Knowledge Base

Multimodal RAG and MCP service for books, journals and historical regional sources.

## Product goal

A non-technical user can attach a book to ChatGPT and say “add this to the regional knowledge base”. ChatGPT performs semantic/layout work through MCP; the service performs deterministic file handling, provenance, storage, ACL enforcement and indexing. The same knowledge is then reusable from ChatGPT, Live models, Wonderful Lections, Street Story and Projects Hub.

## Architecture

```text
                      application/platform OAuth 2.1
                         independent from Supabase
                                    |
                                    v
ChatGPT / Codex / Live adapter -> Regional Knowledge MCP
                                    |
                +-------------------+-------------------+
                |                                       |
                v                                       v
      Supabase Postgres                         S3-compatible object storage
 vector + FTS + ACL/catalog                 original PDF + pages + crops + graph
                |
                v
        compact evidence results
```

GitHub stores **code and documentation only**. Corpus data is never committed.

### Retrieval

Fast search is hybrid: vector/HNSW + lexical/GIN inside Supabase, fused before the model answers. The gateway stays thin. Deep enrichment and image fetches are optional.

### Ingestion

The canonical model is a page/region graph with first-class captions, footnotes, reading order and illustrations. Retrieval chunks are derived from that graph, so rechunking never requires repeating document vision.

### Media

Object storage is authoritative. VibePublish MediaBank is an optional secondary media mirror/catalog for reusable illustrations; it is not the source of truth and private uploads are not mirrored by default.

## OAuth and multi-service identity

Supabase is only the data plane. MCP authentication uses an application/platform
OAuth 2.1 authorization server and a stable application user UUID. Every MCP
remains a separate OAuth resource. See [docs/auth.md](docs/auth.md).

## Status

The public scaffold now includes hybrid retrieval plus deterministic
`start -> pages -> stage -> validate -> finalize` ingestion with multimodal
provenance and a Live-optimized MCP profile. Local tests cover the full
materialization flow. The producer side of the Street Story POI bridge also
stages evidence-backed POI candidates and writes a durable authorization-aware
outbox at finalize; network delivery is still an explicit gate. Supabase/S3 are configured, but the product is intentionally not considered
ready until the independent MCP auth plane and direct Postgres RLS actor bridge
are implemented and a real book is imported end-to-end. No public corpus is implied by the code repository.

See:
- [Architecture](docs/architecture.md)
- [Storage and privacy](docs/storage.md)
- [OAuth resource-server contract](docs/auth.md)
- [Platform identity: one user, many MCPs](docs/platform-identity.md)
- [Rights model](docs/rights.md)
- [MCP surface and Live profile](docs/mcp.md)
- [Cross-project integrations](docs/integrations.md)
- [Street Story POI evidence bridge](docs/poi-integration.md)