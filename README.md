# Regional Knowledge Base

Multimodal RAG and MCP service for books, journals and historical regional sources.

## Product goal

A non-technical user can attach a book to ChatGPT and say “add this to the regional knowledge base”. ChatGPT performs semantic/layout work through MCP; the service performs deterministic file handling, provenance, storage, ACL enforcement and indexing. The same knowledge is then reusable from ChatGPT, Live models, Wonderful Lections, Street Story and Projects Hub.

## Architecture

```text
                       shared Supabase Auth OAuth 2.1
                           one user / many MCPs
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

The target identity provider is Supabase Auth OAuth 2.1/OIDC. Every MCP remains a separate OAuth resource server but receives the same stable user `sub`. Each service enforces its own roles and RLS. See [docs/auth.md](docs/auth.md).

## Status

The public scaffold now includes hybrid retrieval plus deterministic
`start -> pages -> stage -> validate -> finalize` ingestion with multimodal
provenance and a Live-optimized MCP profile. Local tests cover the full
materialization flow; production Supabase/S3/OAuth deployment and real-book
acceptance remain explicit gates. No public corpus is implied by the code repository.

See:
- [Architecture](docs/architecture.md)
- [Storage and privacy](docs/storage.md)
- [OAuth resource-server contract](docs/auth.md)
- [Platform identity: one user, many MCPs](docs/platform-identity.md)
- [Rights model](docs/rights.md)
- [MCP surface and Live profile](docs/mcp.md)
- [Cross-project integrations](docs/integrations.md)
- [Street Story POI evidence bridge](docs/poi-integration.md)