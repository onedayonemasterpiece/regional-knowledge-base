# MCP surface and model optimization

## Tool surface

Keep the public model-facing API small and goal-oriented:

1. `search(query)` — standard read-only search compatible with ChatGPT company knowledge/deep research.
2. `fetch(id)` — standard read-only fetch with full evidence, provenance and illustration descriptors.
3. `book_ingest(...)` — resumable ingestion state/write workflow and file-aware start.
4. `book_pages(...)` — read-only staged page batches returned as model-visible MCP image content.
5. `document_access(...)` — inspect/change visibility and sharing, subject to rights and role checks.
6. `profile()` — tiny identity/profile read for clients that display connected account identity.

Internal vector, FTS, object-store, crop and embedding operations are not model tools.

## Search/fetch compatibility

`search` returns:
```json
{"results":[{"id":"chunk-or-document-id","title":"...","url":"https://..."}]}
```

`fetch` returns:
```json
{
  "id":"...",
  "title":"...",
  "text":"...",
  "url":"https://...",
  "metadata":{
    "document_id":"...",
    "pages":[12,13],
    "provenance":[],
    "footnotes":[],
    "illustrations":[]
  }
}
```

URLs must be stable, user-openable evidence pages, not expiring object-store URLs.

## Ingestion

`book_ingest` is resumable and command-oriented because ingestion is one user goal with shared state:

```text
start(file) -> ingestion_id
book_pages(ingestion_id) -> mixed text metadata + MCP ImageContent blocks
stage(ingestion_id, parsed pages/regions/relations)
validate(ingestion_id)
finalize(ingestion_id)
status(ingestion_id)
```

The OpenAI tool descriptor marks the top-level file parameter with `_meta["openai/fileParams"]`.

`book_pages` returns page renders through MCP image content blocks, not JSON URLs that
the model would need to fetch separately. Each batch is deliberately small (default
4, maximum 8 pages) so vision context is bounded.

Staged books are invisible to retrieval until atomic finalize.

## Live profile

Interactive Live consumers use **only search and fetch by default**. They reuse the shared `live-interaction` framework.

Fast path:
```text
Live -> search -> Supabase vector + lexical + RRF -> compact evidence -> Live
```

Rules:
- default search returns a small candidate set;
- no image bytes in search results;
- illustrations return IDs/captions/availability only;
- fetch retrieves detail on demand;
- deep enrichment is optional/non-blocking;
- ingestion, sharing and rights tools are not in the Live capability bundle.

This keeps tool declarations and latency small for weak/fast Live models.

## Public and private search

Anonymous/public-only search may be exposed separately later. Authenticated search always applies row-level ACL before ranking candidates. Never retrieve private rows and filter them only after vector search.