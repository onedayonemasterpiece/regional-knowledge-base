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

Current implementation checkpoint:
- `start(file)` is implemented: bounded HTTPS file-parameter download, exact
  SHA-256, PDF inspection, private Object Storage persistence and private
  ingestion/document creation;
- `book_pages` is implemented: small deterministic JPEG page renders plus
  bounded native PDF text blocks/bboxes;
- failed start can reconcile and reuse the same document/ingestion/source object
  without duplicating rows;
- `stage/validate/finalize` remain the next implementation slice.

Staged books are invisible to retrieval until atomic finalize.

## Live profile

Interactive Live consumers use a separate optimized server profile
(`RKB_MCP_PROFILE=live`) that exposes **one tool only**:

```text
knowledge_search(query, max_evidence=3)
```

Fast path:

```text
Live
  -> knowledge_search
      -> Supabase vector + lexical + RRF
      -> parallel exact evidence range fetches from object storage
  -> compact evidence pack
  -> Live answer
```

This intentionally avoids the two model round-trips of generic `search -> fetch`.
The full ChatGPT/deep-research profile keeps standard `search` and `fetch`
semantics plus ingestion/access tools.

Rules:
- default evidence count is 3, hard maximum 5;
- no illustration bytes in the first response;
- illustrations are descriptors/IDs until explicitly requested;
- ingestion, sharing and rights tools never appear in the Live profile;
- deep enrichment remains optional/non-blocking;
- Live transport/capture/session lifecycle is provided by the shared
  `live-interaction` framework.

This minimizes declaration bytes, tool-choice ambiguity and conversational latency
for weak/fast Live models.

## Public and private search

Anonymous/public-only search may be exposed separately later. Authenticated search always applies row-level ACL before ranking candidates. Never retrieve private rows and filter them only after vector search.