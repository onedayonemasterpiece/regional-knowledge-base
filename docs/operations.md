# Operations and acceptance gates

## Before production

The service is not production-ready merely because unit tests pass. Required gates:

1. create the Supabase project and enable OAuth 2.1 Server with asymmetric signing keys;
2. configure exact MCP resource binding and prove that a token for another MCP is rejected;
3. apply SQL migrations and run RLS acceptance with at least two users, one workspace and anonymous public reads;
4. create a private S3-compatible bucket with public ACL disabled;
5. configure a 768-dimension external embedding endpoint; verify lexical-only degradation when it is unavailable;
6. implement and exercise book ingestion against a representative born-digital PDF and a scan-only/multicolumn source;
7. prove raw private source PDFs remain inaccessible when normalized content is public;
8. connect the MCP from ChatGPT and test `search`, `fetch`, file-parameter ingestion and model-visible page images;
9. connect the read-only Live profile through `live-interaction` and measure p50/p95 tool latency;
10. verify cross-service identity with Wonderful Lections or Projects Hub using the same Supabase `sub` and distinct MCP resources.

## Performance targets

Initial targets, to be measured rather than assumed:

- search tool p95 excluding model generation: < 1.0 s in normal hybrid mode;
- lexical degraded search p95: < 750 ms;
- fetch p95 without image bytes: < 500 ms;
- default Live search result count: 5–8;
- page-vision ingestion batch: 4 pages, hard maximum 8;
- online server CPU must not perform vector ANN, corpus-wide FTS or cross-encoder inference.

## Ingestion isolation

Ingestion is lower priority than interactive retrieval. Page rendering/cropping concurrency is bounded independently. A large PDF must not starve Live/search requests. Staged data never participates in retrieval until finalize atomically switches `active_revision`.

## Observability

Log operation IDs, user subject hash/pseudonymous ID, ingestion/document IDs, stage, latency and external dependency status. Never log bearer tokens, signed object URLs, source text, page images or private object keys.
