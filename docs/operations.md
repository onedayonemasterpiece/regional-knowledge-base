# Operations and acceptance gates

## Current state

Data infrastructure is available:
- dedicated Supabase Postgres project;
- Session Pooler connectivity from DevCoveer;
- migrations 001–005;
- pgvector/pgcrypto;
- private S3-compatible corpus bucket;
- deterministic ingestion implementation and tests.

Authentication correction:
- Supabase OAuth is **not** required;
- current source now refuses to derive MCP auth from `KB_SUPABASE_URL`;
- the old PostgREST user-JWT backend is disabled by default;
- the next implementation slice must provide application OAuth plus a direct
  Postgres RLS actor bridge.

There is therefore no remaining user action in the Supabase Dashboard related to
OAuth.

## Product beta gates

The next executor must deliver a usable product, not another architecture audit.

1. Implement application-owned OAuth 2.1 for the Knowledge MCP, independent from Supabase.
2. Implement direct Postgres RLS actor binding through `KB_SUPABASE_SESSION_CONNECTION`; remove runtime dependence on forwarding user bearer tokens to Supabase.
3. Add/migrate application-owned user identity and replace `auth.uid()` policy dependence.
4. Re-run two-user/private/workspace/grant/anonymous RLS acceptance against the live database through the new actor bridge.
5. Keep the existing private object-storage acceptance green.
6. Configure a real 768-dimension external embedding provider and verify hybrid search plus lexical degradation.
7. Deploy a stable HTTPS MCP resource on DevCoveer.
8. Prove OAuth discovery, PKCE, exact resource binding, refresh rotation/revocation and authenticated MCP initialize/tools/list.
9. Run a real book end-to-end: attached PDF -> start -> page batches -> model stage -> validate -> finalize.
10. Verify exact source PDF and derived graph/text/crops in object storage and only compact index/catalog data in Postgres.
11. Verify search/fetch returns evidence from that imported book with page provenance.
12. Include at least one illustration/caption/POI relationship in the acceptance source and verify crop/media evidence.
13. Verify raw private source remains private.
14. Return the exact ChatGPT MCP URL and short owner connection/import instructions.

## Performance targets

- hybrid search p95 excluding model generation: <1.0 s;
- lexical degraded p95: <750 ms;
- fetch p95 without image bytes: <500 ms;
- page vision batch: default 4, hard max 8;
- no corpus-wide ANN/FTS/cross-encoder compute on the gateway.
- `finalize` must return control promptly for large books. Long render/index work
  runs server-side under persisted `processing/finalize` state; callers use
  `status` later instead of holding one MCP request/ChatGPT turn open for minutes.

## Ingestion isolation

Page rendering/cropping concurrency is bounded and lower priority than interactive
retrieval. Staged data never participates in retrieval until finalize atomically
switches the active revision.

## Observability

Log operation IDs, pseudonymous actor ID, document/ingestion IDs, stage, latency
and dependency state. Never log bearer tokens, OAuth codes, source text, page
images, object keys or signed URLs.

## Existing producer integrations

Source code already contains:
- POI fact candidates + durable outbox;
- POI-linked historical media evidence;
- rights/access-aware media relations;
- Street Story/Projects Hub contracts.

These must remain non-blocking for book finalize. Network delivery can remain a
post-beta integration step if the local durable outbox and contracts stay intact.
