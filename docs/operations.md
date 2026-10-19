# Operations and acceptance gates

## Current delivery target — 2026-10-19

The [single implementation/debugging handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [architecture](designs/rkb-scalable-kaggle-architecture-20261019.md), [acceptance](testing/rkb-scalable-kaggle-acceptance-20261019.md) and [layers-only cutover](migrations/rkb-layers-only-cutover-20261019.md) govern new work. The earlier beta checklist below records a historical implementation stage; do not restart Supabase OAuth setup, install its old 768-dimensional provider or restore E5 from it.

Preserve the existing service and parallel work until the new path and recovery are verified. Inspect executing revision separately from Git main. BGE inference stays on Kaggle; the existing SQLite/Supabase runbooks remain useful for pre-cutover recovery.

Build the useful vertical path first: authenticated durable intake → exact accepted command → one Kaggle writer apply/receipt → saved result. Establish direct persistent outbound WSS to the existing ASGI broker, then verify warm reconnect and snapshot/tail recovery. Continue with BGE-only search, fresh A/B imports and cases, real source proof, actual delivery and public case page. Transfer only inventoried historical/cartographic and human assets; reimport books.

For duplex, reuse BgeQueue/controller and one dispatcher. Application ACK follows durable commit; lost ACK reconciles the same result. Temporary socket failure preserves the warm model while its Kaggle session remains alive. Expired job claims do not kill the healthy worker. Provider termination requires a new run, restore and warmup; do not promise a surviving process after session end.

Focused verification must cover: proxy/network loss and reconnect; result commit before lost ACK; DevCoveer restart; competing old/new sockets; stale claim versus revoked run; heavy compute without blocked control/heartbeat; separate-process enqueue; and planned successor handover. No parallel HTTP/WSS sender or second broker. Notify after queue commit and reconcile with bounded read-only queries rather than retaining write-lock polling.

Report actual BGE query readiness separately from stored vector coverage. Lexical degradation does not pass BGE quality acceptance. Separate connection health, application readiness, research completion, published revision and confirmed text/image delivery.

Preserve protected thresholds, including warm backend p95 ≤1000 ms, public MCP p95 ≤1500 ms and evidence deadline ≤2000 ms. Cold 3–5 minutes is owner tolerance to verify, not a guarantee. Include cold/pending/errors in evidence; measure dispatch/transport/SQL/encoding and whole response separately. WSS does not remove CPU contention or finite Kaggle lifetime.

Verify backup restore and internal watermark before canonical selection; retain required tail across Dataset and Yandex recovery baselines. Unknown provider sends require original readback. Public page update failure preserves last-good; revocation does not. Keep exact accepted human/model data and independent artifact receipts.

New Telegram authorizations are supplied during implementation. Their absence blocks only dependent transfers, not domain/recovery work. Use existing focused tests and real readback for the changed boundary; no additional approval ladder, second mandatory judge or blanket retest.

## Preserved implementation and historical record


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
