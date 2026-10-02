# Operations and acceptance gates

## Current infrastructure state

As of 2026-10-02 no dedicated Regional Knowledge Supabase account/project is
connected. Supabase configuration will be supplied later through the shared
environment/secrets layer as a separate owner-controlled setup step.

Therefore SQL migrations, OAuth and RLS are **not deployed yet**; current claims
refer only to source code, deterministic tests and fake managed-service E2E.

## Before production

The service is not production-ready merely because unit tests pass. Required gates:

1. create the Supabase project and enable OAuth 2.1 Server with asymmetric signing keys;
2. configure the platform client→resource audience mapping and prove that a token for another MCP is rejected;
3. apply SQL migrations and run RLS acceptance with at least two users, one workspace and anonymous public reads;
4. create a private S3-compatible bucket with public ACL disabled;
5. configure a 768-dimension external embedding endpoint; verify lexical-only degradation when it is unavailable;
6. deterministic start/pages/stage/validate/finalize is implemented locally; exercise the whole workflow against real configured Supabase/Object Storage with a representative born-digital PDF and a scan-only/multicolumn source;
7. prove raw private source PDFs remain inaccessible when normalized content is public;
8. connect the MCP from ChatGPT and test `search`, `fetch`, file-parameter ingestion and model-visible page images;
9. connect the read-only Live profile through `live-interaction` and measure p50/p95 tool latency;
10. verify a second MCP uses the same Supabase `sub` with a different audience/resource;
11. verify one first-party delegated integration (prefer Projects Hub -> Knowledge) can refresh its own Knowledge grant and that revoking it does not affect the user's other MCP grants;
12. verify Knowledge -> Street Story POI delivery with idempotent outbox semantics;
13. verify private-book POI evidence remains private in Street Story and expert review;
14. verify ambiguous book POI identity creates an unresolved link instead of a silent merge;
15. verify unknown author authority remains null and author scoring is domain-specific;
16. verify unresolved contradiction blocks automatic canonicalization regardless of verification score.

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

## Implemented ingestion checkpoint — 2026-10-02

The ingestion path now has deterministic local coverage for:
- ChatGPT file-param shape, bounded DNS-pinned HTTPS source download and exact
  source hashing;
- private source persistence and opaque source-object identity;
- lost/failed/concurrent start reconciliation without duplicate jobs;
- real PyMuPDF inspection, native text blocks and JPEG page rendering;
- model-friendly page/region/relation/illustration staging with deterministic
  server IDs and derived semantic chunk text;
- immutable staged graph snapshots in Object Storage;
- graph coverage/review validation;
- exact illustration crop generation from the source PDF;
- text projection + embeddings/FTS materialization;
- DB-side revalidation before `active_revision` changes;
- active materialized revisions protected from ordinary user-token mutation.

The local fake-E2E runs `stage -> validate -> finalize` with a real PyMuPDF
source/crop while mocking only managed external services. Real provider
acceptance remains a production gate.

CI installs the `ingest` extra so PDF rendering/cropping is exercised rather
than skipped.

## Implemented POI producer/outbox checkpoint — 2026-10-02

Local contracts and tests now cover:
- staged POI candidates with exact page/region provenance;
- contextual versioned author authority where curated evidence exists;
- `null` author score when identity/authority is unknown;
- conservative unknown source-family handling;
- `poi.fact_evidence.v1` construction;
- DB-side event scope/provenance checks;
- durable idempotent outbox written transactionally with revision activation;
- private/workspace `pending_authorization` versus public `pending_delivery`;
- POI ↔ illustration staging and `poi.media_evidence.v1` outbox events, keeping
  image relation/provenance/rights independent from factual claims.

Not yet claimed:
- real Street Story network intake;
- outbox delivery/retry worker;
- user OAuth delegation resolution for private evidence;
- POI identity resolution and contradiction creation E2E;
- Projects Hub expert assignment E2E.
