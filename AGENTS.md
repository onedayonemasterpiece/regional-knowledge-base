# Repository instructions

This repository contains only public source code, schemas, tests and documentation for Regional Knowledge Base.

- Never commit books, magazines, scans, extracted user text, image crops, credentials, access tokens, signed URLs or private metadata.
- **Hard architectural invariant — Kaggle-only BGE.** BGE-M3 dense **inference (queries and documents)** must execute ONLY on the existing Kaggle worker through the durable, actor-scoped queue. On DevCoveer **never install or start** a BGE inference model, ONNX weights, local BGE service, model-serving process or local fallback; do not resurrect a historic unit/script even if an older runbook mentions it. Any reintroduction violates the owner-approved operational boundary; CI enforces the absence of the retired local service and model loader. The correct incident response is to repair the Kaggle queue/controller, not run BGE on the host. See [BGE/Kaggle hard boundary](docs/operations/bge-kaggle-only.md).
- **E5 is different:** the pinned E5-small INT8 local sidecar is the previously accepted always-ready fast/search-and-token-count tier (decision 2026-10-04), never the default required BGE publication gate. Do not delete its shared Python runtime, pinned assets, or the separate BGE *tokenizer-only* file needed to validate imported text lengths. Never silently trigger optional E5 whole-corpus backfills when troubleshooting BGE.

- Follow docs/storage.md for the currently deployed storage boundary. The v2 release uses SQLite authority for corpus/details/FTS/catalog/ACL/graph/POI and local ingestion/publication state when RKB_SQLITE_CORPUS_PATH is enabled; Supabase keeps minimal anchors/scope and E5/BGE vectors only. Retirement and recovery are documented in docs/operations/sqlite-v2.md. The canonical **implementation task** is docs/prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md: move corpus text/details/FTS to SQLite, keep vector search in Supabase, and build source-page proof on demand. The older docs/design/catalog-evidence-page-archive-v1.md and docs/prompts/catalog-evidence-capacity-implementation-20261005.md are superseded historical design and must not be used for new implementation decisions.
- All private reads and all writes are authorized server-side. Model/tool annotations are hints, never authorization.
- Keep the MCP surface goal-oriented and small. The Live profile exposes only `knowledge_search`; the full profile retains `search`/`fetch`. Ingestion is not part of the Live capability bundle.
- Preserve provenance: document -> page -> region -> relation -> illustration. Chunks are derived and replaceable.
- Original uploads are private by default. Never publish an uploaded source merely because its publication date looks old.
- Public visibility requires a verified rights basis. Rights uncertainty fails closed to private.
- Supabase is a data plane, never the MCP OAuth authority. Cross-service identity uses a stable application/platform user UUID. Never forward an end-user bearer token to another MCP; use resource-specific grants or a dedicated service identity.
- New interactive Live work must reuse onedayonemasterpiece/live-interaction.
- Every behavior change needs focused tests and matching documentation.
- The protected product contract is `.devcoveer/requirements.json`. Mass-ingestion readiness is a critical product requirement, not an optimization: preserve the capacity, dense-retrieval-quality, latency, per-book acceptance and change-gate thresholds there. The human-readable companion is `docs/mass-ingestion-readiness.md`. Do not claim bulk-ingestion readiness from successful indexing or vector counts alone.

- Do not reintroduce implicit MCP-auth defaults from `KB_SUPABASE_URL`, Supabase JWKS, Supabase OAuth Server or `auth.uid()`. Production RLS must use the application actor bridge.

## Same-host service integration (mandatory)

Follow the [DevCoveer local interservice policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md)
and [devserver-interservice skill](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-interservice/SKILL.md) for every RKB ↔ Street Story/VibePublish link.
On the same Linux host use loopback-only HTTP or an existing Unix socket with a
dedicated, target-scoped technical identity; never introduce user/browser OAuth
or a public HTTPS path just to connect internal services. Do not reuse a full
owner credential or bypass source ACL. Use stable operation keys, durable
pending, native provider readback and SHA-256 before archival success.
The existing RKB→VibePublish OAuth archive is an unfinished migration: never
claim `source_archive_status=verified` or a finished product without live proof.
