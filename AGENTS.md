# Repository instructions

This repository contains only public source code, schemas, tests and documentation for Regional Knowledge Base.

- Never commit books, magazines, scans, extracted user text, image crops, credentials, access tokens, signed URLs or private metadata.
- **Hard architectural invariant — Kaggle-only BGE.** BGE-M3 dense **inference (queries and documents)** must execute ONLY on the existing Kaggle worker through the durable, actor-scoped queue. On DevCoveer **never install or start** a BGE inference model, ONNX weights, local BGE service, model-serving process or local fallback; do not resurrect a historic unit/script even if an older runbook mentions it. Any reintroduction violates the owner-approved operational boundary; CI enforces the absence of the retired local service and model loader. The correct incident response is to repair the Kaggle queue/controller, not run BGE on the host. See [BGE/Kaggle hard boundary](docs/operations/bge-kaggle-only.md).
- **BGE-only target, owner decision 2026-10-19:** remove E5 from the target runtime, dependencies, vector indexes, fallback, tokenizer requirements and acceptance gates during the scoped implementation. Earlier acceptance of the local E5 sidecar is historical, not a reason to restore it. Preserve the independent SHA-pinned BGE tokenizer and independently required shared Python consumers when retiring E5. Do not run E5 backfills. This documentation update itself changes no runtime.

- **Canonical implementation:** [Kaggle architecture](docs/designs/rkb-scalable-kaggle-architecture-20261019.md) and the single [implementation/debugging handoff](docs/prompts/rkb-scalable-kaggle-implementation-20261019.md), with [acceptance](docs/testing/rkb-scalable-kaggle-acceptance-20261019.md). Target: PostgreSQL+pgvector+BGE in Kaggle; DevCoveer retains stable MCP, verified identities, durable request/typed-command/result/delivery state and worker authority. One writer applies accepted changes; readers use verified snapshots. Before cutover, SQLite corpus/Supabase book vectors remain the existing operational baseline, documented in docs/storage.md and docs/operations/sqlite-v2.md. The 2026-10-05 SQLite/Supabase migration prompt and older page-archive/capacity designs are historical, not instructions for the new implementation. A documentation commit does not establish deployment or migration.
- All private reads and all writes are authorized server-side. Model/tool annotations are hints, never authorization.
- Keep the MCP surface goal-oriented and small. The Live profile exposes only `knowledge_search`; the full profile retains `search`/`fetch`. Ingestion is not part of the Live capability bundle.
- Preserve provenance: document -> page -> region -> relation -> illustration. Chunks are derived and replaceable.
- Original uploads are private by default. Never publish an uploaded source merely because its publication date looks old.
- Target text search covers all active accepted books without book-owner filtering and returns structured bibliography. This does not grant original files, scan/localization previews, full-page pixels, private drafts or automatic publication of stored derivatives. Restricted outputs retain current rights/scope checks after cache hits and delayed delivery. Uncertain pixel/original access stays private. The public case page is a selected publication projection without viewer login.
- Supabase is a data plane, never the MCP OAuth authority. Cross-service identity uses a stable application/platform user UUID. Never forward an end-user bearer token to another MCP; use resource-specific grants or a dedicated service identity.
- New interactive Live work must reuse onedayonemasterpiece/live-interaction.
- Every behavior change needs focused tests and matching documentation.
- The protected product contract is `.devcoveer/requirements.json`. Mass-ingestion readiness is a critical product requirement, not an optimization: preserve the capacity, dense-retrieval-quality, latency, per-book acceptance and change-gate thresholds there. The human-readable companion is `docs/mass-ingestion-readiness.md`. Do not claim bulk-ingestion readiness from successful indexing or vector counts alone.

- Do not reintroduce implicit MCP-auth defaults from `KB_SUPABASE_URL`, Supabase JWKS, Supabase OAuth Server or `auth.uid()`. Production RLS must use the application actor bridge.

- **Transfer layers, reimport books:** follow the [layers-only cutover](docs/migrations/rkb-layers-only-cutover-20261019.md). Preserve historical/cartographic identities, geometry/CRS, temporal data, aliases, provenance and inventoried human work. Do not migrate old searchable book text/chunks/vectors. Fresh PDF/DjVu activation atomically enqueues resumable research for disagreement registry/cases. Retain minimal legacy source references; never fabricate new evidence IDs or erase mixed/manual assets as “old books”.
- **Persistent direct duplex:** each notebook initiates one authenticated outbound WSS connection to the existing DevCoveer broker/ASGI service. Reuse BgeQueue, controller and request ownership; no public notebook listener, second broker or competing HTTP dispatcher. Bound in-flight work/buffers, reconcile existing job/claim/result hashes on reconnect and ACK only after durable application commit. A stale claim does not terminate an otherwise healthy warm worker. Reconnect preserves the loaded model only while that Kaggle session remains alive; session termination requires a new authorized run, restore and warmup.
- Preserve exact accepted model/human payloads, chosen IDs, logical timestamps and revision preconditions for replay. Do not regenerate accepted extraction with a model. Fence stale execution/writer generations at external command/receipt, snapshot and delivery boundaries. Already accepted commands from older epochs remain replayable. Snapshot upload is not verified restore; ready vector counts are not live dense-serving readiness. Preserve existing queues, renderer and outbox instead of adding parallel frameworks.

## Same-host service integration (mandatory)

Follow the [DevCoveer local interservice policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md)
and [devserver-interservice skill](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-interservice/SKILL.md) for every RKB ↔ Street Story/VibePublish link.
On the same Linux host use loopback-only HTTP or an existing Unix socket with a
dedicated, target-scoped technical identity; never introduce user/browser OAuth
or a public HTTPS path just to connect internal services. Do not reuse a full
owner credential or bypass source ACL. Use stable operation keys, durable
pending, native provider readback and SHA-256 before archival success.
The deployed RKB→VibePublish machine identity replaces the old human OAuth
archive path. Do not claim `source_archive_status=verified` without actual native
provider receipt, correct forum topic and SHA readback.

## Illustration pipeline (mandatory)

Before changing source figures, search/fetch linkage, image indexing,
private Telegram mirrors or archive fallback, read
[illustration-pipeline.md](docs/operations/illustration-pipeline.md).
There is **one** accepted source→figure→evidence→crop→private mirror chain,
with exact page/region provenance and actor-scoped retrieval.
Do not add a second index, scheduler or authentication proxy, confuse
printed captions with model observations, treat private unknown-rights
figures as public, or infer that a nearby text passage depicts a picture.
Tests and live acceptance must separately prove real MCP ImageContent,
Telegram topic-4 provider readback, and no-local-crop archive fallback.
