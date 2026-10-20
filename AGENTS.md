# Repository instructions

This repository contains public code, schemas, tests and documentation for Regional Knowledge Base. Never commit books, scans, extracted private corpus, private draft text, credentials, tokens, signed URLs or private runtime metadata.

## Current implementation contract — 2026-10-20

Use the single [implementation/debugging handoff](docs/prompts/rkb-scalable-kaggle-implementation-20261019.md), [architecture](docs/architecture.md) and [acceptance](docs/testing/rkb-scalable-kaggle-acceptance-20261019.md). They describe an approved target, not a deployed PostgreSQL/WSS system. Record actual source/runtime heads before implementation. Reports and testing-journal are dated evidence; retired prompts are not alternative deployment instructions.

- **Kaggle-only BGE is a hard boundary.** Query/document BGE inference runs only on the existing Kaggle worker through the actor-scoped queue. Never install/start BGE weights, ONNX inference, a local model service or local fallback on DevCoveer. Repair the existing queue/controller instead. Keep the independent pinned BGE tokenizer, 1024 FP32 baseline and exact complete input limit 512 tokens unless the protected change gate is passed.
- Target corpus/search/graph/stories/cases storage is PostgreSQL/pgvector in Kaggle. E5 and Supabase retrieval/storage paths are retired in the target; preserve independently used Python consumers while removing E5. Do not run E5 backfills or resurrect old service instructions.
- DevCoveer retains stable MCP/auth, verified persons, request/dialog authority, fair admission, BgeQueue/controller, writer fences and a short control spool. One authenticated outbound notebook WSS reuses that authority; no public notebook listener, second dispatcher, new broker or separate queue framework.
- **Final accepted/saved requires remote durability.** Local receive is pending. Persist exact accepted inputs/model results/resolved commands, IDs, logical times, preconditions and provider intents/receipts; replay never reruns an accepted model choice. Raw spool defaults to 4 h / 512 MiB; active/UNKNOWN and compact receipts have separate reference-aware retention. Daily backup does not justify losing several hours of confirmed work.
- Verify snapshot bytes, versions, internal cursor, restore and SQL/dense readiness before pointer promotion. Fence stale executors at external effects/delivery/pointer CAS; already accepted old-generation commands remain replayable. Read-only search does not wait for a cloud journal write.
- **Only GEO content migrates.** Transfer historical/cartographic layers, geometry/CRS/GCP/temporal/toponym identities and minimal GEO provenance. Do not transfer old books/chunks/vectors/stories/drafts/contributions/cases/non-GEO facts. Reimport books through MCP. Infrastructure identity/grants/config/secrets are not corpus migration and must not be reset. Preserve all product capabilities for new data. See [cutover](docs/migrations/rkb-layers-only-cutover-20261019.md).
- Source facts, dates/calendars, observations, people/organizations, geography and disagreements exist without mandatory Story. Fresh import/accepted-source extraction use one bounded research workflow and atomic activation/frontier.
- A user-facing story contains a saved **existing Variant.body** plus exact default reference, normally 2–6 connected sentences; title/summary/interest assessment never substitute for it. Sufficient episodes must save and read back body during import. Facts-only/needs-context remains explicit. No invented motives, causality, dialogue or forced drama; private drafts stay private; publication approval is separate from reading.
- Use server-side indexes/bounded SQL for source/story/catalog/entity/GEO retrieval. No full-corpus Python scope enumeration or cosine sort. Maintain projections incrementally, autovacuum/targeted ANALYZE; no scheduled whole-corpus reembedding/REINDEX without measured reason.

## Authorization and channels

All private reads and all writes are server-authorized. Model annotations and supplied actor IDs do not grant rights. Resolve provider+tenant+subject to one canonical person. Expert weight is not a capability. Import/reprocess and participant administration remain owner personal-route actions; delegated bots cannot impersonate owner.

Accepted active book text and bibliography are searchable without per-book owner filtering. **Illustrations are public to everyone. Stripe quote/page scan proof requires owner or verified demo/expert/local_historian with current scan_proof_read and matching scope/audience.** Full PDF/DjVu and private drafts remain independently restricted. Artifact class comes from trusted source geometry/purpose, not a caller label. Recheck withdrawal/current grants on cache hits and delayed outputs; shared channels do not union personal rights. Do not invent legal public-domain verification from this product access policy.

Keep the MCP surface small. Live exposes only knowledge_search; full profile retains search/fetch and goal-oriented operation groups. Mira in Projects Hub uses the existing live-interaction runtime and typed RKB adapter, with progressive capabilities and durable request/result/ACK. No second voice app, IAM or user scheduler. Supported clients need real non-owner verification.

Supabase is not an OAuth authority. Use application resource-bound tokens/verified identities and transaction-scoped actor context; never derive auth defaults from KB_SUPABASE_URL, Supabase JWKS/auth.uid(), or forward a user's bearer to another MCP.

## Same-host integrations

Follow the [DevCoveer interservice policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md) and [integration skill](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-interservice/SKILL.md). Same-host RKB↔Street Story/VibePublish calls use loopback/Unix socket and a dedicated target-scoped technical identity. No owner impersonation, browser OAuth, public detour or new proxy. Kaggle is another host and uses authenticated TLS/scoped tasks, never DevCoveer's loopback address as if local.

Street Story owns physical POI identity; Regional Cartography owns map interpretation/geometry; RKB owns source evidence/historical entities/independent disagreement cases. Do not copy their complete databases or create competing authority. VibePublish remains the single uploader/provider integration. Use stable operation keys, exact payloads, durable UNKNOWN and native provider hash/receipt readback before success/GC.

## Illustration pipeline

Before modifying figures, image linkage, proof rendering or archives read [illustration-pipeline.md](docs/operations/illustration-pipeline.md). Preserve one source→figure→evidence→crop→mirror chain, exact geometry/caption relations, labelled model observations, distinct source/display/provider/delivery hashes, real MCP ImageContent, correct MIME and per-item partial results. A nearby paragraph is not proof of an image association. Purpose-scoped renderer access allows authorized proof without granting the user a whole original. Pin localization inputs until terminal callbacks; stale render hashes require relocalization.

## Protected gates

`.devcoveer/requirements.json` is the protected product contract. Preserve its nine stable IDs and quantitative mass-ingestion, capacity, dense-quality, latency and source-fidelity gates; [readiness](docs/mass-ingestion-readiness.md) is the companion. Any intentional change requires typed requirements_update with explicit owner intent. Green unit tests or vector counts are not product acceptance. Behavior changes need focused meaningful tests and matching docs; docs-only normalization does not imply deployment, import, ACL mutation or live performance PASS.
