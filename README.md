# Regional Knowledge Base

Source-grounded regional knowledge: books and articles, actual tellable stories, people/organizations, dates and measurements, historical geography, illustrations and independent disagreement cases. MCP, Slack and Mira in Projects Hub share one verified identity/admission authority, with explicit fast-read and durable-work lifecycles.

## Current project

**[One complete Markdown handoff for implementation and debugging](docs/prompts/rkb-scalable-kaggle-implementation-20261019.md)** — version 3, updated 20 October 2026. It contains the target, preserved functions, import/story prompts, search indexes, storage/recovery, roles, channels, Go/Rust feasibility and choice, voice latency, rollout and acceptance. Supporting documents are normalized to the same design. This commit is documentation, not a claim that the new runtime is deployed.

Target: PostgreSQL/pgvector and BGE-M3 on Kaggle; stable MCP/auth/request controller and short control spool on DevCoveer; direct outbound notebook WSS with bidirectional work/results. Dataset holds verified recovery versions, not an always-running database. Existing Yandex recovery tail protects final accepted/saved writes; local pending alone is not a durability promise.

Owner decisions:

- Migrate **only GEO** and its minimal provenance. Reimport all books through MCP; recreate other content. Keep functions and infrastructure identities/configuration.
- Import useful facts even when no story exists. For a sufficient episode save an actual **Variant.body**, usually2–6 sentences, with exact source dependencies. Show that body by default; summary/potential is not a story.
- Search all active accepted text/bibliography. Illustrations are public; stripe quote/page scans require owner/demo/expert/local_historian proof capability. Originals and private drafts have separate access.
- Keep one research workflow, existing queues/renderers/adapters, source fidelity and numeric readiness gates. Remove E5/Supabase from the target, never add local BGE on DevCoveer.
- Use bounded indexed SQL and incremental maintenance; perform encode, search, fusion and hydration inside Kaggle. Return ready evidence without a query-vector detour. Measure warm latency, ANN quality, storage peak and restore under representative load.
- Keep **Dot and Mira as two independent intelligent clients**. Ephemeral reads use bounded asynchronous telemetry; accepted questions/mutations retain strong remote durability. Preserve utterance recovery and current access checks.
- Start with thin Python over native PG/PyTorch kernels. Go/Rust binaries may replace a component for demonstrated speed/resource or reliability benefit; they do not automatically make the system faster or the notebook permanent. No additional proxy or full rewrite is prescribed.

## Documentation

[Documentation map](docs/README.md) · [Architecture](docs/architecture.md) · [Storage/recovery](docs/storage.md) · [Import](docs/ingestion.md) · [Stories](docs/story-registry.md) · [Rights](docs/rights.md) · [Operations](docs/operations.md) · [Acceptance](docs/testing/rkb-scalable-kaggle-acceptance-20261019.md).

The code baseline at design time still uses SQLite/Supabase and an incomplete story materialization path. The coding agent must implement and verify the target, not treat this README as release evidence. Historical measurements remain in docs/reports and docs/testing-journal.md. Public repository content excludes books, scans, private corpus and credentials.
