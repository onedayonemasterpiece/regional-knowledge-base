# Regional Knowledge Base

Source-grounded regional knowledge: books and articles, actual tellable stories, people/organizations, dates and measurements, historical geography, illustrations and independent disagreement cases. MCP, Slack and Mira in Projects Hub share one verified person and request lifecycle.

## Current project

**[One complete Markdown handoff for implementation and debugging](docs/prompts/rkb-scalable-kaggle-implementation-20261019.md)** — updated20 October2026. It contains the target, preserved functions, import/story prompts, search indexes, storage/recovery, roles, channels, rollout and acceptance. Supporting documents are normalized to the same design. This commit is documentation, not a claim that the new runtime is deployed.

Target: PostgreSQL/pgvector and BGE-M3 on Kaggle; stable MCP/auth/request controller and short control spool on DevCoveer; direct outbound notebook WSS with bidirectional work/results. Dataset holds verified recovery versions, not an always-running database. Existing Yandex recovery tail protects final accepted/saved writes; local pending alone is not a durability promise.

Owner decisions:

- Migrate **only GEO** and its minimal provenance. Reimport all books through MCP; recreate other content. Keep functions and infrastructure identities/configuration.
- Import useful facts even when no story exists. For a sufficient episode save an actual **Variant.body**, usually2–6 sentences, with exact source dependencies. Show that body by default; summary/potential is not a story.
- Search all active accepted text/bibliography. Illustrations are public; stripe quote/page scans require owner/demo/expert/local_historian proof capability. Originals and private drafts have separate access.
- Keep one research workflow, existing queues/renderers/adapters, source fidelity and numeric readiness gates. Remove E5/Supabase from the target, never add local BGE on DevCoveer.
- Use bounded indexed SQL and incremental maintenance; measure warm latency, ANN quality, storage peak and restore under representative load.

## Documentation

[Documentation map](docs/README.md) · [Architecture](docs/architecture.md) · [Storage/recovery](docs/storage.md) · [Import](docs/ingestion.md) · [Stories](docs/story-registry.md) · [Rights](docs/rights.md) · [Operations](docs/operations.md) · [Acceptance](docs/testing/rkb-scalable-kaggle-acceptance-20261019.md).

The code baseline at design time still uses SQLite/Supabase and an incomplete story materialization path. The coding agent must implement and verify the target, not treat this README as release evidence. Historical measurements remain in docs/reports and docs/testing-journal.md. Public repository content excludes books, scans, private corpus and credentials.
