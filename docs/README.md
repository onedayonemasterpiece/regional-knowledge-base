# Documentation map

Updated 2026-10-19. The target is approved for implementation; documentation alone does not establish deployment, migration or production acceptance.

## Start here

| Document | Purpose |
|---|---|
| [Single implementation/debugging handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) | Executable scope, phases, existing-code reuse, open dependencies and required outcome |
| [Canonical Kaggle architecture](designs/rkb-scalable-kaggle-architecture-20261019.md) | PostgreSQL+BGE, direct persistent WSS, durable exact changes, writer/readers, cases and delivery |
| [Acceptance plan](testing/rkb-scalable-kaggle-acceptance-20261019.md) | Recovery, warm reconnect, fresh sources, proof, cases, delivery and joint load |
| [Layers-only cutover](migrations/rkb-layers-only-cutover-20261019.md) | Preserve historical/cartographic and human work; exclude old searchable book corpus |
| [Critical audit](reports/rkb-comprehensive-critical-audit-20261019.md) | Evidence, independent audit findings and material tradeoffs |
| [Story quality audit](reports/rkb-story-quality-critical-audit-20261019.md) | Source-grounded diagnosis, Opus 5.5 competition and new extraction/compose criteria |
| [Protected requirements](../.devcoveer/requirements.json) | Capacity, quality, source-fidelity, latency and change gates |
| [Mass-ingestion readiness](mass-ingestion-readiness.md) | Meaning and scope of the protected readiness thresholds |

Direct owner instructions take precedence. Stable requirement IDs do not require a retired provider: CORE-SUPABASE-CAPACITY remains an identifier, not an order to retain Supabase.

## Supporting contracts

- [Architecture overview](architecture.md), [storage](storage.md), [configuration](configuration.md), [operations](operations.md).
- [Ingestion](ingestion.md), [MCP](mcp.md), [location registry](location-registry.md), [POI integration](poi-integration.md), [integrations](integrations.md).
- [Story registry](story-registry.md), [disagreement design](design/rkb-disagreement-journal-design-20261018.md), [case acceptance](testing/rkb-disagreement-pmi-20261018.md), [request/load design](designs/user-request-load-management-20261019.md).
- [BGE boundary](operations/bge-kaggle-only.md), [indexing](operations/automatic-indexing.md), [archive](operations/telegram-archive.md), [illustrations](operations/illustration-pipeline.md), [SQLite baseline recovery](operations/sqlite-v2.md).
- [Auth](auth.md), [rights](rights.md), [testing](testing.md), [recorded runs](testing-journal.md).

Preserve provenance, identity and source-format safeguards. Where old documents describe SQLite/Supabase/E5 placement or HTTP polling, the canonical design controls the replacement. Public accepted-text search does not grant scans, originals, private drafts or automatic publication of stored derivatives.

The 2026-10-05 SQLite/Supabase migration prompt, v1 page archive/capacity plans and preliminary Kaggle assessment are historical. Do not restart those migrations, restore E5 or rebuild authentication from their old instructions. Dated reports certify only their stated revision and scope.
