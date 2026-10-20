# Documentation map

Updated2026-10-20. Active contracts below describe one target; historical reports record what happened on their stated versions. A documentation merge is not deployment or new live acceptance.

## Start here

1. **[Single implementation/debugging handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md)** — the complete file to give a coding agent.
2. [Architecture](architecture.md), [storage/recovery](storage.md), [configuration](configuration.md), [operations](operations.md).
3. [Protected readiness](mass-ingestion-readiness.md), [target acceptance](testing/rkb-scalable-kaggle-acceptance-20261019.md), [testing method](testing.md).
4. [Warm Kaggle debugging/lifecycle audit with Opus 5.5 and Dot](reports/rkb-kaggle-debug-lifecycle-audit-20261020.md); [previous Go/Rust and voice-path audit](reports/rkb-native-runtime-voice-critical-audit-20261020.md); [story/storage/speed audit](reports/rkb-opus55-story-storage-speed-20261020.md).

## Active domain contracts

| Domain | Documents |
|---|---|
| Import/source fidelity | [ingestion](ingestion.md), [semantic passages](operations/source-backed-semantic-passages.md), [repair](operations/accepted-import-repair.md), [catalog corrections](operations/import-finalization-corrections.md), [article scope](operations/article-search-scope.md) |
| Story and entities | [stories and stored narrative](story-registry.md), [organization graph](organization-graph.md), [participants/fact bridge](design/rkb-participants-poi-fact-bridge-20261018.md) |
| GEO | [location registry](location-registry.md), [geo queue](geo-resolution-queue.md), [POI bridge](poi-integration.md), [model-selected POI](poi-model-link.md), [GEO-only cutover](migrations/rkb-layers-only-cutover-20261019.md) |
| Disagreements/research | [case contract](design/rkb-disagreement-journal-design-20261018.md), [collaboration](design/rkb-kaliningrad-base-collaboration-20261018.md), [case page](requirements/disagreement-case-static-pages-20261019.md), [case PMI](testing/rkb-disagreement-pmi-20261018.md) |
| Access/clients | [MCP](mcp.md), [auth](auth.md), [platform identity](platform-identity.md), [rights](rights.md), [integrations](integrations.md) |
| Queue/latency | [load management](user-request-load-management.md), [load design](designs/user-request-load-management-20261019.md), [CPU scheduling](operations/retrieval-cpu-scheduling.md) |
| BGE/indexes | [Kaggle contract](bge-kaggle.md), [runbook](operations/bge-kaggle.md), [hard boundary](operations/bge-kaggle-only.md), [automatic indexing](operations/automatic-indexing.md) |
| Media | [illustration pipeline](operations/illustration-pipeline.md), [source archive](operations/telegram-archive.md), [archive continuation](operations/archive-read-continuation.md) |

## History and superseded entry points

[Prompt index](prompts/README.md) has one current implementation handoff. Older E5/SQLite/Supabase/capacity/seed-only prompts and old beta handoffs are short retired entry points with immutable Git history links; their old executable instructions are not left underneath a new override banner.

`reports/` and [testing-journal](testing-journal.md) retain dated evidence and exact source heads, including prior failures. They do not impose old storage, migration or access policies on this target, and their PASS is not transferred to PostgreSQL/WSS/new stories. Reports containing quotations or previous consultant opinions retain the original content with its date. New audit decisions explicitly identify accepted and rejected suggestions.
