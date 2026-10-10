# Regional Knowledge Base

A source-backed knowledge and MCP service for regional books, articles and other attributed materials.

A user attaches a source and asks to add it. The MCP-calling model reads the material, preserves its text and illustrations, identifies people, organizations, stories and locations, and reuses existing identities. The backend handles files, storage, access control, source references, indexing and replay safety. It does not infer historical identity from matching names.

## Current storage boundary

SQLite owns the accepted corpus, source/page/region graph, lexical search, catalog, ACL, stories, entity mentions and source-side location registry. Supabase holds the vector search plane and minimal identity/scope anchors, not another full text corpus. Original sources remain in the configured private archive. GitHub holds public code and documentation only.

Retrieval uses the existing multilingual vector spaces with bounded lexical support. Entity enrichment does not require re-embedding unchanged chunks. See [storage](docs/storage.md) and [architecture](docs/architecture.md) for deployment-specific details.

## Source ingestion and locations

The model follows `start → pages → stage → validate → finalize`. During the same source review it supplies `entity_candidates` and `story_candidates`. It searches `entity_list`, inspects `graph_fetch`, then explicitly reuses an entity ID or creates a distinct source-backed entity. `graph_stage` enriches already accepted material without importing it again.

A location may be a city, island, street, square, building, bridge or less precisely identified area. It exists before coordinates or cartographic coverage. Historical/current names are sourced aliases, `located_in` connects places, and exact mentions connect them to passages. Later cartographic and Street Story references enrich the same location. See the current [location-registry policy](docs/location-registry.md).

The existing background geo adapter can discover candidates but does not select identity. The model decides. Maps, source interpretation and accepted external physical identities remain separate; missing maps do not block source import.

## Access and product boundaries

MCP OAuth is independent of Supabase and uses a stable application user identity. Source access and all writes are checked server-side. Private sources and derivatives are not made public by import. Live exposes only the bounded retrieval surface, not ingestion and graph writes.

This repository implements ingestion, retrieval and graph operations. Software tests, a deployed version, completeness of a particular book and broad mass-ingestion readiness are separate statements. The protected capacity, source-fidelity and latency requirements remain in `.devcoveer/requirements.json`.

## Documentation

- [Ingestion](docs/ingestion.md), [MCP](docs/mcp.md), [location registry](docs/location-registry.md)
- [Stories and integrations](docs/integrations.md), [Street Story evidence bridge](docs/poi-integration.md)
- [Storage](docs/storage.md), [rights](docs/rights.md), [OAuth](docs/auth.md)
- [Mass-ingestion readiness](docs/mass-ingestion-readiness.md)
