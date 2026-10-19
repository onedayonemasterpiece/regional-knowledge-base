# Geography enrichment: optional downstream queue

## Применение в переработке 19.10.2026

Нормативный целевой контракт: [единый файл агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md) и [архитектура](designs/rkb-scalable-kaggle-architecture-20261019.md). Текущий runtime ещё не перенесён этой документационной задачей.

- Один PostgreSQL/pgvector corpus в Kaggle, BGE-only (1024 FP32 baseline, exact pinned BGE input≤512); небольшой durable control journal на DevCoveer.
- Прямой исходящий WSS notebook→existing DevCoveer ASGI с двусторонними заданиями/результатами, durable ACK/RESUME и одной BgeQueue authority. Старые polling/локальный E5/Supabase инструкции не целевой deployment.
- Fresh MCP import книг; перенос только historical/cartographic layers и согласованных зависимостей, со stable IDs/provenance и проверенным reanchor. Ручная работа не уничтожается из-за отказа от старого corpus.
- Source-bound facts/dates/observations сохраняются без обязательной Story; эпизод/досье/Variant/private draft/independent case различаются. Один persisted research workflow, bounded compose, без квот и выдуманной драматургии.
- Поиск accepted active book text и bibliography без per-book owner ACL; current withdrawal/revision, pixels/original/private draft grants и delivery audience остаются обязательными.
- One canonical person/request/dialog, fair admission и confirmed result/hash ACK; Мира в PH расширяет existing typed adapter, не создаёт отдельный voice/IAM/queue.

Численные protected gates сохранены: backend p95≤1000ms, MCP p95≤1500ms/hard2000ms, BGE Hit@5≥85%/@10≥90%, RU→DE@10≥85%, ≥30 held-out families, ≥100 sources/100000 chunks, storage≤80% steady/≤90% peak. Документ не является доказательством PASS.

Основные entity, temporal, identity, provenance и ownership контракты ниже сохраняют силу. Их SQL/storage привязки переносятся на PG, а старый per-book search ACL трактуется согласно новому правилу выше. Карта, пользовательский профиль и физический POI не становятся новой канонической копией в RKB.

## Сохраняемый предметный контракт

## Current rule (owner decision, 2026-10-10)

The ordinary workflow is [location-first model ingestion](location-registry.md). The model reads the source, searches existing locations, chooses an ID or creates a location, and records evidence and geographical relationships. No map, coordinates, Street Story identity or geo-queue completion is required. Existing `poi_ref` graph nodes serve as source-side locations; `unresolved` means an external POI reference is absent, not that the source location cannot be used.

Use `book_ingest(stage).entity_candidates` during import and `graph_stage` for accepted material. Both preserve source mentions. `entity_list` searches labels, aliases and literal mentions; `graph_fetch` gives context and neighbors. Model-authored `located_in` connects a building, square, street, island, city or other place. Web clarification is recorded separately in `research_sources`.

## Why the queue still exists

The already deployed queue is a bounded adapter for later owner-catalog and cartography changes. Existing jobs, identities and receipts are retained; no new approval workflow is required for imports. Its durable states make interrupted downstream work resumable, but they are not semantic acceptance rules.

The worker may read the owner catalog and store `candidate_discovery` with candidate references. It leaves the attempt in `awaiting_agent`, with `identity_selected=false`, and never calls semantic stage/apply automatically. A single identical name, a matching external-ID hint, or proximity is not a model identity decision. Lookup failure remains a bounded dependency condition, not an assertion that no location exists.

The importing model can directly enrich a known entity through `graph_stage` using its `entity_id` and an explicitly selected `canonical_poi_ref` or `map_refs`. The canonical owner reference must exist; current coordinates are not retroactive proof of a historical footprint. Source-side location identity and its existing mentions remain unchanged.

## Existing optional operations

`geo_status`/`geo_get` inspect work. `geo_claim`, `geo_stage`, `geo_apply` support explicit external processing; they are not steps users must request when adding a book. Stage/apply preserve actor authorization, evidence scope, lease fencing and replay integrity. An explicit model-selected existing owner ID is not vetoed because its current name differs from an old spelling.

`geo_recheck` can reopen relevant work after a genuinely changed dependency. Backfill can populate queue entries from existing source-side mentions without reimporting books. Candidate discovery, source import and vector indexing remain independently bounded. Do not run repeated identical checks or promote optional verification into an import gate.

## Boundaries

RKB owns evidence, stories, mentions and its source-side place graph. Street Story owns accepted physical POI identity. Regional Cartography owns source-specific map observations and geometric interpretation. No component creates replacement canonical IDs on another component's behalf. No extra Supabase account, graph database, copied modern map or embeddings for every cartographic primitive are required.

Existing search latency and source-fidelity requirements remain unchanged. This queue is not on the Live query path. Updating a location does not change book text or trigger full vector reindexing.
