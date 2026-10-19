# Organizations in the regional evidence graph

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

Checkpoint: 2026-10-09; incremental graph-domain implementation, **not** calendar,
numeric observation or full geographic resolution acceptance.

## Ownership and model semantics

The existing RKB SQLite authority stores all accepted graph entity candidates,
mentions, aliases, typed relations and source/page/region evidence alongside
the accepted document revision. The model submits a bounded `GraphBundle`
in the same existing `book_ingest(stage)` flow or via `graph_stage`
against an owned active document. There is **no server-side historical inference
or LLM**. `GraphService` verifies the existing authorized source graph, including
exact quote membership and source/revision. Canonical POI identity remains with
Street Story: an `organization` is not the building in which it worked.

Supported `GraphEntity.kind`:
- `person`, `organization`, `event`, `historical_thread`, `poi_ref`.

Organizations have stable **explicit** ID/key, current/historical/former
name aliases, language, time scope and exact source evidence. Two institutions
with the same written name are **two candidates**, not an implicit merge.
A name change may be represented as an alias only after source review.
An administrative predecessor is a **directed** and sourced relation,
not the same institution unless separately established. Never convert one
organization's move into a building identity change.

## Controlled directed relations

| Relation | Source kind | Target kind | Meaning |
| --- | --- | --- | --- |
| `affiliated_with` | person | organization | Sourced affiliation; not proof of employment without stronger semantics |
| `participated_in` | person / organization | event | Sourced participation, not causality |
| `member_of` | person / organization / event / poi_ref | historical_thread | Editorial thread grouping |
| `founded_by` | organization | person | A source's claim of founding |
| `operated_at` | organization | poi_ref | Reported location of activity, not building ownership |
| `predecessor_of` | organization | organization | Directed historical institutional succession, not entity merge |
| `occurred_at` | event | poi_ref | Sourced event location, not proximity |

Every edge carries 1–8 **exact source refs**, an optional `time_scope`,
`state` and `review_note`. Same-entity edges and reversed types are
rejected before persistence; SQLite's writer repeats the endpoint validation
rather than trusting model/tool-side annotations. Existing relations are not
relabelled or remapped. The optional legacy PostgreSQL schema migration is
`sql/024_organization_graph.sql`: a compatible check/trigger extension,
**not** a proposal to move the current RKB graph back into Supabase.

Queries: use existing `graph_fetch(entity_id, limit<=20)` for an authorized
organization and its one-hop neighbors/aliases/mentions with source refs;
`graph_related` uses the existing retrieval for candidate evidence.
These calls **do not** promise full graph traversal or cross-source global
entity normalization. `entity_list(kinds=["organization"])` lists accepted
source mentions, not all institutions in the region.

## End-to-end acceptance and unfinished scope

Synthetic in-process end-to-end acceptance covers two distinct institutions,
a person, event and unresolved canonical Street Story place in one accepted
source; exact source evidence; role/direction; alias period; idempotent stage;
unauthorized reader rejection. Existing mass-book retrieval quality/capacity
requirements are unaffected. Production deployment, actual current client
`tools/list`, fresh user-source extraction coverage and latency require
separate runtime verification.

Remaining stage D: typed `organization_role`/affiliation intervals where
role granularity is needed; source-grounded event date assertions with
calendar/precision/uncertainty and exact day queries; numeric observations with
units/statistical boundaries; retraction/history of graph edges; ACL-scoped
depth/cursor traversal. Stage E geospatial owner lives in Street Story, not
this RKB entity table.
