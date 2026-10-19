# Platform identity: one user, many MCP products

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

## Principle

A single person may use Regional Knowledge, Wonderful Lections, Street Story,
Projects Hub and other MCPs, but **Supabase is not the common identity provider**.

The shared concept is a stable platform user UUID plus resource-specific OAuth
grants.

```text
                 application/platform identity
                       stable user UUID
                             |
        +--------------------+--------------------+
        |                    |                    |
 Regional Knowledge   Wonderful Lections    Projects Hub
   OAuth resource       OAuth resource       OAuth resource
        |                    |                    |
   own ACL/RLS            own ACL             own ACL
```

## Beta vs platform convergence

Do not build a new identity platform before the Knowledge beta works.

For the first Knowledge beta:
- one owner account;
- application-owned OAuth 2.1;
- stable UUID subject;
- exact Knowledge resource binding.

Wonderful Lections already provides a proven OAuth behavior donor. It remains
operational and unchanged.

After the beta, extract/converge identity so multiple MCPs can use the same
platform user UUID and sign-in session. That later convergence must not require
changing RKB document owners or reindexing corpus data.

## Resource isolation

Every MCP remains a different OAuth resource.

A token for Wonderful Lections is not accepted by Knowledge and vice versa.
Cross-service access uses an explicit target-resource grant, never bearer-token
forwarding.

## User experience target

Eventually:
1. user signs in once to the common platform identity;
2. each MCP asks only for its own consent;
3. the same platform user UUID is used everywhere;
4. grants can be revoked independently.

This platform target is independent of the database vendor used by any product.
