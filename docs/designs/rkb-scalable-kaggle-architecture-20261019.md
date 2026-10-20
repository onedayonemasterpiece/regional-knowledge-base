# Целевая архитектура RKB: Kaggle PG 17/BGE и короткий DevCoveer control

Обновлено 20.10.2026; канонический путь документа сохранён. Статус: решение для реализации, не выполненный deploy. Проверенный source baseline — [main b1c0689](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4).

**Один самодостаточный [handoff кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md)** содержит функциональный контракт, порядок внедрения и отладки. Этот документ объясняет принятое размещение; подробные query/index paths — в [architecture](../architecture.md), durability/GC — в [storage](../storage.md).

## Решение владельца и область

Рабочая БД PostgreSQL **17** с закреплённым pgvector и BGE-M3 находится на Kaggle. E5 и Supabase в target отсутствуют. Существующий BGE baseline — 1024 FP32, pinned revision/tokenizer, final input≤512 tokens; coherent passages около 256 tokens. Скорость поиска приоритетна относительно экономии хранения при сохранении обязательных capacity/quality gates; факты и exact provenance не сокращаются ради quota. Даже около 0.5 s выигрыша полезны, но требуют измерения полного полезного пути.

Миграция переносит **только geo**: layers/places, geometry/CRS/оси/единицы, transform/GCP, временные aliases, provenance и необходимые минимальные source refs. Негеографические facts, старые книги/chunks/vectors/receipts, stories, cases и drafts не копируются в новую БД; они создаются fresh через проверенный import/workflow. Старое состояние сохраняется отдельно как reference, без второй live write authority. GEO identity manifest со stable layer/place IDs, aliases и CRS готов до production book imports; bulk raster transfer допустим в phase 4. Stage 2 без manifest использует только isolated fixture.

Accepted active book text/bibliography и illustrations доступны всем. Stripe quote scans доступны owner/demo/expert/local_historian. Original PDF/DjVu, full-page pixels и private drafts имеют отдельный private scope. Публичность выбранной иллюстрации не разрешает full-page preview.

## Минимальный состав

| Контур | Роль |
|---|---|
| Existing DevCoveer MCP/ASGI/BgeQueue/controller | Verified identity, единый person/request/dialog lifecycle, fairness/admission, writer/connection authority, existing delivery. Raw spool 4h/512 MiB и компактное state до reconciliation. |
| Один Kaggle writer | PG 17 domain store, BGE, source processing, ordered apply/receipts, serving и snapshots. |
| Readers только по необходимости | Exact verified snapshot, BGE/search/proof; никаких независимых writes, scheduler или sender. |
| Private Kaggle Dataset | Consistent PG physical base с indexes и verified manifest/version/cursor. |
| Existing Yandex recovery storage | Immutable command/control/receipt tail, accepted payloads и одна latest verified daily base независимая от Kaggle. |
| Existing integrations | VibePublish/Telegram archive/delivery, Regional Cartography, Street Story, Projects Hub/Мира; их ownership сохраняется. |

Существуют два независимых интеллектуальных маршрута Dot → RKB и Mira → RKB. Ни один агент не удаляется; Mira не проходит через Dot. DevCoveer — тонкий auth/admission relay existing ASGI. Notebook открывает authenticated outbound WSS, public listener и открытый PostgreSQL не нужны. Ephemeral QUERY/QUERY_RESULT и durable DISPATCH/RESULT/ACK — разные классы одного transport и одной admission authority; HTTP polling и WSS не dispatch одновременно.

## Инварианты

1. Local receipt pending; final accepted/Saved только после exact intent/payload remote confirmation. Saved дополнительно означает applied PG result и remotely verified exact outcome/receipt. Готовые mutations и явно принятые durable requests ждут group flush ≤1 s. Обычный read-turn использует ephemeral QUERY без durable row/RESUME; bounded async audit не задерживает request/response и не обещает durable receipt.
2. Domain effects, applied_ops и непрерывный applied cursor фиксируются одной PG transaction. IDs, logical timestamps и принятые значения не генерируются заново при replay.
3. New submissions проверяют current writer/run/claim/connection generation. Уже принятые команды прежнего epoch остаются обязательными для replay.
4. Canonical pointer, external effect и delivery проверяются внешней authority. Старый writer не выбирает late snapshot и не публикует поздний результат сам.
5. Manifest содержит exact Dataset version/hash, PG minor/extensions/libs/platform/collation provider+version, schema/codec, internal cursor и required WAL. Keyed normalized text использует C collation; incompatible affected indexes исправляются до READY. Upload success не равен restore verification.
6. Dev raw spool: default 4h/hard 512 MiB, проверяемые до cutover; compact UNKNOWN/request/receipt metadata живут до reconciliation. Local GC после remotely sealed payload + verified metadata не ждёт daily base.
7. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers. Yandex base и tail в одном Yandex не независимы друг от друга.
8. Control checkpoint 15 min; changed PG base 4 h либо раньше при replay/byte pressure/planned handover; daily independent-from-Kaggle Yandex base. Без изменений full base не создаётся.
9. Read-after-write использует min_data_seq; обычный reader сообщает data_as_of. Current withdraw/revoke и output scope проверяются до выдачи и delayed delivery.
10. 30 min useful-idle не равен writer lease/Ping/task timeout. Reconnect сохраняет модель лишь в живой Kaggle session; после session end нужны новый run/restore/warmup.
11. Actual SQL+dense query probes подтверждают serving readiness; counts/Pong/process health недостаточны.
12. Один request lifecycle, verified-person fairness, dialog order и result/hash/native delivery ACK действуют в personal MCP, Slack и Мире.

## Почему схема должна быть именно индексируемой

Исходный backend перечисляет все документы перед поиском и каталогом; story semantic path считает similarity в Python. Перенос этих алгоритмов в PG без изменения query shape не решает масштабирование.

Target: активные passage_search и story_search, GIN tsvector + HNSW BGE, B-tree scope/catalog/links, aliases с языком/временем, PostGIS GiST под spatial predicates. Один fused QUERY целиком в Kaggle выполняет query encode + local PG dense/FTS/alias SQL + RRF/dedup + batch hydration; возвращает bibliography/evidence, не промежуточный vector на Dev. Lexical/alias branches идут параллельно encode, dense использует локальный vector. Полный revision map всех книг не пересылается на каждый запрос. Dense не prefilter lexical.

Immutable revision projections готовятся заранее; activation меняет небольшой active pointer и research frontier, не active flags 100000 rows. Inactive versions ограничены GC; current-pointer filtering проверяется совместно с ANN selectivity. Prepared ANN plans проверяются после 6+ executions на крайних scopes; custom-plan/disableprepare только для ANN при доказанном regression, SET LOCAL knobs pool-safe. FTS per-row RU/DE/simple и bounded query configs, без alias loop.

Для маленьких scoped множеств — индексированный filter и exact distance; для broad ANN — bounded iterative scan и честный partial при исчерпании budget. Проверять plans и recall на настоящей application role. FTS ranking, query material, ANN parameters и scope входят в change gate.

## Python default, узкий native при измеренной пользе

Один resident PyTorch FP32 BGE process и native PG kernels выполняют тяжёлую работу; Python остаётся bootstrap/QUERY glue. Tokenizer backend определяется по фактическому pinned run. Нет per-request model load, spawn или compile. Общий CPU/RAM budget включает Torch threads, PG pool/parallel workers, import/proof и maintenance; transport/event loop не блокируются encode.

Precompiled Go/Rust executable допустим с immutable bundle и проверенным architecture/ISA/libc/shared-library ABI, protocol/hash/source pins. Default остаётся Python glue и существующая authority. Go на DevCoveer может заменить transport/serialization component, Rust — дорогую функцию или компонент с воспроизводимой проблемой памяти/lifecycle. Память, deadlines, backpressure и control-loop isolation допускают native выбор для нового компонента без предварительного CPU hot loop; проверенный native reuse не требует полного Python дубля. Готовый fused RKB executor пока не подтверждён.

Native sidecar не является default; нет второго model/controller или дополнительного proxy, полный rewrite не нужен. Проверяется один узкий вариант с удалением заменяемого пути; полезная задержка, ресурсы или отказы должны улучшиться без регрессии quality, прав и recovery. Local IPC допустим, если суммарный выигрыш учитывает copies/serialization/backpressure, Python BGE child, дополнительный lifecycle и cold recovery. Обязательного процентного порога нет. ONNX/GPU/quantization — самостоятельный quality-gated track. Подробности и primary evidence: [runtime assessment](kaggle-regional-knowledge-service-assessment-20261019.md).

## Надёжность без лишних компонентов

Reuse existing BgeQueue, controller, request/outbox и domain registries. Existing S3ObjectStore остаётся transport: immutable seal/continuous segment manifest/CAS ещё реализуются узко. Большой base идёт direct Kaggle→recovery object по scoped key+epoch+expiry, без long-lived delete/global credentials; presigned URL не заменяет внешний promotion fence. Один controller делает bounded sealed-tail compaction и refs CAS, не 86k retained objects/day.

Canonical outcome содержит exact domain values/evidence; BGE floats/FTS/crops — derived dirty/rebuild, не блокируют canonical replay. Delivered artifact bytes/hash/receipt остаются exact. UNKNOWN ограничивается per provider/route с evidence-based operation resolve; не тормозит весь import.

Нет нового брокера, отдельной фабрики Story, распределённого write-кластера, собственного IAM или второй копии POI/map authority.

Один maintenance permit координирует heavy build/snapshot/import; autovacuum и targeted ANALYZE сохраняются. Нельзя по таймеру делать полный REINDEX/VACUUM FULL/reembedding. В общей памяти учитываются BGE, PG query/maintenance workspaces и render. Ускорение проверяется на больших book counts и реальных source families, не выводится из факта установки PostgreSQL.

Stories/facts/cases сохраняют самостоятельную семантику: отсутствие Story не теряет dates, observations или source facts. Existing research workflow имеет bounded compose и продолжение через page/window boundaries. Private draft не превращается в shared Variant; case disagreement не является редакционной оценкой.

## Границы обещаний

Core/GEO runtime cutover отделён от feature-release Миры/VK/Astro/research needs, но все функции обязательны для завершения проекта. NOT RUN не выдаётся за working; регрессия уже включённого канала блокирует switch без молчаливого отключения. Scale gate использует отдельную isolated benchmark DB, originals/new chunks/BGE без старых vectors; controlled synthetic amplification подтверждает только масштаб, dense quality — реальные held-out facts/new passages. Fixture не публикуется в corpus.

Kaggle имеет конечные sessions и quotas; постоянный WSS не создаёт SLA. Редкий cold break владельцем допустим, но 3–5 минут не являются измеренным restore SLA. Warm gates остаются backend p95≤1 s, MCP p95≤1.5 s и evidence hard deadline2 s. Полный human answer и очередь измеряются отдельно.

[Acceptance](../testing/rkb-scalable-kaggle-acceptance-20261019.md), [mass gates](../mass-ingestion-readiness.md), [geo cutover](../migrations/rkb-layers-only-cutover-20261019.md). [Историческая архитектура 19.10](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/designs/rkb-scalable-kaggle-architecture-20261019.md) сохраняет исходное обсуждение; действующие решения изложены здесь связно.
