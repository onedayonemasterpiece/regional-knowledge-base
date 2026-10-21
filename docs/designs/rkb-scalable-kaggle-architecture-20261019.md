# Целевая архитектура RKB: Kaggle PG 17/BGE и короткий DevCoveer control

Дополнение v4: обязательный pinned skill до runtime работы; direct bulk client hosting/storage↔Kaggle без Dev byte proxy; отдельные client refs/controller tickets, exact object verification, временный Dataset GC по refs/leases и truthful MCP effects. Полный контракт — единый handoff §0/5.5–5.6, приёмка DBG-11–13.

Обновлено 20.10.2026; канонический путь документа сохранён. Статус: решение для реализации, не выполненный deploy. Lifecycle/code baseline v4 — [main 3e39046](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/3e39046fe32aaf14cfa088d65d15ff0082425309); предыдущие source audits сохраняют собственные точные heads.

**Один самодостаточный [handoff кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md)** содержит функциональный контракт, порядок внедрения и отладки. Этот документ объясняет принятое размещение; подробные query/index paths — в [architecture](../architecture.md), durability/GC — в [storage](../storage.md).

## Решение владельца и область

Уточнения21.10: [owner alignment](rkb-owner-alignment-20261021.md), [аудит](../reports/rkb-requirements-code-audit-20261021.md). Один owner-only import, клиентское модельное чтение без обязательной повторной backend обработки, domain staging/tokenizer/document vectors в Kaggle. Числа и функции остаются; Case/illustration/DjVu parity не объявлены завершёнными. Go/Rust решение требует actual trace, а не обещания rewrite.

Рабочая БД PostgreSQL **17** с закреплённым pgvector и BGE-M3 находится на Kaggle. E5 и Supabase в target отсутствуют. Существующий BGE baseline — 1024 FP32, pinned revision/tokenizer, final input≤512 tokens; coherent passages около 256 tokens. Скорость поиска приоритетна относительно экономии хранения при сохранении обязательных capacity/quality gates; факты и exact provenance не сокращаются ради quota. Даже около 0.5 s выигрыша полезны, но требуют измерения полного полезного пути.

Миграция переносит **только geo**: layers/places, geometry/CRS/оси/единицы, transform/GCP, временные aliases, provenance и необходимые минимальные source refs. Негеографические facts, старые книги/chunks/vectors/receipts, stories, cases и drafts не копируются в новую БД; они создаются fresh через проверенный import/workflow. Старое состояние сохраняется отдельно как reference, без второй live write authority. GEO identity manifest со stable layer/place IDs, aliases и CRS готов до production book imports; bulk raster transfer допустим в phase 4. Stage 2 без manifest использует только isolated fixture.

Accepted active book text/bibliography и illustrations доступны всем. Stripe quote scans доступны owner/demo/expert/local_historian. Original PDF/DjVu, full-page pixels и private drafts имеют отдельный private scope. Публичность выбранной иллюстрации не разрешает full-page preview.

## Минимальный состав

| Контур | Роль |
|---|---|
| Existing DevCoveer MCP/ASGI/BgeQueue/controller | Verified identity, единый person/request/dialog lifecycle, fairness/admission, writer/connection authority, existing delivery. Raw spool 4h/512 MiB и компактное state до reconciliation. |
| Один Kaggle writer | PG 17 domain store, BGE, source processing, ordered apply/receipts, serving и snapshots. |
| Readers только по необходимости | Exact verified snapshot, BGE/search/proof; никаких независимых writes, scheduler или sender. |
| Private Kaggle Dataset | Основной постоянный versioned persistence: consistent PG physical base с indexes/manifest/cursor и поддержанным recovery continuation; restore/save через exact confirmed version. |
| Yandex S3 | Weekly independent backup подтверждённой Dataset version; существующий online S3 tail рассматривается отдельно, не заменяет выбранный primary молча. |
| Existing integrations | Telegram archive read/write — Telethon в Kaggle, без Vibe/Dev byte proxy. Regional Cartography, Street Story, PH/Mira и отдельно разрешённые социальные публикации сохраняют своё ownership. |

Существуют два независимых интеллектуальных маршрута Dot → RKB и Mira → RKB. Ни один агент не удаляется; Mira не проходит через Dot. DevCoveer — тонкий auth/admission relay existing ASGI. Notebook открывает authenticated outbound WSS, public listener и открытый PostgreSQL не нужны. Ephemeral QUERY/QUERY_RESULT и durable DISPATCH/RESULT/ACK — разные классы одного transport и одной admission authority; HTTP polling и WSS не dispatch одновременно.

## Инварианты

1. Local receipt pending; final accepted/Saved только после exact intent/payload remote confirmation. Saved дополнительно означает applied PG result и remotely verified exact outcome/receipt. Готовые mutations и явно принятые durable requests ждут group flush ≤1 s. Обычный read-turn использует ephemeral QUERY без durable row/RESUME; bounded async audit не задерживает request/response и не обещает durable receipt.
2. Domain effects, applied_ops и непрерывный applied cursor фиксируются одной PG transaction. IDs, logical timestamps и принятые значения не генерируются заново при replay.
3. New submissions проверяют current writer/run/claim/connection generation. Уже принятые команды прежнего epoch остаются обязательными для replay.
4. Canonical pointer, external effect и delivery проверяются внешней authority. Старый writer не выбирает late snapshot и не публикует поздний результат сам.
5. Manifest содержит exact Dataset version/hash, PG minor/extensions/libs/platform/collation provider+version, schema/codec, internal cursor и required WAL. Keyed normalized text использует C collation; incompatible affected indexes исправляются до READY. Upload success не равен restore verification.
6. Dev raw spool: default 4h/hard 512 MiB, проверяемые до cutover; compact UNKNOWN/request/receipt metadata живут до reconciliation. Local GC после remotely sealed payload + verified metadata не ждёт weekly backup после доказанного внешнего покрытия.
7. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers. Yandex base и tail в одном Yandex не независимы друг от друга.
8. Control checkpoint 15 min; changed PG base 4 h либо раньше при replay/byte pressure/planned handover; weekly independent-from-Kaggle Yandex backup; Dataset primary publication/continuation должны отдельно закрывать обещанный durable cursor. Без изменений full base не создаётся.
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

Precompiled Go/Rust executable допустим с immutable bundle и проверенным architecture/ISA/libc/shared-library ABI, protocol/hash/source pins. Default остаётся Python glue и существующая authority. Go на DevCoveer может заменить transport/serialization component, Rust — дорогую функцию или компонент с воспроизводимой проблемой памяти/lifecycle. Память, deadlines, backpressure и control-loop isolation допускают native выбор для нового компонента без предварительного CPU hot loop; проверенный native reuse не требует полного Python дубля. Собственный Go/Rust fused executor не найден; текущий fused Python/native PG путь реализован, его численные live gates ещё не подтверждены.

Native sidecar не является default; нет второго model/controller или дополнительного proxy, полный rewrite не нужен. Проверяется один узкий вариант с удалением заменяемого пути; полезная задержка, ресурсы или отказы должны улучшиться без регрессии quality, прав и recovery. Local IPC допустим, если суммарный выигрыш учитывает copies/serialization/backpressure, Python BGE child, дополнительный lifecycle и cold recovery. Обязательного процентного порога нет. ONNX/GPU/quantization — самостоятельный quality-gated track. Подробности и primary evidence: [runtime assessment](kaggle-regional-knowledge-service-assessment-20261019.md).

## Надёжность без лишних компонентов

Reuse existing BgeQueue, controller, request/outbox и domain registries. NEW содержит части S3 immutable seal/manifest/CAS; сохранить их recovery invariants, но выбранный Dataset-primary adapter и ACK coverage проверить отдельно. Большой base идёт direct Kaggle→recovery object по scoped key+epoch+expiry, без long-lived delete/global credentials; presigned URL не заменяет внешний promotion fence. Один controller делает bounded sealed-tail compaction и refs CAS, не 86k retained objects/day.

Canonical outcome содержит exact domain values/evidence; BGE floats/FTS/crops — derived dirty/rebuild, не блокируют canonical replay. Delivered artifact bytes/hash/receipt остаются exact. UNKNOWN ограничивается per provider/route с evidence-based operation resolve; не тормозит весь import.

Нет нового брокера, отдельной фабрики Story, распределённого write-кластера, собственного IAM или второй копии POI/map authority.

Стоимость итераций — часть этого решения. Один стабильный notebook slug версионируется; здоровый owned run переиспользуется. Минимальный WSS/HELLO приходит до install/restore/BGE, READY — после полезных probes. Совместимые SQL и stateless read-handler updates сохраняют PG/model; остальные process/context changes честно перезапускают нужный worker. Dataset не передаёт каждую правку. Редкая замена БД использует finite maintenance окно, второй cluster — лишь измеренная оптимизация. Migration transitions и accepted commands сохраняют исходный порядок/семантику; UNKNOWN до несовместимой миграции сверяется в исходной generation.

Stop request/ACK, resource deletion, TTL и исчезнувший socket не доказывают compute terminal. Exact provider execution evidence и TG authorization revocation различаются. Expected output inventory задаётся до работы; manifest/provider COMPLETE не заменяют проверки всех обязательных bytes. Подробности и дешёвая последовательность проверки: [handoff §5.4/6.6/14.5](../prompts/rkb-scalable-kaggle-implementation-20261019.md), [критический lifecycle audit](../reports/rkb-kaggle-debug-lifecycle-audit-20261020.md).

Один maintenance permit координирует heavy build/snapshot/import; autovacuum и targeted ANALYZE сохраняются. Нельзя по таймеру делать полный REINDEX/VACUUM FULL/reembedding. В общей памяти учитываются BGE, PG query/maintenance workspaces и render. Ускорение проверяется на больших book counts и реальных source families, не выводится из факта установки PostgreSQL.

Stories/facts/cases сохраняют самостоятельную семантику: отсутствие Story не теряет dates, observations или source facts. Existing research workflow имеет bounded compose и продолжение через page/window boundaries. Private draft не превращается в shared Variant; case disagreement не является редакционной оценкой.

## Границы обещаний

Core/GEO runtime cutover отделён от feature-release Миры/VK/Astro/research needs, но все функции обязательны для завершения проекта. NOT RUN не выдаётся за working; регрессия уже включённого канала блокирует switch без молчаливого отключения. Scale gate использует отдельную isolated benchmark DB, originals/new chunks/BGE без старых vectors; controlled synthetic amplification подтверждает только масштаб, dense quality — реальные held-out facts/new passages. Fixture не публикуется в corpus.

Kaggle имеет конечные sessions и quotas; постоянный WSS не создаёт SLA. Редкий cold break владельцем допустим, но 3–5 минут не являются измеренным restore SLA. Warm gates остаются backend p95≤1 s, MCP p95≤1.5 s и evidence hard deadline2 s. Полный human answer и очередь измеряются отдельно.

[Acceptance](../testing/rkb-scalable-kaggle-acceptance-20261019.md), [mass gates](../mass-ingestion-readiness.md), [geo cutover](../migrations/rkb-layers-only-cutover-20261019.md). [Историческая архитектура 19.10](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/designs/rkb-scalable-kaggle-architecture-20261019.md) сохраняет исходное обсуждение; действующие решения изложены здесь связно.
