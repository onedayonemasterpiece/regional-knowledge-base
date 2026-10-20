# Эксплуатация и проверяемое переключение

Обновлено 20.10.2026. План целевого runtime, не подтверждение deploy. Использовать [единственный handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [acceptance](testing/rkb-scalable-kaggle-acceptance-20261019.md), [storage](storage.md) и [geo-only cutover](migrations/rkb-layers-only-cutover-20261019.md). [Прежний runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations.md) фиксирует исходный этап и не является инструкцией заново устанавливать Supabase/E5.

## Контур

Один PostgreSQL 17/pinned pgvector и BGE в Kaggle; existing MCP/ASGI/BgeQueue/controller на DevCoveer; direct outbound WSS; existing Yandex immutable command/control/receipt recovery tail. E5/Supabase отсутствуют в target. Один writer и внешний fence управляют применением и snapshot pointer; существующие senders отвечают за доставку.

DevCoveer raw spool имеет стартовые 4 часа/512 MiB; это не TTL компактных UNKNOWN/request/receipt metadata. Accepted требует remotely confirmed exact intent/payload; Saved дополнительно требует domain commit и remotely verified exact outcome/receipt. Read-only search не ждёт remote journal.

## Перед cutover

1. Зафиксировать actual executing SHA, PG 17/pgvector/model/tokenizer/schema pins, resource mount/quota и применимые differences от main. Не объявлять draft PR151 или историческую canary production implementation.
2. Проверить изолированный вертикальный путь: authenticated intake → pending → remote seal → один writer apply → saved receipt → result delivery/ACK.
3. Проверить WSS reconnect живой сессии и отдельный сценарий завершения notebook: новая session, restore exact snapshot, replay, SQL+dense readiness. Один queue/controller/attachment; без parallel HTTP dispatcher.
4. Fresh A/B imports проверяют source fidelity, bibliography, BGE/FTS, independent facts/dates/observations, stories, cases, proof и продолжение из нового чата. Source_proof получает настоящий PG corpus adapter.
5. Geo inventory/dry-run переносит только layers/places/geometry/CRS/temporal aliases/provenance и нужные reference stubs. Проверенный GEO identity manifest со stable layer/place IDs, aliases и CRS обязателен до production book imports. Bulk rasters могут переноситься в phase 4; stage 2 без manifest — только isolated fixture. Негеографические facts/stories/drafts/cases и книги — fresh workflow, без копирования прежнего corpus.
6. Выполнить применимые quality/capacity/latency/recovery gates и проверить новые media audiences. Core/GEO cutover оценивается отдельно от feature-release Миры/VK/Astro/research needs; все функции обязательны для whole-project completion. NOT RUN не объявляется работающим. Регрессия уже включённого канала блокирует switch, канал нельзя молча отключать. Старое состояние остаётся обратимым reference; удаление старых копий не является частью docs change.

## Ежедневная диагностика

| Сигнал | Что действительно проверять |
|---|---|
| Connection | WSS attachment/generation, bounded reconnect, queue credits. Pong не означает готовую БД или модель. |
| Serving | Actual SQL query и dense query probe, active revision/material hash, useful evidence, backend/MCP latency и degraded outcomes. |
| Publication | Required BGE coverage, current revision, activation/research frontier; stale/superseded результаты inert. |
| Recovery | Last remotely sealed seq, contiguous applied cursor, verified base/version, replay backlog, checkpoint age, raw spool bytes/age. |
| Resource pressure | PG/BGE combined RSS, CPU, event-loop lag, pool/locks/temp/WAL, disk and Dataset/recovery headroom. |
| User work | Eligible heads, fairness cursor, capacity/dependency waits, oldest runnable age, ETA accuracy; не содержимое private вопросов. |
| Delivery | result hash, native receipt, UNKNOWN reconciliation и текущее право на audience. |

Полный пользовательский вопрос может ждать и продолжаться через durable request. Evidence-search отдельно соблюдает backend p95≤1 s, public MCP p95≤1.5 s, hard deadline2 s. Pending и lexical degradation учитываются в отчёте; ими нельзя скрыть failed dense gate.

## Recovery cadence и GC

Group-commit flush delay для accepted mutations/реальных turns ≤1 s — окно набора, не cloud RTT; ранний local receipt pending. Saved требует domain commit и remote exact outcome/receipt. Existing S3ObjectStore пока только put/get/delete transport; sealed continuous segment chain и conditional manifest CAS реализуются и проверяются отдельно. Control checkpoint 15 min. Changed PG base каждые4 h, раньше по replay/byte pressure или planned handover. Независимая от Kaggle verified Yandex base примерно ежедневно. Без изменений full base/reembed не выполняются.

Local raw GC требует remotely sealed payloads/verified metadata, не oldest daily backup. Compact UNKNOWN/request metadata живёт до reconciliation. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers. Base и tail в одном Yandex не независимы друг от друга; outage этого failure domain не скрывается.

Большой base загружается напрямую Kaggle→Dataset/recovery object, не через Dev 512 MiB spool. Notebook получает scoped key+epoch+expiry upload grant без long-lived delete/global-bucket credentials. Presigned URL не отзывается магически при смене epoch; old-writer upload остаётся orphan без promotion authority. Bounded sealed-tail compaction под тем же controller/CAS сохраняет logical hashes и recovery dependencies.

Dataset version: controller создаёт upload и сохраняет account key/blob token; notebook получает только signed upload URL/session, отправляет large bytes напрямую; controller создаёт version → provider completion → exact-version download/hash → restore/replay/probe → fenced pointer switch. [Kaggle primitive и probe](storage.md) не требуют выдавать notebook account-wide API credentials. Old good + candidate остаются до verification. Delete-old-versions не применять к pinned recovery/reader dependencies. Source bytes и credentials не попадают в публичные artifacts.

## Конкретные отказы

- **WSS drop, notebook жив:** bounded reconnect/RESUME с прежними job/result hashes; модель и PG остаются в RAM. Не создавать notebook только из-за socket failure.
- **Notebook завершён:** revoked old authority, новый run, verified restore и replay. Старые новые submissions не принимаются, но ранее remote-accepted commands старого epoch replay обязательны.
- **Remote recovery недоступно:** bounded pending и backpressure до hard limits; не сообщать accepted/Saved без remote confirmation. Read-only queries продолжаются при готовом serving backend.
- **Vector coverage ready, BGE query failed:** dense serving unavailable/partial. Lexical-only результат честно маркируется и не считается dense acceptance.
- **Stale reader:** ordinary search сообщает data_as_of; min_data_seq route только на достаточный cursor либо explicit pending. Current revoke/withdrawal фильтруется gateway.
- **Send outcome UNKNOWN:** один sender, bounded admission/reconciliation per provider/route, без остановки всего import. Existing operation resolve допускает verified_delivered, not_delivered или abandoned_no_resend только с evidence и operator authority; отсутствие readback не доказывает not_delivered. Не повторять генерацию и не делать blind resend; другие диалоги продолжаются.
- **Poison stage:** сохранить exact checkpoint/input, needs_recovery и операторский incident; лимит retries не удаляет accepted работу.
- **Space/CPU pressure:** не начинать новый тяжёлый batch, завершить текущую transaction; не SIGSTOP/kill посреди commit и не ждать permit под writer lock.

## Maintenance без отдельного оркестратора

Один existing controller выдаёт короткие stage permits на imports, render, snapshot и index maintenance. Поиск имеет измеренный резерв; proof/import получают положительное продвижение через aging. Если минимальный совместный footprint не помещается, это capacity failure, а не выполненная гарантия.

После значимой bulk activation/изменения распределения — targeted ANALYZE; autovacuum включён. Никаких hourly/daily full REINDEX, VACUUM FULL или reembedding. Heavy rebuild только по конкретным evidence, с memory/disk overlap и измеренной паузой. Index/log growth не лечить удалением ещё нужного recovery tail.

Не запускать повторные benchmarks без конкретного оставшегося gate. Для проверки plan/latency использовать разрешённый production-like fixture в отдельной isolated benchmark DB, без нового tenant/IAM; сохранить все attempts, errors, resource/config pins. Scale fixture проходит originals → new chunks → BGE, без старых chunks/vectors и без попадания fixture в public corpus; подробные scale/quality границы — в [mass readiness](mass-ingestion-readiness.md). Source text, query bodies и private origins не выводятся в public report.

## Правильный rollback

До новых accepted target writes допустим возврат routing к проверенному прежнему состоянию. После новых подтверждённых вкладов rollback сохраняет remote accepted tail и совместимый replay; старая целая SQLite поверх новых работ недопустима. Старый snapshot pointer не возвращает отозванные grants и не запускает второго writer. Canonical replay не хеширует заново BGE floats/FTS/crops как domain outcome; derived projections dirty/rebuild отдельно от domain cursor. Existing delivered artifact bytes/hash/receipt сохраняются точно. PG minor/libs/platform/collation compatibility и affected-index rebuild проверяются до READY.

Клиенты/интеграции проверяются реально: Slack Kaliningrad Base, personal MCP, Мира в Projects Hub, public case page и её readback, source archive/proof, Street Story/Cartography refs. Documentation и emulator HTTP200 не заменяют actual user/result/receipt flow.
