# Эксплуатация и проверяемое переключение

Уточнение 21.10.2026: [owner alignment](designs/rkb-owner-alignment-20261021.md) и [аудит](reports/rkb-requirements-code-audit-20261021.md). Dataset — primary persistence, Yandex backup — раз в неделю; direct Telethon в Kaggle, один owner-only импорт. Не переносить в target Vibe uploader, mandatory media-SHA или неподтверждённые pilot limits. Операционная готовность проверяется отдельно от документа.

Обновлено 20.10.2026. План целевого runtime, не подтверждение deploy. Использовать [единственный handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [acceptance](testing/rkb-scalable-kaggle-acceptance-20261019.md), [storage](storage.md) и [geo-only cutover](migrations/rkb-layers-only-cutover-20261019.md). [Прежний runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations.md) фиксирует исходный этап и не является инструкцией заново устанавливать Supabase/E5.

## Контур

Один PostgreSQL 17/pinned pgvector и BGE в Kaggle; existing MCP/ASGI/BgeQueue/controller на DevCoveer; direct outbound WSS; versioned Kaggle Dataset как основной persistence и weekly Yandex backup. Существующий S3 tail требует явного согласования с этим placement, не скрытой подмены Dataset. E5/Supabase отсутствуют в target. Один writer и внешний fence управляют применением и snapshot pointer; существующие senders отвечают за доставку.

DevCoveer raw spool имеет стартовые 4 часа/512 MiB; это не TTL компактных UNKNOWN/request/receipt metadata. Для mutations/принятых долгих вопросов accepted требует remotely confirmed exact intent/payload; Saved дополнительно требует domain commit и remotely verified exact outcome/receipt. Ephemeral `read_query` использует ту же person/resource admission authority, но dispatch/result не ждут fsync, remote seal или durable RESULT_ACK/RESUME. Его bounded async telemetry/history references не пишут в queue SQLite, не захватывают её write locks и не считаются сохранённым вопросом.

Оба выбранных владельцем smart routes сохраняются: Dot→tools и Mira→tools. Дополнительный агент между ними и RKB не нужен. Mira/PH обращается через typed provider к существующему gateway/WSS relay; один исходящий notebook WSS не требует публичного inbound endpoint Kaggle. Одна операция search внутри Kaggle выполняет encode, SQL branches, fusion и batch hydration; в Dev возвращается компактный evidence pack, а не vector/candidates для следующего roundtrip. Обычный Live search возвращает полезный evidence pack без автоматического proof. Связанные с релевантными chunks иллюстрации модель запрашивает отдельно и оценивает визуально; это не блокирует быстрый текстовый search.

## Перед реализацией

Установить/прочитать [kaggle-warm-debugging](../.agents/skills/kaggle-warm-debugging/SKILL.md) целиком по pin и receipt из handoff§0. Skill обязателен для дешёвых Kaggle итераций, не является разрешением на runtime effects. В нём обновлять подтверждённые общие уроки; не создавать второй runbook/controller.

Большие файлы идут по handoff§5.5 напрямую клиентское hosting/storage↔Kaggle; control/auth через Dev, bytes без Dev proxy. Client fileParams URL не перевыпускается сервером; истечение означает reattach той же operation. Controller tickets имеют отдельные attempts, exact object verification, private visibility/output и current grant checks. Жизнь signed URL и уже начатого consumer различается. Временный Dataset — исключение, не транспорт каждой итерации.

GC exact temporary resource использует общий guard с выдачей consumer leases; новые refs закрываются до удаления, существующие consumers/UNKNOWN/recovery защищены. Readback native absence закрывает удаление; quota сверяется только если API наблюдаем, unsupported delete остаётся cleanup_blocked_by_provider. Не удалять permanent DB Dataset и не считать cleanup остановкой compute. Полные сценарии DBG-11–13, без повторного прогона всего корпуса на каждую правку.

## Перед cutover

1. Зафиксировать actual executing SHA, PG 17/pgvector/model/tokenizer/schema pins, resource mount/quota и применимые differences от main. Не объявлять draft PR151 или историческую canary production implementation.
2. Проверить изолированный вертикальный путь: authenticated intake → pending → remote seal → один writer apply → saved receipt → result delivery/ACK.
3. Проверить WSS reconnect живой сессии и отдельный сценарий завершения notebook: новая session, restore exact snapshot, replay, SQL+dense readiness. Один queue/controller/attachment; без parallel HTTP dispatcher.
4. Fresh A/B imports проверяют source fidelity, bibliography, BGE/FTS, independent facts/dates/observations, stories, cases, proof и продолжение из нового чата. Source_proof получает настоящий PG corpus adapter.
5. Geo inventory/dry-run переносит только layers/places/geometry/CRS/temporal aliases/provenance и нужные reference stubs. Проверенный GEO identity manifest со stable layer/place IDs, aliases и CRS обязателен до production book imports. Bulk rasters могут переноситься в phase 4; stage 2 без manifest — только isolated fixture. Негеографические facts/stories/drafts/cases и книги — fresh workflow, без копирования прежнего corpus.
6. Выполнить применимые quality/capacity/latency/recovery gates и проверить новые media audiences. Core/GEO cutover оценивается отдельно от feature-release Миры/VK/Astro/research needs; все функции обязательны для whole-project completion. NOT RUN не объявляется работающим. Регрессия уже включённого канала блокирует switch, канал нельзя молча отключать. Старое состояние остаётся обратимым reference; удаление старых копий не является частью docs change.

Отдельно проверить fast read без durable intake: общий fairness/deadline/current-rights gate, `min_data_seq`, отмена stale context и bounded повтор после разрыва. Недоступный telemetry sink не задерживает dispatch/result и не захватывает queue write lock. Durable vertical path выше остаётся обязательным для принятых вопросов, mutations и внешних effects.

## Ежедневная диагностика

| Сигнал | Что действительно проверять |
|---|---|
| Connection | WSS attachment/generation, bounded reconnect, queue credits. Pong не означает готовую БД или модель. |
| Serving | Actual SQL query и dense query probe, active revision/material hash, useful evidence, backend/MCP latency и degraded outcomes. `text_search_ready` независимо от `archive_read_ready`/`proof_ready`. |
| Publication | Required BGE coverage, current revision, activation/research frontier; stale/superseded результаты inert. |
| Recovery | Last remotely sealed seq, contiguous applied cursor, verified base/version, replay backlog, checkpoint age, raw spool bytes/age. |
| Resource pressure | PG/BGE combined RSS, CPU, event-loop lag, pool/locks/temp/WAL, disk and Dataset/recovery headroom. |
| User work | Eligible heads, fairness cursor, capacity/dependency waits, oldest runnable age, ETA accuracy; не содержимое private вопросов. |
| Delivery | result hash, native receipt, UNKNOWN reconciliation и текущее право на audience. |

Полный пользовательский вопрос может ждать и продолжаться через durable request. Evidence-search отдельно соблюдает backend p95≤1 s, public MCP p95≤1.5 s, hard deadline2 s. Pending и lexical degradation учитываются в отчёте; ими нельзя скрыть failed dense gate.

Text query не выполняет Telegram preflight. Slow/denied/pending proof остаётся отдельным item; уже готовый текст/evidence возвращается без него. Current withdrawal/grants/audience проверяются перед output, включая cache hits, одной batched проверкой top-K dependencies; публичный text scope не строит список всех разрешённых книг. Исключение read-only из journal не разрешает обход rights или чтение до зависимой подтверждённой записи.

## Recovery cadence и GC

Group-commit flush delay для accepted mutations/durable turns ≤1 s — верхняя граница набора, не фиксированный sleep или cloud RTT; готовый singleton при свободном uploader может flush сразу. Ранний local receipt pending. Saved требует domain commit и remote exact outcome/receipt. NEW уже содержит S3 journal/CAS части, но это не Dataset publisher. Под выбранный Dataset persistence проверить exact input/outcome coverage и statuses; непокрытые записи остаются pending, существующие accepted records сохраняются до проверенной замены. Control checkpoint 15 min. Changed PG base каждые4 h, раньше по replay/byte pressure или planned handover. Независимая от Kaggle Yandex backup-копия подтверждённой Dataset version создаётся раз в неделю. Без изменений full base/reembed не выполняются.

Local raw GC требует доказанного внешнего покрытия payloads/metadata, не ожидания старейшего weekly backup; неподтверждённый input не удаляется по одному TTL. Compact UNKNOWN/request metadata живёт до reconciliation. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers. Base и tail в одном Yandex не независимы друг от друга; outage этого failure domain не скрывается.

Большой base загружается напрямую Kaggle→Dataset/recovery object, не через Dev 512 MiB spool. Notebook получает scoped key+epoch+expiry upload grant без long-lived delete/global-bucket credentials. Presigned URL не отзывается магически при смене epoch; old-writer upload остаётся orphan без promotion authority. Bounded sealed-tail compaction под тем же controller/CAS сохраняет logical hashes и recovery dependencies.

Dataset version: controller создаёт upload и сохраняет account key/blob token; notebook получает только signed upload URL/session, отправляет large bytes напрямую; controller создаёт version → provider completion → exact-version download/hash → restore/replay/probe → fenced **recovery pointer** switch. Active routing рабочей БД переключается отдельно по [handoff §6.6](prompts/rkb-scalable-kaggle-implementation-20261019.md), default — конечное PG maintenance окно в текущем notebook, без обязательных двух clusters. [Kaggle primitive и probe](storage.md) не требуют выдавать notebook account-wide API credentials. Old good + candidate остаются до verification. Delete-old-versions не применять к pinned recovery/reader dependencies. Source bytes и credentials не попадают в публичные artifacts.

Один base artifact используется для обеих provider copies с независимым transport readback. При наличии уже разрешённого planned reader/successor candidate verification выполняется на нём; после replay/readiness/fencing использовать этот же restored instance. Не повторять идентичные backup/download/restore ради отдельного названия проверки. Weekly drill может совместиться с такой verification только с полным набором recovery/control-receipt/SQL/dense checks. Если разрешённого run нет, использовать bounded maintenance budget без дополнительного Kaggle run; search reserve и tail/replay budgets сохраняются. Warm serving не требует повторного hashing/repacking всей проверенной base.

## Конкретные отказы

- **WSS drop, notebook жив:** bounded reconnect/RESUME с прежними durable job/result hashes; ephemeral read не входит в durable RESUME и может быть выполнен снова с актуальным request context. Модель и PG остаются в RAM. Не создавать notebook только из-за socket failure.
- **Notebook завершён:** revoked old authority, новый run, verified restore и replay. Старые новые submissions не принимаются, но ранее remote-accepted commands старого epoch replay обязательны.
- **Remote recovery недоступно:** bounded pending и backpressure до hard limits; не сообщать accepted/Saved без remote confirmation. Read-only queries продолжаются при готовом serving backend.
- **Vector coverage ready, BGE query failed:** dense serving unavailable/partial. Lexical-only результат честно маркируется и не считается dense acceptance.
- **Telegram reconnect/FloodWait, archive/proof unavailable:** сохранить cooldown/read continuation и раздельные readiness flags; готовые PG/BGE продолжают обслуживать text queries. Не выдавать новый run в обход exclusive Telegram session lease/cap.
- **Stale reader:** ordinary search сообщает data_as_of; min_data_seq route только на достаточный cursor либо explicit pending. Current revoke/withdrawal фильтруется gateway.
- **Send outcome UNKNOWN:** один sender, bounded admission/reconciliation per provider/route, без остановки всего import. Existing operation resolve допускает verified_delivered, not_delivered или abandoned_no_resend только с evidence и operator authority; отсутствие readback не доказывает not_delivered. Не повторять генерацию и не делать blind resend; другие диалоги продолжаются.
- **Poison stage:** сохранить exact checkpoint/input, needs_recovery и операторский incident; лимит retries не удаляет accepted работу.
- **Space/CPU pressure:** не начинать новый тяжёлый batch, завершить текущую transaction; не SIGSTOP/kill посреди commit и не ждать permit под writer lock.

## Maintenance без отдельного оркестратора

Один existing controller выдаёт короткие stage permits на imports, render, snapshot и index maintenance. Поиск имеет измеренный резерв; proof/import получают положительное продвижение через aging. Если минимальный совместный footprint не помещается, это capacity failure, а не выполненная гарантия.

Warm debug использует тот же owned run и **стабильный notebook slug**: небольшой exact SQL/artifact patch → apply → loaded hash + useful result readback. Минимальный control/HELLO предшествует тяжёлым bootstrap stages. Новый push/install/model load/Dataset version не являются обычной итерацией. Read-handler candidate имеет rollback; mutation/function semantics меняются через ordered barrier с readback UNKNOWN в исходной generation. Малый disposable PG fixture разрешён отдельно от реальных GEO/accepted/Saved; полный порядок, stop proof и output completeness — [Kaggle runbook](operations/bge-kaggle.md), [DBG приёмка](testing/rkb-scalable-kaggle-acceptance-20261019.md).

Provider status, service phase, loaded versions и completeness — отдельные наблюдения с timestamp. Один poller controller, WSS live progress, bounded retry/stage budgets. Неполученный status/push/stop не считается failure/not-started/terminal по умолчанию. STOP ACK/delete/TTL не разрешают reuse exclusive Telegram session; unknown compute capacity остаётся учтённой. Required inventory определяется до работы, partial output сохраняет пригодные результаты. Cleanup не удаляет accepted/UNKNOWN refs и не трогает чужие runs.

Ready query при доступном permit dispatches сразу; batching объединяет уже ожидающие запросы и не добавляет паузу ради заполнения. Скорость важнее экономии ресурсов самой по себе: полезные bounded caches, FP32/index projections допустимы в 80/90% capacity gates. Измерять E2E и search latency также во время snapshot/restore, учитывать CPU/I/O contention; выигрыш даже 0.5 s имеет смысл, но не объявляется без сопоставимого замера.

После значимой bulk activation/изменения распределения — targeted ANALYZE; autovacuum включён. Никаких hourly/daily full REINDEX, VACUUM FULL или reembedding. Heavy rebuild только по конкретным evidence, с memory/disk overlap и измеренной паузой. Index/log growth не лечить удалением ещё нужного recovery tail.

Не запускать повторные benchmarks без конкретного оставшегося gate. Для проверки plan/latency использовать разрешённый production-like fixture в отдельной isolated benchmark DB, без нового tenant/IAM; сохранить все attempts, errors, resource/config pins. Scale fixture проходит originals → new chunks → BGE, без старых chunks/vectors и без попадания fixture в public corpus; подробные scale/quality границы — в [mass readiness](mass-ingestion-readiness.md). Source text, query bodies и private origins не выводятся в public report.

## Правильный rollback

До новых accepted target writes допустим возврат routing к проверенному прежнему состоянию. После новых подтверждённых вкладов rollback сохраняет remote accepted tail и совместимый replay; старая целая SQLite поверх новых работ недопустима. Старый snapshot pointer не возвращает отозванные grants и не запускает второго writer. Canonical replay не хеширует заново BGE floats/FTS/crops как domain outcome; derived projections dirty/rebuild отдельно от domain cursor. Existing delivered artifact bytes/hash/receipt сохраняются точно. PG minor/libs/platform/collation compatibility и affected-index rebuild проверяются до READY.

Клиенты/интеграции проверяются реально: Slack Kaliningrad Base, personal MCP, Мира в Projects Hub, public case page и её readback, source archive/proof, Street Story/Cartography refs. Documentation и emulator HTTP200 не заменяют actual user/result/receipt flow.
