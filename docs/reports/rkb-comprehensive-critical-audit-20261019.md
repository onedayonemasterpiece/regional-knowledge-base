# Комплексный критический аудит переработки RKB

Дата: 2026-10-19. Результат: согласованный целевой дизайн и самодостаточное [задание кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Данный аудит не является deploy, импортом книг или production acceptance.

## Область и основания

Прочитаны указанные владельцем disagreement design/ПМИ и PR142, illustration audit, request-load design, исходный Slack discussion и дальнейшие owner decisions. Проверены актуальный RKB main `8799f8a01e63070f19fe4bea714777d1be9bbae6`, релевантные исходники PR151 `adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4`, PH adapter/voice architecture `69a350eabe8bc92006f64dbe43247432172557ab`. Новое read-only story исследование:17 convenience records/3dossiers; оригиналы для него не перечитывались.

Две независимые консультации фактического **Claude Opus 5.5** через Claude Code/claude_consult:
- [Архитектура](rkb-opus55-redesign-20261019.md), task `dvt_589bc01f91d24233886235798734c9d5`, artifact SHA256 `9e2e479ddaa804073bface72f0f6d777069a92097d4dcb4812880f1fdbd3bbdd`.
- [Story quality](rkb-opus55-story-quality-20261019.md), task `dvt_29da98bf906e406cb8871f8c4e74b472`, artifact SHA256 `c452f5bf247ab992ccbbde2039a06274a8498fff744d63d648bf2434613ddc1e`.

Режим provided_only: консультанты получили evidence bundles, не выполняли code/runtime tests. Это не OpenCode аудит и не замена Opus другой моделью.

## Главные риски и принятые решения

| Риск | Подтверждение/причина | Принятое решение |
|---|---|---|
| Потеря подтверждённого model/human результата при рестарте | Source SHA/parser не воспроизводят once-accepted inference; один ephemeral PG недостаточен | Exact immutable payload до применения; frozen IDs/time/defaults; durable resolved command; PG effects+applied_ops+cursor atomic. |
| Split-brain и поздняя публикация старым writer | Local epoch не ограничивает внешний side effect | External authority/fence у DevCoveer, CAS canonical pointer, immutable versions, scoped sender. Старые уже accepted commands replay. |
| Backup формально существует, но не восстанавливает актуальное состояние | MAX(seq), произвольный latest и unsafe file copy не доказывают полноту | Consistent physical snapshot+needed WAL/internal cursor, exact version, restore verification и защищённый tail. |
| Compaction теряет данные из-за отставшего Yandex | Одна свежая Kaggle baseline не покрывает старую внешнюю копию | Min retained verified watermark и unfinished consumers, safe old+candidate replacement, backpressure вместо TTL deletion. |
| Потеря DevCoveer disk не покрыта daily backup | Другой failure domain, не notebook restart | Отдельное RPO решение/independent tail; вопрос владельцу, без выдуманной zero-loss гарантии. |
| «Прямое соединение» остаётся polling и выключает warm BGE на expired claim | Broker PermissionError→403; worker останавливается; separate urllib calls | Authenticated outbound WSS на existing ASGI/broker, same queue, application ACK/RESUME, typed stale_claim, bounded buffers и control event loop. |
| WSS установлен, но SQL polling сохраняет latency | wait_result poll .05s и read BEGIN IMMEDIATE | Notify после commit, read без write lock, bounded cross-process wakeup/reconcile. |
| Ужесточение story отбора теряет даты и числа | Metadata сейчас зависит от assertion созданной Story | Facts-first source-bound path, optional episode links, same extraction contract для fresh/accepted source, bounded compose. |
| «Красивее» становится выдумкой или бюрократией | Драма не следует из источника, второй judge не создаёт truth | Existing angle/gaps/Variant evidence, готовые prompts, короткий human source A/B; zero stories/короткий эпизод допустимы. |
| Поиск переехал, source_proof перестал работать | backend.corpus зависит от SQLite; absent guards/render bounds | PG corpus adapter и reuse существующего proof pipeline; pixel grants, exact rendered hash/transform, partial items, durable MIME/refs. |
| Публичный текст случайно открывает полный scan/private draft | Это разные поверхности; renderer может отдавать целую страницу | Public accepted text search отдельно от current pixel/draft grants и public-page whitelist. |
| Упрощение стирает роль, календарь, manual layers или private history | Функции распределены по main и ещё не слитому PR151 | Полная feature matrix, inventory слоёв и manual dependencies, source-bound dates/observations, owner map/POI refs, exact versions/attribution. |
| Второй scheduler/IAM/voice app усложняют проект | Уже есть BgeQueue, request lifecycle, PH/Mira | Reuse: one durable user queue/canonical person, typed PH adapter расширяется; нового приложения пока нет. |
| PH adapter принимается за полный RKB integration | Фактический protocol search-only и strict envelope | Real resource-bound provider, progressive capabilities, pending/result/ACK/artifacts, actual non-owner multiuser E2E. |
| Case создаётся на каждую пару или разницу дат | Pair hash и lexical names не выражают вопрос | Stable subject/question identity, pair-only proposal dedup, source context и temporal/units comparability. |
| Новый вывод наследует старое human endorsement | Mutable summary может смешать версии | Current/last-supported revisions отдельно, exact support, based_on boundary, поздние материалы видимы. |
| Public page/нативная публикация объявлены готовыми до readback | Build/send intent не означает внешний результат | First verified URL, last-good on update failure, generation fence, provider UNKNOWN reconciliation, explicit revocation/rebuild. |
| VK/source requests потеряются при фокусе только на books/story | Предыдущий design содержит полный community workflow | Сохранить research needs/reviews/subscriptions/editorial queue, VibePublish ownership, PH task refs, no duplicate provider client. |

## Что принято из архитектурного аудита Opus и что уточнено

Приняты external fencing, durable resolved commands, one PG working corpus, verified physical snapshot, bounded large payload refs, единственный persisted research pass, read-after-write watermark и reuse существующих queues/adapters.

Уточнения обязательны:
- Accepted model text/spans и human command не восстанавливаются по source hash/parser; exact payload — канонический recovery input.
- Pair of claims — dedup proposal, не case identity. Третий источник не обязан породить новое дело.
- Activation order/cursor доказывает техническую обработку, не полноту ANN/semantic candidate discovery.
- Маленький revocation list не является доказательством правильных scopes. Проверки current grants/withdrawals обязательны на output/cache/delivery.
- Не вводить mandatory GPU, второй verify-service, искусственные проценты research coverage или одинаковый application SHA для любой совместимой patch.
- Заморозить runtime-generated UUID/timestamps/defaults; old epoch accepted commands replay, а не отбрасываются.
- Preview/transform сохраняются до terminal locator callback; ACK preview не разрешает GC.
- Production layer identities предшествуют fresh book links; rollback после новых вкладов не заменяет target пустой/старой БД.

## Без лишней архитектуры

Минимальный полезный срез: один прогретый writer, existing control queue с WSS, exact result/replay, fresh A+B import, source facts → coherent story → independent case и разрешённый proof. Readers вводятся по измеренной необходимости. Функциональная полнота не требует всех внешних integration тестов до первого component check.

Нельзя сэкономить за счёт подтверждённых данных, scoped access, numeric gates или отказа от уже спроектированных функций. Можно сэкономить на новых сервисах, повторном чтении источников, зеркальных реестрах, втором боте, лишнем LLM judge и обязательных workflow церемониях.

## Документационная согласованность

Новый handoff имеет полный текст требований, промптов, migration и gates. Active docs обновляются целевыми разделами; исходный SQLite/Supabase/E5 runtime подписан как baseline. PR142 шесть самостоятельных design/testing docs перенесены в main без потери product semantics; конфликтующие execution choices заменены приоритетным target addendum. Датированные old acceptance reports сохранены как исторические свидетельства, не переписаны в новое PASS.

Protected manifest изменён штатным typed requirements_update по явному intent владельца. Девять IDs и численные thresholds сохранены; последний operation `op_567f795f149119ddbd42996d`, digest `27eba82d1ab0bbcd6130982c9cd914d965c49b50476294242f96a9714b33f43a`. Исходные операции vendor/role/request alignment: `op_cc34eddf02ce55fe925a07ff`, `op_c3b6e129274453f28357bf7b`.

## Не выполнено этой задачей

Не было production rollout, миграции слоёв/книг, выдачи новых grants, настройки Telegram sessions, re-extraction corpus или новых runtime/load/voice tests. Story A/B, реальный WSS soak/reconnect, PG capacity/latency/restore и actual user channels остаются приёмкой реализации.

Dot получил вопросы о manual data scope, pixel audience, exact recovery payload/RPO и образцах хороших stories. Получены ACK прежнего списка и story-дополнения, содержательные решения не приписаны. Dot уточнил, что WSS reconnect сохраняет модель лишь в живой Kaggle-сессии; завершившаяся сессия требует нового run/restore/warmup. Уточнение внесено в архитектуру и handoff. Независимая implementation work продолжается, unresolved choices ограничивают соответствующий cutover/publication, а не весь проект.

Все конкретные source refs, первичные technical docs и Slack decision links приведены в §17 [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). [Acceptance matrix](../testing/rkb-scalable-kaggle-acceptance-20261019.md) содержит наблюдаемые результаты вместо обещания «швейцарских часов» по одной установленной БД.
