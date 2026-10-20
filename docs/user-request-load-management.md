# Управление пользовательской нагрузкой РКБ

Обновлено 20.10.2026. Согласованный продуктовый контракт для реализации; этот документ не является нагрузочной приёмкой. Технические детали — [request design](designs/user-request-load-management-20261019.md), полный [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Цель и область

Менеджер обслуживает запросы **базы знаний региона** во всех поддерживаемых клиентах: personal MCP, Slack и Мира в Projects Hub. Он не управляет всеми проектами DevCoveer. Один человек может пользоваться несколькими клиентами и диалогами, оставаясь одним fairness subject.

Если ресурсов достаточно, независимые запросы выполняются параллельно. Обычная нагрузка ставит уже принятый вопрос в durable ожидание; пользователь не обязан отправлять его снова. Нельзя обещать бесконечный приём при конечном диске: до подтверждённого accepted действует проверка хранения и admission.

Dot→tools и Mira→tools — два независимых умных маршрута. Dot сохраняется; внутри обычного запроса Миры нет дополнительного хода через Dot. Физический путь Миры: PH typed provider→existing DevCoveer auth/admission→WSS, инициированный notebook. Один Kaggle query объединяет encode, SQL и ограниченную гидратацию; нет HTTP self-call, второго WSS relay или пересылки query-вектора через Dev.

## Три режима одной authority

| Режим | Что обещает продукт |
|---|---|
| read_query | Быстрый ограниченный read с RAM permit, request_id корреляции, generation/deadline и min_data_seq. QUERY/QUERY_RESULT/CANCEL_QUERY не создают долговечный вопрос, fsync, remote intake/seal, RESUME или resultSaved. Разрыв/отмена завершает попытку; поздний ответ отбрасывается. |
| durable_question/request | Принятый вопрос с точным сохранённым входом, порядком диалога, последующим результатом и result/hash/native ACK. Перезапуск или закрытие клиента не теряют уже подтверждённую работу. |
| mutation/provider-effect | Точная команда/намерение, preconditions, remote intake и outcome/receipt, fence и UNKNOWN reconciliation. Отмена ожидания/чтения не отменяет уже принятый эффект. |

Это режимы существующего admission/request controller, а не три очереди или три владельца данных. Mode определяется разрешённой операцией, не клиентской меткой. Вложенный read полного принятого вопроса не создаёт второй вопрос/permit. Готовый read исполняется сразу в доступном search reserve, без ожидания batch window; он не занимает место в durable истории только из-за наличия request_id.

## Приём, сохранность и завершение

Для durable_question/request и mutation/provider-effect early local receipt означает pending. Финальный accepted даётся после remotely confirmed exact input/intent; Saved дополнительно требует domain commit и remotely verified exact outcome/receipt. Результат работы и его receipt защищены тем же recovery contract. Владелец не должен заново повторять подтверждённый вопрос после restart. Accepted model/human data не восстанавливаются новой модельной генерацией. Эти обещания не приписываются ephemeral read_query.

Raw spool DevCoveer имеет implementation defaults 4h/512 MiB. Это не срок жизни request или private draft. Terminal receipt horizon 30 days относится только к verifiable server-issued request IDs: expired ID никогда не запускает новую execution; arbitrary legacy keys не GC вслепую. Compact UNKNOWN/request/receipt metadata сохраняются до reconciliation; долговременные inputs/results находятся в canonical PG/recovery objects. Local GC после remote seal не ждёт старую daily base.

Каждый принятый вопрос получает собственный итоговый result/delivery outcome. При result_ready повторяется доставка/получение прежнего результата, а не исследование и создание нового Variant. Same key + different payload — conflict. Uncertain external send сохраняется как UNKNOWN с bounded admission/reconciliation per provider/route; весь import не останавливается. Existing operation resolve допускает verified_delivered, not_delivered или abandoned_no_resend только с evidence/operator authority; исход не угадывается. Незатронутые диалоги и работы продолжаются.

## Порядок и равноправие

Внутри диалога durable вопросы/эффекты исполняются по атомарной последовательности, с durable контекстом предыдущих ответов. Следующий вопрос не обгоняет недоставленный или uncertain predecessor. Context строится на старте исполнения из сохранённой истории, а не только из RAM агента при поступлении вопроса.

Ephemeral read допускается тем же controller без durable dialog row и учитывает min_data_seq. Существующий control index хранит unresolved mutations по canonical person и typed resource/scope dependencies для всех маршрутов Dot/Mira/MCP. При пересечении с pending/UNKNOWN записью на старте чтения и перед выдачей вернуть prior_write_pending и известную freshness; нельзя говорить «актуальное состояние» или «сведений нет». Изменение общего ресурса касается всех затронутых читателей, не только автора и его диалога. Типизированные зависимости отделяют accepted-book search от несвязанной UNKNOWN правки private draft; недоказанную независимость считать зависимостью. После подтверждённого save нужен достаточно свежий snapshot/projection либо честный pending. Это не UNKNOWN самого чтения; per-dialog порядок durable turns остаётся отдельным правилом. См. [handoff §§5.2, 10.2](prompts/rkb-scalable-kaggle-implementation-20261019.md).

Между людьми действует work-conserving fair rotation: пачка из десяти вопросов одного человека не заставляет остальных ждать всю пачку. Десять его диалогов не дают десять независимых долей. На старте роли эксперта и владельца не означают priority пользователя. Все claim paths, включая explicit ID, wake и resume, проходят один guard.

Свободные независимые ресурсы используются: если ждёт один человек, искусственная очередь «только один запрос» не нужна. Долгий уже выполняющийся bounded stage не прерывается ради нового пользователя.

## Измеренный допуск ресурсов

Один existing request lifecycle и BgeQueue/controller распределяют stage permits. PostgreSQL 17/BGE/render/import/snapshot в Kaggle конкурируют за общий CPU/RAM/I/O; DevCoveer имеет отдельный небольшой control envelope. E5 и Supabase не являются target stage/dependency.

Concurrency выбирается по измеренной ёмкости, а не формулой «N запросов на X GB». Увеличение/уменьшение доступных ресурсов перечитывается; снижение запрещает новые тяжёлые starts, но не уничтожает accepted запросы или active transaction. Missing telemetry не даёт расширять допуск.

Во время импорта сохраняется минимум один supported search reserve по CPU, BGE/SQL, RAM и I/O, а не только свободная строка очереди. Новый import batch допускается между durable checkpoints; готовый query не ждёт фиксированного micro-batch window. CPU/thread budgets и PG parallelism учитываются совместно, не по одной памяти. Proof/import также получают положительное продвижение через aging. Если минимальный этап не помещается вместе с резервом, это capacity failure; нельзя объявлять бесконечное ожидание нормальной гарантией. text_search_ready и archive_ready независимы внутри существующего run; latency не оправдывает дополнительные runs сверх owner/session cap.

## Прогноз ожидания

Уведомления ниже относятся к durable принятому вопросу. Короткий read возвращает status в своём deadline и не обещает отложенный финальный ответ.

- ETA до готового ответа **≤10 секунд**: дополнительных waiting сообщений нет.
- ETA **>10 секунд**: сообщить приблизительное оставшееся время или диапазон.
- Прогноз строится арифметически по измеренным стадиям, фактическому fair порядку и ресурсам; отдельная модель/LLM для ETA не нужна.
- Существенное увеличение прогнозируемого времени отражается без потока сообщений. Частый внутренний пересчёт не означает частую доставку.
- Если у person несколько вопросов, waiting/update относится только к ближайшему к обработке и отправляется только в его origin. По остальным вопросы не теряются; правило включается, когда очередь подходит.
- Готовый итог отправляется сразу и подавляет ещё неотправленные waiting notices. Сообщения «ускорились» или служебная история выполнения не нужны.

Детали measurement window и coalescing заданы как проверяемые implementation defaults в [техническом проекте](designs/user-request-load-management-20261019.md). При отсутствии калибровки нельзя выдавать придуманную цифру и объявлять ETA функцию принятой.

## Capacity wait и внешняя недоступность

Полная computational queue или занятый permit — capacity_wait, не outage. У durable принятого вопроса это ожидание не расходует outage retry budget и не требует повторного вопроса. Read_query даёт ограниченный честный status в пределах deadline; он не превращается скрыто в durable question и не возобновляется автоматически после reconnect.

Если нужный внешний сервис доказанно недоступен и безопасного пути результата нет, сохранить и доставить один понятный финальный ответ: «Сервис временно недоступен, попробуйте позже». После него не запускать бесконечную генерацию новых ответов на тот же request. Poison stage сохраняется для needs_recovery, а не удаляется после лимита попыток.

Честная lexical degradation допустима, когда результат остаётся полезным, но её ограничения явны. Пустой degraded поиск не доказывает отсутствие сведений. Current capabilities/source withdrawal и audience проверяются на execution, cache reuse и delivery; требование завершить вопрос не разрешает утечку. Policy delta сверяется со всеми source/variant/grant dependencies каждого item, включая cross-book текст и bibliography. При пересечении исключается весь item с его display/continuation, оставшийся ответ получает degraded/evidence_withdrawn даже при нуле items, не empty ok. Незатронутые чтения продолжаются; неполный provenance, compacted delta или недоказанный scope требуют fail closed, один свежий cursor недостаточен.

До подтверждения блокировки новой RKB-выдачи existing authority barrier и exact intent должны стать remote-durable; ждать Kaggle для этого не требуется. Сам PG withdrawal и внешняя public purge остаются pending до соответствующих commit/readback, без преждевременного Saved. Это durability изменения политики, не обязательная запись каждого read; см. [handoff §§6, 10.1](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Короткие MCP calls и полный диалог

Evidence-search сохраняет backend p95≤1 s, public MCP p95≤1.5 s и hard deadline2 s. Полный пользовательский вопрос может ждать дольше; queue wait и time to final answer измеряются отдельно. Не ждать десятки секунд внутри search HTTP ради очереди.

Обычный read_query не зависит от per-read fsync, queue SQLite write lock или remote journal на critical path. Только режим, обещающий сохранённый полный вопрос/эффект, проходит durable intake; готовые inputs/intents идут в group commit с batching window≤1 s, не обещанием cloud RTT. Клиент без push для такого режима должен иметь authenticated resume/result retrieval/ACK; простого HTTP request_id недостаточно для обещания финального ответа в закрытый клиент. Готовый read этот batching window не ждёт.

Slack использует один Kaliningrad Base bot, Dot остаётся самостоятельным умным клиентом инструментов, не identity пользователя или владельцем результата. Мира расширяет existing PH typed RKB adapter и shared live-interaction. PH v2 явно передаёт status/freshness/min-cursor/continuation; старый строгий evidence/mode normalizer получает успех лишь для полностью представимого результата. Иначе — existing unavailable/error, не пустой hybrid и не потеря freshness. Сохраняются default 3/max 5 evidence, query≤1000 символов и прежние text/metadata limits; нет скрытого усечения. Actual saved story body, источники и разрешённые details остаются доступны.

READ telemetry идёт в отдельный bounded buffer/batched sink вне критического пути и write lock очереди; допустим counted drop, тела/секреты не пишутся. PH `_mark_semantic_observed`, utterance checkpoints, порядок диалога и durable receipts — прикладное recovery state, не такие логи. Tool-lock wait измеряется отдельно от execution duration; независимые reads могут перекрываться только в bounded admission, без blanket удаления locks или обхода порядка effects. Stale generation/deadline, revoked authority и withdrawn source исключают выдачу даже при cache hit.

Для read PH асинхронно сохраняет exact ordered evidence/context refs в existing private tool/utterance store по `(session, turn_generation, tool_call_id)`, с привязкой к committed turn и disposition. Ответ этого checkpoint не ждёт. Crash до записи означает явный context_gap/evidence_context_unavailable; нельзя подменять прежние источники новой выдачей или привязывать соседний transcript по времени. Continuation/context ref не даёт capability, права проверяются заново. Принятые durable вопросы сохраняют прежний exact result/context и ACK; см. [handoff §10.4](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Простой пользовательский опыт

Сохраняется схема: вопрос, при необходимости ETA/существенное уточнение, итог. В этой итерации нет отдельного интерфейса очереди, нового priority market или пользовательской отмены durable вопроса/эффекта. Transport CANCEL_QUERY при barge-in/deadline освобождает только интерес к ephemeral read и не создаёт такую функцию. Служебные statuses нужны API/оператору, а не потоку объяснений пользователю.

«Мира, хорошо подумай» — возможное будущее явное делегирование Dot, не базовый маршрут. После подключения реального PH provider сравнить Mira/Dot на одинаковых вопросах, corpus/revisions и правах: качество, полнота, опора на источники, время первого полезного и полного ответа. Включать делегирование только при показанной пользе; преимущество заранее не предполагается.

## Приёмка

Проверить 10+1 burst, несколько диалогов одного person, два клиента одной identity, explicit-ID bypass, context после restart, UNKNOWN общего ресурса во всех затронутых маршрутах/читателях и продолжающийся несвязанный поиск при UNKNOWN private draft, реальные result/hash/native ACK и delayed revoke. Отдельно ETA10 s threshold/coalescing/restart, measured resize, proof/import aging, import+search reserve и typed outage/capacity outcomes.

Для read отдельно проверить immediate dispatch без fsync/seal/batch window, fairness RAM permits в той же authority, CANCEL_QUERY и поздний ответ, reconnect без RESUME, prior_write_pending, min_data_seq, cache/revoke/withdrawal полного cross-book item и bibliography, compacted policy fail closed, v1 несовместимый ответ и сохранение actual body. PH checkpoint восстанавливает точные упорядоченные refs либо явный context gap, не новую выдачу под прежним ответом. Измерить tool_lock_wait, admission, encode, SQL/hydrate, transport/validation и end-to-end отдельно; потеря telemetry не меняет domain outcome. До реального PH→RKB provider голосовые timings и сравнительный pilot остаются NOT RUN. Полная [PMI](testing/rkb-scalable-kaggle-acceptance-20261019.md) сохраняет прежние protected gates.

Protected quality/latency/capacity gates сохраняются. Наличие pending response, green unit suite или emulator успеха не подтверждает пользовательскую функцию. [Исторические требования 19.10](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/user-request-load-management.md) доступны как исходная запись.
