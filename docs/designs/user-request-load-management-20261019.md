# РКБ: технический проект управления пользовательской нагрузкой

## Нормативное уточнение target и reuse PR151 — 19 октября 2026

Этот блок имеет приоритет над прежними проектными рекомендациями ниже. Полный порядок реализации: [каноническая архитектура](rkb-scalable-kaggle-architecture-20261019.md) и [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md).

Ранний анализ main `a753a4e` / PR151 `18f16cb` сохраняется как историческое чтение исходников. Его утверждения «нет fairness/dialog sequence/ACK» нельзя переносить на поздний draft PR151 `adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4`. Поздние branch body/contracts фиксируют person fairness, dialog sequence/explicit-ID guard, native result ACK, BGE consumer checkpoint recovery, private drafts и load requirements. Это материал для reuse, не live PASS. Перед интеграцией сравнить актуальный head, migrations и tests; не разрабатывать заново уже реализованные guards и не запускать двух request owners/controllers.

### Принятая физическая граница

Один PostgreSQL/pgvector и BGE находятся на Kaggle. DevCoveer сохраняет MCP/ASGI, authorization, durable request/receipt control journal и существующий BgeQueue/controller. Notebook устанавливает исходящий WSS к существующему ASGI broker. Прежние рекомендации сохранить запросы только в corpus SQLite, оптимизировать E5 query-first или строить новый scheduler отозваны. E5 удаляется; control journal не становится копией domain data.

На Kaggle совместно учитывать PG connections/transactions/buffers, BGE inference/tokenizer, proof/render, source/archive I/O, импорт и snapshot. На Dev — MCP/event loop, control commits и bounded WSS/request buffers. Раздельный API не означает независимую RAM/CPU/IO ёмкость.

Сохранять один поисковый резерв и кооперативный допуск следующего import batch; дополнительно proof/import получают положительное продвижение с bounded aging. Не ждать permit внутри SQL write transaction. Если измеренный минимальный этап не помещается вместе с serving reserve, уменьшить checkpointed batch или изменить ресурсный envelope и зафиксировать failure; не объявлять starvation нормальной реализацией гарантии.

### Общий lifecycle и клиенты

Fairness — по verified person и eligible dialog heads. Все claim пути, включая explicit ID, wake и resumed checkpoint, используют тот же guard. Контекст следующей реплики строится из durable результата предыдущей; stale generation не может записать новый итог. Dependency wait сохраняет consumer checkpoint и освобождает реально неиспользуемый permit.

Native intake ACK, result commit и final-delivery ACK/readback различаются. При result_ready повторяется доставка/получение, а не исследование и генерация. Unknown send требует reconciliation; отсутствие readback не доказывает неотправку. Capacity wait не расходует outage retry budget. Retry не создаёт новую Story/Variant/case; изменившийся payload под прежним ключом конфликтует.

Сохранить arithmetic ETA: ≤10 секунд без notices, >10 секунд/существенное изменение — durable coalescing только ближайшего вопроса человека; final приоритетнее waiting. Отдельный evidence-search соблюдает защищённый deadline, даже когда полный вопрос ожидает в очереди.

Мира остаётся клиентом Projects Hub; текущий adapter search/evidence-only расширяется на pending/request/result/ACK/proof/stories/cases. Slack использует единственный Kaliningrad Base bot. Второй клиент проверяется реальным result sink/ACK, без нового Slack-only lifecycle.

### Research и приёмка

Facts, dates/calendar, observations, люди/роли и география сохраняются без обязательного Story-parent; main8799 такого независимого пути ещё не даёт. Один research frontier и source/search receipts используются для facts, episodes, POI и независимых disagreements. Связное объяснение практики допустимо без героя, точной даты или конфликта; квот историй нет. Существующие angle/gaps/Variant сохраняются. Частный draft не становится общим вариантом автоматически.

Статический case build учитывается в resource admission. Failure обновления сохраняет разрешённый last-good; desired/published revision и generation fence защищают от stale builder. Отзыв публикации отменяет last-good разрешение. Первый URL требует actual upload/readback.

Проверять exact deployed SHA, WSS reconnect/run replacement, restore/replay, consumer checkpoint, person/dialog fairness, native второй-client ACK, no-repeat Variant, позитивное продвижение proof/import и поиск при каждом тяжёлом этапе. Accepted book text/bibliography публичны без owner ACL; restricted originals/drafts/writes сохраняют отдельную проверку. Девять protected requirements, включая quality/capacity/latency, не ослабляются. Telegram-сессии не блокируют domain/direct-download/proof tests. Публикация документации не означает выполнения этих проверок.

## Preserved implementation and historical record


Дата анализа: 19 октября 2026. Статус: **предлагаемый проект по чтению исходников**, не реализация и не нагрузочная приёмка.

## Короткий вывод

Нужен не новый брокер, а единый допуск пользовательского запроса поверх уже создаваемого в PR #151 долговечного inbox → execution → result → outbox. Для внешнего BGE сохранить существующий BgeQueue и Kaggle worker. Добавить общую идентичность диалога, последовательность, справедливый выбор пользователя, ресурсный допуск стадий, измеряемый прогноз и долговечные уведомления. Индексатор должен использовать остаточную ёмкость того же допуска, сохраняя свои checkpoints и публикацию revision.

Ключевое разделение: **полный пользовательский вопрос может ждать в долговечной очереди; отдельный MCP evidence-search сохраняет защищённый deadline 2 секунды**. Нельзя реализовать новую очередь как длинный sleep внутри search HTTP и нельзя считать ответ pending итоговым ответом на весь вопрос.

Первый самостоятельный участок реализации: обобщить существующий request runtime и его claim/outbox, включить в него Slack и один второй клиент, обеспечить порядок, восстановление и честное ожидание; затем на этой же цепочке включить измеренный ресурсный допуск. Не строить параллельный Slack-only scheduler.

## 1. Покрытие и доказательность

Прочитаны требования [user-request-load-management.md](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/docs/user-request-load-management.md), актуальное дерево main и перечисленные ниже исходники.

Зафиксированные версии:
- main: `a753a4ef84291e73c8cff2c80ef69352d43b2b8e`;
- draft PR #151: `18f16cb23de167cec2f2697640cc79da1577b444`; PR открыт, draft, не merged на момент чтения. [PR](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/151).

Все утверждения о поведении ниже имеют уровень **«механизм в коде»**, если прямо не названы историческим отчётом или предложением. Production SHA, текущая RAM/CPU, реальный трафик, runtime backlog, распределение времени готового ответа и работа конкретной Миры в этом анализе не проверялись. Серверные probes, Kaggle запуска, тестов и изменения runtime не было. Данные PR о прохождении CI не равны нашей проверке и тем более production-приёмке.

## 2. Что уже существует

### 2.1. Долговечность пользовательского запроса: использовать PR #151

В [expert_requests.py, PR SHA](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/18f16cb23de167cec2f2697640cc79da1577b444/src/regional_knowledge/expert_requests.py):
- `rkb_expert_requests` в corpus SQLite сохраняет input, person, origin, state, generation, execution lease/token, immutable result hash/JSON и delivery receipt;
- `accept` проверяет verified Slack observation и identity, ставит synchronous=FULL, дедуплицирует event/message, сохраняет запрос до ACK;
- `claim` использует BEGIN IMMEDIATE, generation/token fencing, разрешает reclaim expired lease;
- `record_result` сохраняет точный результат, повтор той же операции идемпотентен;
- доставка имеет sending/unknown/retry_wait/delivered, детерминированный delivery_id и проверку текущих прав.

В [slack_expert.py, PR SHA](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/18f16cb23de167cec2f2697640cc79da1577b444/src/regional_knowledge/slack_expert.py) уже есть независимые receive/deliver loops, ACK после commit, native echo/readback, Retry-After и запрет слепого повторения uncertain send. В [expert_events.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/18f16cb23de167cec2f2697640cc79da1577b444/src/regional_knowledge/expert_events.py) есть durable generation-bound wake events.

**Чего нет:** claim выбирает ORDER BY created_at,id, в том числе допускает запрос по конкретному request_id. Нет dialog sequence/head guard, fairness, resource admission, ETA, длительного stage waiting/checkpoint/renewal. Приём и `_identity` жёстко завязаны на Slack/app/team. Wake fill сейчас рассматривает все pending, а не только допущенные головы. Без изменения claim фильтра любое клиентское обращение по id обойдёт внешнюю сортировку.

### 2.2. BGE уже имеет правильную внешнюю границу

В [bge_queue.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/bge_queue.py) есть отдельная persistent SQLite/WAL, unique(actor,idempotency), immutable identity, claim token, lease, run lifecycle и quarantine поздних результатов. `claim` сортирует query перед document и допускает не более одного claimed job на ready run. Это **очередь вычисления embedding**, не очередь пользовательских диалогов.

Существующие технические числа, не новые рекомендации:
- очередь незавершённых BGE jobs ограничена 256;
- claim job lease 180 секунд;
- heartbeat loss 120 секунд;
- cold start ограничен 900 секундами;
- run lifetime 11 часов, idle 30 минут, смена начинается в 10 ч 45 мин;
- готовые query jobs сохраняются 24 часа;
- worker кодирует batch=1 и забирает следующую работу после completion.

[Controller](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/scripts/production/bge_kaggle_controller.py) уже различает pending/dispatching/dispatched и reconciles неизвестный launch. [Worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/scripts/production/bge_kaggle_worker.py) возвращает encoder_seconds, heartbeat diagnostics и startup_seconds. Локальный BGE не разрешён независимо от RAM.

### 2.3. Поиск уже возвращает ограниченное ожидание, не полный ответ

[server.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/server.py) передаёт search/knowledge_search в backend. В сигнатуре нет общего dialog_id или client request id. [multilingual_retrieval.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/multilingual_retrieval.py) создаёт actor-scoped query job, ждёт BGE максимум 0,8 секунды и возвращает main_job_id + starting/pending/degraded при незавершённой внешней работе. Query cache не должен стать дедупликацией всего пользовательского вопроса: одинаковый текст в разных контекстах всё ещё разные вопросы.

**Дефект классификации для новой функции:** RuntimeError из enqueue, включая `BGE queue full`, сейчас превращается в main_state=unavailable. Нехватка места в bounded computational queue не доказывает внешний outage. В новом request runtime такой исход должен переводить уже принятый вопрос в ожидание допуска. Нужны типизированные причины, не общий catch RuntimeError.

[Защищённые требования](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/.devcoveer/requirements.json): warm backend p95 ≤1000 мс; public MCP p95 ≤1500 мс; evidence-search hard deadline ≤2000 мс. Новая пользовательская очередь не отменяет эти метрики.

### 2.4. Импорт уже возобновляемый и частично ограниченный

[indexing.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/indexing.py): reconciler сканирует недостающие вектора, actor/revision/hash guards, `BGE_DOCUMENT_WINDOW=64`, приоритет обязательного BGE перед optional E5. Никакой network call под SQLite writer lock при установке remote vector acknowledgments; done document jobs удаляются после установки. Есть уже phase_seconds и progress/stall status. [story_vector_index.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/story_vector_index.py) тоже использует ту же BGE queue: резервирование надо защищать на общей границе enqueue, не только в book-indexer.

[stage_service.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/stage_service.py): staged graph, ограничение stage до 8 страниц, finalize материализует данные и вставляет chunks порциями по 100, затем activation. [sqlite_revision.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/sqlite_revision.py): publication state и атомарное переключение активной revision; pending replacement не должен уничтожать поиск по прежней принятой версии.

Это подтверждает разумные точки **не начинать следующий batch**, но не доказывает безопасность произвольной остановки процесса посередине finalize/render. `SIGSTOP`, kill, удержание writer lock в ожидании ресурса и новая «пауза книги» не нужны по умолчанию.

### 2.5. Локальный ресурс не равен Kaggle

[e5_service.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/e5_service.py) содержит один inference thread и FIFO capacity=10; passage batch ≤4, query batch=1, submit timeout 3 секунды, queue deadline 2,5 секунды. Token-count идёт отдельно через to_thread. Значит query-first в Kaggle не защищает E5 от импортных passage jobs или tokenization, а RAM может меняться не только из-за embedding.

[sqlite_corpus.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/src/regional_knowledge/sqlite_corpus.py) использует WAL и busy_timeout 15 секунд. WAL улучшает сосуществование чтения/записи, но не создаёт двух параллельных writer и не доказывает deadline. PR #151 переносит ожидание BEGIN IMMEDIATE в to_thread в [sqlite_data.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/18f16cb23de167cec2f2697640cc79da1577b444/src/regional_knowledge/sqlite_data.py), что защищает event loop от lock wait, но не от любой синхронной CPU-работы.

[Исторический CPU runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a753a4ef84291e73c8cff2c80ef69352d43b2b8e/docs/operations/retrieval-cpu-scheduling.md) содержит измерения на старом SHA, CPUWeight=250 у MCP, CPUWeight=70/CPUQuota=50% у indexing. Это контрдоказательство тезису «никакой защиты нет», но не актуальное измерение мощности и не гарантия одного поиска при любом импорте.

## 3. Минимальная архитектура

### 3.1. Один владелец request lifecycle

Расширить таблицы и методы `ExpertRequests`, постепенно обобщить название в коде. Не копировать их в новую RequestQueue. Существующие BgeQueue, vector publication, story outbox и Slack delivery adapter остаются специализированными исполнителями, не становятся конкурирующими пользовательскими планировщиками.

Два уровня допуска:
1. Принять и надёжно записать пользовательский вопрос; назначить место внутри диалога.
2. Выдать короткий permit для следующей реально выполняемой стадии по ресурсам и справедливости.

Ожидание BGE сохраняет владение головой диалога, но освобождает локальные CPU/RAM permits, когда стадия действительно освободила память. Не держать поток, semaphore или SQL transaction на время cold start. Не запускать новый executor на каждый внутренний пересчёт ETA.

### 3.2. Общая идентичность для всех клиентов

Добавить к существующей записи:
- `client_kind`, `tenant_id`, verified `person_id`;
- `dialog_key`, `dialog_seq`, `client_request_key` с уникальностью в namespace authenticated client/tenant;
- существующий `origin` как проверенный transport route, не строку назначения от модели;
- `context_cursor`/ссылку на сохранённую историю законченных предыдущих ходов;
- stage/checkpoint, dependency job id, wait reason, next eligible time;
- timestamps accepted/admitted/stage start/stage end/result ready/delivery confirmed;
- ETA revision/last notification и resource permit generation/expiry, если permit имеет отдельную запись.

Ни client-supplied person_id, ни свободный dialog_key не дают доступа к чужому диалогу. Разрешённость dialog/route проверяет сервер. Slack dialog = app/team/channel/thread. Мира/direct MCP используют свой устойчивый conversation id, связанный с authenticated application actor. Один человек в разных клиентах имеет один fairness subject, когда существует verified identity binding; не объединять по имени, email без проверки или сходству текста.

Сохранять request order по атомарно назначенному dialog_seq, не по wall clock и случайному UUID. Повтор той же client_request_key с другим payload возвращает conflict. Одинаковые тексты с разными ключами принимаются как отдельные вопросы.

**Важная граница клиента:** naked search MCP сейчас не знает человеческий диалог и не умеет сам отправлять финальное сообщение в любой клиент. Для полной гарантии Мира и direct clients обязаны поддержать submit/resume + durable result delivery (existing MCP events или authenticated polling), persistent context и route. Обычный HTTP request id не заменяет dialog identity. Для клиентов без push хранить результат до authenticated retrieval/ack; честно не обещать delivery в закрытый клиент без транспортного механизма. Не создавать новый MCP endpoint: расширять контракт на существующем RKB resource. Все существующие search/knowledge_search/expert search entrypoints должны использовать общий stage admission, иначе прямой клиент сможет обойти резерв и исказить ETA. Для уже управляемого запроса передавать проверенный request/generation context, не захватывать второй permit вложенным вызовом. Для низкоуровневого вызова без user-request context применять тот же короткий ресурсный допуск и явный bounded pending, но не приписывать ему несуществующие гарантии финальной разговорной доставки.

### 3.3. Контекст и последовательность

Выбирается только самая ранняя незавершённая запись dialog. Следующий вопрос не исполняется, пока предыдущий ответ не записан и его доставка/доступность в исходном диалоге не подтверждена. Это консервативная первая версия, предотвращающая перестановку ответов и ложный контекст. `unknown` send удерживает диалог до reconciliation, но не блокирует другие диалоги.

Контекст для второй реплики собирается **на старте исполнения**, включая сохранённый итог первой, а не только снимок при приёме второй. Persisted input + durable prior result обеспечивают восстановление; нельзя полагаться исключительно на RAM агента. Поздний результат старой generation не принимается. ACL проверяются вновь на исполнении, чтении source и доставке.

Отказ в доступе/изменение identity не разрешают доставить приватный старый ответ. Сохраняется blocked с reason; безопасное уведомление возможно лишь по ещё разрешённому маршруту. Требование завершения не отменяет безопасность.

## 4. Справедливость без приоритетов пользователей

Предложение для первой версии: work-conserving round robin **по person**, внутри человека round robin по eligible dialog heads. Один пользователь с десятью диалогами не получает десять долей по сравнению с человеком с одним диалогом.

В транзакции claim:
1. Убрать не-головы, already-owned heads и requests с dependency/retry not-before.
2. Найти пользователей с ресурсно допустимой следующей стадией.
3. Выбрать следующего после сохранённого fairness cursor; у него следующую eligible голову диалога.
4. Атомарно сохранить permit/claim/generation и продвинуть cursor.

Сначала дать каждому ожидающему person возможность одного активного продвижения, затем выдавать оставшиеся ресурсы повторными кругами; если ждёт только один person, не оставлять ресурсы пустыми из-за искусственного глобального лимита «одна работа человеку». Один долгий уже выполняемый этап не прерывается ради нового пользователя. При любом следующем освобождении ресурса новый пользователь участвует в очередном круге, не ждёт всю принятую пачку соседа.

Fairness по стартам не гарантирует одинаковые секунды CPU: короткие и длинные вопросы различны. Это осознанный минимальный компромисс. Weighted priorities и сложный deficit scheduler пока не нужны. Чтобы серия лёгких работ не вытеснила тяжёлую навсегда, сохранять age; если самая старая runnable стадия не помещается из-за постоянно запускаемых новых работ, временно не запускать конкурирующие за тот же ресурс стадии, дать уже выполняемым закончиться и накопить её измеренный рабочий объём. Не блокировать независимый внешний ресурс.

Все пути claim, включая claim(request_id), wake-event dispatch и resumed checkpoint, обязаны использовать одинаковый admission guard. Wake event сообщает о работе, но не предоставляет обход fair selection и resource permit.

## 5. Ресурсный допуск

Не вводить заранее «2 запроса на 8 GB». Admission основывается на измерениях текущего окружения и профилях стадий, а не только на числе незавершённых вопросов.

Ресурсы учитывать раздельно:
- локальная доступная память с cgroup limit/current и системным available; RSS/HWM процессов и запас под OS, SQLite, E5, не-RKB нагрузки;
- CPU/cgroup quota, CPU pressure, event-loop lag и наблюдаемая latency при текущей параллельности;
- SQLite writer/transaction duration, WAL growth, disk headroom и I/O pressure;
- E5 единственный encoder, очередь и token-count;
- Kaggle ready/warming/unavailable, фактические worker permits и pending query/document;
- vector DB connection pool/latency и provider Retry-After;
- ограниченное число активных executors и payload bytes, чтобы «ожидание» не стало бесконечным RAM backlog.

Для стадии s измерить верхнюю рабочую оценку дополнительной RAM `m_s` и duration по классу/размеру входа. Admission допускает только если прогноз peak с уже выданными permits плюс safety reserve помещается в текущий budget и measured CPU/latency envelope не нарушен. `available` не расходовать дважды: уже занятая работа учтена в current usage, будущий peak разрешённых стадий учитывать отдельно.

RAM/CPU увеличение перечитывается; повышение concurrency только в пределах измеренного профиля и без latency regression. Снижение ресурсов немедленно запрещает новые тяжёлые starts, не отменяет принятые вопросы и не убивает active transaction. После завершения текущих bounded стадий лимит сходится к новому envelope. Если RAM стала меньше минимального serving footprint, гарантировать поиск физически невозможно: приостановить новые импортные этапы, сохранить вопросы, выдать операторский capacity incident. Не изображать это внешним outage и не обещать адаптацию без нижней границы.

Защита от дрожания: отдельные точки входа/выхода из pressure, минимальное устойчивое окно перед увеличением допуска; численные значения выбрать измерениями. При missing/stale telemetry использовать последний подтверждённый безопасный envelope, отключив расширение; если его нет, необходима калибровка до включения гарантии.

## 6. Как обеспечить хотя бы один поиск во время импорта

Минимальный вариант: **ресурсный резерв для одного поискового пути + кооперативный допуск следующего import batch**, а не полная пауза импорта.

1. Измерить пик и CPU/time profile одного поиска одновременно с каждой существенной стадией импорта: page render, token-count, stage merge/validation, finalize inserts, fragment rebuild, activation, E5 passage, BGE document, archive/proof cache I/O.
2. Не начинать импортный batch, если после его пика не остаётся memory/CPU/connection envelope для одного поиска. Этот резерв держать даже пока пользовательская очередь пуста: иначе пришедший вопрос будет заблокирован уже начавшейся огромной операцией.
3. BGE query-first сохранить. Уже выполняющийся document encode не прерывать; query идёт после него. Наличие 64 document window при total 256 помогает, но общей reservation требует и story indexer/прочие producers. Проверка capacity + enqueue должна быть атомарной; локальный расчёт capacity вне enqueue недостаточен при нескольких producers.
4. В E5 добавить query-first выбор в существующую bounded serial queue либо ограничить passage producers до одного краткого in-flight batch без накопления; первый вариант надёжнее при нескольких producers. Не добавлять второй encoder. Token-count тоже охватить локальным stage budget.
5. При pressure не брать следующий page/batch/документ; довести уже начатую транзакцию до commit/rollback. Не ждать permit внутри SQL write transaction. Не открывать network call под writer lock.
6. Не менять required BGE activation, hashes, ACL, источник и качество импорта. Pending новая revision оставляет старую принятую searchable. Checkpoints и done vectors используются повторно после возобновления.

Точки пауз сначала только между уже существующими durable этапами. Внутренние page/crop циклы finalize не объявлять restart-safe по факту наличия for-loop: если именно их peak нарушает резерв, отдельно выделить минимальный checkpointed batch и доказать idempotency. WAL и низкий CPUWeight сами по себе этого не доказывают.

**Смысл гарантии:** импорт не отбирает всю допустимую мощность поискового пути. Это не обещание мгновенного BGE при cold Kaggle или при внешней аварии. Импорт может продвигаться медленно или временно не получать новых permits под длительной пользовательской нагрузкой; SLA импорта не задан. Старvation импорта измерять и сигнализировать оператору, не отнимать защищённый поисковый резерв тайным правилом.

## 7. Прогноз до ответа и уведомления

### Измерения

Хранить в существующем request events/агрегатах времена по стадиям и e2e: accepted → confirmed final delivery. Не оценивать весь ответ по одному bge_encoder_seconds: есть fair queue, cold start, vector search, source fetch, работа клиента/ответа и доставка. pending jobs и незавершённые requests не считать нулевыми durations. Разделять cold/warm, cache hit/miss, degraded, resource envelope и размер класса.

Предлагаемый стартовый estimator: rolling последние 100 завершённых измерений класса за 24 часа; медиана service time, для пользовательского диапазона эмпирические P50–P90 при достаточном n. При n<20 показывать low-confidence диапазон по калибровочным измерениям, а не статистически убедительный «p95». Это параметры прогноза, не догадка о мощности. Window/n проверяются на ошибке прогноза и могут быть скорректированы до выпуска.

### Арифметика

Симулировать тот же fair scheduler на копии текущих eligible heads, active residual times и текущих resource permits. Для зависимой очереди внутри диалога учитывать предшественников и сохранённый контекст. Последовательность локальных/внешних стадий считать по критическому пути, не складывать параллельные интервалы дважды. `ETA = predicted final delivery timestamp − now`, а не `количество запросов × среднее`.

Остаток длительного этапа оценивать по эмпирическому остаточному времени для его текущего возраста; `max(0, median − elapsed)` не должен бесконечно давать ноль для медленного задания. Для cold Kaggle использовать startup distribution и фактический ready state. Future arrivals неизвестны: прогноз условен на текущем составе очереди и обновляется при изменении.

До включения функции собрать хотя бы начальную калибровку e2e для поддерживаемых классов, включая второй клиент. Если образцов вообще нет, честный текст «время пока не удалось оценить» лучше выдуманной цифры, но **это не полное выполнение согласованного требования приблизительной оценки**: такой класс нельзя объявлять принятым без калибровки. Проверка этого пробела входит в release gate.

### Политика сообщений

- ETA до готового ответа ≤10 секунд: никаких waiting сообщений.
- ETA >10 секунд: один wait notice с примерно оставшимся временем/диапазоном, без LLM.
- Если несколько незавершённых вопросов одного person, уведомлять только ближайший к обработке; выбор глобален по person, не по Slack-треду. Сообщение отправляется только в origin именно этого вопроса, без раскрытия других диалогов.
- Пересчёт внутренний при изменении очереди, ресурсов, стадии/зависимости и на редком timer. Отправка отдельно от пересчёта.
- Предложение для «существенно»: изменение predicted finish относительно ранее сообщённого finish не менее max(10 секунд, 25% от ранее оставшегося времени). Это предотвращает сообщения только из-за естественного countdown.
- Updates объединять; не чаще одного за 30 секунд для одного person, кроме первого wait notice. При ожидающем coalesced update отправлять последнюю оценку, а не историю. Числа 10/25%/30 секунд являются предлагаемой UX-политикой и проверяются fake-clock тестами, не выдаются за уже согласованные ресурсы.
- При сокращении ETA не посылать «ускорились»; готовый итог всегда побеждает pending wait/update. Когда наступает следующий вопрос, применить правило заново.

Чтобы restart и provider retries не умножали сообщения, хранить notification revision и outbox entry транзакционно. Минимально расширить существующий outbox на типы final/wait/eta_update, сохранив result immutable; при необходимости небольшая child message table у того же lifecycle, не второй delivery daemon. Unknown notification delivery тоже reconciles, не blindly resend. Устаревшие ещё не отправленные ETA messages подавляются при result_ready; uncertain already-sent сообщения не «отменяются» фиктивно.

## 8. Недоступность, повтор и восстановление

Различать:
- `capacity_wait`: локальная загрузка, полная computational queue, занятый E5, очередь vector connections. Вопрос хранится, без TTL «повторите позже»;
- `dependency_starting/pending`: Kaggle действительно запускается/работает. ETA включает ожидание; active CPU permit освобождён;
- `dependency_unavailable`: подтверждённая provider failure/отсутствие безопасного пути результата, а не просто высокий elapsed;
- `delivery_retry_wait`: явная неотправка с Retry-After;
- `delivery_unknown`: неизвестный внешний эффект, только reconciliation;
- `needs_recovery`: повторяющаяся внутренняя ошибка/poison stage, запрос сохранён, операторское восстановление, не silent discard.

Не подменять честную разрешённую lexical/E5 деградацию полным отказом, если задача может быть отвечена с явно обозначенной ограниченностью. Но пустой degraded search не доказательство «ничего нет», а незавершённый BGE не итог. Когда без внешнего сервиса нельзя выполнить конкретный вопрос, записать и доставить отдельный терминальный ответ «Сервис временно недоступен, попробуйте позже». После него не начинать бесконечную генерацию новых ответов на тот же request. Отдельно связать accepted request с dependency job: завершение вопроса снимает его интерес к BGE работе. Не отменять общую deduplicated query job, пока её ждёт другой запрос. Для jobs без потребителей нужна безопасная ограниченная retirement policy в существующей BgeQueue: иначе её maintenance продолжит поднимать worker для pending работы даже после терминального пользовательского ответа. Не удалять document jobs, являющиеся publication recovery state, и не менять active claimed effect без fencing.

Предлагаемые границы: remote calls сохраняют существующие bounded deadlines; explicit Retry-After соблюдается; безопасные no-effect/read-only transient retries ограничить тремя попытками стадии с backoff, затем классификация unavailable либо needs_recovery. Для Kaggle warming использовать существующий 900-секундный startup bound, а не три полных cold launches на вопрос. При доказанной недоступности можно завершить раньше. Недостаток мест в очереди никогда не расходует outage retry budget.

Число попыток — ограничитель автоматической работы, не механизм потери accepted request: needs_recovery хранит вход/checkpoint, создаёт операторский сигнал и имеет процедуру исправления/возобновления. Это не считается успешно завершённым запросом до финального результата. Не обещать, что три повтора исправят любой баг.

Execution lease PR151 максимум 300 секунд не покрывает возможные 900 секунд cold start. Нужны checkpoint + release execution lease во время dependency wait либо fenced heartbeat/renewal для фактически работающего executor. Не просто увеличить lease до часа: это делает crash recovery медленным. Все операции после reclaim защищены generation; недоступный worker не должен удерживать permit бесконечно.

После restart:
1. Восстановить pending/heads/fair cursor, проверить истёкшие permits и reclaimable execution leases.
2. Прочитать dependency job по прежнему actor/idempotency/id; не дублировать Kaggle launch. Учитывать существующее 24-часовое удаление готовых query jobs: accepted request должен сохранить достаточный checkpoint/result либо безопасно восстановить read-only embedding по тому же actor/identity, если cache уже удалён; отсутствие cache не означает потерю самого вопроса.
3. Если result_ready, продолжать только delivery, не выполнять вопрос заново.
4. Если sending/unknown, native receipt/echo/readback. Отсутствие ответа readback не равно доказательству неотправки.
5. Сохранить очередность, не пропускать uncertain predecessor. Остальные диалоги продолжаются.

**Непреодолимая граница:** exactly-once видимая доставка не доказуема одной локальной записью при отсутствии provider idempotency/readback. PR уже правильно выбирает unknown вместо слепого resend. Если reconciliation не может установить исход, нужна bounded частота проверок и операторский incident, без удаления записи. «Гарантирован каждый ответ» должна проверяться на поддерживаемых транспортных receipts, а не обещаться магически.

## 9. Ограниченность очереди без тихих отказов

Нельзя одновременно обещать бесконечный приём и конечный диск. Для обычных bursts durable inputs остаются на диске, а в RAM выбирается лишь bounded окно кандидатов. Полезные защитные пределы payload уже есть, их сохранять. Ввести измерение backlog bytes/age, WAL/disk headroom, отдельный резерв для сохранения intake/result/delivery. Не архивировать и не GC незавершённые requests по возрасту.

При жёстком disk-full/неисправности authority нельзя отправлять «принято», пока commit не завершён. Slack не ACK transient commit failure и получает transport retry; direct clients получают явную неуспешную приёмку с idempotency key для повтора. Это авария хранилища до принятия, не обычная очередь и не молчаливая потеря accepted запроса. Результат приёма имеет проверяемый accepted_id. Для принятых requests не вводить drop-oldest, per-user discard или очередь TTL ради красивых метрик.

Не писать query text, provider secrets или приватные origin metadata в публичный GitHub/отчёты. Для метрик достаточно opaque ids, classes, durations, counters; ACL/контент остаются в текущей private SQLite boundary.

## 10. Альтернативы и trade-offs

1. **Новый Redis/Celery/Kafka:** отвергнуть для первой версии. Дублирует inbox, lease и recovery, добавляет эксплуатацию и транзакционную границу с SQLite без измеренной необходимости.
2. **Поставить глобальный semaphore вокруг search:** недостаточно. Не durable, нет per-dialog context/fairness, не переживает restart и скрывает HTTP waits.
3. **Всё сделать через BgeQueue:** неверная единица. Не все вопросы используют BGE; queue не хранит dialog/context/final delivery. Сохранить её только для embedding.
4. **Одна глобальная FIFO:** проще, но нарушает равноправие при пачках одного person; существующий claim ORDER BY заменить на fair heads, а не наслаивать вторую очередь поверх него.
5. **Полностью останавливать импорт при любом вопросе:** слишком грубо, ограничивает throughput без необходимости и опасно при принудительной паузе транзакции. Предпочесть резерв + batch admission; вводить новый checkpoint только при доказанном пике.
6. **Только CPUWeight/Quota:** полезная уже существующая защита, не учитывает RAM, E5 queue, SQLite locks и Kaggle.
7. **Отдельная control SQLite:** может изолировать request writes от больших import writes, но добавляет cross-DB coordination и не нужна без измеренного contention. Сначала использовать corpus SQLite из PR151, короткие транзакции и индексы. Переход рассматривать лишь если сохранение/claim не укладываются при настроенных batches.

Удаляемая дублирующая обязанность: client-side «сам реши когда запускать запрос» и старый unconditional oldest-first claim заменяются единственным server-side fair admission. При этом не удаляются security fences, BGE priorities, required publication gates и unknown-send защита. Новые поля нужны для отсутствующих инвариантов, а не ради универсального framework.

## 11. План реализации без скрытой смены области

1. Перед работой обновить main/PR151 SHAs, проверить migrations и не затирать продолжающуюся expert-разработку. Не считать draft feature deployed. Изолировать совместимый commit/PR для shared request contract; не объявлять весь PR151 готовым из-за этой функции.
2. Обобщить identity/origin adapters, добавить dialog sequence/context и actor round-robin; test explicit-id bypass и cross-client fairness.
3. Обобщить durable result/notification outbox и restart recovery. У клиента Миры определить настоящий result sink и ACK; интеграцию не имитировать Slack.
4. Добавить stage metrics и pressure sampling без управления runtime, выполнить разрешённую калибровку. На этой базе зафиксировать resource envelope, минимальный serving footprint и ETA seeds.
5. Включить admission на тех же claim/batch boundaries; typed capacity/outage reasons; query reserve для BGE/E5/local import stages. Не менять поиск/fusion/chunking под видом scheduling.
6. Выполнить acceptance matrix и контролируемый rollout immutable SHA. Метрики и rollback сохраняют новые durable records, не запускают два scheduler одновременно.

## 12. Приёмка: проверки предложены, не запускались

### Детерминированные тесты с fake clock и simulated resources
- Два зависимых вопроса одного диалога: B не claimable до результата/доставки A; B получает ответ A в контексте, в том числе после restart.
- A отправляет десять запросов, B один; B входит в ближайший fair круг. A создаёт десять диалогов: это не десять fairness subjects.
- Несколько клиентов одного verified человека имеют общий fairness subject; spoofed dialog/person/tenant отвергнуты.
- Явный claim(request_id), wake event, duplicate delivery и retry не обходят admission.
- Разные диалоги реально параллельны при свободных ресурсах; один blocked/unknown dialog не останавливает остальных.
- Рестарт на каждой границе: до/после intake commit, claim, dependency enqueue, result commit, before/after external send. Ни потерянных accepted inputs, ни stale generation writes, ни слепого повторного send.
- ETA 10 секунд включительно даёт ноль notices; >10 — один. Значительное увеличение finish, coalescing, no spam after restart, только ближайший person request; final подавляет ещё не отправленное waiting.
- BGE queue full/E5 queue full → capacity_wait, не outage. Provider failure → ограниченный retry или явный финальный unavailable. pending не считается failed sample.
- Реальное уменьшение available budget: новых тяжёлых starts нет, текущая bounded работа корректно завершает транзакцию, все accepted requests остаются.
- Импортный producer, включая story vectors, не забирает query reserve. Memory reservation атомарна при двух конкурентных claimers.
- ACL revoke после enqueue/после source read/до delivery: приватная информация не утекает; safe recovery не подменяет человека owner credential.

### Калибровка и нагрузочная приёмка в разрешённом окружении

Отдельно измерить cold/warm/cache-hit, короткий retrieval, multi-step evidence fetch/ответ, source proof, импортные стадии, E5 token-count и внешний vector latency. Для каждой клетки фиксировать exact SHA, client, corpus/revision, model/space, actual resource envelope, n, service/queue/e2e durations, RSS/HWM, pressure, SQLite transaction/WAL, concurrency и полезный результат.

Проверки с одним и несколькими пользователями, burst 10+1, длинный/короткий запрос, ресурсное увеличение и снижение; одновременно каждый существенный импортный этап и минимум один поиск. Не только «HTTP 200»: поиск должен дать корректное авторизованное evidence, импорт сохранить прежнюю active revision и затем корректно продолжиться. Проверить body/result readback в исходном диалоге второго клиента, не только backend.

Сохраняются защищённые latency и quality gates; отдельно публикуются queue wait и time to final answer, чтобы очередью не спрятать ухудшение search. Большая пользовательская очередь закономерно увеличивает e2e, но не разрешает многосекундный скрытый search call. Если релевантный gate не проходит, фиксируется failure, не ослабляется порог.

ETA приёмка: backtest на новых, не использованных для калибровки requests, signed error, absolute error, доля попаданий в показанный диапазон, ошибки cold/warm и resource-resize отдельно. Численную цель accuracy нельзя выдать как согласованную пользователем; выбрать техническую цель после baseline и документировать её до rollout. Обязательная базовая проверка: прогноз действительно основан на измерениях и той же fair последовательности, а не на произвольном числе.

## 13. Что остаётся неизвестным и не должно быть обещано по этому документу

- Текущий production SHA и actual VM/cgroup resources, текущая внешняя доступность.
- Точные runtime пики render/finalize/fragment build и writer hold times на большом импорте.
- Длительность end-to-end ответа от Миры/экспертного исполнителя и наличие долговечного result sink.
- Насколько PR151 продвинулся после зафиксированного SHA; его draft acceptance содержит отдельные незавершённые области.
- Реальная ошибка ETA, достаточный безопасный concurrency envelope, требуемые safety margins.
- Полная гарантия unattended delivery при необратимом provider outage/утрате доступа невозможна без операционной процедуры; unknown должен оставаться честным, не стираться.

Эти неизвестные не требуют нового архитектурного обсуждения с нуля. Они превращаются в конечные проверки на разрешённом окружении перед включением гарантии. До них статус проекта: исследовано и предложено; runtime не изменён и нагрузочная готовность не заявлена.
