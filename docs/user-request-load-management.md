# Управление пользовательской нагрузкой РКБ

Обновлено 20.10.2026. Согласованный продуктовый контракт для реализации; этот документ не является нагрузочной приёмкой. Технические детали — [request design](designs/user-request-load-management-20261019.md), полный [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Цель и область

Менеджер обслуживает запросы **базы знаний региона** во всех поддерживаемых клиентах: personal MCP, Slack и Мира в Projects Hub. Он не управляет всеми проектами DevCoveer. Один человек может пользоваться несколькими клиентами и диалогами, оставаясь одним fairness subject.

Если ресурсов достаточно, независимые запросы выполняются параллельно. Обычная нагрузка ставит уже принятый вопрос в durable ожидание; пользователь не обязан отправлять его снова. Нельзя обещать бесконечный приём при конечном диске: до подтверждённого accepted действует проверка хранения и admission.

## Приём, сохранность и завершение

Early local receipt означает pending. Финальный accepted/Saved даётся после remotely confirmed exact input/intent; Saved дополнительно требует domain commit и remotely verified exact outcome/receipt. Результат работы и его receipt защищены тем же recovery contract. Владелец не должен заново повторять подтверждённый вопрос после restart. Accepted model/human data не восстанавливаются новой модельной генерацией.

Raw spool DevCoveer имеет implementation defaults 4h/512 MiB. Это не срок жизни request или private draft. Terminal receipt horizon 30 days относится только к verifiable server-issued request IDs: expired ID никогда не запускает новую execution; arbitrary legacy keys не GC вслепую. Compact UNKNOWN/request/receipt metadata сохраняются до reconciliation; долговременные inputs/results находятся в canonical PG/recovery objects. Local GC после remote seal не ждёт старую daily base.

Каждый принятый вопрос получает собственный итоговый result/delivery outcome. При result_ready повторяется доставка/получение прежнего результата, а не исследование и создание нового Variant. Same key + different payload — conflict. Uncertain external send сохраняется как UNKNOWN с bounded admission/reconciliation per provider/route; весь import не останавливается. Existing operation resolve допускает verified_delivered, not_delivered или abandoned_no_resend только с evidence/operator authority; исход не угадывается. Другие диалоги продолжаются.

## Порядок и равноправие

Внутри диалога запросы исполняются по атомарной последовательности, с durable контекстом предыдущих ответов. Следующий вопрос не обгоняет недоставленный или uncertain predecessor. Context строится на старте исполнения из сохранённой истории, а не только из RAM агента при поступлении вопроса.

Между людьми действует work-conserving fair rotation: пачка из десяти вопросов одного человека не заставляет остальных ждать всю пачку. Десять его диалогов не дают десять независимых долей. На старте роли эксперта и владельца не означают priority пользователя. Все claim paths, включая explicit ID, wake и resume, проходят один guard.

Свободные независимые ресурсы используются: если ждёт один человек, искусственная очередь «только один запрос» не нужна. Долгий уже выполняющийся bounded stage не прерывается ради нового пользователя.

## Измеренный допуск ресурсов

Один existing request lifecycle и BgeQueue/controller распределяют stage permits. PostgreSQL 17/BGE/render/import/snapshot в Kaggle конкурируют за общий CPU/RAM/I/O; DevCoveer имеет отдельный небольшой control envelope. E5 и Supabase не являются target stage/dependency.

Concurrency выбирается по измеренной ёмкости, а не формулой «N запросов на X GB». Увеличение/уменьшение доступных ресурсов перечитывается; снижение запрещает новые тяжёлые starts, но не уничтожает accepted запросы или active transaction. Missing telemetry не даёт расширять допуск.

Во время импорта сохраняется минимум один supported search reserve. Новый import batch допускается между durable checkpoints. Proof/import также получают положительное продвижение через aging. Если минимальный этап не помещается вместе с резервом, это capacity failure; нельзя объявлять бесконечное ожидание нормальной гарантией.

## Прогноз ожидания

- ETA до готового ответа **≤10 секунд**: дополнительных waiting сообщений нет.
- ETA **>10 секунд**: сообщить приблизительное оставшееся время или диапазон.
- Прогноз строится арифметически по измеренным стадиям, фактическому fair порядку и ресурсам; отдельная модель/LLM для ETA не нужна.
- Существенное увеличение прогнозируемого времени отражается без потока сообщений. Частый внутренний пересчёт не означает частую доставку.
- Если у person несколько вопросов, waiting/update относится только к ближайшему к обработке и отправляется только в его origin. По остальным вопросы не теряются; правило включается, когда очередь подходит.
- Готовый итог отправляется сразу и подавляет ещё неотправленные waiting notices. Сообщения «ускорились» или служебная история выполнения не нужны.

Детали measurement window и coalescing заданы как проверяемые implementation defaults в [техническом проекте](designs/user-request-load-management-20261019.md). При отсутствии калибровки нельзя выдавать придуманную цифру и объявлять ETA функцию принятой.

## Capacity wait и внешняя недоступность

Полная computational queue или занятый permit — capacity_wait, не outage. Это ожидание не расходует outage retry budget и не требует повторного вопроса.

Если нужный внешний сервис доказанно недоступен и безопасного пути результата нет, сохранить и доставить один понятный финальный ответ: «Сервис временно недоступен, попробуйте позже». После него не запускать бесконечную генерацию новых ответов на тот же request. Poison stage сохраняется для needs_recovery, а не удаляется после лимита попыток.

Честная lexical degradation допустима, когда результат остаётся полезным, но её ограничения явны. Пустой degraded поиск не доказывает отсутствие сведений. Current capabilities/source withdrawal и audience проверяются на execution, cache reuse и delivery; требование завершить вопрос не разрешает утечку.

## Короткие MCP calls и полный диалог

Evidence-search сохраняет backend p95≤1 s, public MCP p95≤1.5 s и hard deadline2 s. Полный пользовательский вопрос может ждать дольше; queue wait и time to final answer измеряются отдельно. Не ждать десятки секунд внутри search HTTP ради очереди.

Обычный read-only search не зависит от remote journal на critical path. Реальный human turn принимает durable lifecycle; готовые accepted mutations/turns идут в group commit с batching window≤1 s, не обещанием cloud RTT. Клиент без push должен иметь authenticated resume/result retrieval/ACK; простого HTTP request ID недостаточно для обещания финального ответа в закрытый клиент.

Slack использует один Kaliningrad Base bot. Мира расширяет existing PH typed RKB adapter и shared live-interaction. Dot — заменяемый executor, не identity пользователя и не владелец результата.

## Простой пользовательский опыт

Сохраняется схема: вопрос, при необходимости ETA/существенное уточнение, итог. В этой итерации нет отдельного интерфейса очереди, нового priority market или функции отмены запроса. Служебные statuses нужны API/оператору, а не потоку объяснений пользователю.

## Приёмка

Проверить 10+1 burst, несколько диалогов одного person, два клиента одной identity, explicit-ID bypass, context после restart, UNKNOWN одного диалога при продолжающихся остальных, реальные result/hash/native ACK и delayed revoke. Отдельно ETA10 s threshold/coalescing/restart, measured resize, proof/import aging, import+search reserve и typed outage/capacity outcomes.

Protected quality/latency/capacity gates сохраняются. Наличие pending response, green unit suite или emulator успеха не подтверждает пользовательскую функцию. [Исторические требования 19.10](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/user-request-load-management.md) доступны как исходная запись.
