# Технический проект общего request lifecycle и нагрузки

Обновлено 20.10.2026. Предлагаемая реализация согласованных [продуктовых требований](../user-request-load-management.md), не отчёт о deployment/tests. Самодостаточный [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) содержит полный порядок внедрения.

## 1. Reuse и одна authority

Расширить existing request inbox/execution/result/outbox и BgeQueue/controller. Поздний draft PR151, проверявшийся head adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4, уже содержит материал person/dialog/ACK/private-draft guards; перед интеграцией сравнить актуальный diff. Не считать его автоматически merged/deployed и не строить параллельный RequestQueue.

DevCoveer управляет verified identity, request/dialog/fairness, admission, writer/connection authority и доставкой. PostgreSQL 17/pinned pgvector+BGE на Kaggle хранит working domain corpus и исполняет работы. Existing S3ObjectStore предоставляет put/get/delete transport к Yandex; immutable sealing, exact verification, sequence chain и CAS ещё требуется реализовать узко поверх него. MDH patterns можно reuse без нового сервиса.

BgeQueue остаётся очередью computation, а не человеческих диалогов. Specialized import/story/outbox workers исполняют permits одного controller. Direct outbound WSS имеет один dispatcher; HTTP pull не конкурирует с ним.

## 2. Identity, idempotency и границы ACK

В compact request state сохраняются verified person/client/tenant, dialog_key/dialog_seq, server-issued request ID и client event key, immutable input/result refs и hashes, origin route, generation/claim, checkpoint/dependency, next_eligible_at, timestamps и delivery outcome. Raw body вынесен из долгоживущей control БД. Indexed eligible-head reads используют bounded window; unique client namespace и dialog sequence защищают от дублей.

Server проверяет binding provider/tenant/subject→person и право на dialog/route. Client-supplied actor или свободный dialog_key не дают доступ. Одинаковый текст с разными request keys — отдельные вопросы; same key с другим payload — conflict. Embedding cache не является deduplication человеческого вопроса.

| Состояние | Доказательство |
|---|---|
| received/pending | Local receipt с устойчивым correlation/request key; remote durability ещё не обещана. |
| accepted | Exact input/resolved intent remotely verified и включён в непрерывный sealed recovery manifest. Просто orphan upload не принят. |
| applied | PG domain effects, applied_ops и continuous cursor зафиксированы одной transaction. |
| saved/result_ready | Дополнительно remotely verified exact outcome/receipt; это не просто cloud intake. |
| delivered | Native delivery receipt/readback конкретного result hash по текущему разрешённому route. |

Group commit собирает готовые accepted mutations и реальные user turns **не более 1 секунды до flush**. Это batching window, не гарантированный cloud RTT. Failed verification оставляет pending; final accepted/Saved запрещён. Read-only search/candidate fetch не требует remote append на critical path. Managed request передаёт existing context и не создаёт второй request/permit во вложенном tool call.

Raw spool implementation defaults **4 часа / hard 512 MiB**, калибруются до cutover. Local GC после remote seal + verified metadata не ждёт daily base. UNKNOWN, pending requests и unfinished consumers не удаляются по raw TTL. Terminal receipt horizon **30 дней** допустим только для verifiable server-issued request IDs: expired ID не запускает новую execution. Arbitrary legacy idempotency keys не GC вслепую и не трактуются повторно как новый запрос.

## 3. Диалоги и конечные результаты

dialog_seq назначается атомарно, не сортировкой UUID/wall clock. Eligible только первая незавершённая глава диалога. Следующий вопрос ждёт сохранённого результата и подтверждённой доставки/доступности predecessor в исходном dialog. UNKNOWN удерживает только свой dialog; остальные продолжаются.

Контекст собирается на старте из durable input и завершённых prior results. Поздняя generation не записывает новый итог. После result_ready повторяется только retrieval/delivery прежнего hash, не extraction, research или новый Variant.

При изменении capability/identity/route сохраняется blocked reason; безопасное уведомление отправляется только по ещё разрешённому route. Owner credentials не подставляются за эксперта. Отдельно проверяются draft/artifact/audience scopes и source withdrawal перед чтением, warm-cache reuse и delivery.

Personal MCP и Мира должны реально поддерживать submit/resume/result/ACK и устойчивый dialog identity. Naked search HTTP не обеспечивает доставку финального разговорного ответа в закрытый клиент. Клиент без push использует authenticated result retrieval/ACK. Один Slack Kaliningrad Base bot и existing PH adapter разделяют этот lifecycle; Dot — заменяемый executor.

## 4. Fair selection

Начальная политика: work-conserving round robin по **canonical person**, внутри человека — по eligible dialog heads. Десять его диалогов не дают десять долей. Сначала каждый ожидающий person получает возможность продвижения, затем свободная ёмкость раздаётся следующими кругами.

Claim transaction:
1. исключает non-head/already-owned/dependency-not-ready/retry-not-before;
2. выбирает person со следующей resource-eligible стадией после сохранённого cursor;
3. выбирает его eligible dialog head;
4. атомарно фиксирует permit/claim/generation и cursor.

Explicit claim(request_id), wake-event dispatch и resumed checkpoint обязаны проходить тот же guard. Wake notification сообщает о работе, не выдаёт отдельное разрешение. Один долгий running stage не прерывается ради нового человека; следующий освобождённый ресурс участвует в fair круге.

Равенство по стартам не обещает одинаковые CPU-seconds. Weighted priorities/deficit framework сейчас не нужны. Для тяжёлых runnable stages хранить age: если их постоянно вытесняют новые лёгкие работы, временно не запускать конкурирующие starts, дать текущим завершиться и накопить нужный ресурс, сохранив search reserve.

## 5. Stage admission

На Kaggle совместно учитывать PG/BGE/render/source I/O/import/maintenance; на Dev — event loop/control/WSS buffers. E5/Supabase не target dependencies. Один loaded BGE model, bounded PG pool и parallel workers; memory включает model/activations, buffers, все query workspaces, maintenance и OS reserve.

Permit покрывает measured incremental peak и stage duration для класса/размера входа. Already used RAM и будущие peaks не списываются дважды из available. CPU pressure, event-loop lag, PG locks/temp/WAL/disk и actual query latency учитываются вместе. Dependency wait освобождает только фактически неиспользуемые resources, сохраняет checkpoint/dialog ownership; не держать SQL transaction/semaphore ради cold start.

Resize перечитывается. Снижение запрещает новые тяжёлые starts, не уничтожает accepted inputs и не прерывает active transaction. Повышение concurrency допустимо в проверенном envelope. Hysteresis и last-safe bounds защищают от stale telemetry; если минимальный serving footprint не помещается, нужен capacity incident.

Claim по явному ID и прямые search entrypoints не обходят admission. Low-level search без human request context получает тот же короткий ресурсный guard и bounded pending, без ложного обещания финальной разговорной доставки.

## 6. Один поиск во время импорта

Измерить supported search одновременно с каждой тяжёлой стадией: encode, render, source download, stage validation, finalize inserts, activation, index maintenance и snapshot. Резервировать его envelope даже при пустой user queue.

Не брать следующий import/story batch, если он съест этот резерв. Query-first на общей BgeQueue boundary действует для всех producers; capacity check+enqueue атомарны. Уже running encode завершается. Safe pause только между durable checkpoints; не использовать SIGSTOP/kill и не ждать permit под writer lock.

Proof/import получают bounded aging и положительное продвижение. Если минимальный batch не помещается вместе с search reserve, выделить меньший доказуемо restart-safe batch или увеличить measured envelope. Starvation не объявлять гарантией. Pending replacement сохраняет прежнюю active revision; resumed job использует установленные hashes/results и не кодирует весь corpus снова.

## 7. ETA без LLM

Измерять accepted→confirmed final delivery и отдельные queue/service stages. Разделять cold/warm, cache hit/miss, degraded, input class и resource envelope. Pending/error не становится нулевой duration.

Implementation starting defaults: последние 100 завершённых измерений класса за 24 h; median service time, при достаточной выборке P50–P90 диапазон. При n<20 использовать low-confidence range из калибровки, а не фиктивный p95. Параметры проверяются held-out ETA error до rollout; это не назначение concurrency наугад.

Estimator симулирует тот же fair scheduler по eligible heads, active residuals и permits. Учитывает predecessors/context, критический путь стадий и фактический cold-start state, не складывает параллельные интервалы дважды. Остаток медленного задания не обязан стать нулём после median elapsed. Future arrivals неизвестны: прогноз условен на нынешней очереди.

Политика:
- ETA≤10 s: waiting notice отсутствует; >10 s: приблизительный диапазон/время.
- Waiting/update только ближайшего request данного person, только в его origin, без раскрытия других диалогов.
- Начальное определение существенного изменения: predicted finish сдвинулся на max(10 s,25% прежнего remaining).
- Coalescing не чаще одного update/30 s на person, кроме первого notice; уведомление содержит последнюю оценку.
- Не отправлять «ускорились»; final result подавляет ещё неотправленные wait/update.
- Notification revision и outbox фиксируются durable; restart/retry не множат сообщения. UNKNOWN notice также reconciles.

Без начальной калибровки нельзя выдумывать ETA и считать эту функцию принятой. Не вводить UI управления очередью или отмену запроса в эту итерацию.

## 8. Отказы и восстановление

| Причина | Поведение |
|---|---|
| capacity_wait | Обычная нагрузка/полная computational queue; accepted request сохраняется, outage retry budget не расходуется. |
| dependency_starting/pending | Реальная provisioning/work dependency; checkpoint и освобождение неиспользуемого permit, честный ETA. |
| dependency_unavailable | Доказанная невозможность безопасного результата; bounded retry или один terminal «Сервис временно недоступен, попробуйте позже». |
| delivery_retry_wait | Явная неотправка; соблюдать Retry-After. |
| delivery_unknown | Bounded admission/reconciliation per provider/route; whole import не останавливается. Native receipt/echo/readback, без blind resend или новой генерации. |
| needs_recovery | Poison stage/internal failure; input/checkpoint остаются, операторский recovery, не успешное завершение. |

Existing operation resolve принимает verified_delivered, not_delivered либо abandoned_no_resend с проверяемым evidence и operator authority. Отсутствие readback не доказывает not_delivered, а abandonment не разрешает resend. Ограниченные UNKNOWN места конкретного provider/route не блокируют независимые routes, read-only search или весь import.

Три безопасных no-effect/read-only transient attempts с backoff — начальный технический предел, не обещание исправить любой сбой. Kaggle warming ограничен одним lifecycle controller, не тремя cold launches на вопрос. Execution lease renew/release относится к реально работающему stage, а не увеличивается до часа ради внешнего ожидания.

После restart восстановить control checkpoint 15 min и remote tail; reclaim expired generations, проверить dependencies по прежним IDs, продолжить checkpoints. Result_ready отправляется повторным retrieval/delivery, sending/unknown сначала reconciles. Исчезновение bounded query cache не теряет human request; можно восстановить read-only embedding, но нельзя заново сгенерировать принятые source/model решения.

Request завершение освобождает его интерес к shared job, а не удаляет работу других consumers. Document publication recovery jobs нельзя retired как устаревший query cache. Exactly-once внешняя доставка без provider idempotency/readback не объявляется гарантией; unresolved UNKNOWN остаётся компактным metadata до разбирательства.

## 9. Remote recovery chain

Каждый immutable segment содержит first/last sequence, count, payload hashes, codec/schema и previous-segment hash. Manifest CAS продвигает только проверенный непрерывный chain; orphan upload не accepted. Remote receipt/state соответствует exact operation/result, а не приблизительному «upload был». Bounded sealed-tail compaction под existing controller объединяет segments в immutable bundles, сохраняет logical IDs/hashes/seq и CAS refs; old objects удаляются после verification/dependency check, не накапливаются по 86k в день.

Control checkpoint 15 min не откладывает фиксацию actual grants/revocations/accepted turns. Changed PG base 4 h, раньше по replay/byte pressure/planned handover; daily Yandex base независима от Kaggle. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers, local raw GC уже sealed bytes этого не ждёт. Base и tail в одном Yandex имеют общий failure domain.

Canonical outcome hash включает exact domain IDs/body/evidence, исключая derived BGE floats/FTS/crops. Dirty projections восстанавливаются отдельно; canonical replay не ждёт dense rebuild. Durable accepted BGE artifacts можно reuse. Existing delivered artifact bytes/hash/receipt остаются exact и не подменяются regenerated bytes.

Большие base bytes идут напрямую Kaggle→recovery object по scoped key+epoch+expiry upload authorization без long-lived delete/global credentials; Dev владеет manifest/GC. Presigned URL не отзывается автоматически при новом epoch, поэтому orphan upload от старого writer не имеет права на promotion. Conditional CAS support проверяется runtime probe; generic S3 put не является CAS.

До hard cap и исчерпания disk reserve admissions ограничиваются. Нельзя ACK accepted при failed remote seal или удалить accepted работу ради красивых метрик. UNKNOWN metadata и arbitrary legacy keys не чистятся по 30 day TTL. Content/credentials/private origins не выводятся в public metrics.

## 10. Приёмка

Детерминированно проверить dialog order/context,10+1 burst, несколько клиентов одного person, explicit-ID/wake/resume bypass, ресурсный resize, race двух claimers, ETA10 s boundary/coalescing и final priority. Внешние effects проверить на boundaries до/после remote seal, domain commit, outcome seal, send/readback. Отдельно wrong generation, orphan segment/CAS conflict/gap, receipt expiry never-new-execution и arbitrary legacy keys.

В разрешённом окружении измерить реальные stages/ETA, import+search+proof и maintenance overlap, positive progress, actual второй-client result/hash/native ACK и current revoke. Case page build использует тот же permit/fence: desired/published revision, last-good при failed update, readback первого URL; withdrawal отменяет право на last-good.

Protected backend p95≤1000ms/MCP≤1500ms/hard2000 ms сохраняются. Queue wait и final answer time показываются отдельно; ожидающий полный вопрос не оправдывает скрытый долгий search call. Quality/capacity gates — [mass readiness](../mass-ingestion-readiness.md). [Исторический исходный проект](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/designs/user-request-load-management-20261019.md) доступен для сравнения механизма, не как конкурирующая target архитектура.
