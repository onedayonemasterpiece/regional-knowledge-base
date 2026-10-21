# Технический проект общего request lifecycle и нагрузки

Обновлено 20.10.2026. Предлагаемая реализация согласованных [продуктовых требований](../user-request-load-management.md), не отчёт о deployment/tests. Самодостаточный [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) содержит полный порядок внедрения.

## 1. Reuse и одна authority

Расширить existing request inbox/execution/result/outbox и BgeQueue/controller. Поздний draft PR151, проверявшийся head adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4, уже содержит материал person/dialog/ACK/private-draft guards; перед интеграцией сравнить актуальный diff. Не считать его автоматически merged/deployed и не строить параллельный RequestQueue.

DevCoveer управляет verified identity, request/dialog/fairness, admission, writer/connection authority и доставкой. PostgreSQL 17/pinned pgvector+BGE на Kaggle хранит working domain corpus и исполняет работы. Existing S3ObjectStore предоставляет put/get/delete transport к Yandex; immutable sealing, exact verification, sequence chain и CAS ещё требуется реализовать узко поверх него для durable входов/эффектов/результатов, а не для каждого read. MDH patterns можно reuse без нового сервиса.

BgeQueue остаётся очередью computation, а не человеческих диалогов. Specialized import/story/outbox workers исполняют permits одного controller. В той же admission authority read_query получает bounded RAM permit, без отдельной persisted queue или нового scheduler. Direct outbound WSS имеет один dispatcher; HTTP pull не конкурирует с ним.

Dot→tools и Mira→tools — независимые умные маршруты; Dot сохраняется, но не вставляется в обычный запрос Миры. Физический путь: PH typed provider→existing Dev auth/admission→WSS, инициированный notebook. Существующие MCP/OAuth/control ownership не переносятся в новый proxy. Один typed query RPC исполняет на Kaggle encode+SQL+bounded hydrate; vector1024 не возвращается на Dev ради следующего SQL вызова. No HTTP self-calls, второй WSS relay или public notebook listener. Default — существующие Python ASGI/controller и pinned native PG/Torch; смена языка может только заменить измеренно дорогой участок без второй authority/численного контракта/лишнего hop.

## 2. Identity, idempotency и границы ACK

У durable_question/request и mutation/provider-effect в compact request state сохраняются verified person/client/tenant, dialog_key/dialog_seq, server-issued request ID и client event key, immutable input/result refs и hashes, origin route, generation/claim, checkpoint/dependency, next_eligible_at, timestamps и delivery outcome. Raw body вынесен из долгоживущей control БД. Indexed eligible-head reads используют bounded window; unique client namespace и dialog sequence защищают от дублей. Приведённая ниже таблица относится к этим durable режимам.

Server проверяет binding provider/tenant/subject→person и право на dialog/route. Client-supplied actor или свободный dialog_key не дают доступ. Одинаковый текст с разными request keys — отдельные вопросы; same key с другим payload — conflict. Embedding cache не является deduplication человеческого вопроса.

| Состояние | Доказательство |
|---|---|
| received/pending | Local receipt с устойчивым correlation/request key; remote durability ещё не обещана. |
| accepted | Exact input/resolved intent remotely verified и включён в непрерывный sealed recovery manifest. Просто orphan upload не принят. |
| applied | PG domain effects, applied_ops и continuous cursor зафиксированы одной transaction. |
| saved/result_ready | Дополнительно remotely verified exact outcome/receipt; это не просто cloud intake. |
| delivered | Native delivery receipt/readback конкретного result hash по текущему разрешённому route. |

Group commit собирает готовые durable inputs/resolved intents **не более 1 секунды до flush**. Это batching window, не гарантированный cloud RTT. Failed verification оставляет pending; final accepted/Saved запрещён. Готовый read_query этого окна не ждёт. Managed request передаёт existing context и не создаёт второй human request/permit во вложенном tool call.

Raw spool implementation defaults **4 часа / hard 512 MiB**, калибруются до cutover. Local GC после remote seal + verified metadata не ждёт daily base. UNKNOWN, pending requests и unfinished consumers не удаляются по raw TTL. Terminal receipt horizon **30 дней** допустим только для verifiable server-issued request IDs: expired ID не запускает новую execution. Arbitrary legacy idempotency keys не GC вслепую и не трактуются повторно как новый запрос.

### Ephemeral read protocol

Trusted operation mode выбирается существующим dispatcher: read_query, durable_question/request либо mutation/provider-effect. Клиент не может объявить effect чтением. Для read_query разрешены QUERY/QUERY_RESULT/CANCEL_QUERY; RAM state содержит request_id корреляции, verified person/scope, deadline, turn/connection generation, требуемые auth/grant epochs и min_data_seq. Нет per-read durable row, fsync, remote intake/seal, RESUME, saved status или resultSaved. Потеря процесса завершает попытку; её request_id не означает сохранённый ответ.

Ready read немедленно получает доступный search permit. Encode и SQL/hydrate исполняются рядом с PG; lexical/alias ветки могут перекрываться с encode в пределах измеренного общего CPU budget. Не вводить фиксированное ожидание micro-batch. Bounded response и приоритет control frames не дают длинному evidence payload вытеснить keepalive/cancel. Полные PDF/images не идут в QUERY_RESULT. Не держать event loop на синхронном inference/SQL/serialization и не добавлять self-HTTP ради разделения функций.

Deadline/cancel/new generation исключает позднюю выдачу. Отменять SQL/не начатое вычисление можно только в границах принадлежащего read consumer; уже running/shared encode не уничтожает работу других consumers. Reconnect не replay/resume старый read. Не строить каскад автоматических повторов на PH/Dev/notebook; любой допустимый доказанно no-start retry укладывается в исходный deadline, а возвращённый pending/degraded не запускает скрытый новый вопрос. Cancel транспортного read не меняет durable mutation receipt, delivery ACK или UNKNOWN.

## 3. Диалоги и конечные результаты

Для durable вопросов/эффектов dialog_seq назначается атомарно, не сортировкой UUID/wall clock. Eligible только первая незавершённая глава диалога. Следующий вопрос ждёт сохранённого результата и подтверждённой доставки/доступности predecessor в исходном dialog. Этот guard порядка удерживает свой dialog; проверка неопределённых изменений общего ресурса для других читателей действует отдельно.

Read_query не получает durable dialog_seq. Existing control index использует canonical person и typed resource/scope dependencies unresolved mutations для всех маршрутов Dot/Mira/MCP, не новый journal. На фактическом старте read и перед выдачей пересечение с pending/UNKNOWN mutation даёт prior_write_pending и observed freshness, без latest/definitive not-found. Для общих ресурсов guard охватывает всех затронутых читателей. Статические dependency classes отделяют accepted-book search от unrelated private-draft write; недоказанную независимость считать зависимостью. Read сам не становится UNKNOWN. После подтверждённого save min_data_seq должен быть достигнут данными и нужной projection; иначе bounded pending/unavailable. Stale connection/turn generation исключает поздний payload; текущая policy проверяется по §5. Незатронутые работы продолжаются. См. [handoff §§5.2, 10.2](../prompts/rkb-scalable-kaggle-implementation-20261019.md).

Контекст собирается на старте из durable input и завершённых prior results. Поздняя generation не записывает новый итог. После result_ready повторяется только retrieval/delivery прежнего hash, не extraction, research или новый Variant.

При изменении capability/identity/route сохраняется blocked reason; безопасное уведомление отправляется только по ещё разрешённому route. Owner credentials не подставляются за эксперта. Отдельно проверяются draft/artifact/audience scopes и source withdrawal перед чтением, warm-cache reuse и delivery.

Для durable режима personal MCP и Мира должны реально поддерживать submit/resume/result/ACK и устойчивый dialog identity. Naked search HTTP не обеспечивает доставку финального разговорного ответа в закрытый клиент. Клиент без push использует authenticated result retrieval/ACK. Один Slack Kaliningrad Base bot и existing PH adapter разделяют authority; Dot остаётся самостоятельным умным executor, не обязательным intermediary Миры.

PH current main940864a имеет injected provider и strict двухполевый normalizer evidence/mode. Target v2 явно передаёт execution mode/status, freshness/min-cursor, request correlation/generation и continuation/detail refs. Durable ACK/resume не приписываются read. Compatibility adapter v1 отдаёт success лишь для полностью представимого результата; иначе existing unavailable/error, не empty hybrid и не исчезновение security/freshness. Сохраняются default3/max5 evidence, query≤1000 characters, text≤8000/item и≤24000 total, metadata≤12000 bytes/object и прежние depth/string/container bounds. Complete BGE input≤512 pinned-tokenizer tokens; нет silent truncation. Bounded excerpt помечается и ведёт к exact saved body/details. Реальный provider, actual body/image readback и две user identities нужно доказать отдельно от наличия interface/health200.

PH existing private tool/utterance store асинхронно сохраняет exact ordered evidence/context refs по `(session, turn_generation, tool_call_id)` с committed-turn binding и disposition. Ephemeral read не ждёт checkpoint; crash gap явно даёт context_gap/evidence_context_unavailable. Нельзя привязать orphan по времени или выдать новый ranking за прежние источники. Continuation/context ref не capability; reuse снова проходит policy. Короткие bounded/coalesced записи не должны задерживать обязательную utterance фиксацию под общим lock. Это dialog recovery, не lossy telemetry; durable result/context сохраняет §6 ACK chain. См. [handoff §10.4](../prompts/rkb-scalable-kaggle-implementation-20261019.md).

## 4. Fair selection

Начальная политика: work-conserving round robin по **canonical person**, внутри человека — по eligible dialog heads. Десять его диалогов не дают десять долей. Сначала каждый ожидающий person получает возможность продвижения, затем свободная ёмкость раздаётся следующими кругами.

Claim transaction:
1. исключает non-head/already-owned/dependency-not-ready/retry-not-before;
2. выбирает person со следующей resource-eligible стадией после сохранённого cursor;
3. выбирает его eligible dialog head;
4. атомарно фиксирует permit/claim/generation и cursor.

Explicit claim(request_id), wake-event dispatch и resumed checkpoint обязаны проходить тот же guard. Wake notification сообщает о работе, не выдаёт отдельное разрешение. Один долгий running stage не прерывается ради нового человека; следующий освобождённый ресурс участвует в fair круге.

Для read_query тот же controller ведёт bounded RAM waiters/permits и person fairness; он не пишет cursor/claim в SQLite для каждого чтения и не ставит read за durable import head. Это один resource admission algorithm с mode-specific persistence. Несколько клиентов человека не увеличивают его долю, read не обходит CPU search reserve и проверки previous-write freshness. Ready dispatch не задерживается ради накопления batch или искусственного последовательного обслуживания одного свободного ресурса.

Равенство по стартам не обещает одинаковые CPU-seconds. Weighted priorities/deficit framework сейчас не нужны. Для тяжёлых runnable stages хранить age: если их постоянно вытесняют новые лёгкие работы, временно не запускать конкурирующие starts, дать текущим завершиться и накопить нужный ресурс, сохранив search reserve.

## 5. Stage admission

На Kaggle совместно учитывать PG/BGE/render/source I/O/import/maintenance; на Dev — event loop/control/WSS buffers. E5/Supabase не target dependencies. Один loaded BGE model, bounded PG pool и parallel workers; memory включает model/activations, buffers, все query workspaces, maintenance и OS reserve.

Permit покрывает measured incremental peak и stage duration для класса/размера входа. Already used RAM и будущие peaks не списываются дважды из available. CPU pressure, event-loop lag, PG locks/temp/WAL/disk и actual query latency учитываются вместе. Dependency wait освобождает только фактически неиспользуемые resources, сохраняет checkpoint/dialog ownership; не держать SQL transaction/semaphore ради cold start.

Resize перечитывается. Снижение запрещает новые тяжёлые starts, не уничтожает accepted inputs и не прерывает active transaction. Повышение concurrency допустимо в проверенном envelope. Hysteresis и last-safe bounds защищают от stale telemetry; если минимальный serving footprint не помещается, нужен capacity incident.

Claim по явному ID и прямые search entrypoints не обходят admission. Low-level read_query без managed human request context получает тот же короткий RAM resource guard и bounded status, без ложного обещания финальной разговорной доставки. Compact verified identity/scope/epochs заменяют per-query whole-corpus allowed-ID enumeration. Kaggle делает indexed selection и batched current top-K/dependency eligibility; cache не является authority. Если projection authority отстаёт от требуемого epoch, синхронизировать в пределах deadline либо fail closed; Dev проверяет актуальность перед выдачей.

Scoped policy delta сверяется с полными source/variant/grant dependencies каждого item, включая cross-book body/summary и bibliography. Отозванная dependency исключает весь affected item с display/continuation, не только цитату; оставшийся ответ — degraded/evidence_withdrawn даже при нуле items, не empty ok. Незатронутые reads продолжаются. Compacted delta history, неполный provenance или недоказанный актуальный scope требуют fail closed для затронутого scope/result; свежий cursor не заменяет эту проверку. Existing barrier и exact intent становятся remote-durable до подтверждения блокировки новой RKB-выдачи, без ожидания Kaggle. PG withdrawal/public purge остаются pending до commit/readback и только затем получают соответствующий Saved/подтверждение очистки. Policy mutation не добавляет seal на обычный read. См. [handoff §§6, 10.1](../prompts/rkb-scalable-kaggle-implementation-20261019.md).

## 6. Один поиск во время импорта

Измерить supported search одновременно с каждой тяжёлой стадией: encode, render, source download, stage validation, finalize inserts, activation, index maintenance и snapshot. Резервировать CPU, BGE/SQL, RAM и I/O envelope даже при пустой user queue; свободный слот без CPU не считается резервом. Совместно ограничить Torch threads, PG parallel workers и прочую работающую нагрузку по фактической quota, а не предположению о числе ядер.

Не брать следующий import/story batch, если он съест этот резерв. Query-first на общей controller/BgeQueue boundary действует для всех producers; durable capacity check+enqueue атомарны, read admission/permit атомарен в той же authority без SQLite write. Уже running encode завершается. Готовый read не ждёт фиксированного micro-batch window. Safe pause только между durable checkpoints; не использовать SIGSTOP/kill и не ждать permit под writer lock.

Proof/import получают bounded aging и положительное продвижение. Если минимальный batch не помещается вместе с search reserve, выделить меньший доказуемо restart-safe batch или увеличить measured envelope. Starvation не объявлять гарантией. Pending replacement сохраняет прежнюю active revision; resumed job использует установленные hashes/results и не кодирует весь corpus снова.

text_search_ready и archive_ready независимы в existing run: verified accepted text может обслуживаться раньше готовности archive transfer. Proof/original требует фактически доступный verified asset. Owner-authorized Kaggle run cap и exclusive leases реальных Telegram sessions сохраняются; новый reader run не создаётся автоматически ради latency.

## 7. ETA без LLM

Для durable режима измерять accepted→confirmed final delivery и отдельные queue/service stages. Для read отдельно измерять полный deadline path и stage durations, не фиктивный saved/delivery lifecycle. Разделять cold/warm, cache hit/miss, degraded, input class и resource envelope. Pending/error не становится нулевой duration.

Implementation starting defaults: последние 100 завершённых измерений класса за 24 h; median service time, при достаточной выборке P50–P90 диапазон. При n<20 использовать low-confidence range из калибровки, а не фиктивный p95. Параметры проверяются held-out ETA error до rollout; это не назначение concurrency наугад.

Estimator симулирует тот же fair scheduler по eligible heads, active residuals и permits. Учитывает predecessors/context, критический путь стадий и фактический cold-start state, не складывает параллельные интервалы дважды. Остаток медленного задания не обязан стать нулём после median elapsed. Future arrivals неизвестны: прогноз условен на нынешней очереди.

Политика:
- ETA≤10 s: waiting notice отсутствует; >10 s: приблизительный диапазон/время.
- Waiting/update только ближайшего request данного person, только в его origin, без раскрытия других диалогов.
- Начальное определение существенного изменения: predicted finish сдвинулся на max(10 s,25% прежнего remaining).
- Coalescing не чаще одного update/30 s на person, кроме первого notice; уведомление содержит последнюю оценку.
- Не отправлять «ускорились»; final result подавляет ещё неотправленные wait/update.
- Notification revision и outbox фиксируются durable; restart/retry не множат сообщения. UNKNOWN notice также reconciles.

Без начальной калибровки нельзя выдумывать ETA и считать эту функцию принятой. Политика notices относится к durable вопросу, не обещает отложенную доставку ephemeral read. Не вводить UI управления очередью или пользовательскую отмену durable запроса в эту итерацию; transport CANCEL_QUERY для read deadline/barge-in необходим отдельно.

### Телеметрия вне критического пути

READ diagnostics идут в отдельный bounded RAM buffer→batched sink, не в queue SQLite файл/write lock, не под await remote append и не перед QUERY_RESULT. Сохранять safe correlation/refs, sizes, mode, freshness и durations; тела query/evidence/drafts, credentials и private origins не логировать, включая notebook stdout. Drop-on-full со счётчиком допустим; crash loss telemetry явно не равен потере domain data. Не заводить второй надежный event log ради read metrics.

PH `_mark_semantic_observed`, utterance commit/recovery/checkpoints и durable accepted/outcome/effect ACK не относятся к disposable telemetry. Сохранять их семантику. Измерять tool_lock_wait отдельно: execution duration после захвата lock его не включает. Bounded parallel независимых reads допускается через existing admission; blanket удаление session/tool locks не должно дать effects обогнать диалог или нарушить native audio/utterance lifecycle. Host-local monotonic stage durations сопоставляются по request_id, а не вычитанием несинхронных часов разных hosts.

## 8. Отказы и восстановление

| Причина | Поведение |
|---|---|
| capacity_wait | Обычная нагрузка/полная computational queue; accepted request сохраняется, outage retry budget не расходуется. |
| dependency_starting/pending | Реальная provisioning/work dependency; checkpoint и освобождение неиспользуемого permit, честный ETA. |
| dependency_unavailable | Доказанная невозможность безопасного результата; bounded retry или один terminal «Сервис временно недоступен, попробуйте позже». |
| delivery_retry_wait | Явная неотправка; соблюдать Retry-After. |
| delivery_unknown | Bounded admission/reconciliation per provider/route; whole import не останавливается. Native receipt/echo/readback, без blind resend или новой генерации. |
| needs_recovery | Poison stage/internal failure; input/checkpoint остаются, операторский recovery, не успешное завершение. |

Эта таблица задаёт durable recovery. У read_query нет отдельного UNKNOWN, needs_recovery row или отложенного RESUME: клиент получает bounded status, а stale response отбрасывается. prior_write_pending относится к известной неопределённой предыдущей записи, не к побочному эффекту самого чтения. Если attachment уже доказанно отсутствует, вернуть честную unavailable/degraded возможность сразу, не тратить весь hard deadline на заведомое ожидание.

Existing operation resolve принимает verified_delivered, not_delivered либо abandoned_no_resend с проверяемым evidence и operator authority. Отсутствие readback не доказывает not_delivered, а abandonment не разрешает resend. Ограниченные UNKNOWN места конкретного provider/route не блокируют независимые routes, незатронутый read-only search или весь import.

Для durable workflow три безопасных no-effect transient attempts с backoff — начальный технический предел, не обещание исправить любой сбой. Это не три последовательных deadline для ephemeral read и не разрешение повторять UNKNOWN effect. Kaggle warming ограничен одним lifecycle controller, не тремя cold launches на вопрос. Execution lease renew/release относится к реально работающему stage, а не увеличивается до часа ради внешнего ожидания.

После restart восстановить durable control checkpoint 15 min и remote tail; reclaim expired generations, проверить dependencies по прежним IDs, продолжить checkpoints. Result_ready отправляется повторным retrieval/delivery, sending/unknown сначала reconciles. Исчезновение bounded query cache не теряет durable human request; можно восстановить read-only embedding под прежним управляемым запросом, но нельзя заново сгенерировать принятые source/model решения. Ephemeral reads в recovery chain не включаются и в новую generation автоматически не переносятся.

Request завершение освобождает его интерес к shared job, а не удаляет работу других consumers. Document publication recovery jobs нельзя retired как устаревший query cache. Exactly-once внешняя доставка без provider idempotency/readback не объявляется гарантией; unresolved UNKNOWN остаётся компактным metadata до разбирательства.

## 9. Remote recovery chain

Recovery chain относится к durable input/intent/outcome/control state, не к ephemeral QUERY/QUERY_RESULT или необязательным read logs. Каждый immutable segment содержит first/last sequence, count, payload hashes, codec/schema и previous-segment hash. Manifest CAS продвигает только проверенный непрерывный chain; orphan upload не accepted. Remote receipt/state соответствует exact operation/result, а не приблизительному «upload был». Bounded sealed-tail compaction под existing controller объединяет segments в immutable bundles, сохраняет logical IDs/hashes/seq и CAS refs; old objects удаляются после verification/dependency check, не накапливаются по 86k в день.

Control checkpoint 15 min не откладывает фиксацию actual grants/revocations/accepted turns. Changed PG base 4 h, раньше по replay/byte pressure/planned handover; weekly Yandex backup подтверждённой Dataset version независим от Kaggle. Dataset является primary persistence; существующий online S3 tail не переутверждается молча. Один active owner-only import делит ресурсы с reads, второй import lease не выдаётся. Remote-tail GC покрывает designated retained verified baselines и unfinished consumers, local raw GC уже sealed bytes этого не ждёт. Base и tail в одном Yandex имеют общий failure domain.

Canonical outcome hash включает exact domain IDs/body/evidence, исключая derived BGE floats/FTS/crops. Dirty projections восстанавливаются отдельно; canonical replay не ждёт dense rebuild. Durable accepted BGE artifacts можно reuse. Existing delivered artifact bytes/hash/receipt остаются exact и не подменяются regenerated bytes.

Большие base bytes идут напрямую Kaggle→recovery object по scoped key+epoch+expiry upload authorization без long-lived delete/global credentials; Dev владеет manifest/GC. Presigned URL не отзывается автоматически при новом epoch, поэтому orphan upload от старого writer не имеет права на promotion. Conditional CAS support проверяется runtime probe; generic S3 put не является CAS.

До hard cap и исчерпания disk reserve admissions ограничиваются. Нельзя ACK accepted при failed remote seal или удалить accepted работу ради красивых метрик. UNKNOWN metadata и arbitrary legacy keys не чистятся по 30 day TTL. Content/credentials/private origins не выводятся в public metrics.

## 10. Приёмка

Детерминированно проверить dialog order/context,10+1 burst, несколько клиентов одного person, explicit-ID/wake/resume bypass, ресурсный resize, race двух claimers, ETA10 s boundary/coalescing и final priority. Внешние effects проверить на boundaries до/после remote seal, domain commit, outcome seal, send/readback. Отдельно wrong generation, orphan segment/CAS conflict/gap, receipt expiry never-new-execution и arbitrary legacy keys.

В разрешённом окружении измерить реальные stages/ETA, import+search+proof и maintenance overlap, positive progress, actual второй-client result/hash/native ACK и current revoke. Case page build использует тот же permit/fence: desired/published revision, last-good при failed update, readback первого URL; withdrawal отменяет право на last-good.

Read acceptance: ready dispatch без fsync/remote seal/fixed batch window; одна RAM admission authority и CPU search reserve; отсутствие vector round-trip/self-HTTP/второго WSS relay; deadline/cancel/old-generation output; reconnect без RESUME; min_data_seq и cross-route/shared-resource prior_write_pending при продолжающемся unrelated draft/book search; current top-K eligibility/cache/revoke/withdrawal целого cross-book item, compacted policy fail closed; strict v1 refusal против v2 status; сохранённый body/details и explicit oversize behavior. PH восстанавливает exact ordered refs по ключу turn/call либо явный context gap, без нового ranking вместо старого. Затопленный/сломанный telemetry sink не держит write lock и не задерживает read/durable effect. PH lock-wait и first-useful/full-answer timings измеряются отдельно. Green tests не подтверждают реальную provider wiring.

После подключения реального PH provider провести [сравнительную PMI](../testing/rkb-scalable-kaggle-acceptance-20261019.md) Mira/Dot на тех же queries, corpus/revisions и rights: quality/completeness/source grounding, time to first useful/full answer. Возможное будущее «Мира, хорошо подумай» делегирует Dot лишь при доказанной пользе и не входит в base implementation. Пока provider/pilot не проверены, timings/advantage — NOT RUN, не вывод из health200 или интерфейса.

Protected backend p95≤1000ms/MCP≤1500ms/hard2000 ms сохраняются. Queue wait и final answer time показываются отдельно; ожидающий полный вопрос не оправдывает скрытый долгий search call. Quality/capacity gates — [mass readiness](../mass-ingestion-readiness.md). [Исторический исходный проект](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/designs/user-request-load-management-20261019.md) доступен для сравнения механизма, не как конкурирующая target архитектура.
