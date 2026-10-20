# Приёмка новой RKB: PostgreSQL, WSS, истории и пользовательские каналы

Обновлено: 2026-10-20. **NOT RUN**: план будущего внедрения, не runtime report.

Самодостаточный [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) содержит тот же нормативный набор gates. Этот файл предназначен для отметок конкретных запусков с exact SHA/config/corpus и evidence. Числа и сценарии ниже синхронизированы с его §14; нумерация сохранена для прямой сверки. При изменении критериев обновлять оба места в одном PR.

Аудированная документационная база — `3e39046fe32aaf14cfa088d65d15ff0082425309`. Перед запуском фиксировать фактические source/runtime heads. Условия отличаются от прежнего SQLite/Supabase/E5; старые PASS не переносятся автоматически. Дополнительные предметные сценарии сохраняет [ПМИ разногласий](rkb-disagreement-pmi-20261018.md).

## 14. Приёмка: обязательные численные gates и конкретные отказы

Все новые проверки в этом файле — **NOT RUN** на момент документационного изменения. Исторические результаты из старых отчётов не относятся автоматически к PostgreSQL/WSS/new story pipeline.

### 14.1. Protected product gates

Девять требований из `.devcoveer/requirements.json` сохраняются. Исторический ID CORE-SUPABASE-CAPACITY оставлен для стабильных ссылок; его новый смысл — measured storage headroom, а не обязательность Supabase.

| Gate | Критерий |
|---|---|
| Масштаб | Не менее 100 book-equivalent sources и 100 000 active chunks либо больший реальный corpus; benchmark готовности, не продуктовый лимит. |
| Ёмкость | Steady ≤80% каждого применимого storage budget; peak крупнейшего reimport, snapshot/restore и cutover ≤90%. Учитывать heap/vectors/indexes/TOAST/WAL/temp/system и overlap, отдельные control/recovery/cache budgets. |
| Dense quality | Не менее 30 held-out source-grounded fact families. BGE-only Hit@5 ≥85%, Hit@10 ≥90%; критический RU-query→DE-source slice Hit@10 ≥85%. |
| Ablations | BGE-only, lexical-only и configured fusion отдельно; dense не prefilter через lexical. E5-only не нужен целевому решению. |
| Warm backend | Ranked-candidate retrieval p95 ≤1000 ms на representative corpus/concurrency. |
| Public MCP | p95 ≤1500 ms; hard evidence interaction deadline 2000 ms. Не маскировать долгую очередь 10-секундным budget или исключением ошибок. |
| Chunk/source | Semantic passages около 256 BGE tokens; exact augmented input вместе с prefix ≤512, без silent truncation; all printed book material/provenance сохранены. |
| Per-book | Natural/paraphrased и relevant cross-language evidence queries на точной новой active revision до accepted readiness. |
| Story | Малый blind source comparison + негативные/положительные случаи; facts/metadata сохранены при zero story, связность улучшена без выдумки. Квот/минимальной длины нет. |
| Каналы/queue | Реальный не-owner, actual provider identity, dialogue order/ACK/fairness, measured ETA/resizing, private scopes, current revocation при поздней доставке. |
| Надёжность | Проверенный notebook restore, durable exact results, snapshot+tail, WSS reconnect/fencing/duplicate/UNKNOWN outcomes и отдельно обозначенная защита DevCoveer disk loss. |

В latency отчёте показывать p50/p95, end-to-end и этапы queue/dispatch/wire/SQL/encode/render; cold/warm и cache-hit/miss отдельно, error/pending denominator не исчезает. Не выдавать retrieval Hit@K за accuracy готового исторического ответа. Для ANN сравнить с exact vector search на той же выборке; фильтрация tenant/source/revision после ANN может уменьшить recall, что требует bounded refill/настройки, а не «в корпусе ничего нет».

### 14.2. Какие проверки что разрешают

| Граница | Обязательное подтверждение |
|---|---|
| Production GEO/runtime cutover | Protected scale/capacity/dense/latency/source gates; DUR/FEN/WSS/REC, MIG, IMP, STO/MET/CAS, SRC/IDX, PRF rights, ID-1, fair request core и один настоящий non-owner personal MCP. Новые знания/истории/cases обязаны работать до переключения. |
| Включение конкретного нового канала/функции | Реальные CHN/Slack/Mira (включая phone/voice), VK, PUB/Astro, NEED/PH задачи относятся к их release. Пока NOT RUN/BLOCKED, соответствующая новая функция не включается и не объявляется готовой. Регрессия **уже включённого** канала блокирует переключение; его нельзя молча отключить ради cutover. |
| Завершение всего задания владельца | Все перечисленные в §3 функции реализованы и прошли собственные applicable gates. Частичный core cutover не является завершением всего проекта; оставшиеся каналы остаются явной незаконченной частью, не исключённым scope. |

Для 100 book-equivalent / 100 000 chunks использовать **изолированную benchmark DB/fixture**, не публичный tenant и не новую IAM-систему. Manifest описывает разрешённые originals/source units и текущие chunk/tokenizer/BGE/lexical версии. Старые chunks/vectors не зачитываются в target gate. Масштабный load fixture может пройти только новый source/chunk/retrieval путь без дорогой story/case композиции; это не production import и не отмена её функций. Контролируемое синтетическое увеличение допускается лишь как явно обозначенная инженерная нагрузка, не доказательство dense quality или 100 реальных книг. Dense held-out families и языковые slices проверять на реальных source-grounded фрагментах новых passages; не считать копии одним за другим независимыми доказательствами. Test DB никогда не активируется в public search, удаляется по bounded fixture retention.

### 14.3. Минимальная матрица конкретных рисков

| ID | Сценарий | Ожидаемый наблюдаемый результат |
|---|---|---|
| DUR-1 | Crash после intake ACK, до model result | Request и полный нужный input доступны из новой сессии; нет просьбы переслать accepted input. |
| DUR-2 | Crash после сохранения точного model payload, до PG apply | Replay использует тот же payload/hash/UUID/time; новая inference не подменяет его. |
| DUR-3 | Crash после PG commit, до result ACK | Applied_ops и cursor возвращают duplicate с тем же outcome; нет второй Story/case/доставки. |
| DUR-4 | CAS conflict / unexpected SQL error | Детерминированный отказ сохранён и cursor идёт дальше; неизвестная ошибка не пропущена молча. |
| FEN-1 | Два writer epochs / поздний старый upload | Только действующая authority меняет pointer/delivery; старые уже accepted commands всё ещё replay. |
| WSS-1 | Idle/proxy disconnect при прогретой модели | Reconnect/RESUME того же процесса, без kernels_push и повторного model load. |
| WSS-2 | Drop до RESULT, после commit, до ACK | Bounded reconciliation IDs/hashes; повторный эффект отсутствует, hash mismatch явный. |
| WSS-3 | Старый и новый sockets одного run | Только один dispatch attachment; old generation fenced. |
| WSS-4 | Expired claim / heavy encode / cross-process enqueue | Stale claim не убивает warm worker; control/ping живы; очередь из другого процесса доставляется promptly. |
| FAST-1 | Свободный ready read, отключённый/заполненный telemetry sink | QUERY отправлен без fsync/seal/fixed batch wait; drop/error счётчик растёт, результат и permits не ждут журнал. |
| FAST-2 | Fused search / oversize query/result / распространённый FTS term | Вектор не идёт через Dev; caps bytes/tokens/candidates и actual SQL cost проверены; no silent truncation/false empty success. |
| FAST-3 | Barge-in, reconnect, late SQL/forward и четвёртый параллельный пользователь | Старый output/audio fenced; cancel достигает поддерживаемых стадий, PG очищен, credits освобождены по факту; чужой batch не убит, read не RESUME. |
| FAST-4 | Mira write UNKNOWN → тот же ресурс через Dot; reader ниже min_data_seq | Resource/scope index across routes даёт prior_write_pending; независимый accepted-corpus query продолжает работать. Нет ложного последнего значения/not-found. |
| FAST-5 | Scoped revoke в полёте, cross-book item, continuation/cache; notebook offline | Все зависимости item проверены; affected text/bibliography не выдаются, unrelated response сохраняется. Remote-durable barrier блокирует новую выдачу без ожидания Kaggle; PG effect pending не назван Saved. |
| FAST-6 | Telegram reconnect/FLOOD_WAIT без потери PG/BGE | Text search продолжает работать, proof имеет собственный pending; число runs и exclusive session leases неизменны. |
| PH-1 | Factory не подключена / v1 normalizer встречает v2 status | Honest unavailable/config gate; не пустой hybrid и не объявление live support по fake provider. |
| PH-2 | Tool-lock wait / user interrupt / input checkpoint | Весь wait измерен; новое чтение не держится за отменённый read; mutations сохраняют порядок, utterance recovery не стал lossy telemetry. |
| PH-3 | Reconnect после read до/после evidence context checkpoint | Exact scoped refs либо явный context gap; новый ranking не выдаётся за старый, текущие grants проверены. |
| NAT-1 | Native candidate с несовместимым ABI/CPU/hash или лишним IPC | Не READY; если эксперимент выбран, сравнение включает полный путь/ресурсы/lifecycle, без implicit model-space change. |
| VIS-1 | Opt-in page continuation / новый visual context | Reuse только exact same page/render; при потере контекста actual ImageContent возвращается, review не подменён text-only. |
| REC-1 | Concurrent backup и записи | Restore имеет подтверждённый внутренний cursor и нужный WAL; replay непрерывен. |
| REC-2 | Corrupt snapshot / wrong version / несовместимый codec | Candidate не становится canonical; доступна прежняя verified baseline или явный controlled recovery. |
| REC-3 | Yandex candidate upload/verify fail | Старая baseline сохраняется, remote tail GC учитывает её watermark и consumers; local sealed spool может compact независимо. |
| REC-4 | Потеря DevCoveer control disk | Все final accepted/saved восстанавливаются из verified remote tail; локальный received остаётся pending, epoch/UNKNOWN/grants/receipts восстановлены. |
| REC-5 | Remote tail outage / local byte cap / старый daily base | Нет false accepted/Saved, backpressure до переполнения; local GC не ждёт daily base, remote GC сохраняет покрытие. |
| REC-6 | Expired operation ID / same key different hash / UNKNOWN старше 4 h | Expired не исполняется заново, hash conflict явный, UNKNOWN не исчезает по TTL. |
| REC-7 | Crash на upload/manifest/control checkpoint boundary | Выбирается только целая sealed chain без gaps; orphan не становится принятой командой; outcome receipt восстанавливаем. |
| REC-8 | Base на другом collation/runtime; derived BGE после иной batch/thread конфигурации | Несовместимый index repair до READY; canonical outcome неизменен, projection lag честный и quality gate сохранён. |
| REC-9 | Много provider UNKNOWN без readback | Нет autoresend/false delivery; scoped explicit resolve, unrelated imports/saves работают в своих budgets. |
| REC-10 | Старый worker завершил presigned upload после epoch change | Orphan не становится current; DELETE/admin недоступен notebook, large bytes не попали в control spool. |
| IMP-1 | Fresh A→B, B→A и concurrent activation | Каждая revision активирована корректно, research frontier сохранён, позднее основание замечено по cursor. |
| IMP-2 | Retry stage/finalize/reextract/rename | Нет дубля book/findings/Story/Variant/case; corrected metadata не меняет corpus. |
| IMP-3 | Table/caption/footnote без эпизода | Facts, dates, observations, entities и exact evidence доступны, zero new story допустим. |
| STO-1 | Эпизод через windows/страницы | Сохранённое continuation приводит к одному связному досье, не набору chunk stories. |
| STO-2 | Короткий связный рассказ / everyday practice | Нет ложного отказа из-за длины, отсутствия конфликта или точного дня. |
| STO-3 | Новый источник / contradiction / uncertain identity | Не выдуманы причинность и единый эпизод; case независим; exact approved variant не переписан. |
| STO-4 | Spoken rendering / exact source | Читается сохранённый body/корректная адаптация, не summary-prefix; нет утраченной развязки/оговорки; bibliography отдельно, exact source неизменен. |
| STO-5 | Инструкции или команды внутри source text | Источник сохранён как данные/evidence; его текст не меняет actor, grants, workflow или инструменты агента. |
| STO-6 | Body отсутствует / `variants=[]` / compact get | Кандидат явно без рассказа; ни рейтинг, ни summary не выдаются за narrative. Достаточный эпизод обязан сохранить default body. |
| STO-7 | Save → retry → restart → get/search/read-export/voice | Один exact body/revision/hash, доступен по Story read scope до publish_ready; private draft не раскрыт. |
| STO-8 | Заголовок «Как/Почему» при известном лишь результате | Честный title/needs_context, ни сочинённого механизма, ни оценки потенциала вместо body. |
| MET-1 | Calendar/units/people boundary | Julian/Gregorian precision, exact decimals, period/method, тёзки и historical aliases не нормализованы с потерей смысла. |
| CAS-1 | Третий источник/merge/новый аргумент после вывода | Stable subject case, aliases и evidence history; новое основание явно вне старого based_on. |
| CAS-2 | Модель через личное подключение / ошибочная identity | Нет ложного human support; attribution_review_required адресно, история сохранена. |
| MIG-1 | Layer export/import/lookup | Counts/IDs/CRS/geometry/transform/provenance и external owner refs проверены. |
| MIG-2 | Legacy anchor / другое издание / поздний delta | Нет ложного reanchor; layer writes accounted; book corpus не мигрирован. |
| MIG-3 | Rollback после target writes | Новые принятые вклады сохранены; whole old DB не затирает target tail. |
| SRC-1 | Public text search / stale reader / withdrawal | Все разрешённые active accepted book texts доступны, stale/revoked material не возвращается; min_data_seq соблюдён. |
| IDX-1 | Broad/scoped prepared query на 100k+ chunks, большие stories | EXPLAIN/latency подтверждают bounded SQL, indexable ANN, keyset catalog и batch hydrate; нет full-corpus Python path. |
| IDX-2 | Новая revision / body update / shadow rebuild / withdrawn source | Инкрементальная projection и dirty revision работают; stale output запрещён, peak ≤90%, gates повторены. |
| IDX-3 | Autovacuum/maintenance/snapshot при import+search | Search reserve/deadline сохранены; maintenance не запускает без нужды full reembed/REINDEX. |
| SRC-2 | Dense vs exact ANN / filtering | Recall и eligible top-K измерены; empty after filter не выдан за отсутствие evidence. |
| PRF-1 | PG corpus adapter / revoked pixel grant | Proof работает на target, unauthorized preview/full page не строится и не отправляется. |
| PRF-2 | Preview retained / late polygon / rerender | Hash/transform/generation совпадают; lost preview требует новый locator task. |
| PRF-3 | Mixed ready/pending/failed pages | Успехи возвращены по items, continuation сохранён, actual MIME верен. |
| PRF-4 | Owner/demo/expert/local_historian и обычный читатель | Все получают illustration; первые четыре категории при scope получают proof, остальные — denial; full PDF не открыт. |
| PRF-5 | Full page labelled illustration / shared-channel request | Серверная classification/geometry и current audience закрывают обход; public cache не содержит proof. |
| ARC-1 | Oversize/stream break/FLOOD_WAIT/duplicate upload | Ресурсы ограничены, bytes/hash проверены, independent session leases соблюдены, receipt reconciled. |
| QUE-1 | Один пользователь с многими dialog и explicit IDs | Canonical-person fairness без обхода fair head. |
| QUE-2 | UNKNOWN delivery / новая реплика / cancel | Порядок своего диалога сохранён, другие люди работают, подтверждённые эффекты не удалены. |
| QUE-3 | Capacity shrink/grow / long import / ETA | Permits пересчитаны, search reserve есть, >10 s notice coalesced и final приоритетен. |
| ID-1 | Non-owner / forged actor / revoked grant / private draft | Server-side denial, нет owner fallback или доступа к чужому draft/history. |
| CHN-1 | Slack + личный MCP + PH/Mira | Одна identity/история; реальные request/result/hash ACK, pending и reconnect, evidence отображено. |
| CHN-2 | Physical phone / длительный voice / четыре пользователя | Реальный mixed-load отчёт; emulator pass отдельно, права/контекст не смешаны. |
| PUB-1 | First publish / failed update / late build | URL только после первого readback; last-good URL остаётся, stale build не заменяет новый. |
| PUB-2 | Revocation / public HTML/OG/assets | Fenced rebuild/tombstone и cache readback; никаких private drafts/pixels в экспорте. |
| VK-1 | Duplicate schedule / cancellation_unknown / reserve race | Один provider effect, slot не освобождён до проверенной отмены. |
| VK-2 | Partial comment pages / changed/deleted parent/policy | Нет ложной полноты, утраты допустимых ответов или автоматического удаления по stale policy. |
| NEED-1 | Independent import нужного сборника / PH task done | Research need связан после semantic check; imported/task done не объявлены fulfilled/спор решён. |

Это набор различающих сценариев, а не предписание создать по сервису/fixture на строку. Повторяющиеся механизмы проверяются одним meaningful тестом с несколькими условиями; существующее доказательство переиспользуется для неизменённого кода.

Тесты с реальными источниками по умолчанию используют isolated case/corpus fixture и один test_run_id. Не публиковать вымышленные экспертные мнения в live cases, subscriptions или VK. Если требуется canonical mutation, заранее baseline/allowed effects и узкая idempotent CAS compensation своих действий, сохраняющая поздние реальные вклады; rollback_conflict не лечится force overwrite. Component tests не требуют подделывать Slack event; actual user-sender E2E отдельно и без impersonation.

### 14.4. Скорость полезного ответа, выбор native и сравнение Mira/Dot

**Статус: NOT RUN.** Сначала подключить настоящий PH provider; сейчас нет измеренного голосового PH→RKB пути. Собрать один trace ID через клиент/PH→Dev→Kaggle→клиент. Записывать monotonic durations каждого host, включая PH capability activation, utterance checkpoint, tool-lock wait, admission wait, encode, lexical/alias, ANN, fusion, hydrate, serialization/validation и доставку. Queue wait до запуска инструмента не пропадает из E2E. Межхостовые wall clocks не считать синхронными; суммы p95 отдельных стадий не являются p95 общего ответа.

Сравнивать на одном exact corpus/model/config: до и после устранения hops/polling/full scans, затем **только при оставшемся основании §4.6** один native candidate. Отдельно показывать single-user и четыре пользователя, concurrent import и scheduled snapshot/maintenance; warm/cold, cache hit/miss, длину query и source scope. Размер выборки/повторы/метод percentile указать. Все попытки, timeout/cancel/degraded/denied входят в отчёт соответствующей категории, ошибки не исчезают из denominator. Protected 1000/1500/2000 ms остаются самостоятельными gates; ожидаемый выигрыш 0.5 s не является уже достигнутым результатом или причиной ослабить качество.

Для голоса две главные метрики: **конец реального пользовательского высказывания → первый полезный слышимый source-grounded ответ** и → полный ответ. Отдельно tool start→usable evidence и model/TTS/audio delay. «Сейчас посмотрю» и первый несодержательный звук не засчитываются. Метрика backend search не включает всю ASR/model/TTS цепочку и не подменяет её. Включить barge-in, ошибку ASR имени/даты и последующий уточняющий запрос без смешения generations.

После actual PH wiring провести парное сравнение **Мира и Dot**, не интеграцию одного внутрь другого:

- Одинаковые source-grounded задания, corpus/revisions, права/аудитория, scope и известные evidence. В наборе — RU→DE, имена/топонимы, даты/измерения, неоднозначность, cross-book разногласия, actual story body, отсутствующие сведения, forbidden proof/private draft и продолжение после reconnect. Использовать существующие held-out families, не заводить второй огромный benchmark.
- Для сравнения интерпретации дать одинаковый текст запроса и сопоставимый common tool profile/budget. Отдельно измерить реальные пользовательские маршруты с их ASR/voice/Slack и зафиксировать различия доступных tools; не приписывать модели преимущество другого профиля или тёплого кеша.
- Слепая проверка по первоисточникам: точность, полнота, корректность source refs/атрибуции, честная неопределённость, сохранённый body вместо summary, отсутствие unsupported claims. Tool Hit@K не является оценкой финального рассказа. Сопоставить время первого полезного/полного ответа, число tool calls, ошибки и стоимость доступных операций; не ранжировать по красноречию.
- Результат — paired таблица с примерами преимуществ/провалов и evidence, без заранее назначенного победителя. Будущая опция «Мира, хорошо подумай»/Dot delegation рассматривается лишь при наблюдаемой пользе для определённого класса вопросов и приемлемой задержке. **В базовой реализации эту опцию не создавать.**

Это ограниченная приёмка конкретных изменений, не обязательный постоянный трёхъязычный tournament или второй LLM-судья. Для неизменённого пути переиспользовать подходящее проверенное evidence; повторить только затронутые gates.

### 14.5. Быстрая отладка, lifecycle и миграции

Сначала install/read receipt skill (§0). До живого run локально/CI проверить handler candidate/rollback со stub-моделью, migration lost-ACK/replay и fixture reset guard. Затем провести один ограниченный smoke на доступном разрешённом run: ранние bootstrap stages/WSS, live SQL patch, code-version readback, небольшой artifact и cooperative Stop с provider readback. Сценарии потерянного push/ACK, зависшего supervisor, нескольких output pages и плохого hash сначала проверять детерминированными fixtures на выбранном adapter; не создавать второй живой run только для имитации ошибки. Повтор реальной проверки нужен для конкретной неизвестной provider boundary. Stage 0 receipt фиксирует installed SDK, version-specific status **и его execution semantics**, pagination, фактический stop path и writable paths; невыясненные ownership/stop/recovery возможности не объявляются serving/TG readiness. Доступность новой private Dataset version в живой session — отдельная проверка выбранного пути, не блокер обычного SQL/code patch.

| ID | Сценарий | Ожидаемый наблюдаемый результат |
|---|---|---|
| DBG-1 | Несколько SQL/function/read handler исправлений, битый import или первый probe | Тот же run/model, для SQL прежний PG; exact loaded hashes и полезный результат изменились ожидаемо, model-load/download/install counters не выросли без причины. Битый candidate оставляет/возвращает прежний handler; incompatible restart обозначен отдельно |
| DBG-2 | Push принят, ответ потерян; lookup ошибся/remote version продвинулась; два runtime instance с одним nonce | Стабильный slug, один исходный launch intent; нет повторного push/двойного TG grant, второй instance fenced. Ошибка lookup остаётся UNKNOWN; чужая/latest версия не подтверждает исход |
| DBG-3 | STOP ACK потерян, worker/parent завис, lease истёк, resource удалён, поздний heartbeat | Только свой process group; cancel/delete/TTL не terminal proof. TG slot не переиспользован; credential revocation не объявляет compute terminal. Terminal generation не воскрешается |
| DBG-4 | Controller restart при active/unknown run и deterministic bootstrap failure | Existing owned run reconciled/reused; новый controller не создаёт duplicate. Startup failure до READY виден по stage/reason; конечный retry budget не превращается в crash→push loop |
| DBG-5 | Partial/многостраничный output, чужая version, missing manifest/member, плохой hash или resume старого файла | Expected inventory задан controller до операции; exact-version native pagination/bytes проверены. Failed bytes не accepted, пригодные независимые artifacts сохранены; SDK capability фактически выбранной версии, partial/unknown явны |
| DBG-6 | C accepted/sealed при v1, ещё не dispatched либо ACK потерян; запрошена v2, DDL ACK потерян и active routing сменился | Barrier закрывает новую v1 фиксацию и завершает всю ранее accepted v1 queue под v1; UNKNOWN сверяет в исходной generation. Transactional schema/ledger atomic, live/replay outcomes совпадают; ID/hash mismatch — конфликт |
| DBG-7 | DDL lock wait, cancelled concurrent index, смена function return shape | Конечный lock/job timeout; INVALID index не READY; scoped repair и pool generation корректны, чужой actor не протёк, старый работающий путь не снят без необходимости |
| DBG-8 | Замена snapshot старее Saved; failure до/после первой новой записи; reset с реальным Saved/GEO или прежним product binding | Base+tail/ordered migrations сохранены; live PGDATA overwrite запрещён. После writes откат только с catch-up/roll-forward. Reset реальных данных отклонён; immutable test_fixture не повышается до production и не выдаёт product ACK |
| DBG-9 | Нет места для candidate/two clusters; зависимость или artifact обновлены во время run | Maintenance admission блокирует превышение peak, действующие данные сохранены; выбран конечный PG-only restart/окно, не notebook explosion. Deps/model cache не переустанавливаются на SQL patch |
| DBG-10 | Истёк debug budget/idle, control потерян; исходный runtime path попал в notebook output | Bounded self-stop выполняется как защита, а не terminal proof; ownership/recovery refs остаются. Private PGDATA/credentials не опубликованы; cleanup не удаляет UNKNOWN/accepted results |
| DBG-11 | FileParams URL истёк в очереди; opaque ref; два uploads/lost ACK; resume/hash/size/class mismatch | Реальный client ref отдельно от controller ticket; source_ref_expired просит reattach той же operation. Attempt-specific keys, provider/version/hash readback и first-verified CAS; без Dev bulk proxy и duplicate import. Stage0 фиксирует фактический TTL/capability; private visibility/output, SSRF/archive bounds и native ImageContent сохранены |
| DBG-12 | Temporary Dataset ещё нужен mount/transfer/UNKNOWN/recovery; lease issuance одновременно с GC; delete ACK потерян/нет API | Общий guard закрывает новые bindings; старые consumers reconciled, expiry не proof. Только owned exact temporary resource, repeated native absence; quota проверяется если наблюдаема. Без delete capability cleanup_blocked_by_provider; permanent DB и чужие данные сохранены |
| DBG-13 | Existing import/search/story/proof из ChatGPT/Dot; non-owner; client не вызвал tool; timeout после effect; смешанный write tool | Реальные разрешённые функции сохранены. Typed server-observed outcomes отдельно от client evidence; отсутствие call не означает denial. Truthful hints, текущая authorization, один operation ID/readback, без обхода approval или искусственного повторного product approval |

DBG-1, DBG-2/3/4 и поддержанный artifact path проверяются до длительного цикла реализации на Kaggle. DBG-6/7/8 и production recovery — до первого реального accepted/Saved, а не после импорта книг. DBG-11–13 проверяются для выбранных фактических clients/storage до включения соответствующего transfer/MCP пути. Остальные applicable проверки входят в соответствующий runtime/maintenance release; эта матрица не требует прогонять все каналы/100 книг при каждом SQL исправлении. В отчёте нужны фактические edit→apply→useful timings, число starts/restarts/model loads и повторных bytes, отдельно cold/warm. До выполнения — NOT RUN.

