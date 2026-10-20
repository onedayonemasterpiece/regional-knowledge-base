# RKB: аудит Go/Rust, быстрого поиска и голосового маршрута

Дата: 2026-10-20. Тип результата: **проектирование и проверка исходников/документации**. Runtime, Kaggle notebook, imports, inference и нагрузочные проверки в этом изменении не запускались. Ускорение на 0.5 s не измерено и не обещано.

Нормативный результат — [один полный handoff v3 для кодового агента](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Этот отчёт сохраняет доказательства, вопросы, ответы и принятые решения; он не альтернативная инструкция внедрения.

## 1. Итог решения

Go/Rust executable технически можно запускать из Kaggle Python notebook. Это не снимает session lifetime/quotas/ABI/recovery. Предложение Grok верно как способ запуска, но не доказывает ускорение конкретного RKB. Новое решение использует один законченный QUERY в Kaggle: encode → bounded dense/FTS/alias SQL → fusion → batch hydrate → компактный evidence. Query vector не путешествует обратно через Dev. Shared controller проверяет права/permits/deadline и передаёт готовый результат.

Baseline — thin Python и один resident PyTorch FP32 плюс PostgreSQL/pgvector/PostGIS. Go может заменить конкретный transport/executor component; Rust — определённую дорогую функцию либо компонент с воспроизводимой проблемой памяти/lifecycle. Native допустим также для нового компонента, если сокращает сложность/отказы; полный Python дубль ради сравнения существующего проверенного native reuse не нужен. Однако готового совместимого fused RKB native executor в аудированной кодовой базе не обнаружено. Новый proxy, второй dispatcher/authority или ещё одна модель не вводятся.

Выбор конечный: один representative trace; устранение лишних операций; при оставшемся значимом основании — один узкий native кандидат с учётом IPC, copies, ABI, supervision и CPU contention. Нет полезного выигрыша — оставить Python и закрыть вопрос. Прохождение gates само по себе не запрещает дальнейший доказанный полезный выигрыш: владелец отдельно приоритизировал скорость. ONNX/GPU/quantization — самостоятельное изменение численного inference path с прежними quality gates.

Два самостоятельных интеллектуальных маршрута сохранены: **Dot → RKB tools** и **Мира → RKB tools**. Никого из них не удаляют. В обычном запросе Миры отсутствует дополнительный проход через Dot. Возможное «Мира, хорошо подумай» остаётся будущей опцией после сравнительного испытания, без baseline реализации.

## 2. Доказательная база и границы

| Источник | Что действительно установлено | Что из него не следует |
|---|---|---|
| RKB main `b1c0689b1a09373a900a317a3bac9cf4f86fdaf4` | Документационная v2 и legacy code, worker/queue, all-story Python cosine и full allowed-doc scope. Есть reusable AsyncConnectionPool/direct SQL adapter. | Целевой PG/WSS/fused QUERY уже работает или имеет измеренный p95. |
| PH main `940864ae70bbcf334610712fa0b33cf2f781d3d3`; served head из read-only проверки Dot `fde5a50b3e10c52ebfccd37cea382e5819e76fcf` | Protocol/strict search adapter существует. В данном launcher `create_app()` не получает regional_knowledge_factory. | Наличие real PH→RKB provider, успешный multiuser voice E2E. Health 200, сообщённый Dot, не доказывает integration. |
| Live interaction `1a9d2228743447954f22eb681f680f70cbf31aae` | Tool lock охватывает await; duration начинается после lock/capability stages; cancellation фильтрует поздний output, но не прекращает running adapter. | Измеренное число потерянных миллисекунд или утверждение обо всех способах остановки session. |
| Kaggle Dockerfile `e096cbc5b6ff50ab49130187f8677dedb9b5e9bd` | Linux runtime использует system/native programs; executable process возможен. | ABI, CPU flags, disk/Internet и lifetime следующего конкретного run гарантированы. |
| Исторические N5/N6 search reports | На своих версиях измеряли backend и connector отдельно. | Разность времён доказывает цену Python, Dot, WSS или отдельного сетевого hop; нынешний голосовой маршрут измерен. |
| Grok attachment `Kaggle_Efficient_Solutions.md` в Slack | Предложение prebuilt binary/Python hybrid, Go/Rust/native hot loops. | Универсальное 10–100× ускорение RKB, обязательный GPU или доказанная экономия RAM/энергии. |

В текущем worker JSON 1024-float vector возвращается на Dev; BgeQueue result polling имеет шаг 0.05 s, а reads используют write transaction. В `PostgresDataClient` методы, похожие на PostgREST HTTP, фактически вызывают parameterized SQL через pool: это **не отдельные HTTP hops**. Его legacy scope перечисляет все rkb_documents; переиспользовать pool, удалить enumeration. В старом evidence fetch top5 уже запускается через async gather; проблема не должна описываться как обязательная последовательная загрузка всех книг.

## 3. Реальные консультации Opus 5.5

Обе консультации выполнялись через DevCoveer в `provided_only`, read-only, provider Anthropic, actual model **claude-opus-5-5**, без fallback. Модель получила исходные решения владельца, точный code evidence, проект и ответы root; не имела инструментов для изменения проекта.

| Раунд | Task | SHA-256 сохранённого Markdown артефакта | Результат |
|---|---|---|---|
| 1: architecture/native/edge cases | `dvt_75300b48b3234a0ca591700421dd5ef3` | `4b78ef247f66870c48c423acf9fd05a363feb4a247943e776023e6af91ec6051` | Обнаружены universal durable-wire/read противоречия, повторные hops, risks authorization/logging/cancel/CPU; задано 12 сложных вопросов. |
| 2: adversarial closure | `dvt_98c2a0e6538f446c99c672a6f6e2c70a` | `294d50d35b26b9bbef97e60e9cf82ac19bb6ed1727bf6cae51477838fc5976c6` | Opus не нашёл архитектурных блокеров, потребовал F1/F2 текстовые уточнения и перечислил implementation edge cases. Root внёс F1/F2 с сохранением сильной durability. |

Фраза об отсутствии блокеров — вывод консультанта о предоставленном проекте, не сертификат runtime. Последние редакционные уточнения root перечислены ниже; третьего benchmark или скрытого implementation acceptance не было.

### Сильнейшие аргументы сторон

**За native:** новый WSS/executor код всё равно требуется; Go предлагает стандартные context/deadline и bounded concurrent I/O, Rust — контроль владения/памяти. Отдельный transport process может изолировать keepalive от Python blocking/GIL. Для нового компонента это допустимые основания выбора, как подчеркнул Dot.

**Против обязательного native:** BGE остаётся PyTorch/Python, SQL/ANN — PostgreSQL C, tokenizer может уже использовать Rust (actual backend проверить). Sidecar добавляет IPC и supervisor. Go proxy на Dev поверх существующей Python authority может увеличить число hops или создать второй владелец прав/admission. Full-corpus cosine в Rust остаётся full-corpus алгоритмом. Реальное исправление — indexed SQL, отсутствие polling/write lock и vector roundtrip.

**Принято:** Python baseline; native не запрещён, но должен заменить конкретный путь и показать полезный эффект/устранённый отказ с полной ценой владения. Не требуется нулевой IPC как догма. Go GC, Tokio blocking thread и Python thread cancellation не дают автоматической остановки уже выполняющейся работы. Инференс и транспорт — разные change gates.

### Двенадцать вопросов Opus и закрывающие решения

| Вопрос | Принятое решение |
|---|---|
| Что означает «прямо WSS»? | Два умных клиента, тонкий existing Dev auth/admission relay и исходящее notebook WSS. Public listener/второй LLM не нужны. |
| Где исчезают исторические ~840 ms между backend/connector? | Текущей атрибуции нет. Не делать причинного вывода; измерить реальные стадии после wiring. |
| Read после UNKNOWN в своём диалоге? | Explicit prior_write_pending/context gap, min_data_seq; не выдумывать результат. F1 расширяет предупреждение на другой маршрут/общий ресурс. |
| Где async log и что внутри? | Bounded buffer→отдельный rotating sink, вне queue SQLite lock, refs/timings без private bodies/secret. Critical audit и PH utterance state сохраняются. |
| Как не выдать revoked/withdrawn data? | Scoped current authority barrier/delta, exhaustive item dependencies, батч top-K; stale/непроверяемый scope fail closed, continuation не capability. |
| Не возникнет ли вторая read authority? | Тот же controller, modes/permits/fair canonical person и общий search reserve; RAM read не отдельный scheduler. |
| Каков CPU budget? | Пока неизвестен для target run; Stage0 фиксирует nproc/cgroup/RAM, затем общий PG/Torch/maintenance pool. Один resident model/forward. |
| Где bounds запросов/ответов? | Query1000chars/4096bytes/512 exact tokens; QUERY16KiB, максимум32 explicitly selected source IDs; evidence3/max5,8000chars/item/24000total/12KiBmetadata-item/256KiBresponse. No truncation. |
| Сколько retries? | Максимум один transport retry лишь при достоверно не начавшемся выполнении и внутри прежнего deadline. Sent/uncertain read не RESUME; оба клиента понимают degraded без цикла повторов. |
| Можно оставить PH v1? | Freshness/context gap требуют v2. Versioned provider/normalizer/adapter меняются вместе; совместимость строить лишь для реально оставшихся v1 consumers, иначе не нужна. |
| Кто вызывает PG при Go? | Baseline Python fused handler. В native эксперименте ownership и IPC фиксируются явно, one authority/model; бесплатность encode IPC не предполагается. |
| Требует ли fast path много notebooks? | Нет, baseline writer=reader в одном run. Existing Telegram exclusive session/run cap сохранён; рост только после фактических квот/сессий. |

### F1: cross-route pending write — исправлено

Сценарий Opus: правка draft через Миру стала UNKNOWN, затем тот же пользователь читает draft через Dot. Только dialog_seq этого не защищает. В существующей authority вводится индекс unresolved mutations по resource/scope/person, включая читателей общего ресурса. Любой пересекающийся read получает prior_write_pending/observed cursor. Typed dependency classes не блокируют поиск accepted books из-за несвязанной private draft mutation. Durable per-dialog ordering остаётся самостоятельным правилом.

### F2: отзыв составного evidence — исправлено с уточнением гарантий

Каждый item несёт все source/variant/grant refs, включая cross-book текст; bibliography dedup не стирает зависимости. Gate проверяет delta по реальному scope/items. Affected item удаляется целиком вместе с недопустимыми display/bibliography/continuation полями; результат становится degraded/evidence_withdrawn, не пустой ok. Не относящийся к ответу отзыв не обнуляет чужую выдачу.

Remote-durable barrier может закрыть **новую выдачу через RKB** при offline Kaggle. Но это не PG commit, не удаление CDN-копий и не Saved domain withdrawal. Эти стадии остаются pending до собственных receipt/readback. Утверждение, будто reader visibility cursor после этого нужен только для recall, не принято как общее правило: при неполной provenance, compacted delta или неизвестном policy scope остаётся fail-closed, min_data_seq/freshness не ослабляется. Уже доставленные клиенту bytes невозможно обещать отозвать.

### Другие замечания, превращённые в конкретные checks

- Cancel до admission не получает permit; disconnect снимает ожидание; late forward удерживает ресурс до фактического завершения, но не продолжает новую реплику.
- PG statement_timeout transaction-local. Поздний backend cancel не должен попасть в следующий pooled query: ack/rollback либо discard connection перед reuse.
- Ready singleton dispatch/flush выполняется сразу; group commit≤1s — потолок накопления, не обязательная пауза. Read_query не journaled.
- Text/archive/proof readiness раздельны внутри разрешённых runs. Telegram reconnect/FLOOD_WAIT не выключает готовый text search и не отменяет session cap.
- Context checkpoint связывается по session/turn_generation/tool_call_id и exact ordered evidence refs. Нет checkpoint — явный gap, не повторный ranking под видом исторических оснований.
- Actual Live cancellation и tool-call association проверяются при bounded parallel reads; mutation order не ломается. Error cases и lock/capability wait входят в timings.
- Не строить v1 adapter, отдельный queue/framework, обязательный second model judge или новый binary codec без реального потребителя/основания.

## 4. Вклад Dot и реальный PH gap

Dot read-only проверил served PH отдельно от main; root/параллельный code review подтвердили соответствующие primary source paths. Запущенный launcher не подключает factory; это первая работа интеграции, не доказанный voice benchmark.

В Live host lock получается до capability resolution; timer стартует только перед adapter. Поэтому duration_ms исключает lock wait, resolution/transition, предыдущие calls batch и отправку; error branch может не иметь duration. Нужны observed received/lock_acquired/adapter_started/completed/enqueued, затем provider-written/first useful audio. Не складывать p95 разных стадий и не вычитать wall clocks разных hosts.

Tool cancellation маркирует IDs и отменяет progress, но adapter продолжает исполняться. Поздний result подавляется на выдаче, а lock ещё занят. Нужен propagated cancellation/deadline и generation fence; accepted mutation после отмены произнесения сохраняет reconciliation, не просто task.cancel().

PH `_mark_semantic_observed(committed=True)` — фиксация потреблённого входного высказывания. SQLite FULL/RLock её стоимость не делает disposable log. Сохранить восстановление input, а обычные read telemetry и scoped context refs писать асинхронно. Async checkpoint короткий/coalesced; его lock не должен надолго блокировать следующий input commit.

Локальный history fallback — короткие transcripts, не evidence. Tool-result cache ограничен памятью; provider resume_handle отдельный возможный механизм, но не гарантированный собственный checkpoint PH. Новый versioned record использует existing store и exact bindings/refs/disposition. Transcript без evidence не доказывает источники; computed/enqueued не равно delivered/heard. Доступ к восстановленным refs проверяется заново.

## 5. Приёмка и функциональная сохранность

[Единая ПМИ §14](../testing/rkb-scalable-kaggle-acceptance-20261019.md) содержит FAST/PH/NAT/VIS сценарии и §14.4 сравнение Mira/Dot. Common text queries/corpus/rights/tools сравнивают интерпретацию; реальные voice/ASR/Slack маршруты измеряются отдельно. Оцениваются source-grounded точность/полнота/атрибуция/неопределённость, actual story body и первое полезное/полное сообщение. «Сейчас посмотрю» не полезный ответ. Будущая think-hard delegation не реализуется до доказанного выигрыша.

Сохраняются девять critical IDs,100books/100kchunks,80/90%capacity,30factfamilies,BGEHit@5≥85%/Hit@10≥90%/RU→DE≥85%,1000/1500/2000ms,256-token passage target/512 exact hard input. Typed requirements_update: `op_2c5753fab39228c2e6a9bfb1`, normalized digest `36673891af2b44a536409962950e78a91707e96e89d875b87336b5e16f5cf9a6`. Изменена неоднозначность universal durable read, не численные gates и не гарантия accepted/Saved.

Сохранены import/MCP, source facts отдельно от Story, actual Variant.body/варианты/history, dates/calendar/measurements/people/organizations, GEO/toponyms/maps/POI ownership, independent disagreement cases/expert/private/reviews, role model, Slack/PH/Mira, illustrations/proof и уже спроектированные publication/source-needs/subscriptions. Только GEO content мигрирует; книги импортируются заново. Storage/index maintenance incremental; speed допускает полезные bounded caches/FP32/projections, а capacity gates остаются. Candidate restore переиспользуется как allowed successor; TG cap не расширяется ради тестов/backup.

## 6. Первичные ссылки

- [RKB PostgreSQL adapter](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4/src/regional_knowledge/postgres_backend.py), [queue](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4/src/regional_knowledge/bge_queue.py), [worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4/scripts/production/bge_kaggle_worker.py), [stories](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4/src/regional_knowledge/story_registry.py).
- [PH actual launcher](https://github.com/onedayonemasterpiece/projects-hub/blob/fde5a50b3e10c52ebfccd37cea382e5819e76fcf/scripts/run_server.py), [strict adapter](https://github.com/onedayonemasterpiece/projects-hub/blob/fde5a50b3e10c52ebfccd37cea382e5819e76fcf/src/projects_hub/regional_knowledge.py), [utterance adapter](https://github.com/onedayonemasterpiece/projects-hub/blob/fde5a50b3e10c52ebfccd37cea382e5819e76fcf/src/projects_hub/live_adapter.py), [PH store](https://github.com/onedayonemasterpiece/projects-hub/blob/fde5a50b3e10c52ebfccd37cea382e5819e76fcf/src/projects_hub/store.py), [Live lock/cancel/timing](https://github.com/onedayonemasterpiece/live-interaction/blob/1a9d2228743447954f22eb681f680f70cbf31aae/python/live_interaction/session_host.py).
- [Kaggle Dockerfile](https://github.com/Kaggle/docker-python/blob/e096cbc5b6ff50ab49130187f8677dedb9b5e9bd/Dockerfile.tmpl), [subprocess](https://docs.python.org/3/library/subprocess.html), [PyTorch C++](https://docs.pytorch.org/tutorials/advanced/cpp_frontend.html), [HF Tokenizers](https://huggingface.co/docs/tokenizers/index), [PyTorch numerical accuracy](https://docs.pytorch.org/docs/2.11/notes/numerical_accuracy.html), [Go GC](https://go.dev/doc/gc-guide), [Go build targets](https://pkg.go.dev/cmd/go), [Rust platform targets](https://doc.rust-lang.org/rustc/platform-support.html), [Go cancellation](https://go.dev/doc/database/cancel-operations), [Tokio blocking cancellation](https://docs.rs/tokio/1.54.0/tokio/task/fn.spawn_blocking.html), [PG17 timeouts](https://www.postgresql.org/docs/17/runtime-config-client.html), [PG shutdown](https://www.postgresql.org/docs/17/server-shutdown.html), [ONNX threads](https://onnxruntime.ai/docs/performance/tune-performance/threading.html).
- [Owner voice/logging](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792482460462009), [speed priority](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792482620784689), [Grok attachment proposal](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792483558674749), [two smart routes](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792484250610649), [future think-hard and comparison](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792484392240799), [Dot native/reuse](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792484480688449), [Dot PH details](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792484540349259).

Нерешённых продуктовых выборов по языку/маршрутам/durability не оставлено. Фактические Kaggle ABI/egress/quotas, latency/CPU/storage, restored readiness, PH wiring и реальные клиенты — измеримые deployment prerequisites и acceptance, пока **NOT RUN**. Они не подменяются уверенностью консультанта.
