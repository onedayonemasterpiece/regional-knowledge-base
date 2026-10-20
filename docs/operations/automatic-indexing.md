# Автоматическое индексирование и публикация revision

Обновлено 20.10.2026. Целевой PG 17/BGE контракт для [единого handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md); новая реализация ещё не принята.

## Один lifecycle без ручного backfill

До production book imports требуется проверенный GEO identity manifest со stable layer/place IDs, aliases и CRS; bulk raster transfer может идти позднее в phase 4. Stage 2 без manifest допускает только isolated fixture. Обычный пользователь использует book_ingest start/pages/stage/validate/finalize/status. После finalize existing controller продолжает persisted publication/research workflow без открытого чата и без ручного запуска backfill. E5/Supabase в target нет; required embedding space один — pinned BGE-M3 1024 FP32.

Staging сохраняет точные accepted source payloads, source/revision/material hashes и progress. Accepted требует remote-sealed exact intent/payload. Saved дополнительно требует domain commit и remotely verified exact outcome/receipt; local pending receipt не означает ни accepted, ни Saved. Searchable revision активируется только после required source/index/quality gates; будущие экспертные conclusions, карта или Street Story не блокируют activation. Revision projections immutable; после готового staging маленький active pointer и research frontier переключаются атомарно в PG. Массовый UPDATE active flag у 100000 строк не является activation path.

Pending replacement сохраняет прежнюю accepted active revision. Если pending версия superseded, её поздние vectors/results inert и не могут её активировать. Supersession имеет явный outcome, не исчезнувший import.

## Порции работы и fairness

Existing BgeQueue/controller принимает book и story jobs с общими query-first guard, idempotency и capacity reservation. Query reserve нельзя обойти вторым producer. Проверка свободного места и enqueue атомарны.

Ephemeral `read_query` использует ту же fairness/admission authority, но не создаёт durable encoder job/user turn и не ждёт fsync, remote seal или durable RESUME. При свободном permit запрос dispatches сразу; batch объединяет уже ожидающую работу, не ждёт заполнения ради throughput. Mutations и принятые долгие вопросы сохраняют durable lifecycle. Асинхронная bounded read telemetry не пишет в queue SQLite и не держит её write locks.

У исходного кода document window 64 и общий BGE backlog 256; это исходные ограничения для оценки, а не доказанный PG+BGE capacity envelope. Target batch/window фиксируются после измерения source sizes, RAM/CPU, WSS buffers и serving latency. Следующий bounded batch стартует только с permit; already running encode не прерывается. Proof/import получают aging и положительное продвижение.

Index worker читает exact minimal search material напрямую из domain authority, не public evidence hydration с PDF/illustrations. Не пересылать полную книгу через MCP/WSS JSON и не держать network call под writer lock. Completed results устанавливаются batch transaction с hash/revision checks, не отдельным round trip на каждый vector.

В целевом размещении domain authority, BGE и PG installation находятся на Kaggle: vectors не делают круг через Dev для записи в соседнюю БД. Пользовательский search — одна операция того же worker: encode/parallel lexical branches → dense/fusion → batched hydration → компактный evidence pack. Query vector и промежуточные candidate arrays остаются локальными. Reuse BgeQueue/controller не означает сохранение старой сетевой границы encode→Dev→PG.

Reads используют parameterized set-based SQL или небольшую SQL function; PL/pgSQL уместен для конкретной атомарной activation/CAS/applied_ops/outbox. Existing `_rpc` — локальный SELECT через connection pool, без нового HTTP/RPC service. BGE inference, WSS/provider I/O и remote ACK не находятся внутри PG transaction. Actor context задаётся transaction-local из verified authority; functions по умолчанию SECURITY INVOKER. Performance проверяется по actual plan/rows/buffers/time, включая custom/generic plans, а не по самому наличию RPC. Правила grants/search_path и смены signature — [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), §6.6.

## Индексы и serving projection

Книжные passage_search и story_search — отдельные active projections одного BGE space. GIN lexical, HNSW dense и B-tree scope/continuation indexes соответствуют [query paths](../architecture.md). Story search больше не загружает и не сортирует все vectors в Python.

Bulk initial load предшествует initial index build. После крупной activation/изменения распределения выполнить targeted ANALYZE. Autovacuum включён; нет полного REINDEX/VACUUM FULL/reembed по hourly/daily timer. Тяжёлый build, snapshot и import batch конкурируют через один controller budget.

Для неактивного staging использовать обычный index build. CREATE INDEX CONCURRENTLY нужен только на serving table, когда требуется сохранить конкурентные writes: это отдельный нетранзакционный шаг с дополнительными scans/ожиданиями и peak budget. После failure/lost ACK проверять определение и readiness в native catalog; invalid index требует явного завершения/cleanup по migration record. IF NOT EXISTS не доказывает совпадение или готовность. DDL имеет короткий lock_timeout и конечный statement/job budget; additive schema/backfill/validate/switch применяются по реальной необходимости, без rebuild всех индексов после каждой правки.

Ready derived row переиспользуется при точном совпадении source/material/model version/hash. Новый encoder result не заменяет принятую source transcription или человеческую правку. Canonical command outcome не хеширует заново BGE floats/FTS/crops: exact domain values/evidence сохраняются, derived projection может стать dirty и rebuild отдельно от canonical replay. Durable accepted BGE artifacts reuse; actual dense readiness проверяется после восстановления projection. Existing delivered artifact bytes/hash/receipt не подменяются.

Status/search не запускают повторное full hashing, repacking или reembedding неизменённого verified material. Проверка hashes при принятии/установке/recovery и при изменении integrity остаётся обязательной. Bounded caches и нужные FP32/index projections сохраняются ради скорости в общих 80/90% capacity gates, а не удаляются ради экономии с последующим recompute каждого запроса.

После durable installation и protected receipt job body освобождается; queue не становится второй vector database. Удаление старых derived revisions не удаляет evidence/history.

## Пробуждение, прогресс и recovery

Notify публикуется после commit; controller имеет bounded recovery scan, чтобы restart/потеря сигнала не оставили незавершённый import. Wake event не даёт обход fairness. Reads/health не открывают SQLite BEGIN IMMEDIATE; cross-process wakeup работает для всех producers.

Состояния различают source accepted, indexing pending, model/worker unavailable, active searchable, superseded, needs_recovery. Ноль missing vectors не доказывает live query readiness. Actual SQL+dense probe проверяет правильный material/model и useful evidence. Status counts не раскрывают private records.

`text_search_ready`, `archive_read_ready` и `proof_ready` публикуются отдельно. Telegram/FloodWait или незавершённый crop не переводят прогретые PG/BGE в unavailable; readiness не требует архивного preflight на каждом text query. Один run по-прежнему требует exclusive genuine Telegram session lease: разделение capability readiness не увеличивает установленный владельцем cap runs.

Progress checkpoint содержит source/revision, installed vector cursor, unfinished dependencies и exact job IDs. Restart продолжает их, не делает reencode всего corpus. Persistent mismatch или poison stage сохраняет checkpoint и diagnostic incident; bounded retry не превращается в GC accepted work. Optional archive/mirror verification не удерживает publication critical path, но остаётся отдельной resumed работой.

Совместимые SQL/function/schema patches применяются к живому PG в том же notebook без повторной установки dependencies или загрузки BGE. Exact migration ID/hash и transactional DDL record фиксируются вместе; lost ACK сверяется в исходной DB generation под writer authority/PG lock. Incompatible migration закрывает новую v1 фиксацию и доводит всю прежнюю accepted/sealed v1 queue до outcomes, включая undispatched/UNKNOWN; commands и schema/function/executor transitions replay в исходном порядке, не через latest SQL по умолчанию. Нужные artifacts/codecs входят в remote recovery chain до первого реального accepted/Saved. Immutable test fixture без product bindings допускает reset, production/GEO/accepted state — нет. Редкая DB replacement использует finite maintenance окно; полный порядок — [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), §5.4 и §6.6.

Read-only query-result cache имеет ограниченный lifecycle и актуальные dependency/scope checks. Исчезновение такого cache допускает повтор ephemeral read, но не удаляет durable human request, если поиск был его стадией: consumer checkpoint/result защищён отдельно. Shared encoder job retires лишь после учёта всех consumers; document publication jobs не удаляются как «старый поиск».

## Приёмка

Проверить два конкурентных producers, import+search, superseded revision, crash до/после result install, lost ACK, failed remote seal, atomic activation+research и stories с несколькими revisions. Удалить O(all-documents) scope enumeration, per-candidate title reads и Python story-vector scan. Coverage проверяется вместе с per-book natural/paraphrased/cross-language queries.

Полные численные gates: [mass readiness](../mass-ingestion-readiness.md). [Исходный automatic-indexing runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/automatic-indexing.md) содержит прежние service/migration014/E5/Supabase детали и не задаёт новый target.
