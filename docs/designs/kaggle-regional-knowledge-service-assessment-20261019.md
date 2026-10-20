# Оценка Kaggle как рабочего контура RKB

Обновлено 20.10.2026. Решение принято для реализации; эта оценка не запускает notebook, не переносит данные и не доказывает production readiness. Единый [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) — точка передачи кодовому агенту. [Полное прежнее обсуждение](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/designs/kaggle-regional-knowledge-service-assessment-20261019.md) сохранено по immutable ссылке.

## Принятая конфигурация

PostgreSQL 17 с pinned pgvector и один BGE-M3 process в Kaggle; existing DevCoveer MCP/controller/BgeQueue с коротким raw spool и compact control state; direct outbound WSS. Private Dataset содержит PG physical snapshots. Existing Yandex recovery storage держит immutable exact command/control/receipt tail и verified daily base. E5/Supabase target не требует.

Главный критерий — быстрое извлечение из большого корпуса; скорость приоритетна относительно экономии storage при сохранении обязательных quality/capacity gates. Выигрыш даже около 0.5 s полезен, если подтверждён на user-visible пути; без измерения это не обещание. Поэтому архитектура включает actual indexed query paths, а не только замену database vendor. Не строить write-cluster, новый broker или второй semantic registry.

В перенос входит только geo со stable IDs/provenance. GEO identity manifest layer/place IDs/aliases/CRS обязателен до production book imports; bulk rasters можно перенести в phase 4, stage 2 без manifest ограничен isolated fixture. Всё негеографическое создаётся fresh; отсутствие миграции книги не означает разрешения удалить прежний reference. Searchable text/bibliography и illustrations доступны всем; stripe proof — owner/demo/expert/local_historian; full source PDF/private page pixels закрыты.

## Что подтверждено и чего это не доказывает

| Основание | Вывод |
|---|---|
| RKB [main a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/a8cf41b50b8ccb62409d046d009d53c09d74b1d7) | Existing queue/controller, Kaggle BGE, import, graph, proof и stories можно reuse. Corpus/FTS/stories ещё SQLite, vectors Supabase; новый PG runtime не реализован одной документацией. |
| [sqlite_backend](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/sqlite_backend.py) | Full catalog/allowed-map enumeration и последовательные alias calls необходимо заменить индексируемыми scoped queries. |
| [story search](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_registry.py) | Python all-story vector ranking не масштабируется как требуемый indexed serving path. Это механизм кода, не новый latency measurement. |
| [Pinned BGE worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/scripts/production/bge_kaggle_worker.py) | FP32, 1024, input 512 и четыре CPU threads — исходный model contract. Совместное размещение PG не создаёт дополнительных CPU. |
| Поздний draft PR151, проверявшийся head adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4 | Material для reuse request/dialog/fairness/ACK/private-draft guards. Перед интеграцией обновить head/diff; draft не считать deployed. |
| Исторические diagnostic PG canaries другого проекта | Подтверждают возможность отдельных механизмов, но не PG 17/BGE/source-proof/restore E2E RKB. Старые версии PG 18 и маленькие upload tests не являются pins/приёмкой target. |

## Реальные ограничения платформы

[Kaggle Notebooks](https://www.kaggle.com/docs/notebooks) сообщает конечную session lifetime: CPU/GPU до 12 h, autosaved working space20 GB. Это не SLA, не гарантия durable транзакций и не измерение доступной RAM/CPU конкретного запуска. До cutover прочитать actual quota, mount/free space, concurrent run availability, Internet permissions и provider limits.

[Kaggle Datasets](https://www.kaggle.com/docs/datasets): 200 GB на Dataset, 200 GB суммарно private datasets, максимум 50 top-level files; uploaded archives распаковываются. Budget учитывает retained versions, current+candidate и reader pins. Private corpus нельзя сделать public ради освобождения quota.

[Official CLI](https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md) поддерживает new version, processing status и delete-old-versions. В [закреплённом CLI source](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/src/kaggle/api/kaggle_api_extended.py) `_upload_blob` получает create_url/token через `start_blob_upload(ApiBlobType.DATASET)`, direct upload завершает `upload_complete`, а version связывает `UploadFile.token`. Поэтому узкий adapter может оставить account key/blob token/finalize у Dev controller, выдать notebook только signed URL/session и передать bytes напрямую. Runtime probe остаётся обязательным. Dataset не документирован как transactional append/WAL sink. Upload request недостаточен: exact version должна пройти readback/hash/restore/probe. Не обещать incremental upload или selective old-version GC без проверки API.

Прямой outbound WSS устраняет необходимость публичного notebook endpoint и допускает duplex messages в живой session. Он не продлевает provider lifetime, не сохраняет RAM завершённой сессии и сам по себе не ускоряет encode/SQL. Реальный proxy/Internet путь проверяется внедренцем без обхода лимитов.

## Fused QUERY и выбор языка v3

Сохраняются оба интеллектуальных маршрута Dot → RKB и Mira → RKB; Mira не проходит через Dot. DevCoveer — тонкий auth/admission relay существующего outbound notebook WSS, без public Kaggle listener. Один ephemeral QUERY выполняет encode + PG dense/FTS/alias SQL + RRF + batched hydrate целиком в Kaggle; наружу возвращаются bounded bibliography/evidence, mode и freshness. Vector round trip на Dev исключается. Async read audit не задерживает request/response; durable mutations/jobs используют отдельный класс сообщений той же authority.

Default — Python glue, один resident PyTorch FP32 BGE и native PostgreSQL kernels. [BGE worker main b1c0689](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4/scripts/production/bge_kaggle_worker.py) содержит CLS/L2 FP32 batch1, четыре Torch threads и один interop. [PyTorch frontend](https://docs.pytorch.org/tutorials/advanced/cpp_frontend.html) уже опирается на C++; [Tokenizers](https://huggingface.co/docs/tokenizers/index) имеет Rust implementation, но actual AutoTokenizer backend конкретного pinned run проверяется, не предполагается.

Native executable технически осуществим: [Kaggle Dockerfile e096cbc5](https://github.com/Kaggle/docker-python/blob/e096cbc5b6ff50ab49130187f8677dedb9b5e9bd/Dockerfile.tmpl) содержит Linux runtime/build tools и Go-built git-lfs. Это source evidence, не проверка ABI следующей session. Этот base уже менялся с Python 3.12 на 3.13. Нужен заранее собранный immutable bundle с source/toolchain/hash, architecture/ISA/libc/shared-library ABI и versioned protocol; без compile/spawn/model load на каждый запрос. Notebook supervisor должен оставаться живым, читать bounded pipes, учитывать exit/signals и корректно завершать PG. Session end/SIGKILL не обещает cleanup или сохранение RAM.

| Выбор | Разумная граница |
|---|---|
| Python default | Убрать лишние hops, full-corpus scans и blocking event-loop work; PG и PyTorch выполняют тяжёлые kernels. |
| Go | Заменить конкретный transport/serialization component, в том числе на DevCoveer, если итоговый путь быстрее/стабильнее или устраняет воспроизводимый эксплуатационный дефект; удалить прежний путь, не добавлять proxy/вторую authority. |
| Rust | Заменить конкретную дорогую CPU/copying функцию либо компонент с воспроизводимой проблемой памяти/lifecycle; не переписывать registry/DB/inference по общему лозунгу о скорости. |
| ONNX/GPU/quantization | Отдельная проверка numerical/material/model compatibility и retrieval gates; не побочный эффект смены языка транспорта. |

Python остаётся baseline существующей authority и fused QUERY. Для нового компонента память, deadlines, backpressure и control-loop isolation — самостоятельные основания рассмотреть native вариант даже без доказанного CPU hot loop. Проверенный совместимый компонент можно переиспользовать без предварительного полного Python дубля; готовый fused RKB executor аудит не подтвердил. Native sidecar не является default, но local IPC не запрещён: если сохраняется Python BGE child, учесть copies/serialization/backpressure и новый lifecycle. После минимального fused пути и репрезентативного trace проверяется один узкий кандидат; принять только подтверждённое улучшение полезной задержки/ресурсов/отказов без регрессии quality, прав и recovery, с удалением заменяемого пути и одной authority/model. Незначимый результат завершает выбор в пользу baseline; три полные реализации не нужны.

Один CPU/RAM envelope включает Torch, PG pool/parallel queries, import/proof/maintenance и native runtime, если он обоснован. [Go runtime](https://pkg.go.dev/runtime#GOMAXPROCS) учитывает cgroup limits, но не резервирует CPU для PG/Torch; [ONNX Runtime](https://onnxruntime.ai/docs/performance/tune-performance/threading.html) имеет собственные thread pools/spinning. Нет обязательного порога 10%: stage trace локальных monotonic durations и воспроизводимые эксплуатационные проверки должны показать выигрыш полезной задержки/ресурсов/отказов с учётом IPC/serialization/cold restore, без приписывания исторической задержки Dot. [PyTorch numerical accuracy](https://docs.pytorch.org/docs/2.11/notes/numerical_accuracy.html) не обещает побитового равенства разных batch/backend; качество проверяется фактическими held-out queries, token IDs, pooling/prefix и existing gates, не придуманным cosine threshold.

Primary runtime contracts: [Go target/ISA](https://pkg.go.dev/cmd/go), [cgo](https://pkg.go.dev/cmd/cgo), [Rust targets](https://doc.rust-lang.org/rustc/platform-support.html), [Python subprocess](https://docs.python.org/3/library/subprocess.html), [PG shutdown](https://www.postgresql.org/docs/17/server-shutdown.html). Это обоснование осуществимости и границ, не выполненный benchmark.

## Минимальное восстановление

Accepted следует remotely confirmed exact payload/intent в existing Yandex; Saved дополнительно требует domain commit и remote exact outcome/receipt; early local receipt pending. Existing S3ObjectStore пока только put/get/delete transport: immutable seal и continuous segment manifest/CAS ещё реализуются. [Yandex PutObject](https://yandex.cloud/en/docs/storage/s3/api-ref/object/upload) уже документирует If-Match/If-None-Match; runtime проверка относится к выбранному SDK/bucket и writer race. Group-commit batch window ≤1 s, не cloud RTT, применён к mutations и явно принятым durable requests. Обычный read-only turn не получает durable row/ACK: request/response не ждёт S3 или audit log; logging идёт через bounded async buffer с отдельным счётчиком потерь.

Dev raw spool default 4h/hard 512 MiB; compact UNKNOWN/request/receipt metadata живут до reconcile. Local GC после remote seal не зависит от суточной base. Control checkpoint 15 min, changed PG base 4 h либо раньше по replay budget/byte pressure/planned handover. Один daily verified Yandex base независим от Kaggle; Yandex base и tail не независимы друг от друга.

Base физическая, с нужным WAL и indexes; restore проверяет PG 17 minor, extensions/libs/platform/collation provider+version, schema/codec, internal cursor и exact data. Keyed normalized text использует C collation; affected-index correction предшествует READY. Большие bytes идут direct Kaggle→recovery object по scoped key/epoch/expiry без global/delete credentials. Presigned URL не прекращает работать автоматически при epoch change; late upload не promotable благодаря external fence. Remote-tail GC сохраняет все цепочки для designated retained verified baselines и unfinished consumers. Не оставлять «backup» без нужного ему tail. При remote outage нет финального accepted ACK; backpressure включается до byte cap, а уже защищённые requests не expires.

Canonical outcome hash не включает заново вычисленные BGE floats/FTS/crops: domain values/evidence exact, derived dirty/rebuild и readiness отдельно. Existing delivered artifact bytes/hash/receipt не заменяются. Bounded sealed-tail compaction под одним controller/CAS сохраняет logical sequence/hashes, ограничивая object count.

## Скорость и цена обслуживания

| Риск | Принятое решение | Проверяемый предел |
|---|---|---|
| O(N) request preparation | SQL candidates→RRF→batch hydrate; keyset catalog, indexed links/aliases | Plans, scanned rows, round trips, latency при росте corpus |
| ANN scope starvation | Immutable revision + small active pointer; narrow indexed exact path; broad bounded iterative ANN, refill/partial | Actual prepared plan после 6+ executions и selectivity extremes; SET LOCAL pool-safe, custom plan только при необходимости |
| Частые слова/aliases | GIN per-row RU/DE/simple FTS, bounded query configs и indexed alias branch | Нет20 sequential full-timeout calls; не маскировать failed dense lexical веткой |
| PG+BGE RAM contention | Один model process, bounded pool/parallelism, совместный memory reserve | RSS/peak/temp/pressure вместе с import/render |
| Backup/index amplification | Changed bases, bounded replay, autovacuum/targeted ANALYZE, один maintenance permit | No hourly/daily full REINDEX/VACUUM FULL/reembed; overlap≤90% |
| Старый reader | exact version pin, data_as_of/min_data_seq, current revoke barrier | Не возвращает отозванные bytes или stale write result как актуальный |
| Доставка после retry | Single sender, operation/hash/native receipt, bounded UNKNOWN per provider/route и evidence-based resolve | Не угадывать not_delivered по отсутствию readback; abandoned_no_resend не разрешает повтор |

[Architecture](../architecture.md) задаёт GIN/HNSW/B-tree/PostGIS paths. PostgreSQL FTS и SQLite BM25 не эквивалентны по score; relevant quality gate обязателен. Текст/span refs и derived projections не размножают целые source pages. Размер vector(1024) сам по себе не оценивает всю БД; индекс/WAL/temp и physical backup overlap измеряются.

## Клиенты и предметные функции

Personal MCP, Slack Kaliningrad Base и Мира в Projects Hub используют одну canonical identity/request/dialog authority. Dot остаётся заменяемым executor; новый бот/voice app/IAM не нужен. PH adapter должен поддержать реальные pending/result/ACK/proof/story/case операции, screen bibliography/evidence и восстановление после voice reconnect. Native client/result readback проверяется отдельно от backend.

Facts, dates/calendar, observations, people/roles, geography и disagreements не требуют обязательной Story. Existing source registration, episode continuation, angle/gaps/Variant и private drafts сохраняются. Один persisted research workflow и bounded compose; отдельная фабрика или второй judge не нужны. Source_proof получает PG adapter; direct archive download использует независимые Telegram authorizations. Владелец назвал `TELEGRAM_RKB1`/`TELEGRAM_RKB2`; их реальная готовность/доступ проверяются до запуска, по одной session с exclusive lease на Kaggle run. Runs ограничены проверенным session pool и Kaggle capacity, без клонирования auth keys.

## Решение о выпуске

Продолжать реализацию выбранной архитектуры без нового vendor tournament. Core/GEO cutover и feature-release Миры/VK/Astro/research needs имеют отдельные gates; все функции обязательны для whole-project completion. NOT RUN не означает working, regression включённого канала блокирует switch. Scale fixture изолирован в отдельной benchmark DB без нового tenant/IAM, старых chunks/vectors и публикации в public corpus; originals/new chunk/BGE path обязателен. Synthetic amplification — только scale evidence, dense quality использует реальные held-out facts/new passages. Перед cutover проверить actual SQL+dense readiness, remote ACK и loss/replay, fresh imports, geo-only transfer, roles/media и настоящие клиенты. Gates остаются ≥100 books/100000 chunks; storage≤80% steady/≤90% peak; ≥30 families, BGE Hit5≥85%/Hit10≥90%, RU→DE Hit10≥85%; backend p95≤1 s, MCP p95≤1.5s/hard2 s.

Редкий cold interruption допустим в принятой владельцем модели, но ориентир 3–5 min нельзя выдавать за измеренный restore SLA. Если warm gate не выполнен, оптимизировать доказанный bottleneck и переизмерить, а не переименовать pending в успех.
