# Архитектура базы знаний региона

Уточнение 21.10.2026: [решения владельца](designs/rkb-owner-alignment-20261021.md), [аудит старого/нового кода](reports/rkb-requirements-code-audit-20261021.md). Ядро частично реализовано; code/wiring/tests/live разделяются. Один owner-only импорт, модельное чтение без обязательной повторной backend подготовки; PG staging/tokenizer/document vectors в Kaggle. Дела о расхождениях и ранее работавшие media/DjVu функции остаются обязательными.

Дополнение v4: обязательный pinned skill до runtime работы; direct bulk client hosting/storage↔Kaggle без Dev byte proxy; отдельные client refs/controller tickets, exact object verification, временный Dataset GC по refs/leases и truthful MCP effects. Полный контракт — единый handoff §0/5.5–5.6, приёмка DBG-11–13.

Обновлено 20.10.2026. Это целевой контракт, а не отчёт о внедрении. Исходная реализация проверена на [main b1c0689](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/b1c0689b1a09373a900a317a3bac9cf4f86fdaf4): corpus/FTS/graph/stories ещё используют SQLite, книжные vectors — Supabase, story vectors — отдельный SQLite/Python путь.

Единое самодостаточное [задание кодовому агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md) задаёт внедрение и отладку. Здесь описаны компоненты и пути данных; [storage](storage.md), [configuration](configuration.md), [управление нагрузкой](designs/user-request-load-management-20261019.md) и [приёмка](testing/rkb-scalable-kaggle-acceptance-20261019.md) раскрывают соответствующие контракты.

## Размещение и владельцы

| Компонент | Ответственность |
|---|---|
| DevCoveer: существующий MCP/ASGI, BgeQueue/controller | Тонкий auth/admission relay QUERY; проверенная identity, общие fairness/permits и writer/run/connection authority. Durable request/dialog/delivery lifecycle для соответствующего класса работы. Короткий raw spool и компактные request/UNKNOWN/receipt metadata; рабочего корпуса и vectors здесь нет. |
| Один Kaggle writer | PostgreSQL 17 с закреплённым pgvector, принятый corpus, bibliography/provenance, факты, entities/relations, истории/варианты, даты/observations, cases/private drafts и индексы. BGE-M3 и разрешённая обработка исходников. |
| Kaggle readers при измеренной необходимости | Восстановленная проверенная snapshot, SQL/BGE/search/proof; запрещены независимые domain writes, jobs/outbox и выбор canonical snapshot. |
| Private Kaggle Dataset | Основной постоянный versioned persistence: consistent PG snapshots с indexes/manifest/cursor и поддержанным continuation. Старт и сохранение идут через точную подтверждённую version, не через writable mounted PGDATA. |
| Yandex S3 | Независимая резервная копия подтверждённого Dataset-состояния раз в неделю. Старый online S3 tail — существующая реализация, не подмена выбранного primary; новый durability binding закрывается явно без потери принятых records. |
| Telegram archive / Telethon в Kaggle | Прямые upload/download оригиналов и иллюстраций без VibePublish/Dev proxy; успех по доступному media locator, photo по умолчанию и document при реальной необходимости разрешения. |
| Regional Cartography / Street Story | Картографическая интерпретация/геометрия и физическая POI identity соответственно. RKB сохраняет source-place graph и ссылки, не создаёт второй канонический каталог. |
| GitHub / публичная case page | Код и публичная документация; выбранная безопасная проекция дела с постоянным URL и проверенным readback. Source bytes, private drafts и recovery objects в GitHub не попадают. |

E5 и Supabase отсутствуют в target. Один BGE space: существующий pinned model/tokenizer, 1024 dimensions, FP32, точный полный embedding input не более 512 токенов. Ориентир семантического passage — около 256 токенов. Model inference выполняется только в Kaggle.

## Один транспорт и одна authority

Сохраняются два независимых интеллектуальных маршрута: Dot → RKB и Mira → RKB. Mira не проходит через Dot; Dot сохраняет собственный прямой путь к тому же RKB. DevCoveer выполняет auth/admission/relay, без промежуточного reasoning-агента. Notebook сам открывает постоянный authenticated outbound WSS к существующему ASGI по HTTPS/443; публичного Kaggle listener и открытого PostgreSQL нет.

По одному transport идут разные классы: ephemeral QUERY/QUERY_RESULT для bounded reads и durable DISPATCH/RESULT/ACK для jobs/mutations. QUERY не получает durable queue row, fsync/cloud seal или mutation RESUME; общий controller выдаёт ему read permit из того же ресурсного учёта/search reserve. HTTP pull и WSS не становятся двумя dispatchers. Job IDs/claims и durable replay сохраняются для своего класса работы.

Каждая domain запись проверяет current writer epoch, run, claim и connection generation. QUERY проверяет request/turn/connection generation, общий deadline, текущие grant/scope и min_data_seq; late/cancelled result не переходит в новую реплику и не replay после reconnect. Reconnect заменяет attachment и fencing старого socket; stale claim — ошибка конкретного задания, revocation run — отдельное событие. Пока Kaggle-сессия жива, временная потеря WSS сохраняет загруженные PG/BGE. Завершение самой сессии требует нового run, restore и warmup.

Неожиданная ошибка вычисления durable document-vector использует тот же bounded reconnect, что и reconciliation source command: сохраняются исходные job ID/claim, а загруженная модель не заменяется. Успешный результат остаётся до ACK; постоянная ошибка исчерпывает существующий reconnect budget, вместо вечного занятого слота без результата. Событие `native_job_retry` содержит только run/job/kind и класс ошибки, без исходного текста.

Ping/Pong, полезный спрос, task progress, короткая writer lease и 30-минутный useful-idle — разные таймеры. Сокет без полезной работы не удерживает notebook бесконечно. Frames, bytes, inflight credits, reconnect backoff и task deadlines ограничены. Большие payloads передаются существующим bounded byte transport по проверенным refs, а не огромным WSS JSON.

При плановом drain новые чтения и импорт закрыты, но точный текущий writer и проверенный Dataset prefix сохраняются только для финального checkpoint и его внутренних RPC. Общая проверка binding остаётся READY-only, включая правки Case/Story, приём оригиналов и шаги импорта. Ожидание checkpoint не останавливает dispatch/receive его RPC. После его исхода по прежнему аутентифицированному каналу отправляется STOP; отсутствие сокета или heartbeat не освобождает reservation до подтверждённого terminal конкретной версии Kaggle. Старый writer после смены current run, потери канала или expiry не получает права публиковать checkpoint.

Отладка переиспользует healthy owned run со **стабильным slug**. Минимальный HELLO/control приходит до install/restore/BGE, READY после probes. SQL и совместимый stateless read handler меняются без повторного model load; mutation executor — через ordered migration/reconciliation, context/deps — через controlled restart. Snapshot replacement по умолчанию требует finite PG maintenance окно, не двух clusters. UNKNOWN push/stop не запускает replacement; provider terminal относится к exact execution, cancel ACK/delete/TTL недостаточны. Expected outputs определены до работы. Полный контракт и новые cases — [handoff §5.4/6.6/14.5](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Долговечность и ACK

Ранний local receipt означает **pending**. Финальный accepted/Saved разрешён только после remote confirmation точных payload и intent; Saved дополнительно означает domain commit и remotely verified exact outcome/receipt; облачно сохранённый intake ещё не является applied результатом. Этот контракт относится к mutations и явно принятым durable requests. Group commit удерживает их готовую группу не более 1 секунды до flush; provider latency не выдаётся за гарантированную секунду сохранения. Обычный read-only turn не становится durable request только потому, что его задал человек.

NEW уже содержит S3 transport и части immutable journal/CAS. Это не доказательство Dataset-primary publication. Переносить его recovery semantics на выбранный Dataset-путь либо явно согласовать дополнительный tail binding; не сохранять обязательный online S3 незаметно под названием weekly backup. Segment связывает sequence range/count, payload hashes и previous hash; orphan upload не accepted. Один existing controller выполняет bounded compaction sealed segments с сохранением logical IDs/hashes/seq и CAS refs, чтобы не накапливать по одному объекту на каждую секунду бесконечно.

Обычный read-only evidence search не ждёт remote journal ни перед dispatch, ни перед ответом. Audit refs/hashes/timings пишутся параллельно через bounded buffer, без raw query/private evidence в logs и без общей blocking write-lock на serving path. Переполнение/потеря такого audit отражается счётчиком и не выдаётся за durable receipt. Если нужны гарантированные продолжение/сохранение результата, это явный durable request другого класса. Зависимый read несёт min_data_seq и состояние неподтверждённой записи диалога; быстрый ответ не должен ложно объявлять её отсутствие.

DevCoveer raw spool: стартовый горизонт 4 часа, hard byte cap 512 MiB, с проверкой реального envelope до cutover. Компактные UNKNOWN/request/receipt metadata живут до reconciliation, а не до spool TTL. Local GC требует remotely sealed payloads и verified metadata; недельная backup не задерживает local GC после доказанного внешнего покрытия, а более свежий непокрытый хвост остаётся pending. Remote-tail GC, напротив, сохраняет всё необходимое designated retained verified baselines и unfinished consumers.

До получения apply receipt spool резервирует максимальный размер outcome; после точного APPLIED или REJECTED_PRECONDITION учитывает сохранённый размер outcome. Admission вычисляет занятый объём из существующих записей в той же транзакции, без отдельного хранимого счётчика `used_bytes`; старая колонка удаляется при открытии очереди. Исходные команды, receipts и полный резерв ещё неизвестных исходов сохраняются.

Изменившаяся PG base создаётся каждые 4 часа, раньше при replay/byte pressure или плановой передаче writer. Без изменений полная копия не создаётся. Control checkpoint — каждые 15 минут; это не задержка фиксации новых grants, accepted commands или receipts. Подробности и failure domains: [storage](storage.md).

## Схема для быстрого извлечения

Canonical source data и serving projections находятся в одной PG БД. Searchable поля типизированы; JSONB нужен для переменного provenance, не для обхода всех dossier snapshots при каждом запросе.

Новые native packet evidence — ссылки на passage/page/region. Для собственного пакета модель указывает `chunk_key`, для ранее принятого фрагмента — `chunk_id`; текст читается существующим `fetch`. Проверка формы выполняется в Python, SQL сохраняет принадлежность ссылок источнику и права повторного использования сущностей. Новые findings/graph не сохраняют копии `original_excerpt`/`exact_quote` и не требуют единственности цитаты. Исторические принятые байты и legacy whole-book replay сохраняют прежнюю интерпретацию; новая семантика вводится отдельной упорядоченной миграцией SQL028, без изменения SQL001–027. Это описание кода не подтверждает его установку в живой notebook.

| Данные | Минимальные ключи / индексы |
|---|---|
| document / revision / region / passage | PK/FK; source SHA, revision и exact locators; B-tree по document, revision, ordinal для book scope и продолжения. |
| passage_search активного принятого корпуса | Stable passage ID, material version/hash, language, tsvector, BGE vector(1024); GIN для FTS, HNSW cosine для broad dense. Старые search revisions не заполняют active ANN. |
| story_search | Отдельная типизированная projection того же BGE space; editorial revision/material hash, GIN/HNSW. Source passages и пересказы не конкурируют в одной неразличимой выдаче. |
| catalog | Нормализованный title и ID; B-tree и keyset pagination, SQL filters. Hydration только выбранной страницы. |
| aliases / entity mentions | B-tree normalized_name/language и entity→evidence links; язык, вид alias и temporal scope сохранены. Дополнительный trigram GiST на names только для нужного fuzzy top-K. |
| assertions / cases / source dependencies | Индексированные FK и обратные link keys. Поиск по entity/source/state не раскрывает каждый JSON snapshot. |
| geo | PostGIS geometry с явным SRID и GiST при пространственном query path; временные интервалы индексировать под реальные predicates. |
| dates / observations | Типизированные calendar, date_role, precision, subject, metric, exact decimal, unit, period и evidence. Индексы соответствуют календарному/метрическому запросу, не меняют семантику сравнения. |

Revision projections immutable. После готового staging атомарно переключается небольшой active pointer и создаётся persisted research frontier; activation не переписывает 100000 строк с active flag. Inactive versions ограничиваются retention/GC, а current-pointer filter вместе с iterative/exact path не допускает ANN scope starvation. Неудачная новая revision не лишает поиска прежнюю принятую. Вывод старых derived indexes в GC не уничтожает immutable evidence history.

## Fused QUERY: весь retrieval внутри Kaggle

Один typed QUERY передаёт query, проверенный scope, request/turn generation, остаток общего deadline и min_data_seq. DevCoveer не запрашивает embedding отдельным round trip и не отправляет вектор обратно в SQL. Kaggle возвращает bounded evidence/bibliography DTO с freshness и retrieval mode; отдельные большие pixel artifacts используют свой authorized transport.

1. Dev проверяет principal/current grants и получает permit общей authority; Kaggle проверяет scope, revision/visibility и допустимую freshness.
2. В том же notebook запускаются lexical/alias SQL и query encode одного resident BGE. Независимые SQL branches не ждут encode; dense SQL использует полученный vector локально.
3. Здесь же выполняются bounded dedup/RRF и один batched hydrate top-K exact evidence с обязательной библиографией. Полный corpus и промежуточные vectors не пересылаются Dev.

4. Kaggle проверяет current source eligibility, Dev — current person/grant/turn fence перед выдачей. При отзыве или недоборе допускается bounded refill либо честный partial/degraded в оставшемся budget; это не новое полное retry окно.

Для source search существующий batched hydrate тем же SQL-запросом
присоединяет название из `source_catalog` по source/revision. Прежний порядок
top-K сохраняется через ordinality. Тонкий поиск, evidence и exact fetch показывают
принятый `catalog.title`; если названия нет — прежний
ID. Отдельный fetch каталога после поиска не выполняется. Это описание кода;
живое исправление требует установки API и совместимого native read-handler.

Нельзя каждый раз строить allowed-map всех книг, отправлять весь каталог JSON в vector query, вычислять все story similarities в Python или делать запрос на каждый title/evidence. Поиск active accepted book text не требует per-book owner ACL. Private drafts и mutation scopes сохраняются отдельно.

Broad ANN использует прямой ascending ORDER BY distance и LIMIT до window/tie-break/hydration. Для селективного book/geo/time scope допустим индексированный shortlist и exact distance. Для широкого filtered ANN использовать поддерживаемый pinned pgvector iterative scan с конечными ef/scan/memory/deadline budgets. Strict order означает порядок найденных кандидатов, а не perfect recall. Dense branch не ограничивается lexical top-K.

GIN не отменяет стоимость ранжирования очень частого слова; lexical/alias branches имеют общий budget. FTS language configuration задаётся per row как RU/DE/simple и версионируется; query проверяет лишь bounded набор нужных configurations, без alias loop; исходные имена сохраняются, aliases не доказывают identity. Не переносить SQLite BM25 scores в PostgreSQL ts_rank_cd как эквивалентные. Короткий Seq Scan маленькой таблицы допустим; обход всего corpus в обычном поиске — дефект.

Actual prepared ANN plan проверяется после не менее шести executions и на крайних selectivity. При доказанном generic-plan regression использовать custom plan/disable preparation только для ANN, не для всей БД. pgvector knobs задаются через SET LOCAL внутри transaction и не протекают между pool clients.

Canonical command outcome hash включает exact domain IDs/body/evidence, но не заново вычисленные BGE floats, FTS или crops. Derived projections при необходимости отмечаются dirty и rebuild отдельно; durable accepted BGE artifacts можно reuse. Dense lag является отдельной readiness и не блокирует canonical replay. Принятые domain/preview locators и их provenance сохраняются. Для Telegram photo возможная рекомпрессия не проверяется custom hash: доступность media по locator — отдельная граница, не integrity PG snapshot.

Spatial predicates используют индексируемые ST_DWithin/ST_Intersects и согласованный CRS. Не вычислять ST_Transform для каждой строки без соответствующего expression index. Не создавать partition/HNSW на каждую книгу и не индексировать все поля без query owner.

## Язык runtime и общие ресурсы

Default — тонкий Python bootstrap/control и QUERY handler, один resident PyTorch FP32 BGE process, нативные PostgreSQL/pgvector/PostGIS kernels. Вызов Python API не означает вычисление tensor math в Python; фактический tokenizer backend проверяется вместе с pinned assets. Encode и blocking operations вынесены из event loop; модель/процессы не загружаются и не создаются на каждый запрос. Torch threads, PG pool/parallel workers, import/proof и maintenance делят один measured CPU/RAM budget с search reserve; четыре Torch threads и один interop — исходный профиль, не универсально свободные ядра.

Заранее собранный Go/Rust executable допустим в совместимой Linux-сессии; baseline остаётся Python glue, native sidecar не является default, полный rewrite не требуется. Go на DevCoveer может заменить измеренный transport/serialization component существующей authority, а не добавиться proxy перед ним; Rust — конкретную дорогую функцию или компонент с воспроизводимой проблемой памяти/lifecycle. Память, deadlines, backpressure и control-loop isolation могут обосновать native выбор для нового компонента и до доказанного CPU hot loop. Подходящий проверенный native компонент допускается переиспользовать без полного Python дубля; готовый fused RKB executor пока не подтверждён.

Проверяется один узкий вариант: полезная задержка, ресурсы или отказы должны улучшиться без регрессии quality, прав и recovery. Замена сохраняет один controller/model и удаляет заменяемый путь. Local IPC допустим при подтверждённом суммарном выигрыше; его copies/serialization/backpressure и дополнительный lifecycle, включая Python BGE child, входят в стоимость вместе с cold recovery. Нулевой IPC и обязательный процентный порог не являются условиями выбора. ONNX, GPU, quantization и новый tokenizer/inference backend — отдельный quality-gated track.

Release bundle закрепляет source/toolchain/hash, Linux architecture/ISA/libc и shared-library ABI, protocol/model/tokenizer/PG pins. Сборка вне notebook, immutable artifact проверяется до исполнения. Supervisor удерживает lifecycle, собирает exit status, ограниченно читает logs и корректно завершает PG; SIGKILL/session end не гарантируют cleanup. Бинарник не продлевает Kaggle lifetime. Детали и первичные источники — в [оценке runtime](designs/kaggle-regional-knowledge-service-assessment-20261019.md).

## Функции, которые сохраняются

Импорт start/pages/stage/validate/finalize/status, exact regions/quotes/captions/footnotes, dedup и resumability; source facts без обязательной Story; Story → revision → publication Variant, history/edit/merge/archive/restore; даты и calendars, observations/comparison, люди/организации/relations; independent disagreement registry/cases, вклады/аргументы/conclusions; private drafts; geo queue/POI outbox; source proof и archive continuation; публичная case page с last-known-good и revocation; единый request lifecycle Slack, personal MCP и Миры.

Миграция переносит **только геоданные**: layers/places/geometry/CRS/temporal aliases/provenance и минимальные ссылки на использованные источники. Всё остальное, включая негеографические исторические факты, stories, drafts/cases и книги, создаётся через fresh import/workflow. Старое состояние сохраняется как reference до проверенного cutover; это не перенос его corpus в target. До production book imports фиксируется проверенный GEO identity manifest: stable layer/place IDs, aliases и CRS. Bulk raster transfer может идти в phase 4; stage 2 до manifest допускает только isolated fixture, не production import.

Текст и библиография активных принятых книг, а также разрешённые иллюстрации доступны всем. Stripe quote scans доступны owner/demo/expert/local_historian; полный PDF/DjVu и full-page pixels остаются private по отдельному grant. Превью не должно раскрывать большую область, чем разрешённый artifact. См. [rights](rights.md).

## Границы готовности

Core/GEO runtime cutover и feature-release gates Миры, VK, Astro/case pages и research needs оцениваются отдельно. Все эти функции обязательны для завершения всего проекта; NOT RUN не означает работающую функцию. Регрессия уже включённого канала блокирует switch: молча отключать канал для прохождения core gate нельзя.

Ready означает совместимость snapshot/cursor плюс действующий SQL query и dense query probe с полезным результатом. Physical manifest включает PG minor, extensions/libs/platform и collation provider/version; keyed normalized text использует C collation для детерминированного сравнения. Compatibility/version check и при необходимости корректный rebuild только затронутых indexes предшествуют READY. Не предполагать непроверенную Kaggle UI feature для закрепления окружения. Stored-vector counts, HTTP 200 или Pong readiness не доказывают. Search имеет backend p95 ≤1 s, public MCP p95 ≤1.5 s и hard evidence deadline 2 s; ожидание полного пользовательского вопроса измеряется отдельно.

Источники реализации и проектирования: [исходный backend](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/sqlite_backend.py), [story search](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_registry.py), [pgvector](https://github.com/pgvector/pgvector), [PostgreSQL FTS](https://www.postgresql.org/docs/current/textsearch-indexes.html), [PostGIS indexes](https://postgis.net/documentation/faq/spatial-indexes/). [Полная прежняя архитектура](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/architecture.md) — историческая запись, не план установки target.

Current native batch base validation is defined by ordered SQL029. The compiler validates the model JSON once; SQL keeps source and original identities, the pinned inference space, passage uniqueness/order, nonempty text and the 512-token bound. Optional review fields do not become a second closed JSON schema. Historical whole-book validation and already accepted command bytes remain unchanged. This source change is not evidence of a live migration.

The native read role receives only source_id, page_id and physical_page_index from source_stage_pages for accepted Story evidence links (SQL030 and cold-start role configuration). Raw model_page and image_receipt columns remain inaccessible to that role; Story/source eligibility checks still apply.

The resident read-handler slot forwards SQL_HEALTH's exact transport-owned lease callback to the captured query handler. Health still checks the current connection/resource and uses neither BGE inference nor tokenization; ordinary read callbacks keep their existing arguments. A source fix to this wrapper is not evidence that an already running notebook has loaded it.

Current-packet compilation checks complete textual-region and span coverage only on pages in the original accepted packet. Indexed prior pages remain reference context: a selected accepted passage does not require loading or covering unrelated regions on its page. Reference identity, source membership and selected span validity are still checked. Legacy whole-source validation retains complete coverage of every page. This compiler change requires a compatible executor release; it is not a live installation receipt.

Model-facing span references contain page, region and original Unicode offsets; the current native compiler derives their internal provenance digest from known source text. Historical serialized hash fields remain readable without changing accepted bytes, and legacy validation of a supplied hash remains unchanged. Source identity, offset bounds, word boundaries and complete coverage still apply; this source change is not a live rollout receipt.

Pending research may defer only a contiguous uncovered suffix of the last current page's body text, including a suffix inside a region selected with exact `span_refs`. The model chooses semantic chunk boundaries, near 256 tokens, at sentence boundaries; the backend never determines boundaries from punctuation or splits/joins passages. Exact trailing transcription remains accepted. Middle gaps and uncovered footnotes still reject. Status computes pending tail pointers and offsets from existing source regions and passage provenance through the same authenticated native maintenance channel, returns `continue_tail`/`tail_refs`, and refuses finalize while any remain or coverage is unavailable. No tail table or local domain cache is added. The next text packet resolves explicitly referenced prior regions for model-selected cross-page passages; malformed optional semantic input still cannot drive text-phase context reads. The four-page packet limit is unchanged.

For a ready restored Dataset, `startup.coverage` is its restored cursor/control plus the locally retained native apply receipts after that cursor. Startup and API reconnect do not open full-history, adoption or periodic history-verification sessions. Unknown apply outcomes do not advance the applied cursor; the existing reconciliation, writer, schema barrier and product SQL permissions remain responsible for writes. Source tail inspection still uses its existing bounded native operation. Genesis before the first Dataset retains its original initialization path.
