# Архитектура базы знаний региона

Обновлено 20.10.2026. Это целевой контракт, а не отчёт о внедрении. Проверенный исходный main — [a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/a8cf41b50b8ccb62409d046d009d53c09d74b1d7): corpus/FTS/graph/stories ещё используют SQLite, книжные vectors — Supabase, story vectors — отдельный SQLite/Python путь.

Единое самодостаточное [задание кодовому агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md) задаёт внедрение и отладку. Здесь описаны компоненты и пути данных; [storage](storage.md), [configuration](configuration.md), [управление нагрузкой](designs/user-request-load-management-20261019.md) и [приёмка](testing/rkb-scalable-kaggle-acceptance-20261019.md) раскрывают соответствующие контракты.

## Размещение и владельцы

| Компонент | Ответственность |
|---|---|
| DevCoveer: существующий MCP/ASGI, BgeQueue/controller | Проверенная identity, request/dialog/fairness, admission, writer/run/connection authority, доставка. Короткий raw spool и компактные request/UNKNOWN/receipt metadata; рабочего корпуса и vectors здесь нет. |
| Один Kaggle writer | PostgreSQL 17 с закреплённым pgvector, принятый corpus, bibliography/provenance, факты, entities/relations, истории/варианты, даты/observations, cases/private drafts и индексы. BGE-M3 и разрешённая обработка исходников. |
| Kaggle readers при измеренной необходимости | Восстановленная проверенная snapshot, SQL/BGE/search/proof; запрещены независимые domain writes, jobs/outbox и выбор canonical snapshot. |
| Private Kaggle Dataset | Версионированные PG base snapshots с indexes, manifest, hashes и внутренним applied cursor. Dataset не является живой БД или append-log. |
| Существующий Yandex recovery storage | Immutable command/control/receipt tail и точные accepted payloads; отдельная проверенная суточная base для восстановления при потере Kaggle. Base и tail в одном Yandex не независимы друг от друга. |
| VibePublish / Telegram archive | Точные оригиналы и иллюстрации. Существующий владелец upload/delivery сохраняется; Kaggle получает scoped direct-download доступ. |
| Regional Cartography / Street Story | Картографическая интерпретация/геометрия и физическая POI identity соответственно. RKB сохраняет source-place graph и ссылки, не создаёт второй канонический каталог. |
| GitHub / публичная case page | Код и публичная документация; выбранная безопасная проекция дела с постоянным URL и проверенным readback. Source bytes, private drafts и recovery objects в GitHub не попадают. |

E5 и Supabase отсутствуют в target. Один BGE space: существующий pinned model/tokenizer, 1024 dimensions, FP32, точный полный embedding input не более 512 токенов. Ориентир семантического passage — около 256 токенов. Model inference выполняется только в Kaggle.

## Один транспорт и одна authority

Notebook открывает постоянный authenticated outbound WSS к существующему DevCoveer ASGI по HTTPS/443. По одному соединению идут задания, прогресс, результаты и application ACK. PostgreSQL наружу не публикуется. HTTP pull и WSS не становятся двумя одновременно действующими dispatchers; очередь, job IDs, claims и controller общие.

Каждая запись проверяет current writer epoch, run, claim и connection generation. Reconnect заменяет attachment и fencing старого socket; stale claim — ошибка конкретного задания, revocation run — отдельное событие. Пока Kaggle-сессия жива, временная потеря WSS сохраняет загруженные PG/BGE. Завершение самой сессии требует нового run, restore и warmup.

Ping/Pong, полезный спрос, task progress, короткая writer lease и 30-минутный useful-idle — разные таймеры. Сокет без полезной работы не удерживает notebook бесконечно. Frames, bytes, inflight credits, reconnect backoff и task deadlines ограничены. Большие payloads передаются существующим bounded byte transport по проверенным refs, а не огромным WSS JSON.

## Долговечность и ACK

Ранний local receipt означает **pending**. Финальный accepted/Saved разрешён только после remote confirmation точных payload и intent; Saved дополнительно означает domain commit и remotely verified exact outcome/receipt; облачно сохранённый intake ещё не является applied результатом. Group commit удерживает готовые accepted mutations и реальные пользовательские turns не более 1 секунды до flush; provider latency не выдаётся за гарантированную секунду сохранения.

Existing S3ObjectStore обеспечивает put/get/delete transport. Immutable sealing, exact verification и CAS manifest ещё предстоит реализовать поверх него; generic put не доказывает CAS. Segment связывает sequence range/count, payload hashes и previous hash; orphan upload не accepted. Один existing controller выполняет bounded compaction sealed segments с сохранением logical IDs/hashes/seq и CAS refs, чтобы не накапливать по одному объекту на каждую секунду бесконечно.

Обычный read-only evidence search не ждёт remote journal и не пишет туда каждую страницу результатов. Долговечный человеческий диалог использует request lifecycle, а его отдельные чтения — короткий serving path.

DevCoveer raw spool: стартовый горизонт 4 часа, hard byte cap 512 MiB, с проверкой реального envelope до cutover. Компактные UNKNOWN/request/receipt metadata живут до reconciliation, а не до spool TTL. Local GC требует remotely sealed payloads и verified metadata; суточная backup не задерживает local GC. Remote-tail GC, напротив, сохраняет всё необходимое designated retained verified baselines и unfinished consumers.

Изменившаяся PG base создаётся каждые 4 часа, раньше при replay/byte pressure или плановой передаче writer. Без изменений полная копия не создаётся. Control checkpoint — каждые 15 минут; это не задержка фиксации новых grants, accepted commands или receipts. Подробности и failure domains: [storage](storage.md).

## Схема для быстрого извлечения

Canonical source data и serving projections находятся в одной PG БД. Searchable поля типизированы; JSONB нужен для переменного provenance, не для обхода всех dossier snapshots при каждом запросе.

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

## Путь поиска

1. Проверить текущий доступ и query/scope; получить один query embedding от прогретой BGE.
2. Независимо выбрать bounded lexical, dense и exact/alias candidates.
3. Выполнить dedup/RRF, затем batch hydrate top-K exact evidence и обязательную библиографию.
4. Проверить текущие revision/withdrawal и применимые output capabilities перед cache reuse и delayed delivery. При отбрасывании stale candidates сделать bounded refill либо явно отметить partial.

Нельзя каждый раз строить allowed-map всех книг, отправлять весь каталог JSON в vector query, вычислять все story similarities в Python или делать запрос на каждый title/evidence. Поиск active accepted book text не требует per-book owner ACL. Private drafts и mutation scopes сохраняются отдельно.

Broad ANN использует прямой ascending ORDER BY distance и LIMIT до window/tie-break/hydration. Для селективного book/geo/time scope допустим индексированный shortlist и exact distance. Для широкого filtered ANN использовать поддерживаемый pinned pgvector iterative scan с конечными ef/scan/memory/deadline budgets. Strict order означает порядок найденных кандидатов, а не perfect recall. Dense branch не ограничивается lexical top-K.

GIN не отменяет стоимость ранжирования очень частого слова; lexical/alias branches имеют общий budget. FTS language configuration задаётся per row как RU/DE/simple и версионируется; query проверяет лишь bounded набор нужных configurations, без alias loop; исходные имена сохраняются, aliases не доказывают identity. Не переносить SQLite BM25 scores в PostgreSQL ts_rank_cd как эквивалентные. Короткий Seq Scan маленькой таблицы допустим; обход всего corpus в обычном поиске — дефект.

Actual prepared ANN plan проверяется после не менее шести executions и на крайних selectivity. При доказанном generic-plan regression использовать custom plan/disable preparation только для ANN, не для всей БД. pgvector knobs задаются через SET LOCAL внутри transaction и не протекают между pool clients.

Canonical command outcome hash включает exact domain IDs/body/evidence, но не заново вычисленные BGE floats, FTS или crops. Derived projections при необходимости отмечаются dirty и rebuild отдельно; durable accepted BGE artifacts можно reuse. Dense lag является отдельной readiness и не блокирует canonical replay. Уже delivered artifact сохраняет exact bytes/hash/receipt и не подменяется новой картинкой под прежним receipt.

Spatial predicates используют индексируемые ST_DWithin/ST_Intersects и согласованный CRS. Не вычислять ST_Transform для каждой строки без соответствующего expression index. Не создавать partition/HNSW на каждую книгу и не индексировать все поля без query owner.

## Функции, которые сохраняются

Импорт start/pages/stage/validate/finalize/status, exact regions/quotes/captions/footnotes, dedup и resumability; source facts без обязательной Story; Story → revision → publication Variant, history/edit/merge/archive/restore; даты и calendars, observations/comparison, люди/организации/relations; independent disagreement registry/cases, вклады/аргументы/conclusions; private drafts; geo queue/POI outbox; source proof и archive continuation; публичная case page с last-known-good и revocation; единый request lifecycle Slack, personal MCP и Миры.

Миграция переносит **только геоданные**: layers/places/geometry/CRS/temporal aliases/provenance и минимальные ссылки на использованные источники. Всё остальное, включая негеографические исторические факты, stories, drafts/cases и книги, создаётся через fresh import/workflow. Старое состояние сохраняется как reference до проверенного cutover; это не перенос его corpus в target. До production book imports фиксируется проверенный GEO identity manifest: stable layer/place IDs, aliases и CRS. Bulk raster transfer может идти в phase 4; stage 2 до manifest допускает только isolated fixture, не production import.

Текст и библиография активных принятых книг, а также разрешённые иллюстрации доступны всем. Stripe quote scans доступны owner/demo/expert/local_historian; полный PDF/DjVu и full-page pixels остаются private по отдельному grant. Превью не должно раскрывать большую область, чем разрешённый artifact. См. [rights](rights.md).

## Границы готовности

Core/GEO runtime cutover и feature-release gates Миры, VK, Astro/case pages и research needs оцениваются отдельно. Все эти функции обязательны для завершения всего проекта; NOT RUN не означает работающую функцию. Регрессия уже включённого канала блокирует switch: молча отключать канал для прохождения core gate нельзя.

Ready означает совместимость snapshot/cursor плюс действующий SQL query и dense query probe с полезным результатом. Physical manifest включает PG minor, extensions/libs/platform и collation provider/version; keyed normalized text использует C collation для детерминированного сравнения. Compatibility/version check и при необходимости корректный rebuild только затронутых indexes предшествуют READY. Не предполагать непроверенную Kaggle UI feature для закрепления окружения. Stored-vector counts, HTTP 200 или Pong readiness не доказывают. Search имеет backend p95 ≤1 s, public MCP p95 ≤1.5 s и hard evidence deadline 2 s; ожидание полного пользовательского вопроса измеряется отдельно.

Источники реализации и проектирования: [исходный backend](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/sqlite_backend.py), [story search](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_registry.py), [pgvector](https://github.com/pgvector/pgvector), [PostgreSQL FTS](https://www.postgresql.org/docs/current/textsearch-indexes.html), [PostGIS indexes](https://postgis.net/documentation/faq/spatial-indexes/). [Полная прежняя архитектура](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/architecture.md) — историческая запись, не план установки target.
