# Codex — универсальный каталог, доказательные страницы и ёмкость RKB

Это исполнительная постановка следующего этапа. Выполни реализацию по `docs/design/catalog-evidence-page-archive-v1.md` (v1, 2026-10-05), сохранив текущий работающий RKB. Не начинай заново импорт или архитектуру. Не подменяй результат новым аудитом, набором unit tests или сохранённой фоновой задачей.

Основной репозиторий: `onedayonemasterpiece/regional-knowledge-base`. Связанный: `onedayonemasterpiece/vibepublish`. Проектирование выполнено на RKB main `1819474562975ce6b0ba3716b809d3102a56d4d0` и VibePublish main `5d3d1de4cf661bf1715bc06f39686de15e44f4da`. Перед изменениями прочитай текущие main/runtime, AGENTS, известные незавершённые jobs/PR и требования; более поздние исправления не откатывай. Если документ поставлен отдельным PR, прочитай именно закреплённую версию документа из переданной владельцем ссылки, не теряй требования из-за отсутствия файла в старом checkout.

## Цель

Новые книги, журнальные выпуски и отдельные статьи импортируются одним общим MCP workflow. Пользователь видит каталог конкретных изданий и авторов, получает точные цитаты; имеющий соответствующие права — обложку и настоящую страницу с жёлтыми полосами точного текста. Страницы архивируются в приватном Telegram постепенно, без ожидания ChatGPT. Supabase не переполняется от корпуса порядка 100 книг.

## Неподвижные ограничения

1. Смысловое чтение, транскрипция скана и выбор семантических границ выполняются ChatGPT/явно подключённой моделью. Сервер — deterministic transport/validation/render/queue, не OCR/VLM/LLM parser.
2. Сохранить search/fetch, natural-language ingestion/reprocess, source identity, multi-page provenance, source/display/provider digest distinction, E5+BGE+FTS и Live profile. Не вносить special-case по автору, названию или UUID тестовой книги.
3. Source privacy, user/component ACL, feature entitlement и rights policy — отдельные проверки. Supabase не MCP OAuth authority. Не передавать user bearer другому сервису и не расширять доступ технической identity.
4. Публичные репозитории не содержат книги, сканы, извлечённый пользовательский текст, private IDs, архивные URL и credentials. Реальные fixtures/замеры — в приватном retained artifact. Новые временные файлы — managed central artifacts по host policy; legacy untracked files не удалять вслепую.
5. Не покупать тариф, не менять модели/размерности ради quota, не добавлять новый vector engine или retrieval-window table. Не разворачивать долговременный корпус в случайном dev worktree.
6. Telegram: общий предел <=20 исходящих media files в любом rolling 60s на connection/account, все темы/источники/обложки/иллюстрации/pages/albums вместе; provider FloodWait/SlowMode также соблюдать. Переиспользовать существующий Vibe limiter/replay, не второй независимый.
7. Ссылка page archive topic задаётся владельцем в приватном запуске. Проверить существующий Knowledge Base connection/grant/target; не считать ссылку разрешением обойти grant. Не создавать новый чат или альтернативную сессию автоматически.

## Поставка A — каталог и идентичность

Реализуй минимальные publication/serial/source-binding сущности, не меняя смысл processing revision. Поддержи book_edition, serial_issue, article; optional work grouping; отдельный и содержащийся в выпуске article. Выпуск имеет title/ISSN/volume/issue/year; статья — свои авторы, title, DOI при наличии и source spans. Combined issue labels и неизвестные поля допустимы.

Contributors: ordered display_name/role/provenance; coauthors — несколько author; editor/translator/compiler/corporate_author отличны от author. Сохрани legacy authors[] projection. ISBN-10/13, ISSN/eISSN, DOI/local_id: raw/normalized/validation/provenance; неверное не исправлять догадкой. Publisher/place/edition/date/language, source_annotation отдельно от model_summary; cover/front/back/title_page/container_cover с честным absent статусом.

Классификации независимы: resource_type, publication_form (monograph/source_edition/reference/etc), audience/purpose (scholarly/popular_science/educational/etc), subjects, geographic/time coverage. Versioned vocabularies + namespaced extensions, не большой enum на всё.

Новый catalog list/find/get с cursor и limit<=20, пустой query для list, фильтрами. Показывай реально импортированное и незавершённое отдельно, не считай старые ревизии новыми книгами. Citation результата должен называть правильное издание/выпуск/статью и её авторов. Исправление метаданных не переиндексирует неизменённый текст.

Введи semantic_unit_id/component ranges: continuation и chunks не перескакивают между статьями журнала. Общая страница с двумя статьями требует region/span boundaries. Продолжение на другой странице — по принятой связи, не по случайной соседней записи.

## Поставка B — точные quotes и proof capability

Добавь goal-oriented evidence presentation с режимами quote/page/cover/bundle, сохранив стандартные search/fetch. Quote извлекается из immutable accepted text по source+revision+region/span и Unicode [start,end), с hash точных UTF-8 bytes. Не генерируй цитату и не цитируй model observation как печатный источник. Multi-page/noncontiguous fragments и редакторские разделители явно различены.

Сохраняй span-to-page line/word quads и transform к archived raster. Embedded text layer допустимо читать детерминированно; для image-only модель задаёт геометрию во время review, сервер не запускает OCR. Блочный bbox или одна рамка у нескольких split parts НЕ exact highlight. При недостаточной геометрии возвращай highlight_unavailable, а не жёлтый целый абзац.

Highlight — временное наложение прозрачных жёлтых bands поверх настоящей чистой страницы. Не перерисовка генеративной моделью. Отдельные quads для строк/колонок; rotation/crop/skew проверяемы. Cache TTL/bytes, ключ page hash+quote spans+overlay profile; не постоянная картинка для каждого запроса.

Server-side matrix: entitlement AND source/component access AND imagery rights. Скрытый инструмент не заменяет проверку. Пользователь статьи не получает весь scan с закрытой соседней статьёй. Source revocation и cache также проверяются. Реальный scan подтверждает наличие текста, не истинность всего ответа.

## Поставка C — WebP и асинхронный архив

PDF/DjVu уже на сервере: рендери страницы там детерминированно, не заставляй ChatGPT делать upload того же raster обратно. Отдельные page attachments допускаются с durable capture до accepted. Не сохраняй только временные download URLs.

Источник -> bounded render -> atomic disk spool+SQLite journal -> Vibe queue -> pacing -> native verified readback -> sealed manifest -> final registration. Stable source/page/profile keys, lease, idempotent receipts, resume после каждого crash window. Unknown outcome не пересылается новым ключом. Одна страница хранится один раз и связана с многими chunks через page/spans.

В Vibe исправь узкий opt-in sanitized WebP-document contract: текущий image ingress незаметно превращает WebP в PNG и хранит две BLOB-копии. Проверяй actual MIME/bytes/dimensions/orientation/readback, сохраняя legacy behavior других клиентов. Lossless WebP — baseline; качество мелкого текста подтвердить. Preview не авторитетный scan. Original source bytes не менять.

Очередь имеет byte quotas, free-disk floor, bounded concurrency и backpressure. Не рендерить весь корпус заранее. Progress локально; без per-page Supabase updates/status scans. Финал — один idempotent domain RPC с compact manifest pointer/count/digest и revision fence. Для большого manifest данные готовятся shard-wise вне Supabase, а доступность переключается атомарно. Один API commit не означает обязательную одну гигантскую SQL строку.

N-1 verified pages не включает enhanced scans. Search/text quotes могут работать раньше. Scan выдаётся только после готовности полного source archive, регистрации manifest, готовности геометрии конкретной цитаты и проверки прав. Последующая потеря страницы — degraded/unavailable. Повторный processing import неизменённого source переиспользует pages.

Не копируй старый IllustrationMirror per-item update pattern на 30k страниц. Сохраняй provider receipts локально; после всех upload/readback установи один готовый manifest. После verified delivery освобождай лишние RKB/Vibe ingress bytes, сохраняя replay proof; pending/unknown items не GC.

## Поставка D — ёмкость, lifecycle и размещение

Сначала воспроизведи aggregate-only audit через `scripts/production/audit_storage_budget.py`. Не выводи credentials/source text. Раздели actual database, tables/TOAST/indexes, active/inactive/staged, legacy vectors и binary stores. idx_scan=0 не разрешает удаление индекса. Неактивное не значит безопасное к удалению.

Инженерный DoD: 100 book-equivalent sources / 100k active chunks / 30k pages; Supabase steady-state <=400MB всей квотируемой БД, включая baseline/indexes/TOAST. Проверить 10/25/100 и largest-source reimport peak. Текущие float32 E5+BGE требуют 564,8MB на100k chunks только на значения, без indexes/text. Одной чисткой цель не доказывается.

Базовый вариант: Supabase compact catalog/ACL/rights/revision/manifest state; тяжёлые active source text/geometry/FTS/E5/BGE — существующий private PostgreSQL/pgvector на разрешённой production-инфраструктуре. Переиспользовать SQL/адаптеры. Более простой all-Supabase вариант оставить только при реальном доказательстве <=400MB и качества/latency до migration; halfvec — проверяемая оптимизация, не гарантия. Модели/dimensions/полнота не жертвуются ради quota.

Private query store, spool, Vibe SQLite/assets/WAL, cache и backups тоже измеряются/ограничиваются. Плановый hot-query бюджет <=3GB на benchmark; подтвердить или явно согласовать отклонение. Не переносить проблему в неограниченный локальный BLOB-store. Нет разрешённой инфраструктуры — blocker, не скрытый purchase.

Если данные разделены: ACL/component scope применяется до candidate ranking, active revision и hashes fence все projection reads; перед выдачей recheck access. Ни auth обхода, ни global-topK-then-filter как единственного механизма. Готовить query snapshot, затем выбирать catalog pointer; crash/rollback tested.

Убрать старые горячие vectors/FTS после verified cold snapshots, pinned citation/recovery checks и grace policy. Старые exact evidence IDs продолжают разрешаться. Accepted text/geometry хранить без бесконтрольных дублей; chunks — производные spans. Резервные копии и восстановление приватные. До destructive cleanup — dry-run, проверяемый backup/restore и operator checkpoint; не VACUUM FULL/DELETE всего inactive наугад.

## Приёмка и отчёт

Выполни D01–D16 из design, а не только unit tests. Важные наборы: book native PDF, image-only scan, двухколоночный выпуск с разными авторами/статьями на одной странице, отдельная статья, разные издания, unknown ISBN/cover, multiline/rotated/footnote/repeated quote. Semantic holdout и capacity corpus различны. Не подгонять систему только под известные шесть вопросов; сравнивать одинаковые frozen baseline/candidate inputs, exact citations, complete-answer evidence и отрицательные запросы.

Транспортный stress: минимум60media units, два workers, restart, все темы и albums в общем rolling budget; fake provider для массы, реальный canary1–3 разрешённых страниц в точной теме. Проверить MIME/readback, последний missing page, commit after timeout, revision change, stale URL, spool full, revoked grants, отсутствие дублей.

Проверить actual production runtime после controlled rollout и actual ChatGPT schemas, не только наличие функций в main. Нагрузочные/production evidence отчёты сохранять приватно; в публичном report только обезличенная методика, schema, тесты, aggregate выводы без corpus locators/text.

Работай небольшими PR/поставками. После каждого результата сохраняй проверяемый checkpoint: commit, tests, migration/rollout state, незакрытые DoD. При долгом deterministic test — один durable job, не параллельные копии и бесконечный polling. Если RPC потерян, сначала operation/work state. Если инструмент действительно недоступен, сообщи конкретный blocker и дай сохранённый handoff, не утверждай успех.

Итоговый ответ должен содержать: что доступно для нового импорта; реальную ёмкость и её границы; что хранится в каждой системе; readiness/permissions; выполненные и невыполненные D01–D16; ссылки на PR/commits, приватный acceptance и команды повторения. Не объявлять весь продукт готовым, пока остаётся неподтверждённый capacity, визуальная точность или client-visible MCP contract.
