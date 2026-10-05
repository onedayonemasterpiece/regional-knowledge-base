# Каталог, точное подтверждение источника и ёмкость RKB — v1

> **SUPERSEDED 2026-10-05. Исторический design, не текущая архитектура.**
> Текущая исполнительная постановка:
> [rkb-sqlite-supabase-ondemand-codex-v2-20261005.md](../prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md).
> Предварительный Telegram-архив всех страниц, локальный PostgreSQL/query plane
> и тяжёлая схема из этого документа отменены.
Дата: 2026-10-05. **Статус: целевой проект следующей поставки; не описание уже реализованных возможностей.** Цель владельца: универсальный импорт книг, журналов и статей, каталог конкретных изданий, цитаты и ограниченная правами визуальная проверка, асинхронный архив страниц, вместимость от 100 книг без раздувания Supabase и временных хранилищ.

Исполнительная постановка: [prompt](../prompts/catalog-evidence-capacity-implementation-20261005.md). Текущие механизмы: [MCP](../mcp.md), [ingestion](../ingestion.md), [storage](../storage.md), [rights](../rights.md). Частные source IDs, тексты, архивные ссылки и production-выгрузки сохраняются вне публичного репозитория. Этот документ не разрешает публиковать корпус или менять права.

## 1. Проверенная база и границы

База проектирования: RKB main `1819474562975ce6b0ba3716b809d3102a56d4d0`; VibePublish main `5d3d1de4cf661bf1715bc06f39686de15e44f4da`. Перед реализацией сверить более поздние изменения, не откатывать их.

Общие исправления уже включают resumable PDF/DjVu import, source identity/reprocess, multi-page chunks, точный source_text, заданную моделью display rotation, раздельные source/display/provider digests, archive replay, E5/BGE/FTS, предупреждения о фрагментации и ограниченный continuation context. Они не гарантируют, что модель правильно разберёт любой новый журнал. Смысловые границы, роли текста и ориентацию модель должна задавать в общем ingestion-контракте, а не в одноразовом скрипте для конкретной книги.

StartMetadataInput пока содержит title/authors/year/language. book_find — ограниченный поиск с непустым query, не листаемый каталог. В текущей схеме нет полноценной модели выпуска/статьи/издания, quote spans и полного page archive. Native text geometry блочная; части длинного региона могут наследовать один bbox. Это недостаточная точность для выделения цитаты.

VibePublish уже реализует durable rolling budget 20 media/60s. Но текущий verify_image преобразует WebP в sanitized PNG, а assets хранит original и derivative BLOBs в SQLite. Поэтому нужна узкая совместимая доработка provider ingress и его GC, а не только смена MIME в RKB.

Смысловую работу выполняет ChatGPT/явно подключённая модель. **На сервере RKB не добавлять OCR/VLM/LLM-разбор.** Разрешены детерминированное чтение существующего text layer, декодирование, проверка структуры, сборка принятых строк, координатные преобразования, компрессия и наложение выделения. Не вводить новый векторный движок или большой библиографический граф ради этой поставки.

## 2. Библиографическая модель

### 2.1. Разные идентичности

Различать произведение/группу изданий, конкретную публикацию, загруженный экземпляр/файл и ревизию обработки. Новое издание — не processing revision. Два скана одного издания могут иметь разные source IDs/hashes. Повторная обработка файла не создаёт новое издание и не требует повторного архива неизменённых страниц.

Минимальные сущности:

| Сущность | Назначение |
|---|---|
| publication | Книга конкретного издания, выпуск журнала или статья, со своими метаданными |
| serial | Название продолжающегося издания, варианты названия, ISSN; не отдельный выпуск |
| publication_source | Связь публикации с существующим source/document и при необходимости page/region/span ranges |
| source revision | Принятый граф конкретного файла и ревизии его обработки |

Достаточно optional work_group_id для явно подтверждённого объединения изданий. Не склеивать автоматически похожие заголовки или записи с одним ISBN. ISBN относится к изданию/формату, ISSN — к продолжающемуся изданию, DOI — к объекту регистрации; они не заменяют source hash и права [S4–S6].

Типы v1: book_edition, serial_issue, article. Выпуск: serial_id, volume_label, issue_label, date/year, special_issue_label. Номера — строки, включая объединённые номера/приложения. Статья: свои title/contributors/identifiers, optional parent_publication_id, printed_page_range и точные source ranges. Статья без известного журнала допустима; отсутствующие сведения не выдумывать.

Статья внутри выпуска — компонент общего источника, а не повторный бинарный импорт всех страниц. Отдельный PDF той же статьи может быть другим экземпляром с явной связью. После подтверждённого сопоставления не удваивать одинаковые поисковые проекции; предположение о совпадении не равно принятому merge.

**semantic_unit_id обязателен для статей:** chunk, footnotes и continuation expansion остаются внутри одной статьи, если переход не подтверждён явной связью. Соседняя статья в том же номере — не продолжение. Продолжение на удалённой странице задаётся связью. Если на странице несколько статей, нужны region/span ranges, не только page range. Контекст заголовка статьи и авторов должен доходить до её чанков без многократного копирования строк.

### 2.2. Поля и происхождение

Поля: title, subtitle, alternative_titles, contributors, language(s), publication year/date с точностью, publisher/place, edition_statement, volume/issue labels, identifiers, source_annotation, optional model_summary, classifications, subjects, geographic/time coverage, cover descriptor, parent relations и доступные экземпляры. Unknown/null/absent — нормальные значения; не подставлять текущий год или сгенерированную обложку.

Contributor: display_name, role, position, optional person/organization identifier, provenance. Роли: author, editor, translator, compiler, illustrator, photographer, corporate_author; словарь расширяемый. Соавторы — несколько author с сохранённым порядком. primary_author только по явному источнику/указанию; первый автор не автоматически главный. Legacy authors[] остаётся совместимой проекцией.

Identifiers: scheme/raw/normalized/validation_status/provenance. ISBN-10, ISBN-13, ISSN, eISSN, DOI, local_catalog_id; контрольные цифры проверять детерминированно. Невалидный прочитанный номер сохранить raw и пометить, не исправлять догадкой. Старые книги без ISBN поддерживаются. ISSN печатного и электронного издания различать; номер страницы не выдавать за идентификатор.

Source annotation — напечатанная аннотация с citation ref. Model summary — отдельное model_generated поле, не «издательская аннотация». User-supplied metadata и external registry metadata имеют своё происхождение. Внешнее обогащение не обязательный запрос на каждый импорт.

Обложка: реальная front_cover/back_cover/title_page с page_asset_id и optional crop. Титульный лист не называть обложкой. У статьи можно показать container_cover с явной подписью, что это обложка выпуска. Отсутствие обложки не делает текстовый импорт неуспешным.

Для существенных полей: источник значения, точная ссылка на страницу/регион либо ручное изменение, reviewed/inferred/unknown, schema version. Исправление года/автора/обложки не должно пересоздавать векторы неизменённого текста. Не дублировать длинные справочники в каждой записи.

### 2.3. Расширяемые классификации

| Ось | Начальный словарь |
|---|---|
| resource_type | book_edition, serial_issue, article |
| publication_form | monograph, collective_volume, source_edition, reference_work, catalogue, atlas, guidebook, memoir, textbook, proceedings, research_article, review, essay, news_item, unknown |
| audience/purpose | scholarly, popular_science, educational, reference, general_public, literary, publicistic, unknown |
| subject | История, архитектура, искусство, естествознание и другие темы отдельного словаря |
| coverage | География и исторические периоды, отдельно от жанра |

Научная монография сочетает независимые оси. Классификация не означает подтверждённую достоверность содержания. Словари версионируются, коды стабильны; namespaced extensions, aliases/deprecated mappings допустимы. Новый жанр не требует PostgreSQL enum migration. UDC/BBK могут храниться как внешние классификационные номера, без обязательного автоматического назначения.

## 3. MCP и каталог

Сохранить search/fetch, ingestion/reprocess и Live. Не требовать от пользователя UUID, cursor или название embedding space. book_find остаётся совместимой обёрткой.

Новые сценарии можно реализовать двумя goal-oriented инструментами:

- catalog(command=list|find|get, query?, filters?, cursor?, limit<=20): все доступные публикации/издания/экземпляры; list допускает пустой query; стабильная pagination; фильтры type/contributor/year/identifier/status.
- evidence_present(mode=quote|page|cover|bundle, evidence_id/publication_id, selectors?, cursor?): точное представление разрешённого источника.

Имена могут соответствовать существующему стилю, но не размножать low-level tools. Каталог отдельно показывает publication_id, число sources, edition/issue identity, metadata provenance, текстовую и визуальную готовность. Старые processing revisions не считаются новыми книгами. «Запись создана», «частично разобрана», «индексируется», «поиск готов», «все сканы архивированы» — разные статусы. По умолчанию показывать реально импортированные публикации; незавершённые — отдельным фильтром. Статья и выпуск — разные публикации, но могут ссылаться на один файл.

Citation в search/fetch: собственные авторы/название статьи и сведения содержащего выпуска либо конкретного издания; physical index и printed label. Не приписывать статьи редактору выпуска. Stable RKB evidence URL предпочтительнее внутреннего Telegram locator.

Проверять не только Python functions, но live tools/list, регистрационные schemas и реально видимое подключение ChatGPT. На аудите richer full server и клиентская шестиметодная схема расходились. Новые методы должны быть доступны после корректного обновления подключения. Live-профиль не загружать архивами и административными методами по умолчанию.

## 4. Цитаты, геометрия и права

### 4.1. Текст

Quote извлекается сервером из immutable accepted source_text, не генерируется. Selector: source/processing revision/region/span и диапазон Unicode code points [start,end). Не смешивать code points с UTF-8 bytes; quote hash — SHA-256 точных UTF-8 bytes. Search normalization требует alignment назад к исходной строке.

Ответ содержит ordered fragments, точные тексты, physical pages/printed labels, source/publication/component, revision и hashes. Непримыкающие фрагменты не склеивать в «непрерывную цитату». Разделитель/многоточие — отдельно помеченная редакторская вставка. Сноска отдаётся отдельно со связью. Model observation/summary не становится printed quotation.

Для image-only источника явно указывать accepted model transcription. Хеш подтверждает целостность этой строки, не отсутствие ошибок чтения печати. Сомнительные символы сохраняются как uncertainty; непрочитанный текст не заполняется догадкой.

### 4.2. Точное выделение

Нужен alignment source range -> page -> line/word quads. Для embedded PDF text геометрия извлекается детерминированно и сверяется с принятым текстом; для скана spans/геометрию задаёт модель в визуальном разборе. Сервер не запускает OCR. Построчная геометрия допустима, но sub-line exact highlight требует соответствующей точности.

Сохранять offsets, review provenance, renderer profile, source dimensions/cropbox/rotation, display transform и archived pixel dimensions. Для колонок и повёрнутого текста — раздельные quads, не общая рамка. Block bbox и унаследованный bbox частей региона не достаточны для exact highlight.

При неоднозначном/отсутствующем alignment — highlight_unavailable и, если разрешено, цитата либо невыделенная страница. Не заливать целый абзац и не называть это точным. Повторная фраза разрешается по selector/occurrence, а не первым find(). Переносы, лигатуры, headers/footers и printed pagination проверяются отдельно.

Хранить чистую страницу один раз. По запросу — временная производная raster + прозрачные жёлтые полосы только по выбранным spans. Текст не генерировать/исправлять/дорисовывать. Multi-page quote возвращает несколько ограниченных страниц/фрагментов с cursor. Cache key: page digest + quote/ranges + transform + overlay profile; TTL и byte quota. Не создавать постоянный Telegram highlight на каждый чанк/вопрос. Возвращать accuracy=exact|line|region|unavailable; запрос exact не деградирует тихо.

Изображение подтверждает присутствие цитаты в источнике, а не истинность исторического утверждения или вывода модели.

### 4.3. Доступ

Различать catalog_read, quote_read, cover_read, scan_proof_read; имена согласовать с текущими grants. Scan требует одновременно feature entitlement, content/component access и право выдачи source imagery. Одного тарифного флага или скрытого tool descriptor недостаточно: server-side проверка при каждом запросе и выдаче cache.

Читатель одной статьи может не иметь права видеть соседнюю статью на той же странице. Full-page выдача требует прав на содержащий source; иначе разрешённый crop/masked rendition или denied. Обложки и приватные Telegram ссылки тоже проверяются. Обычный ответ содержит стабильный авторизованный RKB locator, не автоматически частные архивные URL.

Revocation/удаление инвалидируют выдачу и cache. Дата публикации, ISBN, гражданство автора и genre не разрешают публичное распространение. Существующая rights policy независима от feature entitlement.

## 5. Архив страниц без блокирования ChatGPT

### 5.1. Поток

Для принятого PDF/DjVu сервер сам детерминированно рендерит страницы из exact source. ChatGPT передаёт разбор/ориентацию/spans, а не пересылает уже имеющиеся страницы обратно. Отдельные image attachments поддерживаются, когда источник реально состоит из файлов страниц; их bytes должны быть durably получены до accepted. Нельзя ставить в долгую очередь только истекающий download_url.

```text
source + approved mapping -> bounded render -> durable spool/SQLite
 -> VibePublish durable queue -> shared Telegram pacing -> native readback
 -> sealed page manifest -> one final idempotent registration -> visual ready
```

Page topic задаётся приватной operator config рядом с существующими source/illustration topics. Предоставленная владельцем ссылка не означает автоматического права текущего service grant: preflight проверяет exact connection/chat/topic. Не создавать новый чат/credential или альтернативный Telegram клиент без необходимости.

### 5.2. WebP и идентичность

Целевой профиль — sanitized lossless WebP DOCUMENT без EXIF/location, с сохранёнными dimensions/rotation. Проверить мелкий шрифт, диакритику, таблицы и карты. Стартовый longest edge около 2400px — настраиваемый профиль, не универсальная гарантия читаемости. Нужны higher-resolution profile или детальный crop при необходимости. Preview 768px не является архивной страницей.

VibePublish должен возвращать/отправлять именно WebP по opt-in profile; не менять незаметно поведение других PNG/JPEG клиентов. MIME, extension, actual bytes и native readback проверяются вместе. WebP — производная source, её SHA не равен PDF/старому PNG. Исходный PDF/DjVu остаётся неизменным. Разделять source/canonical raster/provider rendition hashes; не обходить sanitizer ложным application/octet-stream.

Page identity в owner/source scope: source digest + physical index + renderer/geometry/display profile. Rechunking того же источника переиспользует pages. Другая rendition имеет свой digest/profile. Глобальная дедупликация не раскрывает наличие чужого файла.

Один page asset связан с многими chunks через page/region/span. Не записывать Telegram URL во все chunks. Manifest: opaque page key, media entry/native identity, rendition hash, dimensions, format, profile/mapping. Крупный manifest шардируется с общим digest; bounded page reads не скачивают весь корпус.

### 5.3. Очередь, лимит и сбои

Переиспользовать Vibe media budget: суммарно <=20 media files в любом rolling 60s на connection/account, включая источники, обложки, иллюстрации, страницы и album members. Разные темы одного чата не имеют отдельных квот. FloodWait/SlowMode — дополнительные ограничения; соблюдать более строгий предел. Vibe использует MTProto: Bot API FAQ — справка, не обещание пропускной способности [S3].

Не busy-wait в MCP/provider lane. При нехватке capacity — durable defer с retry_after. Stable request/asset keys после таймаута/restart. Unknown send outcome сначала reconcile, не fresh send. Lease, два workers, rolling history и изменение часов покрыть тестами. Отзыв прав до dispatch блокирует job.

Spool: atomic files/fsync и SQLite receipts, не RAM. Нужны global/tenant byte quotas, max pending bytes, minimum free disk, limits dimensions/file size/concurrency. Рендерить по мере освобождения очереди, не все 100 книг заранее. До принятия переполненного batch вернуть resumable backpressure; accepted данные не терять. Pending/unknown jobs не удаляются TTL как завершённый cache.

После verified delivery GC удаляет ненужные ingress originals/derivatives в RKB и VibePublish, сохраняя компактные replay receipts. Учесть SQLite/WAL и PNG-дубли; перенести накопление на другой диск — не экологичное решение. Не затрагивать чужие assets или uncertain sends.

States: queued/rendering/uploading/verifying/ready_to_commit/ready/retry_wait/blocked. Progress из локального durable журнала, с агрегатами и bounded refresh, не Supabase scan на каждый page/status.

### 5.4. Итоговый commit и gate

После всех verified pages проверить полный expected index set, bytes/digests/dimensions/source binding и seal manifest. Blank pages/cover учитываются, если входят в source; их нельзя незаметно пропускать ради ready.

Одна idempotent domain RPC регистрирует готовый manifest и CAS-переключает readiness по source/revision/mapping. Это может быть несколько SQL statements в одной транзакции. Не требовать гигантского JSON с тысячами записей: детальные shards предварительно подготовлены в private query store, Supabase получает компактный pointer/count/digest. Доступность меняется атомарно. Количество обычных Supabase progress writes не растёт с числом страниц; auth reads и start/final/exceptional transitions учитывать отдельно.

Сбой после Telegram success до commit: повторить commit без повторной отправки. Lost commit response: тот же результат. Reprocess во время upload не позволяет старому worker активировать другой mapping. При split storage сначала подготовить query-store manifest, затем catalog pointer; выдача только при согласованных version/digests. Проверить crash windows без distributed transaction.

Три независимых готовности: text/index ready, whole-source page archive ready, quote geometry ready. Поиск и текстовая цитата не ждут Telegram. **Scan proof требует целиком готового source archive и проверенной геометрии выбранной цитаты**, а также всех прав. N-1 страниц не включает scan. Для статьи внутри выпуска full-source gate относится к выпуску; standalone source имеет свой gate. Cover может иметь отдельный статус, не равный готовности всей книги. Поздняя потеря архивной страницы переводит visual capability в degraded/unavailable, не подставляет другой scan.

## 6. Вместимость и экологичность

### 6.1. Инженерный профиль

Приёмка: **100 book-equivalent sources, 100 000 active chunks, 30 000 pages**, разнообразные структуры и длины. Это измеримый baseline, не обещание про 100 неограниченно больших томов. Измерить 10/25/100 sources, повторный импорт наибольшего source и peak памяти/диска.

Supabase Free ограничивает размер БД 500MB [S1]. DoD: steady-state <=400MB всей квотируемой БД, включая system baseline/TOAST/индексы/служебные таблицы, то есть >=20% запаса. Не путать quota DB, file storage и provisioned disk.

E5 vector(384) + BGE vector(1024) сейчас требуют **5 648 bytes/chunk** только для значений по формуле pgvector [S2]. 100k chunks =564,8MB до HNSW и текста. Halfvec обеих проекций =283,2MB; это не доказательство вместимости остального. INT8 модель E5 не делает stored vectors автоматически INT8.

### 6.2. Размещение

Базовый целевой путь: Supabase хранит компактные catalog/ACL/rights, source/revision pointers и manifest readiness; private PostgreSQL/pgvector на существующей production-инфраструктуре — active text/FTS/vectors/detailed geometry. Переиспользовать SQL/адаптеры и actor bridge, не вводить другой search engine. Telegram — binaries/cold archive; S3 около1GiB только временный cache.

Более простое исключение: оставить query plane в Supabase, если эксперимент ДО migration доказывает <=400MB на полном benchmark, качество/latency и приемлемый reimport peak. Halfvec/index alternatives проверять отдельно. Не удалять E5/BGE, не обрезать dimension и не сокращать корпус ради quota. Placement decision принимает измерение, а не предположение «после чистки всё поместится».

Private query store тоже ограничен: измерить heap/TOAST/indexes/WAL/backup/temporary peaks. Плановый ориентир hot dataset <=3GB на benchmark; подтвердить либо явно пересогласовать. Найти существующий production host и его бюджет; временная dev-worktree машина не долгосрочное хранилище. Нет разрешённой ёмкости — явный blocker, не автоматическая покупка сервиса.

### 6.3. Split-storage consistency и ACL

Supabase — authority доступа/выбранной revision. Перед поиском получить разрешённые source/component scopes и применить ВНУТРИ candidate retrieval, не фильтровать только глобальный top-K после ранжирования. Технические worker credentials не расширяют пользовательский scope. Перед выдачей перепроверить отзыв прав/revision. Ошибка authority не разрешает использовать чужой/устаревший ACL.

Query plane подготавливается immutable source/revision/hash, затем выбирается compact catalog pointer. Search/fetch/graph discovery/POI references используют один snapshot, не новые vectors со старым text. Resource-specific credentials, без user bearer forwarding. RLS/actor-binding тестируются по обе стороны. Можно использовать короткий scoped cache с version fences, но не бессрочный доступ после revocation.

### 6.4. Lifecycle

Accepted text/geometry — один immutable source-revision representation. Chunks — ordered spans, search_material воспроизводим. Materialized копия допустима с измеренным обоснованием ускорения и active-only/retention policy, не полная строка в каждом metadata JSON.

Один active hot projection; текущий staging защищён; краткий rollback retention ограничен и входит в peak budget. Старые hot vectors/FTS удалять после verified cold evidence snapshot и проверки pinned citations/recovery. Старые evidence IDs должны разрешать точную старую revision, не похожий новый chunk.

Неактивное не равно мусору. Dry-run различает active, staged/ready, pinned, rollback, expired temporary, доказанно test-owned. Source и cited evidence не удаляются вместе с заменимым index. Legacy vectors/index — только после проверки потребителей. idx_scan=0 не достаточное основание. VACUUM FULL не применять вслепую: блокировки и spare space планируются отдельно [S1].

Никаких binary/base64 страниц в Supabase. Подробные quads/offset sidecars — компактно и вне горячего каталога; не миллионы отдельных JSON-строк на каждый символ. Image delivery не проводить через Supabase Storage только ради URL. Vibe assets GC и очередь тоже входят в capacity budget.

Приватные backups accepted text/geometry/manifests и БД: compressed/checksummed, bounded retention, restore на чистый host. Telegram не гарантирует вечную доступность: source unavailable надо честно сообщать. GitHub/публичный Kaggle не backup частного корпуса. При исчезновении страницы нельзя синтезировать доказательство.

## 7. Definition of Done

| ID | Проверка |
|---|---|
| D01 | Старые search/fetch/ingest/reprocess/Live не сломаны; server tools/list, registration и реальное подключение ChatGPT согласованы |
| D02 | Каталог всех импортированных publications с cursor; edition/source/revision counts различены; поиск contributor/year/identifier |
| D03 | Book/issue/article, соавторы/переводчик, unknown/invalid identifiers, combined issue, metadata provenance, versioned taxonomy |
| D04 | Article boundaries ограничивают chunk/continuation/footnotes; две статьи на одной странице не смешаны; удалённое продолжение статьи сохраняется |
| D05 | Quote byte/hash/ranges точны, multi-page fragments и editorial separators различены; observations/summary не цитаты |
| D06 | Yellow bands по нужным quads; rotations, columns, footnotes, repeated phrase, hyphens/ligatures, image-only; нет точности — unavailable, не fake highlight |
| D07 | Entitlement AND source/component rights; direct calls, revocation/cache и parent-page privacy не обходят ACL |
| D08 | Фактический WebP DOCUMENT после sanitation/readback; MIME/extension/bytes/dimensions/digest/rotation подтверждены; clean page отдельно от overlay |
| D09 | 60media units, разные темы/публикации, два workers, restart: rolling<=20; albums/FloodWait/timeouts/unknown outcomes не дают дублей |
| D10 | Durable ACK не ждёт Telegram; цель p95<=2s для малого уже полученного batch, upload bytes измерять отдельно; spool full даёт backpressure |
| D11 | N-1 не включает scan; последняя verified+final commit включает; retry commit не переотправляет; revision drift fails closed |
| D12 | Обычные Supabase progress/binding writes не O(pages): start/final плюс bounded exceptional transitions; progress локально, auth reads отдельно |
| D13 | 100sources/100k chunks/30k pages со всеми indexes/TOAST/geometry: Supabase<=400MB; query/spool/Vibe/cache/backup и reimport peak измерены |
| D14 | Lifecycle dry-run/GC сохраняют active/staged/pinned/recovery и exact old citations; restore/rollback проверены |
| D15 | Frozen holdout: exact/semantic/names/dates/article/cross-page/boundary/negative-unanswerable; полнота evidence отдельно от маркера; известные complete-answer cases не регрессируют незаметно |
| D16 | Реальные commits/PR, deploy status, приватный acceptance report и команды воспроизведения; непроверенное/недоступное явно названо |

Для D06/D15 минимум один реальный разрешённый материал каждого типа плюс synthetic/property fixtures без user text. Native PDF, image-only scan, two-column magazine, standalone article обязательны. Не повторять один source 100раз с одинаковыми vectors как доказательство corpus capacity/retrieval. Capacity corpus и semantic holdout — разные наборы. Сравнивать baseline/candidate при одинаковых query sets/snapshot/rights/depth/model spaces. Visual golden tests проверяют геометрию и отсутствие выделения чужих строк, не просто наличие жёлтого пикселя.

Реальный Telegram canary — 1–3 заранее разрешённых страниц; массовый timing/restart тест на deterministic provider fixture, не спам 30k страниц. Показать фактический readback в целевом topic. Удалять только собственные canaries при доступном delete, не обходить grant.

## 8. Поставка

P0: текущие размер/контракт/quality baseline и capacity experiment, без нового бесконечного общего аудита. P1: catalog/source/component identity. P2: quote spans/geometry/rights. P3: WebP provider contract, durable pages, sealed manifest commit. P4: bounded lifecycle/placement migration по доказанной необходимости. P5: интегрированный новый импорт трёх типов, нагрузка, restore, rollout.

Небольшие проверяемые поставки, checkpoints и rollback. Production cutover — после тестов и сохранения recovery. Не объявлять успех по unit tests или stored job. Не держать окно бесконечным polling: читать saved operation state после unknown effect; при отсутствии необходимого инструмента назвать blocker и сохранить handoff.

При невыполненном D01–D16 конечный статус partial/blocked с конкретной причиной, не «универсальный импорт готов». Не покупать upgrade, не ослаблять права и не удалять corpus как скрытый обход capacity.

## 9. Первичные источники

[S1] https://supabase.com/docs/guides/platform/database-size — DB quota, disk/DB distinction, системный baseline, VACUUM caveats.

[S2] https://github.com/pgvector/pgvector#reference — vector/halfvec byte formulas и поддерживаемые представления. Расчёт выше не результат load test.

[S3] https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this — Bot API limits; внутренний project budget применяется отдельно для MTProto.

[S4] https://www.isbn-international.org/content/what-isbn/10 — edition/format identity.

[S5] https://www.issn.org/understanding-the-issn/what-is-an-issn/ — serial identity.

[S6] https://www.crossref.org/documentation/schema-library/markup-guide-record-types/journals-and-articles/ — journal/issue/article metadata.

[S7] https://developers.google.com/speed/webp/docs/cwebp — lossless WebP; читаемость проверяется на конкретном render profile.
