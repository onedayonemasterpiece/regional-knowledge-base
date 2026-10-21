# RKB: аудит требований, преемственности кода и эффективности миграции

Дата: 21 октября 2026 года. Автор и интегратор: **RKB-AUDIT / MAIN-ChatGPT**.

**Вердикт:** новая реализация — существенное, но неполное ядро PostgreSQL/BGE/WSS. Она не является функционально эквивалентной заменой прежней RKB. Обнаружены и расхождения требований, и сужение пользовательского сценария, и лишние переходы данных. При этом значительная часть проверенного предметного кода сохранена, а ряд новых решений действительно лучше старых. Исправление — адресная интеграция и восстановление контракта, не переписывание всего сервиса и не откат полезного ядра.

Это аудит исходников и требований, **не runtime-приёмка**. Во время подготовки отчёта не выполнялись импорт книг, запись в Telegram, запуск/остановка Kaggle, переключение production, миграция OAuth или удаление данных. Изменения документации не означают устранение перечисленных программных дефектов.

## 1. Основания, версии и границы доказательств

| Обозначение | Точный источник | Что он доказывает |
|---|---|---|
| OWNER | [Прямой ответ владельца 21.10.2026](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792573398928879) и текущая переписка | Продуктовые решения и разрешение привести исходную постановку и активные документы в main в соответствие |
| THREAD | [История обсуждения архитектуры](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792422183793029) | Происхождение предложений и решений; агентское сообщение от имени пользователя не становится самостоятельным решением владельца |
| OLD | `481241fa36db30e3f0e6c4c4b88fb3ce84964676` | Последний main до конца 18.10.2026, выбранный по GitHub commit history; исходники старой архитектуры, не автоматически доказанная версия каждого тогдашнего deployment |
| TASK | `85676094cf1a019fd34a29daa930d4ebb16eedb7` | [Исходная постановка v4](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/85676094cf1a019fd34a29daa930d4ebb16eedb7/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md) |
| NEW | `bb7f8ae41dbede8c0af83aafffe794f193244a9e` | Main после PR168, зафиксированный до документальных исправлений этого аудита |
| DOT | [Завершённая независимая проверка Dot](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792574146701999) и предшествующие сообщения `[DOT-AUDIT]` | Независимое чтение закреплённых исходников и требований; runtime не проверялся |
| OPUS | Консультация `dvt_726c14168efd4348b9690b070ed2b0b8` | Реально выполненный критический разбор переданного evidence bundle моделью `claude-opus-5-5`; не самостоятельное чтение всего репозитория |

Ссылки на NEW в отчёте намеренно остаются закреплёнными: после исправления main они должны продолжать показывать исследованную причину. Публичный репозиторий содержит только код, тесты и документы; приватные книги, токены, session strings и содержимое пользовательских баз в отчёт не включены.

Используются четыре отдельных статуса: **код существует; путь подключён; проверка выполнена; реальный пользовательский сценарий принят**. Наличие теста не равно его запуску в этом аудите. Старый отчёт о Telegram/Vibe не доказывает работу нового Telethon. Зелёный CI не заменяет 100 000 чанков, живую BGE, телефон или реального non-owner.

## 2. Восстановленный продуктовый контракт

### 2.1. Что переносится, а что не меняется

Переносятся вычисления и рабочий корпус: единый PostgreSQL/pgvector и BGE-M3 находятся в Kaggle. DevCoveer остаётся тонким MCP/OAuth/control-слоем и управляет ресурсами, очередями, текущими правами и идентичностью запуска. Supabase, E5, локальная SQLite с книжным корпусом и тяжёлая обработка на Dev не являются целевой архитектурой.

Не меняются смысловые обязанности: книгу читает вызывающая интеллектуальная модель, данные подаются порциями, source text отделён от интерпретации, чанки связаны со страницами и иллюстрациями, факты не требуют вымышленной Story, достаточный эпизод сохраняет настоящий рассказ. Не создавать backend OCR, вторую обязательную LLM, новый импортный framework, второй IAM или параллельный реестр знаний.

**Один активный импорт, только владелец.** Несколько читающих пользователей и уже предусмотренные авторизованные редакционные/экспертные действия не дают права на второй параллельный книжный импорт. Конкурирующий start не захватывает второй writer: возвращает понятный busy/resume либо сохраняет разрешённое ожидание без запуска. Предыдущие тесты concurrent imports заменяются проверкой этой границы и конкурентных чтений.

### 2.2. Прямые решения владельца, которые нельзя снова превратить в предложения

| ID | Обязательное решение | Запрещённая подмена |
|---|---|---|
| O-01 | Kaggle Dataset — основное версионируемое постоянное хранилище состояния PG; восстановление при старте и сохранение обратно | Только `object storage` на схеме, S3 как неявная основная БД, legacy SQLite Dataset под видом target PG |
| O-02 | Yandex S3 — независимая копия раз в неделю | Ежедневный default в другом разделе, обязательный новый bucket или скрытый непрерывный второй основной storage |
| O-03 | Kaggle напрямую читает и загружает Telegram-медиа через Telethon | VibePublish как sole uploader, байтовый прокси DevCoveer |
| O-04 | Telegram-медиа проверяются наличием доступного photo/document по зарегистрированному locator | Повторное скачивание/сравнение SHA как обязательное условие успеха; перенос этой политики на integrity снимков PG |
| O-05 | Фото иллюстрации по умолчанию, дополнительный high-quality document только по реальной необходимости разрешения | Всегда две копии, произвольный универсальный предел 1280 пикселей, автоматическая смена политики без наблюдений |
| O-06 | Сохраняются chunk↔illustration, подписи и отдельно маркированные модельные описания; модель получает пиксели для оценки релевантности | Текстовое описание вместо изображения, потеря связей при переезде |
| O-07 | Модель читает источник; повторная подготовка backend не обязательна, если реальные страницы уже доступны клиенту | Native PDF text как единственный авторитет, скрытый полный render/parse книги перед stage |
| O-08 | Реальные рассказы сохраняются в том же интеллектуальном проходе | Только synopsis/seed/оценка потенциала, обязательный второй проход или отдельная LLM-фабрика |
| O-09 | Один owner-only импорт; ресурсы остальных операций в основном идут чтениям | Массовая параллельная загрузка и распределённая система ради гипотетической квоты |
| O-10 | Страйп — отдельный запрос; bbox выбирает модель, код наносит выделение | Автоматическая обязательная генерация proof при каждом поиске/импорте |

Проверка наличия Telegram-медиа не отменяет авторизацию, соответствие locator нужному источнику, тип media, обработку удалённого сообщения и UNKNOWN после отправки. Но она **не должна разрастаться обратно в сравнение байтов**. SHA кода, backup manifest, исходной edition identity и exact PG command — другие механизмы с другой целью.

Утверждение «20 файлов в минуту — универсальный Telegram limit» не подтверждено. Для одного uploader достаточно конфигурируемого pacing и соблюдения фактического FloodWait. Значение, единица учёта и scope отмечаются как локальная эксплуатационная настройка, а не гарантия платформы. Дополнительные sessions не используются для обхода ограничений.

### 2.3. Что сохраняется из оптимизаций

Семантическая цель чанка — около **256 BGE tokens**, не механические страницы и не новый hard cap 256. Строгий предел — **512 токенов полного augmented input**, включая применимые prefix/captions/labelled description. BGE-M3 — **1024 FP32**; менять пространство или длину без отдельного измерения нельзя. Exact offsets и source provenance сохраняются.

Тёплый поиск выполняет encode, lexical/dense, fusion и hydration рядом с PG внутри Kaggle. Ни query-, ни document-векторы не должны делать круг через Dev. Обычный read не ждёт durable journal, Telegram, snapshot, набора batch или модельного исследования. Артефакты и proof могут завершаться отдельно, не удерживая готовый текстовый ответ.

## 3. Что действительно сохранилось от старой версии

Воспроизводимая проверка: `python scripts/audit_rkb_source_inventory.py inventory`. Скрипт читает только точные Git-объекты OLD/NEW, не приложение или runtime.

В OLD — 351 tracked file, в NEW — 666; добавлено 315, удалённых путей нет. Разница составляет 395 изменённых/добавленных файлов, 68 950 добавленных и 8 031 удалённую строку. Эти числа характеризуют diff, **не процент готовности**.

| Модуль | Результат сравнения OLD→NEW | Что переиспользовать |
|---|---|---|
| `stage_graph.py` | Побайтно совпадает, 875 строк | Стабильные IDs, source regions/spans, порядок чтения, связи caption/figure/chunk, model-authored graph, проверка покрытия |
| `source_span_planner.py` | Совпадает, 174 строки | Уже отработанные семантические границы и токенные ориентиры |
| `ingestion.py` | Совпадает, 204 строки | Ограниченная доставка страниц и предупреждения о native preview; не превращать optional helper в semantic authority |
| `source_adapter.py` | Совпадает, 73 строки | Диспетчер PDF/DjVu, механическое декодирование и crop |
| `illustrations.py` | Совпадает, 135 строк | Дескрипторы, caption/model observation, связь с source/page; заменить storage transport |
| `quote_proof.py` | Совпадает, 345 строк | Координатные проверки и детерминированное рисование; адаптировать PG/Telethon и явный модельный locator |
| `story_ingestion.py` | Совпадает, 338 строк | Связь кандидатов с source, а не обещание, что legacy автоматически сохранял полный body |
| `story_reconcile.py` | Совпадает, 751 строк | Существующее продолжение исследования, идемпотентность и leases |
| `accepted_source_reuse.py` | Совпадает, 225 строк | Семантика reprocess/rechunk и повторного использования принятого чтения |
| `bge_contract.py` | Совпадает, 19 строк | Пространство/размерность/границы encoder |
| `source_archive.py`, `illustration_mirror.py` | Изменены, прежний Vibe transport сохранён | Смысл origin/locator и уроки UNKNOWN; не переносить обязательный media-SHA и весь Vibe framework |
| `story_registry.py`, `contracts.py`, `server.py` | Изменены | Проверять изменение маршрутов отдельно от существования объектов |

Проверка показала **не физическое уничтожение старых функций**, а разрыв между сохранённой библиотекой и новым `kaggle_source` маршрутом. Именно поэтому правильный путь короче нового монолитного импортёра: адаптировать работающую предметную логику к PG/Kaggle и убрать временные ограничения.

Ссылки: [OLD stage_service](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/stage_service.py#L93-L136), [NEW stage_graph](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/stage_graph.py), [NEW source adapter](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/source_adapter.py).

## 4. Матрица существенных находок

Обозначения: **DRIFT** — расходится с действующим требованием; **GAP** — реализация/подключение неполны; **PERF** — лишняя работа/переход; **CLAIM** — недоказанное или неточное утверждение. Приоритет P0 означает препятствие корректной замене legacy, а не запрет независимой разработки.

| ID | Приоритет/класс | Факт NEW | Последствие и направление исправления |
|---|---|---|---|
| F01 | P0 DRIFT | `_native_proof` требует полного native block и совпадения bbox | Модельная транскрипция скана отклоняется. Принимать model-authored source с привязкой к реальным странице/edition, не требовать отсутствующий PDF text layer |
| F02 | P0 PERF | Первый холодный stage вызывает `native_pages()` по всем страницам | Скрытая дополнительная обработка книги. Проверять накопленные заявленные страницы/порции; отсутствие coverage требует чтения, а не фонового чтения за модель |
| F03 | P0 PERF | Каждая новая page derivative проходит S3 archive/readback, затем Dev GET | Обязательная архивация временных картинок и лишний сетевой круг. On-demand изображение и bounded cache, без постоянного архива каждой страницы |
| F04 | P0 GAP | `page.illustrations`, `chunk.illustration_ids` явно отклоняются | Потеря действующего продукта на target. Восстановить семантику stage_graph, PG связи, Telegram photo/document и доступный `illustration_fetch` |
| F05 | P0 GAP | Target allowlist не включает `illustration_fetch` | Сохранённый legacy файл не делает функцию доступной. Проверить весь путь tool→backend→storage→pixels |
| F06 | P0 GAP | 2 MiB/32 страницы/32 passages на всю книгу, 12 накопленных candidates | Лимиты пилота выданы за перенос. Оставить bounded batch, поддержать длинную книгу с persisted continuation |
| F07 | P0 GAP | PDF-only; public importer не принимает reprocess/rechunk | Перенести существующие пользовательские контракты и DjVu; не заставлять модель заново импортировать принятую книгу ради поправки |
| F08 | P0 DRIFT/PERF | Stage graph/research/semantic JSON держатся в Dev queue SQLite; сборка домена выполняется на Dev | База ещё не стала единственным Kaggle-контуром. Отделить короткий control/replay spool от рабочего PG staging, перенести обработку и хранение графа |
| F09 | P0 PERF | `_vectors` получает 1024-мерные document vectors обратно на Dev и добавляет их в import payload | Устранить круг Kaggle→Dev→Kaggle; encoder и установка vector projection рядом с PG |
| F10 | P1 GAP | Новый token counter проверяет `chunk.text`, старый проверял `graph_material` | После возврата caption/description считать весь augmented input, не восстановить silent truncation и не сделать 256 hard limit |
| F11 | P0 DRIFT | Docs называют VibePublish sole uploader; native intake архивирует в S3 | Нет согласованного прямого Telegram write/read. Один минимальный Telethon adapter в Kaggle, reuse IDs и операций, без Vibe dependency |
| F12 | P0 DRIFT | Media verification требует custom SHA/readback | Прямое противоречие OWNER. Для Telegram достаточно доступного media locator; snapshot/command integrity остаётся |
| F13 | P0 GAP | В checked-in RKB не найден publisher target PG Dataset по исследованным API/symbols | Реализовать выбранный Dataset persistence, не считать legacy SQLite archive доказательством |
| F14 | P0 DRIFT | Canonical и несколько active docs задают daily Yandex base | Исправить на weekly везде; online immutable tail — отдельный открытый архитектурный конфликт, не скрытая смена primary |
| F15 | P1 DRIFT/GAP | `public_rights` требуется уже для start | Разделить приватный оригинал, доступ к принятому поисковому тексту, публикацию original и разрешённые иллюстрации; не выводить юридическую лицензию из желания искать |
| F16 | P0 GAP | Не доказана единая сериализация одного owner-only import во всех start/resume путях | Один активный ingestion lease, конкурентные вызовы не запускают второго; reads сохраняют резерв |
| F17 | P1 GAP | `needs_context` сохраняется, но target post-acceptance extraction/reconcile parity неполна | Продолжение по существующему workflow должно создавать недостающий результат без изменения immutable принятого input |
| F18 | P1 GAP | Нет authenticated hot activation stateless handler | SQL apply/replay не заменяет горячую замену приложения. Реализовать узкий update/drain/readback, сохраняя BGE/PG |
| F19 | P1 GAP/CLAIM | Нет собственного Go/Rust компонента и измеренного решения по нему | Выполнить профилирование и ограниченный эксперимент по необходимости; отсутствие rewrite само по себе не дефект |
| F20 | P1 CLAIM | CI и synthetic PG fixtures представлены как близость полного продукта | Вести отдельно code/wiring/test/live и исходные знаменатели ошибок/нагрузки |
| F21 | P1 DRIFT | Несогласованные task instructions и устаревшая локальная рабочая ветка | Перед каждым продолжением фиксировать физический checkout/HEAD/contract; не переносить требования из старого injected контекста |
| F22 | P1 GAP | Полная каталогизация, media/proof, каналы и non-owner сценарии не приняты в target | Не молча отключать их ради успешного cutover; иметь явную parity-матрицу и порядок подключения |

### 4.1. Доказательства F01–F10

В [kaggle_importer.py:284–307](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_importer.py#L284-L307) каждый region обязан находиться ровно один раз в полном native block и иметь его bbox. Это не проверка того, что модель действительно видела страницу; это другое ограничение, исключающее image-only scan и исправление плохого text layer.

В [original_intake_controller.py:328–344](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/original_intake_controller.py#L328-L344) отсутствующие proof-pages добываются для всей книги. [pages():237–282](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/original_intake_controller.py#L237-L282) архивирует derivative через S3 и читает bytes на Dev. Кэши существуют: некорректно говорить, что **каждый** stage обязательно рендерит всю книгу повторно. Проблема — полный холодный проход и зависимость от полноты этого кэша до принятия частичного stage.

[Importer:340–353 и 402–403](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_importer.py#L323-L410) отклоняет illustration/article bindings. [server.py:177–195](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/server.py#L177-L195) ограничивает target tools. [Лимиты](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_importer.py#L41-L46) и PDF-only проверка — исполняемый код, а не только устаревшее описание.

В [Importer:506–544](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_importer.py#L506-L544) Dev получает результат DOCUMENT job, валидирует `result['vectors'][0]` и возвращает passage с добавленным vector. Это подтверждает лишний document-vector roundtrip. Его нельзя скрыть верным утверждением, что QUERY-вектор уже остаётся внутри Kaggle.

### 4.2. Что действительно улучшено и не должно быть отменено

| Улучшение NEW | Доказательство | Граница |
|---|---|---|
| Fused lexical+BGE query в Kaggle | [kaggle_search.py:166–224](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_search.py#L166-L224) | Код есть; требуемый p95 на representative corpus ещё не доказан |
| Полный model-authored Story body | [Importer:472–489](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_importer.py#L472-L489), source-import/story read tests | Не равно качественному извлечению из реальной книги; body не генерируется backend |
| Отказ от silent encoder truncation | [bge_kaggle_worker.py:48–70](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/scripts/production/bge_kaggle_worker.py#L48-L70) | Сохранять отказ на oversized complete input |
| Native page/block continuation | `tests/test_kaggle_original_intake.py`, original_intake controller | Есть внутри текущего малокнижного пути; это не полная post-acceptance semantic continuation |
| Exact command replay, writer generations, UNKNOWN fences | `control_spool.py`, `kaggle_apply.py`, `queue_writer.py`, `source_history_wss.py` | Сохранить semantics; выбранный storage transport должен соответствовать OWNER |
| OAuth continuity без выпуска новых токенов | `oauth_family_projection.py` и тесты | Не удалять права/проверки ради упрощения; фактический клиентский маршрут проверяется отдельно |
| Source facts отдельно от narrative | `source_import_v3.py`, fact contracts/read | Ноль Story допустим для справочного фрагмента; достаточный связный эпизод требует body |

Старый автоматический Story import сохранял seed/assertion/evidence. Нельзя приписывать ему автоматически готовый `Variant.body`, затем объявлять новую body-реализацию регрессией. Именно эта часть переноса улучшена. Ошибка — потеря полного окружения импорта и продолжения, а не сам переход к фактам плюс настоящему рассказу.

### 4.3. Отдельная проверка расхождений и дел: ядро есть, полный цикл отсутствует

Дополнительное замечание владельца в THREAD проверено Dot и прямым чтением NEW. **Case не удалён целиком**, но PostgreSQL-ядро нельзя назвать готовым исследовательским продуктом.

В [kaggle_case_tools.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_case_tools.py) подключаются четыре группы: `disagreement_get`, `disagreement_edit`, `disagreement_contribute`, `disagreement_conclusion`. Они дают отдельное создание/редактирование дела, evidence, вклады, выводы и приватные draft save/publish. Есть current authorization, версии и exact command recovery. Это полезная реализация, которую сохраняем.

Не закрыты следующие обязательные пути:

| ID | Разрыв | Что должно быть принято |
|---|---|---|
| CAS-F1 | Не показан сквозной import → persisted research → обнаружение расхождения → найденное/созданное Case | Один существующий research frontier использует assertions новой книги и предыдущие источники; отдельно созданное вручную дело не доказывает эту связку |
| CAS-F2 | Нет target Case search/BGE, disagreement scan, source requests и public discussion/export | Пользователь находит дело без знания UUID, добавляет основание и запрашивает источник; поиск не подменён lexical-only success |
| CAS-F3 | `CaseCreate` принимает positions, `CaseRevise` не позволяет дополнить их | Третий источник добавляет проверенную позицию в существующее дело, не создаёт дубль и не разрушает историю |
| CAS-F4 | `human_support` в текущем контракте должен быть пустым; read отдаёт current conclusion без last_supported | Реальный человек отдельно подтверждает точную версию вывода; модель не подделывает поддержку. После нового материала current/last-supported представлены различимо |
| CAS-F5 | Нет подключённого target экспорта публичной Astro-страницы | Постоянный URL без логина, desktop/mobile, безопасная публичная проекция, bibliographic provenance, last-good и отзыв |

Основания: [описание native Cases:48–88](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/docs/operations/native-disagreement-cases.md#L48-L88), [контракты human support](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/kaggle_case_contracts.py#L117-L125), [SQL read:389–417](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/sql/kaggle/010_disagreement_cases.sql#L389-L417), [CaseCreate/CaseRevise](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/src/regional_knowledge/disagreement_contracts.py#L19-L43).

`human_support=[]` сейчас честно предотвращает выдуманную поддержку, но не заменяет отсутствующий путь реального подтверждения. Не исправлять это разрешением модели прислать произвольный список одобрений. Не удалять Cases из scope под предлогом одного owner-only книжного импорта: авторизованные экспертные вклады, собственные черновики и подтверждения — отдельные действия, уже предусмотренные продуктом. Старые дела не мигрируют по GEO-only решению, **функции для новых дел сохраняются**.

## 5. Архитектура без повторного изобретения

```text
                    ИНТЕЛЛЕКТУАЛЬНЫЙ КЛИЕНТ
       ChatGPT / Grok / Dot / Mira: видит источник, читает,
       формирует source text, chunks, facts, Story.body, media refs
                              |
                       компактные MCP-вызовы
                              v
                  DEV: существующие OAuth + MCP
       текущие права / один import lease / admission / run ownership
       короткий control+replay журнал / результаты / UNKNOWN / маршруты
                              |
                  существующий двусторонний WSS
                              v
                    KAGGLE: рабочий data plane
       PG+pgvector + resident BGE / source staging / indexed reads
       применение model payload / media/proof по запросу / Telethon
                  |                            |
          прямые file bytes              согласованный snapshot
                  v                            v
          TELEGRAM ARCHIVE           VERSIONED KAGGLE DATASET
       originals / illustrations      основной persistence / restore
                                               |
                                      раз в неделю тот же backup
                                               v
                                          YANDEX S3
                                  независимая резервная копия
```

Временный исходник может поступить в Kaggle напрямую из поддержанного hosting reference клиента. `sandbox:` и opaque ID не являются URL; реальная capability проверяется, а отсутствие её не разрешает молчаливый Dev bulk proxy. Небольшой MCP ImageContent может проходить через Dev, когда этого требует клиент: это не хранение книжного корпуса и не прогон полного PDF через шлюз.

### 5.1. Импорт

Модель использует уже доступный visual source. При его отсутствии запрашивает небольшой batch страниц через существующий helper. Затем отправляет model-authored stage: точная страница/edition, текст и регионы, semantic spans, figures/captions, факты и подготовленные bodies. Backend проверяет структурные связи, диапазоны, ревизии, права и полный tokenizer budget в Kaggle, но не читает книгу заново другой моделью и не требует equality с native text.

PG хранит staging и принятые domain objects; BGE материализует индекс там же. `finalize` атомарно активирует source revision, связанные подготовленные данные и достижимый research continuation. `source_accepted`, `retrieval_ready`, `research_pending` и publication approval остаются разными состояниями.

Оригинал сохраняется напрямую в Telegram. Принятый locator включается в PG-объект и восстановимое состояние. Архивация не обязывает модель повторять чтение. Наличие файла в Telegram подтверждает только соответствующую archive-операцию, не сохранение нового PG snapshot.

### 5.2. Поиск и иллюстрации

Один search → encode + lexical + dense + bounded fusion + hydration в Kaggle → источник, страница, chunk, bibliography и связанные media descriptors. Вектор наружу не нужен. Если у релевантного chunk есть иллюстрация, модель получает изображение через существующий fetch и проверяет его глазами; caption не заменяет картинку. Ожидание картинки явно отделено от дедлайна текстового search.

Не создавать отдельный gallery corpus и второй полный поиск по тем же книгам. Сохранить единый `search_material`: неизменный source text плюс принятые caption/labelled description. Изменение метаданных без изменения embedding input не требует re-embedding.

### 5.3. Страйп

По отдельному разрешённому запросу Kaggle получает original по Telegram locator, извлекает нужную страницу, предоставляет её модели для bbox/полигонов и рисует выделение кодом. Повторно вычислять bbox для неизменной принятой геометрии не нужно. Кэш не должен превращать приватную страницу в публичную illustration. Ошибка proof не обнуляет готовые текстовые evidence.

### 5.4. Масштабирование

Начать с одного writer, одной активной import operation и измеренного резерва для поиска. При добавлении reader учитывать свежесть snapshot/проекций, run identity и реальные доступные independent Telegram sessions. Наличие `TELEGRAM_RKB2` в конфиге не доказывает её авторизацию. Не создавать reader ради обхода account limits и не запускать новый notebook на каждую страницу/SQL правку.

## 6. Persistence, Saved и оставшийся конфликт гарантий

**Закрытое решение:** Dataset primary, weekly Yandex backup. Это не обсуждение выбора другой основной площадки. **Незакрытая реализационная граница:** как часто и каким подтверждённым механизмом закрепляется хвост после последней Dataset version при прежнем требовании не терять подтверждённые данные.

Старый handoff §6.1 и новый код связывают final accepted/Saved с независимым immutable remote tail, фактически реализованным через S3. Одновременно OWNER задаёт Yandex как weekly backup. Нельзя ни молча оставить обязательную online-S3 зависимость, ни удалить гарантию, ни назвать Dataset versioning постоянно изменяемым PG-диском.

Для реализации требуется один проверяемый протокол публикации Dataset: consistent PG snapshot с нужным WAL, exact model inputs/outcomes и control cursor/manifest; проверка native version; безопасное продвижение последней подтверждённой версии. Уже реализованный физический `pg_basebackup` не надо заменять логическим dump только ради новой схемы на рисунке. Возможный tail/checkpoint в Dataset — предмет проверки provider capabilities и стоимости, а не выдуманная мгновенная операция.

| Наблюдение | Разрешённое сообщение пользователю | Чего нельзя обещать |
|---|---|---|
| Input принят в короткий Dev spool | Received/pending с конкретным следующим шагом | Переживёт потерю самого Dev disk без внешнего закрепления |
| Domain commit в живом Kaggle PG | Applied в текущем runtime; persistence pending, если ещё не закреплено | Общий окончательный Saved при недоказанной внешней сохранности |
| Exact state покрыт подтверждённой Dataset version/поддержанным durable continuation | Persisted с точной version/cursor; Saved только в пределах реально доказанной recovery-гарантии | Защита от исчезновения всех provider copies |
| Та же версия скопирована в недельный Yandex backup | Backed up с датой и version | Более свежие данные уже находятся в этой недельной копии |
| Telegram photo/document доступен по locator | Media available / archived | PG knowledge state автоматически сохранён в Dataset |

Существующий bounded Dev spool может сохранить неподтверждённый payload при падении только Kaggle; поэтому неверно утверждать, что весь несохранённый хвост обязательно живёт только на notebook disk. Но потеря Kaggle **и** Dev до внешнего закрепления — другая failure domain. Публикация статуса и GC должны учитывать именно доказанное покрытие, а не последнее удачное сообщение в логах.

До закрытия этого конкретного протокола writes, не покрытые текущей заявленной гарантией, остаются pending. Независимые read-only, тесты, перенос source semantics и подготовка адаптера продолжаются. Не требуется ещё один сервис, синхронная публикация Dataset на каждый search или новый full backup после каждого heartbeat. Частота changed-state checkpoint проверяется нагрузкой/restore budget; weekly — именно внешний Yandex backup. Возраст backup и `oldest_unpersisted_age` показываются отдельно.

При weekly backup нельзя хранить неделями весь raw journal на Dev ради старой базы. Local payload GC после реального durable coverage, compact UNKNOWN/dedup metadata отдельно; remote GC не разрывает поддерживаемую цепочку восстановления. Snapshot bytes идут напрямую, не через 512 MiB control spool. Прежний runtime/tail не удаляются документальным решением до проверенной замены.

## 7. Telegram: простота без потери управления операциями

Один RKB uploader в Kaggle использует прямой Telethon. Переиспользуется семантика source/figure IDs и зарегистрированного archive locator, а не разворачивается VibePublish внутри notebook. Архивные originals и illustrations сохраняют прежние configured topics (2 и 4 после проверки фактической настройки).

Успех: сообщение доступно по точному locator и содержит ожидаемый тип photo/document. Не требуется скачать файл обратно и сравнить checksum. Фото и document могут иметь разные native identities и размеры; это не corruption. Для edition identity и reattach иного файла сохраняется отдельная проверка исходника, не навязываемая как Telegram transport proof.

Для иллюстрации отправляется photo. Фактическая максимальная photo rendition определяется по возвращённой Telegram metadata и pinned upload path. Дополнительный document создаётся только когда исходное разрешение не сохраняется достаточной photo rendition согласно решению владельца. Проверить оба измерения и допустимые пропорции, не только произвольную длину стороны; fallback при непринятом формате должен быть явным. Не обещать byte-exact photo. [Telegram описывает несколько PhotoSize](https://core.telegram.org/api/files), [Telethon различает photo/document через send_file](https://docs.telethon.dev/en/stable/modules/client.html); эти возможности не заменяют тест именно используемой версии.

Pacing задаётся локальной конфигурацией одного uploader; реальный [FloodWait](https://docs.telethon.dev/en/stable/concepts/errors.html) имеет приоритет. UNKNOWN после send не разрешает новое сообщение: сначала сверить зарегистрированную operation/native identity, а при невозможности однозначной сверки оставить UNKNOWN без blind resend. Это компактное управление одной операцией, не обязательная система повторного скачивания и проверки байтов.

Независимые Telegram sessions закрепляются за run эксклюзивно. Потеря WSS или истечение lease не являются доказательством, что старый процесс перестал использовать session. Эти ограничения защищают реальные уже выполняющиеся операции и не оправдывают второй scheduler или перенос байтов на Dev.

## 8. Эффективность: что уже есть, что вернуть, что измерить

| Участок | Уже имеется/сохраняется | Обязательная работа |
|---|---|---|
| Retrieval | Fused native query, ограниченные SQL, deadline/cancellation/output checks | Actual p50/p95 с BGE и representative corpus, одинаковые права/queries, ошибки в знаменателе |
| Chunking | Planner256/512, source-span graph, caption material | Перенести полный augmented tokenizer check на Kaggle и подключить media без потери контекста |
| Import | Stable stage IDs, retry, source activation/research state | Один owner-only import; long-book batches; убрать cold all-book scan и document-vector roundtrip |
| Data transfer | WSS/control и разделение object bytes | Полные originals/media/snapshots напрямую; убрать mandatory S3 page archive; считать фактические bytes по границам |
| Logs | Bounded read telemetry, native stage events | Проверить отсутствие fsync/cloud waits на ready reads и telemetry drop counts |
| Cache | Legacy source/render caches и продолжения | Single-flight точного источника/страницы; не декодировать/скачивать повторно без причины; права проверять на выдаче |
| PG | pgvector/lexical functions, projection/version rules | EXPLAIN/ANALYZE, ANN vs exact, index size, WAL/temp/TOAST, maintenance под нагрузкой |
| Warm debugging | Existing WSS, SQL apply/replay и runtime pins | Узкий hot handler activation, без перезапуска BGE/PG для обычной правки |
| Native language | Требования и некоторые измерительные helpers | Измеренный выбор узкого Go/Rust компонента либо обоснованный no-change; собственного компонента в NEW нет |

### 8.1. Числа, которые не ослабляются этим аудитом

- Representative engineering fixture: не менее 100 book-equivalent sources и 100 000 active chunks; это benchmark, не продуктовый предел и не разрешение параллельных импортов.
- BGE-only known-evidence Hit@5 ≥85%, Hit@10 ≥90%; критический RU-query→DE-source slice Hit@10 ≥85%; ≥30 held-out fact families. Lexical-only и fusion отдельно; lexical не prefilter dense.
- Warm backend ranked-candidate p95 ≤1000 ms; public MCP p95 ≤1500 ms; hard evidence interaction deadline 2000 ms. Media/voice whole-answer latency измеряется отдельно, без подмены search SLA.
- Steady storage ≤80%, peak reimport/snapshot/restore/cutover ≤90% применимого измеренного budget; учитывать версии Dataset и одновременные старый/новый artifacts.
- Примерно256/строго512 полного BGE input, без silent truncation, все значимые source facts/media/provenance сохранены.

Синтетическое размножение нагрузки допускается только как инженерная нагрузка, не как независимые 30 смысловых семейств. Старые принятые книги не мигрируют ради красивого счётчика; отдельный benchmark не публикуется в рабочем tenant.

### 8.2. Go/Rust: требование не забывать, но и не превращать в самоцель

В NEW не найдено собственного `.go`, `.rs`, `go.mod` или `Cargo.toml`. Это подтверждённый source inventory, не утверждение, что PG, PyTorch или tokenizer не используют native code. Исходный §4.6 допускает узкую замену компонента при доказанной пользе latency/resources/reliability; он **не требовал переписать весь backend**.

Нужен явный рабочий пакет `NATIVE-DECISION`: зафиксировать Python baseline и распределение времени queue/auth/WSS/encode/SQL/fusion/hydration/serialization; выбрать один измеренно проблемный участок; при необходимости реализовать Go или Rust prototype с тем же контрактом; сравнить вместе с IPC, cold start, memory, cancel и рестартами. Не переносить модель в иной runtime без parity точных inputs/space/ranking.

Допустимые исходы: принять более эффективный компонент; оставить Python с доказательством отсутствия полезного выигрыша; пометить эксперимент BLOCKED конкретной отсутствующей measurement capability. Отсутствие отчёта — незакрытая работа. Формальные условия «не меньше15% CPU» или «обязательно минус0.5секунды» в требования не добавляются: это предложения консультантов, не решения владельца.

### 8.3. Горячая отладка

[Текущий отчёт о acceptance observer:67–85](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bb7f8ae41dbede8c0af83aafffe794f193244a9e/docs/testing/kaggle-private-retrieval-observation.md#L67-L85) прямо признаёт отсутствие authenticated stateless bundle activation. Handler строится один раз, envelope allowlist фиксирован. Поэтому «SQL replay есть» не закрывает требование дешёвой смены приложения.

Сохраняются stable slug, exact run/provider version, одна WSS, typed update, drain/reconcile и independent readback. Не добавлять универсальную remote-shell platform. Менять только необходимый handler/процесс; BGE/PG остаются резидентными при совместимом обновлении. Несовместимая смена среды отдельно и с восстановлением accepted state. Отчёт показывает model-load count, notebook-save count и cold/warm iteration latency, а не только финальный зелёный тест.

## 9. Преемственность поведения: матрица приёмки

| Область | Legacy source сохранён | Target NEW | Тесты/доказательства | Live parity |
|---|---|---|---|---|
| PDF page delivery | Да | Один page и native continuation, pilot limits | Native-intake tests существуют | Полный сценарий не подтверждён |
| Model scan transcription | Stage graph допускает model source | Обязательный native gate мешает | Нужен положительный image-only scan test | Не принято |
| DjVu | SourceProcessor/DjVuLibre есть | Отклоняется | Нужен target import+proof test | Не принято |
| Иллюстрации | Graph/descriptor/fetch и прежний archive path есть | Stage reject + tool закрыт | Нужен положительный end-to-end media test | Не принято |
| Story.body | Legacy seed не равен body | Body/default variant сохраняются | `test_source_and_authored_story_activate_only_after_exact_remote_outcome` | Качество на реальной книге не подтверждено |
| Reprocess/rechunk | Есть | Public command отсутствует | Нужен exact revision/metadata-only parity | Не принято |
| Post-acceptance research | Reconcile/frontier есть | Частично, создание недостающего результата не завершено | Needs-context resume после restart | Не принято |
| Hybrid search | Старые heuristics не копируются слепо | Fused native path есть | Synthetic/native fixtures, telemetry | Numeric gates не доказаны |
| Telegram direct | Старый Vibe транспорт не равен target | Native Telethon path не найден в inspected RKB source | Нужны media-present/FloodWait/UNKNOWN | Не принято |
| Dataset primary | Старый SQLite archive не подходит | Publisher/restore полного target chain не доказан | Нужен exact-version restore | Не принято |
| Hot handler | Нет доказанной целевой замены | Отсутствие подтверждено собственным runbook | Нужен update без model reload | Не принято |
| Non-owner/Slack/Mira | Часть legacy/adapters есть | Не весь product workflow подключён | Identity/revocation/order/phone отдельно | Не принято |

## 10. Почему мог произойти дрейф

**Документальная причина подтверждена:** даже согласуемый TASK сохранял sole-uploader Vibe, mandatory media-SHA, ежедневный Yandex backup и обязательный backend-native путь, не соответствующие прямым уточнениям владельца. Это нельзя списать только на исполнителя кода.

**Сужение реализации подтверждено:** ограниченный текстовый pilot превратился в основной развиваемый путь; его hard limits и отрицательные тесты защищают ограниченный контракт, но не полную RKB. `unsupported` лучше молчаливого чтения чужого legacy corpus, однако не является завершением переноса.

**Оркестрационный риск подтверждён, причинность не доказана:** точное имя проекта DevCoveer по умолчанию указывает на старый dirty checkout с прежними требованиями Supabase и семью IDs; актуальная работа лежит в других worktrees. Без фиксации физического checkout агент может получить противоречивый контекст. Это наблюдение не доказывает, какой именно prompt видел Codex в каждый момент суток. Для персонального attribution понадобился бы отдельный полный trace; аудит не приписывает намерение или все ошибки одной модели.

**Приёмочный пробел подтверждён:** нет единой выполненной матрицы legacy→target, а статус source/fixture/deploy/product в сообщениях смешивался. Сложные recovery fences местами реализованы раньше очевидных media/DjVu функций. Исправление — укрупнённый parity-план и ранний реальный вертикальный сценарий, а не отмена recovery безопасности.

## 11. Самопроверка выводов MAIN-ChatGPT и консультанта

| Прежняя формулировка | Исправление |
|---|---|
| «Telethon — новый выбор владельца» | OWNER говорит о ранее заданном требовании; исправляем недостоверное отражение, не приписываем владельцу новую мутацию |
| «20/min суммарно по системе» | Непроверенная интерпретация; простой configurable pacing, actual FloodWait, без выдуманного provider SLA |
| «Для импорта backend обязательно рендерит страницы» | Только если клиенту действительно нужны пиксели; повтор при уже доступном source не требуется |
| «Каждый stage заново проходит всю книгу» | Полный холодный proof walk; кэши сокращают повтор, но дизайн всё равно неэффективен |
| «В новой версии нет Story body» | Body есть и это улучшение; отсутствует полная пользовательская приёмка и часть continuation |
| «Старый функционал удалён» | Многие файлы идентичны; потеря именно доступного target поведения |
| «Раз OCR не используется, импорт правильный» | Отсутствие OCR не оправдывает обязательное совпадение с native text |
| «Только README с новой схемой исправит требования» | Исправляются canonical и связанные active docs/manifest; historical evidence явно отделено |

Opus предложил 16 находок, полезные boundary cases и подход reuse-first. Не приняты автоматически: неподтверждённое отсутствие body, объединение reprocess и replay, произвольный15% threshold Go/Rust, грубые Telegram размерные константы, логический snapshot вместо уже реализованного physical backup, утверждение что весь хвост только в Kaggle. Предложения консультанта не становятся требованиями владельца.

Receipt консультации: actualModel=`claude-opus-5-5`, task=`dvt_726c14168efd4348b9690b070ed2b0b8`, artifact SHA256=`b8ea1d68090823970a6f59cdb86b85b364613faf2cbff98a9b36f5df38fa41ef`, 19 461 bytes. Режим provided-only, advisory; runtime и репозиторий самим консультантом не исследовались. Dot, напротив, независимо прочитал закреплённые источники; его итог и промежуточные ссылки доступны в THREAD. Расхождения разрешены прямым чтением кода, не голосованием моделей.

## 12. Порядок исправления и распределение работы

### Пакет A — модельный импорт и функциональная преемственность

В существующих stage/source contracts убрать mandatory native-text/all-book proof dependency, перенести stage/tokenization/document indexing в Kaggle, подключить PDF/DjVu/large-book batches, reprocess/rechunk и media links. Использовать действующие `compile_model_stage`, `merge_stage`, `validate_graph`, planner/search_material; не копировать E5/SQLite/Supabase placement. Добавить один owner-only lease. Сначала пройти небольшой иллюстрированный scan и DjVu, затем длинную книгу и restart без повторного inference.

### Пакет B — прямой Telegram и настоящий Dataset persistence

Минимальный Telethon adapter в Kaggle: archive locator, photo/conditional document, presence-only verification, pending/UNKNOWN, configurable pacing и exclusive session. Без Vibe и без Dev bytes. Отдельно — реальная Dataset publish/restore chain и weekly Yandex copy, с явным сохранением данных при crash и честными persistence statuses. Не выдавать протокол S3 snapshot за Dataset implementation. Обсудить с интегратором только фактическую нерешённую ACK/retention границу, не менять выбор владельца.

### Интегратор — runtime, query efficiency и выпуск

Сохраняет единственное владение live Kaggle writer/controller, существующую ветку и незавершённые операции; интегрирует A/B через PR и проверяет current revisions. Выполняет warm-handler и native-language assessment, общий latency/capacity/dense fixture, полную parity/rights/restore приёмку. Dot — независимые проверки; Opus — точечный консультант, не второй обязательный runtime agent.

Во всех окнах: сначала читать исправленный canonical, owner alignment и этот аудит; показывать короткий префикс автора в Slack; до edits заявлять точный branch/HEAD и scope файлов. Shared contracts/SQL revisions меняет один согласованный исполнитель. Параллельные исходники не означают параллельные импорты или право одновременно запускать notebook. Не брать старый dirty default checkout за актуальную ветку.

## 13. Минимальные доказательства результата следующей реализации

1. **Импорт:** модель видит scan без native layer; partial stage не вызывает обработку незаявленных страниц; один активный owner import; крупная PDF/DjVu продолжается после reconnect; проверен не только отказ, но сохранение правильного результата.
2. **Медиа:** chunk→figure→caption/labelled description→Telegram photo/при необходимости document→actual pixels; в target нет Vibe dependency; после media deletion статус честный, обычный search не ждёт архива.
3. **Знания:** факты без Story допустимы; достаточный эпизод сохраняет настоящий body/default ref; после restart get/search возвращают именно его; post-acceptance continuation достижим.
4. **Локальность:** Dev не хранит рабочий graph/corpus/vector results; QUERY/DOCUMENT vectors остаются Kaggle; измерены bytes и переходы, tokenizer считает final augmented input.
5. **Сохранность:** restart из exact Dataset version; потеря ACK не дублирует send/apply; weekly S3 восстанавливает названную версию; неподтверждённый хвост не назван Saved и не удалён по TTL.
6. **Скорость:** numeric gates из §8.1, чтения во время одного импорта, fair reserve, explicit pending/error denominator; warm code update не выгружает BGE без необходимости; Go/Rust decision с данными.

Подробные дополнительные сценарии и границы PASS находятся в [матрице миграционной приёмки](../testing/rkb-migration-parity-20261021.md). Не объявлять полную готовность по отдельной зелёной ячейке.

## 14. Границы оставшейся неопределённости

Этот аудит не проверял current Telegram quotas, действительность второй session, конкретные доступные photo sizes, провайдерскую частоту Dataset versions, текущий deployed SHA или весь corpus runtime. Не заявляется универсальная невозможность этих функций: требуется проверка выбранного actual adapter/account и исполнимого маршрута. Наличие предыдущего успешного PG+BGE запуска не закрывает сегодняшние readiness/restore gaps.

Проверены закреплённые source trees, существенные функции и тестовые сценарии; это не формальное доказательство корректности всех 68 тысяч новых строк. Для исправляемого компонента нужны focused tests и readback. Отчёт указывает конкретные дефекты и места продолжения вместо обещания, что повторный огромный аудит автоматически доведёт продукт.

## 15. Связанные документы и защита от следующей мутации

Нормативная основа: [единый handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), [уточнения владельца](../designs/rkb-owner-alignment-20261021.md), `.devcoveer/requirements.json`. Исторические отчёты сохраняют свои даты/результаты и не являются инструкцией восстановить Vibe/Supabase/E5 либо обязательный media-SHA в новом target.

Исправления затрагивают placement/storage/import/media/integrations/operations/acceptance и связанные active prompts. Инструкции про native language и hot debugging сохраняются. Проверка публикации должна подтвердить: runtime source files не изменились; девять critical requirement IDs и численные gates сохранены; в активных target docs нет нормативного sole-uploader Vibe, daily Yandex backup или mandatory Telegram hash; old reports не переписаны задним числом. Проверки source/docs не выдаются за runtime acceptance.
