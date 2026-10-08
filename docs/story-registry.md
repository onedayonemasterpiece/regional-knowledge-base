# Реестр историй — Story Registry

**Статус:** реализация в отдельном PR #78. До live-приёмки/rollout это НЕ production.

## Границы и назначение

Единый авторитетный файл SQLite корпуса (RKB_SQLITE_CORPUS_PATH): истории, утверждения, источники/цитаты, ACL, версии, аудит, ответы идемпотентных команд, задание извлечения, FTS. Не создаём новый SQLite, PostgreSQL, Redis или самостоятельный сервис. Чанк — заменяемый поисковый указатель; доказательство ссылается на зарегистрированную версию источника, страницу, регион и диапазон текста. Supabase — существующий векторный data plane, а не владелец редакционных данных или MCP OAuth.

Приложение Projects Hub — внешний клиент через реальный resource-bound RKB grant, а не второй владелец данных. RKB не строит собственного голосового помощника и не публикует в TG/VK/MAX. Прикладная модель выполняет смысловую работу, MCP — разрешённое хранение, транзакционные операции и детерминированные проверки.

## Смысловая модель

Story → StoryRevision → VariantRevision. Assertion → AssertionRevision → EvidenceLink / Assessment. Для исторической цитаты точное совпадение text_match=exact удостоверяет только присутствие текста в исходнике, а не истинность вывода. Внешние свидетельства регистрируются как immutable source versions. Легенда, воспоминание, слух, гипотеза, историческое утверждение и художественная реконструкция различаются. Документированный факт сообщения «автор N пишет об X» и утверждение «X произошло» не становятся одним assertion.

В SQLite отдельные нормализованные таблицы story_records, story_revisions, story_sources, story_assertion_versions, story_evidence, story_assessments, story_variants, story_variant_versions, story_review_decisions, story_publications, story_dependencies, story_grants, story_audit, story_receipts, story_jobs, story_outbox и story_schema_migrations. Изменения атомарны: версия, аудит, receipt и outbox фиксируются в одной транзакции с существующим корпусом.

Изменения векторного/книжного retrieval и требования из .devcoveer/requirements.json не затрагиваются. Story index сейчас локальный FTS, а очередь проекции векторов обозначается awaiting_worker, пока не подключён проверенный внешний исполнитель.

## MCP/function-call профили

| Профиль | Методы | Полномочия |
| --- | --- | --- |
| live | Только knowledge_search, без изменений | Существующий поиск книги |
| story_reader | story_search/get/history/validate/job_get, corpus_read, entity_list | Проверка чтения истории И всех её source dependencies |
| story_contributor | Reader + story_create/edit/archive/source_register/extract | Различные ACL contribute/research/edit внутри метода |
| story_editor | Contributor + story_transition/merge/access/export/publication_record | Уполномоченный редактор/управляющий/издатель, не право от видимости инструмента |
| full | Старые tools и новые Story tools | Серверная проверка прав каждого действия |

Все мутации с idempotency_key; существующие изменяемые объекты — с expected_revision. Структура operations — ограниченный discriminated union. Сохранённый receipt возвращается при повторе того же намерения даже после реконнекта; конфликт версии не перезаписывает чужую правку. Префикс story_ не вводит новый transport или чат-приложение.

Примеры параметров вызовов:

    story_create({
      "seed":{"text":"Слышал о подземном ходе, но источник не помню","origin_status":"unknown"},
      "idempotency_key":"voice-intent-20261009-0001"
    })

    story_edit({
      "story_id":"<story_id-from-create>", "expected_revision":1,
      "operations":[{"op":"add_gap","text":"Где это происходило?","priority":"high"}],
      "idempotency_key":"voice-intent-20261009-0002"
    })

    story_search({"query":"городская повседневность","limit":3,"mode":"lexical"})

Это аргументы функций, НЕ команды для немедленной публикации в каналы. Серверный output успеха возвращает commit_state=saved / queued (для задания без исполнителя), operation_id, resource_ids и committed_revision/job_revision; ошибки содержат явный code/message_ru. User id из входной модели не используется как основание полномочий.

## Редакционное одобрение и публикации

Story readiness вычисляется из конкретных вариантов. Publish_ready ставится только для перечисленных variant revision IDs с сохранённым решением редактора, актуальным fingerprint утверждений/оснований и проверками формулировок, атрибуции, прав и критических пробелов. Атрибутированная легенда может быть готовой без доказательства чудесного события. Изменения утверждений, важных assessment или исходного SHA требуют повторной проверки. Фиксация публикации через story_publication_record — запись наблюдаемого факта с editor_reported, а НЕ provider_verified; отправка осуществляется другим авторизованным провайдером. Story export — read-only пакет; URL отсутствует, пока реально не существует доступного ресурса.

Поля reviewer_kind/assessor_kind из клиентского JSON рассматриваются лишь как заявление вызывающей стороны; сервер отдельно записывает проверенный principal UUID/client_id и actual_executor=application. Human-authorship не доказано одним параметром.

## Сохранность и критерии развёртывания

SQLite WAL, foreign_keys=ON, для story write synchronous=FULL и короткий BEGIN IMMEDIATE. SQLite Online Backup API существующего SQLiteCorpus захватывает реестр/ACL/receipts вместе с корпусом. Обязательная отдельная restore-проверка должна проверить integrity_check, foreign_key_check, приложение/ACL, аудит и идемпотентность на восстановленной базе. Локальный snapshot НЕ является удалённой зашифрованной копией. Начальные RPO ≤1 ч / RTO ≤2 ч — только целевые SLO, не достигнутые измерения.

Перед production обязателен точный CI SHA (Python 3.12/3.13), проверка реального remote tools/list, scope/revocation E2E, реальные источники и карточки, извлечение с lease, функция через Projects Hub/live-interaction при наличии runtime, внешний backup/readback, 10k синтетических карточек и 10 readers/3 writers, p95 latency, и применимые corpus quality gates: BGE-only Hit@5 ≥85%, Hit@10 ≥90%, языковые срезы ≥85%, p95 public search ≤1500 ms, >=100 book-equivalent и >=100k chunks.

Функциональная граница первой ветки не означает прохождение всего перечня. Не заявлять продуктовую приёмку до отчёта о remote MCP и восстановления, не создавать фальшивые цитаты или редакционные карточки из публичных синтетических fixtures.

## План развития хранилища

Если появятся два одновременно пишущих API/worker-хоста, проверенное восстановление перестанет удовлетворять RTO/RPO или устойчиво нарушится write SLO после устранения длинных транзакций — переносим всё взаимосвязанное основное состояние/ACL/истории/каталог/доказательства/задания на PostgreSQL согласованно. Число зарегистрированных пользователей не является причиной миграции.
