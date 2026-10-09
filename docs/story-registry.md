# Реестр историй — Story Registry

**Статус (2026-10-09):** Story Registry работает; PR #85, #86 и #87 объединены с main. Production MCP и конкретный deployed SHA проверяются отдельно от состояния кода; полная межкнижная и векторная продуктовая приёмка не объявлена.

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

## Сквозное сопоставление нескольких книг

Каноническая постановка: [масштабируемое обогащение при импорте](prompts/scalable-cross-book-story-enrichment-20261009.md).

**Добавление книги — единая пользовательская цель.** Вызывающая модель в том же
`book_pages`/`book_ingest(stage)` проходе сохраняет доказательных кандидатов.
После активации `story_reconcile(command="next", document_id=...)` получает
следующую сохранённую опору из `story_reconcile_queue` или возвращает
незавершённый run. Это не отдельный LLM/агент внутри сервера:
`awaiting_agent` означает, что дальнейшее сравнение выполняет внешняя модель.

Для уже принятых книг продолжение старого `story_extract` идёт по прежним
`job_id` и `processed_chunks` без `book_ingest(reprocess)`. При создании
новых доказательных карточек они включаются в следующую порцию межкнижного
сопоставления. Клиент не загружает всё досье и реестр в контекст.

### Ограниченный MCP-проход

1. `story_reconcile(next, document_id=..., idempotency_key=...)` возвращает
   `job_id`, `anchor_story_id`, зафиксированную story revision,
   `next_action=search_then_enqueue`. Можно также начать с выбранной истории
   через `start` и явно отобранные refs.
2. Основная модель делает **два поиска** по разрешённой области:
   `story_search` по именам/проверяемым утверждениям и `knowledge_search`
   (BGE) по старому исходному корпусу. Первая ветка находит прежние
   карточки; вторая — источник без карточки. Реально выполненные каналы
   указываются при `story_reconcile(enqueue)`, max 50 различных refs на run.
3. `claim` выдаёт lease и одну сравниваемую пару, не весь корпус.
   `story_get(view="evidence_page", limit<=10)`,
   `story_get(view="sources_page")` и `corpus_read` возвращают исходные
   цитаты, ID страниц/регионов и авторство; после merge также читаются
   доказательства исходной архивированной карточки.
4. `stage` сохраняет **предложение**, а не придуманную истину:
   `identity_relation` (один эпизод/разная фаза/разные события/не связано/
   неизвестно), `contribution_kinds`, `independence` с причиной,
   `proposed_effect`, исходные `anchor_evidence` и `candidate_evidence`,
   объяснение/неизвестности. Сервер перепроверяет точную source revision,
   страницу, регион и цитату с обеих сторон; неудача не двигает cursor.
5. `apply` под ролью editor проверяет proposal ID, актуальную story revision,
   свидетельства и права. Возможные действия: связать отдельные истории без
   автоматического merge, добавить evidence к конкретному assertion,
   присоединить отдельное атрибутированное утверждение либо сохранить
   отсутствие изменения. Новое доказательство берётся из **другого**
   источника, исходный сюжет/его история не стираются.
6. `story_job_get(job_id, cursor, limit)` содержит фактический `processed_pairs`,
   число pending/applied proposals и постраничные IDs. Новый сеанс
   продолжает по одному ID; команда `next` тоже находит активный run.
   Неполное сопоставление не называется завершённым.

Exact quote подтверждает наличие текста у автора, **не** истинность события.
Два перевода/перепечатки могут иметь общую исходную цепочку и не считаются
двумя независимыми свидетельствами без специальной проверки.
Отдельные этапы объединяются типизированной ссылкой `phase_of`, не merge.
`story_merge` — только явная редакционная операция для проверенного дубля,
с сохранением исходных assertion/evidence, links, contributors, вариантов,
авторства и прежних оценок. Approval другого варианта не наследуется.

`story_search(mode=lexical)` ищет заголовки, summary и актуальные assertion
phrases. Если story semantic/vector branch не запущен, он не объявляется
запущенным: source BGE поиск остаётся отдельным каналом межкнижного discovery.
`completed_under_policy` относится только к зафиксированному конечному
набору сравнений и объявленным каналам, **не** доказывает отсутствие
других сквозных историй. При получении нового источника допустим
догоняющий bounded run.

## План развития хранилища

Если появятся два одновременно пишущих API/worker-хоста, проверенное восстановление перестанет удовлетворять RTO/RPO или устойчиво нарушится write SLO после устранения длинных транзакций — переносим всё взаимосвязанное основное состояние/ACL/истории/каталог/доказательства/задания на PostgreSQL согласованно. Число зарегистрированных пользователей не является причиной миграции.


### Доступ ко всем assessment и направленные связи (совместимое дополнение v7)

`story_get(view="assessments_page", assertion_id=..., limit<=10, cursor=...)`
возвращает **все текущие (не замещённые) оценки** конкретной редакции
утверждения, страница за страницей: `items[].assessment` содержит исходный
typed assessment, `assertion_id` и `assertion_revision` сохраняются.
Следующий `next_cursor` привязан к версии истории и ID утверждения.
После изменения карточки или попытки использовать курсор другой assertion
возвращается validation_failed. Ссылка на evidence перепроверяется по
текущим source ACL при каждом чтении. Прежние `assertion_page` и
`evidence_page` всё ещё возвращают только первые шесть effective assessments;
`assessments_has_more` означает, что продолжение доступно отдельно.

Для `story_reconcile(stage)` есть необязательное typed
`decision.phase_of_source = "anchor" | "candidate"`. Его значение —
**какая из двух историй является фазой другой**, а не порядок дат.
При `link_stories` с `part_or_phase` новое поле записывается в
`story_relations.phase_from_story_id/phase_to_story_id` независимо от
сортировки UUID. В `story_get(view="relations_page")` доступны
`phase_direction` и `phase_direction_status`. Исторические записи, для
которых исходная модель не указала направление, возвращаются
`legacy_unresolved`; миграция не приписывает им направление задним числом.
Конфликт нового направления с уже сохранённым требует явной редакционной
коррекции и не переписывает старое доказательство. SQL v7 добавляет только
две nullable колонки, без сброса данных/ревизий/историй. Для старых MCP
клиентов поле optional; обновлённую client schema нужно подтвердить отдельно.

При enrichment операция `story_reconcile(stage)` может дополнительно
передать `decision.evidence_relation` (по умолчанию `reports`) и
`decision.effective_assessment` — явно сформулированное моделью решение
с `support_status`, `independence`, `semantic_review`, `rationale`,
`method_version`. При `apply` оно проверяется и записывается через обычный
`RecordAssessment` в той же транзакции и той же новой редакции истории:
только точное исходное свидетельство anchor и новое свидетельство candidate,
без механической оценки остальных цитат. Claim новой версии имеет только одно
собственное evidence. `corroborated` не допускается без отдельно заявленной
`independent` оценки двух источников; backend не объявляет её доказанной
самостоятельно. Реальная проверка авторства модели остаётся отдельной задачей:
`assessor_claim_verified=false` и `actual_executor=application`. Отсутствие
assessment сохраняет старую семантику (добавить reports, не оценивать смысл).

Следующая независимая поставка: revision-guarded correction/retraction,
durable frontier/corpus watermark, реальный story-vector worker. Пока `story_search(semantic|hybrid)` сообщает
lexical fallback и не считается прошедшим semantic recall.


### Редакционное исправление межкнижных связей — schema v8

`story_edit(operations=[{"op":"revise_relation", ...}])` позволяет **редактору
исходной anchor-истории**, записавшему исходную связь в reconciliation,
провести три явных действия с проверкой `expected_revision` карточки и
`expected_relation_revision` связи:

- `action="retract"`: убрать связь из обычной навигации, сохранив
  предложение, цитаты, ID и весь журнал без физического удаления;
- `action="restore"`: только для отозванной связи и только после
  повторной проверки доступности и точного текста обеих исходных цитат;
- `action="correct_direction"`: только для активной `phase_of` с
  `phase_from_story_id` / `phase_to_story_id`, точно соответствующими
  двум сторонам текущей связи. Требуется осмысленное `reason`.

Каждое исправление повышает и редакцию anchor-истории, и собственную
редакцию связи, создаёт неизменяемую запись `story_relation_reviews`
с состояниями до/после, подтверждённым OAuth actor ID, клиентским ID
и временем; изменённые опубликованные варианты anchor требуют повторной
проверки. Старые связи начинают с `relation_revision=1`; schema v8
добавляет nullable-independent audit, не переносит/пересчитывает книжный
корпус и не переписывает исторические направления автоматически.

`story_get(view="relations_page")` по-прежнему выдаёт только активные
связи, включая `relation_revision`. `relation_history_page` показывает
также отозванные связи, исходные доказательства и последнюю
коррекцию; `relation_reviews_page` с обязательным `relation_id`
возвращает все версии редакционных решений через revision-bound
cursor, с ACL обеих историй и обоих источников. Нельзя использовать
старый курсор после изменения карточки. Повторная reconciliation не
может обойти отзыв через прежний `INSERT OR IGNORE`: такая попытка
отклоняется, требуется явное `restore`.

Это корректировка *редакционной связи*, не ревизия книжного источника
и не доказательство идентичности одноимённых лиц/зданий. Модели и
бэкенд не проводят скрытого семантического пересмотра: редактор явно
фиксирует основание решения. Отдельная production-проверка нового
MCP schema и deployed SHA обязательна.


### Долговечные решения по парам источников — schema v9

После `story_reconcile.apply` в той же SQLite-транзакции сохраняется
неизменяемая запись `story_reconcile_pair_decisions` с IDs сторон,
`kind=story|chunk`, версией политики, ID исходного proposal, эффектом и
отпечатками **принятого evidence** обеих сторон. Для истории отпечаток
учитывает IDs/revisions текущих утверждений и evidence, version/sha/active
revision source roots; для чанка — ID, revision, text/search-material hashes,
региональные refs и source SHA. Заголовок/сводка не считаются новым
историческим доказательством. Отпечаток anchor фиксируется **после**
присоединения нового свидетельства; иначе тот же кандидат бесконечно
повторялся бы из-за нового story revision. Отмена и ещё не применённый
proposal не являются просмотренным решением и не попадают в dedup.

При `story_reconcile.start/enqueue` прежняя оценённая пара с теми же
evidence и policy не занимает слот frontier, а возвращает
`skipped_decided_pairs`. Когда оба обязательных канала были реально
просмотрены и все найденные кандидаты уже оценивались, состояние —
`already_compared_under_policy`, **не** `no_match_found_under_policy`.
Оно не означает глобальной полноты корпуса. Новое evidence, source
revision или версия policy позволяет повторить сопоставление. Устаревшие
записи audit не удаляются и никакие пары не merge автоматически.

Ограничение `max_candidate_pairs<=50` по-прежнему относится к отдельному
run; v9 не обещает полного corpus-watermark, очереди overflow или
автоматического обхода всех источников после поздней индексации. Это
следующая часть масштабного reconciliation, требующая отдельных
датированных тестов покрытия и реального MCP acceptance.


### Сквозное продолжение за пределами 50 кандидатов — schema v10

Число `max_candidate_pairs <= 50` ограничивает **один модельный пакет**,
но не всю библиотеку или всю историю. `story_reconcile(start)` и
`enqueue` сохраняют выбранные кандидаты сверх лимита в SQLite-таблице
`story_reconcile_overflow`; одна `enqueue` принимает не более 50 refs,
но допускаются последовательные порции. Сохраняются исходный `run_id`,
упорядоченные id/type/revision кандидата, версия политики, provenance
поиска и состояние `pending/continued/already_decided/cancelled`.
Нет сканирования всего графа/книжного корпуса в SQLite write transaction.

Когда все решения текущего фронтира применены, каналы поиска
`story_lexical + source_bge` засвидетельствованы и остался overflow,
результат — **`partial_budget_exhausted` с `next_action=continue_overflow`**,
а не `completed_under_policy` / `no_match`. Постраничный
`story_job_get` показывает `overflow_pending`, `overflow_continued`,
`coverage_state`. Следующий `story_reconcile(next)` до очередной новой
карточки поднимает не более `max_candidate_pairs` refs из durable overflow
в **новый run** (с `origin_run_id`). Его lease/stage/apply, revisions,
авторизация, exact-source proof и receipts остаются прежними. Продолжить
можно после завершения сессии, не перенося весь фронтир в новый контекст.
Фактическую версию кандидата и ACL перепроверяют при выдаче; уже принятые
решения v9 с тем же evidence/policy повторно не анализируются, а
отмечаются `already_decided`. Новое evidence не считается старым.

Если пользователь отменил run, его невыданный overflow явно становится
`cancelled` и не запускается сам. Новое source review и новый run можно
создать по явному намерению. Аудит прошлых предложений/источников не
удаляется. Примеры приёмки: 3 кандидата при лимите 1 обрабатываются в
трёх независимых run, а 51-й при лимите 50 остаётся в SQLite и его
отмена проверяется; idempotent migration и повторный `next` безопасны.

**Честная граница:** v10 обрабатывает все *переданные* внешней моделью
проверяемые кандидаты без общего ограничения 50. Она НЕ доказывает,
что поисковые каналы обнаружили все истории в источниках. Для массового
охвата по-прежнему необходимы corpus/source/index generations, устойчивые
поисковые cursors/watermarks и late-index/alias catch-up. Semantic
`story_search` пока `lexical_degraded`, source BGE — отдельный индекс.
