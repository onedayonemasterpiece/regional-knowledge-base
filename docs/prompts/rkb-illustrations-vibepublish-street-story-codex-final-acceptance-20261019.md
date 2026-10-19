# Codex: завершить и независимо принять RKB → VibePublish → Telegram, не сломав Street Story

**Постановка владельца · 19 октября 2026 · исполнитель: Codex · приоритет: продуктовый результат.**

## Цель

Довести до **проверяемого конечного состояния** уже разработанную цепочку:

**импортированная книга / страница / figure-region → поисковое evidence → связанная иллюстрация → реальные байты через RKB MCP → приватный Telegram topic 4 → восстановление из Telegram**, включая **старые owner-owned архивы** и сохранность параллельной работы **Street Story / VibePublish**.

Это **не новый MVP и не поручение переписать сервисы**. За 18–19 октября выполнены десятки изменений и тестов. Значительная часть продукта уже работает. Нужно восстановить актуальные факты из production, независимо проверить их, устранить **только воспроизводимые остаточные дефекты**, привести противоречащую документацию в порядок, выдать окончательные ссылки/пруфы. Владелец многократно разрешил необходимые исправления, PR/CI/merge и контролируемую выкладку в границах этой задачи. Не завершай работу очередным планом, сводкой чужих слов или одной зелёной CI.

## 1. Текущий снимок состояния (проверен 19.10; ОБЯЗАТЕЛЬНО перепроверь перед действием)

| Компонент | Наблюдавшееся состояние |
| --- | --- |
| RKB `main` | `d491fd4785644524b63479e9d3d8f240df84588a`. Есть поздние интеграции со Street Story: [#145](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/145), [#147](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/147). |
| RKB production API + indexing | Оба **active** из релиза `481241fa36db30e3f0e6c4c4b88fb3ce84964676` (PR144); совпадение фактических `WorkingDirectory` и `PYTHONPATH` проверено. То есть `main` НОВЕЕ live. Новый Street Story delivery-unit также active. Не выкатывай `main` поверх соседней работы без сравнения. |
| VibePublish `main` и production | Оба `d1f880b24788f609398a660ba53dd8c4731b7aec`, API и Telegram worker **active** на одном релизе, [#84](https://github.com/onedayonemasterpiece/vibepublish/pull/84) уже **merged/deployed**, не называй его «ещё не выпущенным». |
| VibePublish SQLite | `PRAGMA user_version=11`; таблица `media_service_entry_grants` существует. **34 exact-entry grants: 30 для иллюстраций topic 4, 4 для оригиналов PDF/DjVu topic 2.** Это текущие read-only числа, но не замена скачиванию реальных байтов. |
| RKB активная ревизия | **151/151** иллюстраций имеют `vibepublish_entry_ref`. Всего 215 записей исторических ревизий, из них **214** с archive ref; единственная без ссылки — **неактивная**, не очередь активного продукта. Текущий запрос по `mirror_error_type IS NOT NULL` вернул 0. |
| Происхождение архива | [Живой отчёт 19.10, RKB PR146](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/146) утверждает и документирует проверку **151/151** native Telegram topic 4, точный `origin.ref`, SHA провайдера; **121** новых service-owned + **30** старых owner-owned. PR146 на момент проверки **open, не merged**: сам проверь его evidence и CI, затем актуализируй/слей при подтверждении. |
| Реальное получение свежей архивной картинки | Штатный `Kalinigrad_base.illustration_fetch(id="002a2514-9fc3-5b5b-ae5e-0999235a33b9")` вновь вернул **ImageContent image/png** с `delivered_crop_origin=telegram_archive` и SHA `837d6ef34e0eaee0af47f47f8258a45f5f18d35d1a447ae9da15a597d494f3d1`. |
| Важная ловушка диагностики | `illustration_fetch("9d4e4dad-7e8d-5351-8ed3-06d96d1652eb")` возвращает ошибку, **но это неактивный синтетический источник**, не одна из 30 реальных активных legacy-иллюстраций. Не объявляй весь legacy fallback сломанным на основании этого ID. PR146 сообщает успешную байтовую приёмку нескольких **активных** старых и новых изображений; повтори независимо. |
| Street Story | `street-story.service` и `regional-knowledge-street-story-delivery.service` active. В Vibe сохранён код полного кадра/Original из [#80](https://github.com/onedayonemasterpiece/vibepublish/pull/80), идемпотентного GET [#83](https://github.com/onedayonemasterpiece/vibepublish/pull/83) и независимого API/worker lifecycle [#85](https://github.com/onedayonemasterpiece/vibepublish/pull/85). Street Story продолжает собственные релизы — не перезапускай/не переписывай его по инерции. |
| Диск | В последнем read-only замере ~**3.7 GB** свободно; фактический остаток меняется быстро. В RKB действует порог **2 GiB** для новых mirror admissions ([#140](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/140)). Не выполняй тяжёлые копии БД без расчёта запаса. |

**Главный вывод из снимка:** массовую загрузку 121 иллюстрации НЕ НУЖНО повторять; активный архив уже заполнен. Остаются независимая приёмка реальных сценариев, проверка полноты/точности, документация, долговременная надёжность и подтверждение соседних продуктов. Статус может измениться: текущие данные важнее любой таблицы в постановке.

## 2. Репозитории и исходные артефакты

- [RKB](https://github.com/onedayonemasterpiece/regional-knowledge-base): `src/regional_knowledge/illustrations.py`, `illustration_mirror.py`, `source_archive.py`, `sqlite_corpus.py`, `supabase_backend.py`, `scripts/production/deploy_story_registry.py`. Канонический [illustration runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/operations/illustration-pipeline.md).
- RKB [#136](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/136) durable GET, [#137](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/137) exact-caption linkage, [#138](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/138) SQLite mirror fix, [#140](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/140) disk guard, [#141](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/141) согласование `PYTHONPATH/WorkingDirectory`, [#143](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/143) recovery FloodWait terminal GET без нового PUT, [#144](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/144) истёкший архивный asset внутри того же fetch. Это **уже слитые исправления**, не стартовые задачи.
- [Актуальный отчёт RKB PR146](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8c6626b908def6885e2bddbf214ead829113f22d/docs/reports/illustration-archive-live-acceptance-20261019.md) — важнейший список уже проверенных инвариантов. Прочитай и **проверь факты**, не принимай PR за деплой.
- [VibePublish](https://github.com/onedayonemasterpiece/vibepublish), особенно `social_operations/media_entry_grants.py`, `service.py`, `storage.py`, `worker.py`, `adapters/port.py`, `deploy/devcoveer/vibepublish-worker.service` и [legacy-entry-reads](https://github.com/onedayonemasterpiece/vibepublish/blob/main/docs/features/media-store/legacy-entry-reads.md). Этот runbook может отставать от уже выполненной миграции schema11 — исправь расхождения после проверки.
- [VibePublish PR87](https://github.com/onedayonemasterpiece/vibepublish/pull/87) — **draft source-only fail-closed rollback**. Не merge/deploy автоматически: предыдущая schema10 версия несовместима с уже живой schema11. Перед любым rollback проверяй реальную возможность восстановления и сохранение чужих публикаций.
- [Street Story](https://github.com/onedayonemasterpiece/street-story) и [RKB delivery #145](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/145), [#147](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/147). PR270/271 Street Story касались происхождения фактов на картинке; не смешивай такие редакторские дефекты с RKB Telegram.
- [Общая policy M2M на DevCoveer](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md): для сервисов на одном сервере локальный loopback HTTP и отдельный наименее привилегированный Bearer, не браузерный OAuth или owner access.
- [Переписка и реальные изображения от dot в личном Slack-треде владельца](https://mira-projects.slack.com/archives/D0C832FNT1B/p1792316954847099) — для восстановления хода работ, но выводы подтверждай собственным live readback. Приватные данные, токены и файлы не публикуй в GitHub.

## 3. Что выполнить самостоятельно

### A. Принять реальный активный архив

1. Сними read-only runtime-профиль RKB API, RKB indexer, Vibe API, Vibe worker, Street Story и RKB→Street Story delivery: **PID, фактический исходный SHA, CWD, PYTHONPATH, active state**. Проверь текущие `main` и PR, чтобы не затереть параллельную работу.
2. Прочитай `PR146` и **независимо пересчитай** активные ревизии. Проверочный SQLite-запрос (исполнять только read-only):

   ```sql
   SELECT count(*) total,
     sum(json_extract(i.payload,'$.vibepublish_entry_ref') IS NOT NULL) archived
   FROM corpus_rows i
   JOIN corpus_rows d ON d.row_key=i.document_id
   JOIN corpus_rows p ON p.row_key=json_extract(i.payload,'$.page_id')
   WHERE i.table_name='rkb_illustrations'
     AND d.table_name='rkb_documents'
     AND p.table_name='rkb_pages'
     AND p.revision=json_extract(d.payload,'$.active_revision');
   ```

3. Сверь каждую активную запись с Vibe `publications`, `revisions.intent.origin`, native `facts.snapshot` и `observed_media[0].sha256`: **тот же tenant, Telegram destination, forum topic 4, `knowledge://illustrations/<illustration_id>`, exact provider SHA, ни одной отсутствующей/дублирующей записи**. Для исторических 30 допустим legacy-origin-v1 с двумя ключами `system/ref`: SHA берётся из неизменного native `download_sha256` evidence, а не из отсутствующего `origin.sha256`. Никогда не переписывай immutable intent ради унификации.
4. Проверить действующие **34** exact grants (30 image, 4 PDF): scope, actor epoch, binding, entry, topic, revocation; без выдачи широкого `list/search`, изменения ownership и без прочтения чужих записей.
5. Если SQL/native метаданные уже дают 151/151 — **НЕ делай повторные PUT**, не импортируй книгу заново, не «чинить» неактивный synthetic row только ради красивых 215/215. Любой реально pending/failed активный случай разбирать через сохранённые operation IDs и реальный terminal state.

### B. Проверить пользовательское извлечение, а не только метаданные

Проведи для нескольких независимых запросов стандартную цепочку **`Kalinigrad_base.search(query) → fetch(evidence_id) → illustration_fetch(illustration_id)`**. Ключевой нюанс: search может вернуть `main_state=starting/pending`, пустые provisional hits и `mode=lexical_degraded`; продолжай **тот же `main_job_id` и тот же query** до `ready` либо зафиксируй отказ/timeout. Пустой предварительный результат — не доказательство отсутствия изображения.

Реальные контрольные естественно-языковые запросы:

- «Покажи старый план Кёнигсбергского зоопарка с расположением вольеров».
- «Найди фотографию обезьян в Кёнигсбергском зоопарке».
- «Покажи иллюстрацию креста-потента Тевтонского ордена».
- «Что известно об открытии Кёнигсбергского зоопарка? Покажи связанную старую открытку» — **текстовый** запрос без заранее известного image ID.
- «Как выглядели Зелёные ворота Кёнигсберга?» — связь только по **точному region/caption**; изображение Зелёных ворот не следует выдавать за Деревянный мост.
- Negative: нерелевантный рисунок на той же странице не должен цепляться к тексту; чужая private картинка должна оставаться недоступной.

Убедись, что `fetch.metadata.illustrations[]` содержит подлинный `illustration_id`, документ/страницу/регион, printed caption и отдельно model observation; `illustration_fetch` возвращает **нативный ImageContent PNG/JPEG**, `delivered_crop_origin`, байтовый SHA-256 и корректные права. **Никаких web image search, Telegram forward, сгенерированных замен или прямого Object Storage чтения вместо штатного RKB MCP.**

Выполни не только новый Object Storage путь, но и **реальный Telegram archive fallback**: как минимум **три активных legacy-owner** и **две новых service-owned** иллюстрации с естественно уже отсутствующим crop. Не удаляй исходники искусственно для теста. Сверь фактический SHA полученных байтов с provider SHA и правильным MIME. Старый синтетический ID `9d4e4dad-7e8d-5351-8ed3-06d96d1652eb` не использовать как положительный кейс. При возможности приложи несколько **настоящих изображений, полученных RKB**, в уже существующий приватный тред Slack владельца и укажи запрос/evidence/page/SHA. Slack может удалить вспомогательный PNG `pHYs`, потому SHA Slack-вложения и SHA исходных PNG не обязательно совпадут; не подменяй доказательство пиксельной идентичности транспортным хешем.

### C. Проверить оригиналы книг и историю импорта

Проверить **4** старых exact-entry topic-2 grants на канонические оригиналы PDF/DjVu: read-only права, native topic/source identity, **реальный byte readback/SHA через авторизованный сервисный путь** хотя бы по каждому из четырёх, если возможно; если нет — точно опиши, что не проверено, не обходи owner OAuth. Текущий отчёт фиксирует 27 `verified` оригиналов и один неактивный synthetic `pending`, но четыре старых PDF не были все заново скачаны в ходе иллюстрационной приёмки. Не смешивай полноту списка оригиналов и качество BGE-поиска; не делай массовый переимпорт без доказанной необходимости.

### D. Принять соседние проекты без регрессий

- VibePublish **#80**: source photo с полной EXIF-ориентацией и исходным aspect ratio, отдельный `original` без overlay, text compositor, без повторного image-generation submit при неизвестном результате. Проверь на **существующем** сохранённом Original/receipt, без новой генерации/публикации и без случайной смены principal.
- **#83**: долговременный media GET — один и тот же operation ID после потери ответа, без дублей; **#85**: рестарт API не перезапускает worker. Если рестарта не требуется, не делай его ради демонстрации; используй действующие regression и историю проверенного release.
- Не перезаписывай активную параллельную разработку Street Story, её локальные worktrees, APK, изображения и данные. Не отправляй тестовые посты пользователям. RKB `main` после production содержит [#145](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/145) (POI delivery) и [#147](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/147) (его release keys): проверь отдельный уже действующий delivery unit, source identity, idempotency и сохранность связки, но **не делай неподтверждённый общий деплой `main`** только ради иллюстраций.
- Согласуй с активными исполнителями только реально конфликтующие изменения. Один оператор владеет записью в live Vibe DB / изменением worker; нет параллельных миграций или двух одинаковых deploy.

### E. Исправить только найденные проблемы и завершить документацию

Если обнаружен дефект: дай минимальный воспроизводимый тест → адресный code fix → CI → merge → **контролируемый релиз из чистого exact-HEAD worktree** штатным DevCoveer путём → проверка реальных PID/SHA и `search/fetch`/provider readback. Не путай `merged`, `deployed` и `live_verified`. Не используй dirty source handoff как release workspace. При `denied`/unknown outcome сначала проверяй сохранённый job/operation, не обходи отказ новым shell-скриптом или повторной публикацией. Не вводи новый сервис, новую очередь или вторую систему доступа.

Для live SQLite v11 **никогда не запускай schema10 worker при откате**. Обязательны согласованное окно с Street Story, настоящая свежая консистентная резервная копия и проверка восстановления, проверенный schema11-compatible fail-closed rollback, достаточно свободного диска. Не делай полный raw copy ~3.9GB на почти заполненном томе, не удаляй единственную страховку. RKB crop admissions не должны съедать запас ниже 2 GiB.

**Убери устаревшие «not done» из документации, если live уже подтверждён.** Проверь [RKB PR146](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/146), его доказательства и CI; по результату слей/исправь, не создавая дубль отчёта. Устаревший `docs/reports/imported-source-audit-20261018.md` может существовать в отдельной ветке/коммите, а не в `main`. Не выдавай 151/151 архивных ссылок за 151 заново скачанных файлов или за готовность **массового** импорта/векторного retrieval; это отдельные требования `.devcoveer/requirements.json`.

## 4. Продуктовая приёмка — что значит DONE

1. **Активные иллюстрации:** актуальный числитель/знаменатель = 100% с проверенными topic4/origin/provider SHA; ни одной действительно активной застрявшей без exact-entry readback и ни одного дубликата. Все неизменные уже verified записи сохранены.
2. **MCP:** несколько осмысленных естественно-языковых запросов дошли до `ready` в BGE (с сохранением `main_job_id` при pending), `fetch` дал фактически верные дескрипторы, `illustration_fetch` вернул реальные image bytes, не только текст.
3. **Telegram восстановление:** фактические байтовые SHA и `telegram_archive` на ≥3 **активных старых** и ≥2 **новых** изображениях; отдельные negative ACL/topic и отсутствие owner bypass.
4. **Originals:** все 4 legacy topic2 grants проверены; по каждому либо реальный авторизованный readback/SHA, либо точный оставшийся блокер. Остальные импортированные книги оценены по активной ревизии и архивному статусу, без ложной «качество поиска = количество чанков».
5. **Street Story:** API/worker, Original/compositor, POI delivery, работающие процессы, чужие jobs и публикации не пострадали; нельзя делать тестовую генерацию/публикацию ради галочки.
6. **Документы и доступность для следующего агента:** одна обновлённая каноническая инструкция + фактический acceptance-report в GitHub с immutable commit links, актуальный статус PR, таблица пройденных/непройденных проверок, вывод по качеству и рискам. Положи краткий результат с ссылками в существующий приватный Slack-тред владельца, если есть разрешённая интеграция.

**Финальное сообщение владельцу:** «готово» только при выполненных проверках выше; если остаётся узкий блокер — укажи конкретный failing tool, ID/sha, границу разрешений, сохранённое состояние, безопасный следующий шаг. Не заменяй работу новым планом. Приоритет — работающий продукт и доказательства, без перфекционизма и лишней инфраструктуры.

## 5. Полномочия и запреты

Владелец прямо разрешил работающим агентам адресные изменения кода, тесты, PR/merge, безопасные штатные релизы и приватную E2E-демонстрацию уже доступных иллюстраций. **Дополнительного общего разрешения на этот же объём не запрашивать.** Отдельно останавливаться только перед непредусмотренным расширением прав, публикацией закрытых источников в публичный канал, потенциальной потерей живой БД, вмешательством в активные операции других проектов или незащищённым irreversible действием.

Нельзя: вручную отправлять 121 иллюстрацию повторно, изобретать Telegram-операции/ID, подключаться под владельцем вместо сервисного principal, отключать ACL/timeout/backup checks, стирать активные worktrees, реимпортировать книги без необходимости, запускать две конкурирующие миграции, выдавать метаданные вместо реальных файлов, объявлять успех по одному CI или статусу `verified`.
