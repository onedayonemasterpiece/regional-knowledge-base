# RKB: повторный критический аудит историй, хранения и быстродействия

Дата: 2026-10-20. Статус: **проектный аудит завершён; новая реализация и её runtime gates — NOT RUN**.

Единственный сводный контракт для внедрения — [задание кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Этот отчёт сохраняет вопросы консультанта, ответы и принятые решения. Он не вводит альтернативных требований. Дословные ответы в приложениях относятся к промежуточным версиям проекта: замечания ниже уже обработаны, а отклонённые предложения не являются инструкциями.

## 1. Что и как проверено

Исходная документационная база: [`a8cf41b50b8ccb62409d046d009d53c09d74b1d7`](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/a8cf41b50b8ccb62409d046d009d53c09d74b1d7). Владелец уточнил scope после предыдущих аудитов: переносится только GEO, книги импортируются заново; иллюстрации доступны всем, scan proof — установленным категориям; история должна иметь сохранённое повествование, а быстродействие и экономное хранение являются приоритетами.

Проведены три отдельные реальные консультации через DevCoveer `claude_consult`, provider Anthropic. Во всех ответах `actualModel = claude-opus-5-5`; подмены другим консультантом не было. Консультанту передавались ограниченные evidence bundles из кода, актуальных документов, решений владельца и ответов на предыдущий раунд. Режим — read-only, `provided_only`: это проверка механизма и согласованности проекта, а не самостоятельное чтение книг или измерение production.

| Раунд | Task | SHA-256 сохранённого Markdown артефакта | Область |
|---|---|---|---|
| 1 | `dvt_d3d1298aa3ac48fd843526573a1967ac` | `f8c59d1b3e22a7176ce127738d6c8b136bfdcf3a6c853cf62a4d32e5902b91b9` | Истории, обычный readback, права, O(corpus) поиск, retention и durable ACK |
| 2 | `dvt_56336751842a482d8e7e368f1c45911b` | `ec56a195560950cce13c0ab4aff71542750bec158b98552eb5893cd40e7d5cf2` | Повторная проверка архитектуры после ответов: restore, collation, derived data, UNKNOWN, prepared ANN, облачная запись |
| 3 | `dvt_0554de869f9145db8e8867c05edc1bc1` | `8c36202ef25ac149fdf08d94455f8a64d32e3b322876ab4ae07c5019a359518a` | Сохранение всех функций, GEO, роли, каналы, случаи разногласий и границы приёмки |

Хэши относятся к полным артефактам сервиса консультаций, а не к отформатированным приложениям этого отчёта. Приватные пути сервиса, credentials и содержимое книжного корпуса сюда не включены.

В третьем раунде Opus не обнаружил потери заявленных функций и счёл ответы на второй раунд достаточными. Он предложил пять точечных уточнений; все внесены. После них консультант разрешил передать документ кодовому агенту без нового проектного раунда. Это не runtime PASS и не гарантия отсутствия всех будущих дефектов.

## 2. Существенные исправления

| Риск | Решение в окончательном проекте |
|---|---|
| Импорт хранит тезис/оценку, но не рассказ | При достаточном эпизоде атомарно сохраняется существующий `Variant.body` и точный default reference; readback проверяет реальные bytes/revision/hash. Summary и potential не заменяют body. |
| Рассказ скрыт compact get, search или publish gate | `story_get`, search, read-export и голос используют сохранённое повествование по правам Story. Публикация и личный авторский черновик имеют отдельные правила. |
| «Как» в заголовке обещает отсутствующий механизм | Заголовок проверяется наравне с текстом. При недостатке источника — честная формулировка/gap, без выдуманной причинности. Неизвестный исход может быть полноценной честной концовкой. |
| Факты исчезают без Story | Assertions, даты, наблюдения, люди и организации извлекаются самостоятельно; facts-only допустим, story quota запрещена. |
| GEO ссылается на непереносимые chunks | Минимальный GEO provenance не требует старого книжного FK. Reanchor только после проверки exact source/edition/hash; иначе явный `legacy_unanchored`. GEO identity manifest создаётся до рабочих импортов. |
| Python полный обход корпуса и stories | Ограниченные SQL ветви, pgvector HNSW, GIN FTS, B-tree/keyset, pg_trgm и PostGIS по назначению; batch hydrate, текущие revision/permissions и конечные budgets. |
| ANN меняет план после первых вызовов | Проверка реального prepared execution после шестого и последующих вызовов и при разных scope. Custom plan включается там, где он нужен; transaction-local настройки не протекают через pool. |
| Активация делает массовые index updates | Immutable projection rows по revision, заранее построенные индексы и небольшая атомарная смена active pointer; superseded rows удаляет bounded GC. |
| Подтверждённая работа существует лишь на диске DevCoveer | Local receive означает pending. Final accepted требует удалённого exact input/intent, Saved — exact outcome/receipt; восстановление не запускает модель заново вместо сохранённого решения. |
| Короткий local journal конфликтует с суточным backup | Local raw spool 4 h / 512 MiB и remote recovery tail имеют разные retention. Local GC следует remote seal; remote GC покрывает назначенные baseline cursors и consumers. |
| Replay ломается на изменении CPU float | Outcome hash охватывает canonical facts/body/IDs/evidence/cursor. Векторы/FTS/crops — версионированные derived projections; delivered artifact сохраняет собственную exact identity. |
| UNKNOWN навсегда запирает весь сервис | UNKNOWN не исчезает по TTL и не вызывает blind resend. Есть scoped budgets и явное подтверждённое resolution; unrelated import/save продолжают работать. |
| Старый worker продвигает или удаляет cloud snapshot | Epoch fencing и CAS указателей у authority; notebook получает только ограниченный upload ticket. Поздний upload остаётся orphan. Large bytes идут напрямую в облако, не через control spool. |
| Restore меняет сортировку/индексы | Manifest фиксирует PG minor, extensions, runtime/architecture и collation. Нужный rebuild выполняется до READY; `REFRESH VERSION` сам индекс не ремонтирует. |
| Demo/expert category не выдаёт обещанный proof | Verified category материализует существующий `scan_proof_read` с аудитом и expiry demo. Render проверяет действующий grant; claimed category ничего не выдаёт, ручной revoke не отменяется чтением. |
| Scale gate пытаются пройти старым корпусом | Отдельная изолированная benchmark DB с новым chunk/BGE/FTS путём. Synthetic amplification допустим только как нагрузка; dense quality проверяется на реальных held-out fragments. |
| Статус core cutover выдаётся за завершение всех функций | Core cutover, release конкретного канала и завершение всего задания имеют разные явные границы. Уже работающий канал нельзя молча отключить ради cutover; весь функциональный scope остаётся обязательным. |
| Истории оценивает та же модель по своим summaries | Малый blind pilot на одних source pages, baseline из pinned прежнего prompt/contract, реальные сохранённые тела, verdict владельца/назначенного человека. Без verdict — NOT RUN. |

## 3. Ответы на десять вопросов первого раунда

1. **Совместимость.** Существующие MCP/Projects Hub/другие клиенты сохраняются, поэтому совместимость внешнего surface нужна. Старые non-GEO IDs и corpus records не мигрируют. Не следует превращать отказ от данных в отказ от клиентов или требование полностью заменить модель Story.
2. **Supabase.** В target нет Supabase corpus/vector query path и передачи Python allowed-map. Наблюдаемый O(corpus) путь удаляется. По одной форме функции нельзя утверждать доказанную несанкционированную утечку: такие выводы аудит не делает.
3. **Неизвестный исход.** Допустима связная история с честной границей знания. Она не обязана оставаться вечным needs_context, если существующего материала достаточно. Запрещено выдумывать развязку, а не заканчивать рассказ неизвестностью.
4. **Draft/роль.** Imported shared body наследует Story read scope; частный авторский draft сохраняет собственный scope. Demo — verified категория с ограниченным сроком; expert/local_historian тоже требуют проверенной identity/category. Pixel access материализуется отдельным существующим grant, а не выводится из self-description.
5. **Remote tail.** Используется удалённый immutable tail с CAS manifest и verified baselines. База в Kaggle Dataset и копия в Yandex не делают последние Yandex-tail изменения независимыми от Yandex. Одновременная потеря remote provider и DevCoveer не покрыта выдуманным zero-loss обещанием.
6. **ACK.** Выбран более строгий контракт: local receive — pending, final accepted/Saved только после соответствующей удалённой защиты. Разрешение владельца держать локальный журнал несколько часов не трактуется как согласие терять подтверждённую работу. Read-only search не платит cloud-write latency.
7. **Kaggle ограничения.** Фиксируются по текущим account/API probes перед внедрением. Документация API и CLI проверена; session limits, quotas, version readiness, cross-host upload и фактическое восстановление пока NOT RUN. Монтирование Dataset не объявляется автоматически обновляющимся.
8. **Retry horizon.** Начальная политика completed request receipts — 30 дней при проверяемом server-issued operation key. Expired key не становится новой командой. Same key/different hash — conflict. Произвольные legacy keys и UNKNOWN нельзя удалить только на основании этого срока.
9. **Старая БД.** Не экспортировать non-GEO corpus в новую систему и не удалять старый runtime в документационной задаче. После проверенного cutover сохраняется ограниченная reference/rollback копия на 7 дней; продление только для конкретного незакрытого инцидента. После target writes rollback не перетирает принятые новые изменения старой базой.
10. **GEO reanchor.** Только exact evidence о совпадении нового источника/издания/страницы. До этого хранится минимальный legacy locator/status. Нельзя создавать фиктивный новый book record для удобства FK или массово переимпортировать книги ради исправления связи слоя.

## 4. Что уточнено или сознательно не принято

Конкурентный аудит полезен, когда его предложения проверяются, а не автоматически становятся требованиями.

- Не вводится второй narrative state machine и не переименовывается весь API. Исправляются существующий `Variant.body`, default reference, default read paths и честное отображение dossier без body.
- Не вводится правило «голос = первые предложения»: оно теряет концовку. Голос читает сохранённый рассказ либо явно сохранённую адаптацию с теми же фактами/оговорками.
- Библиография вынесена из рассказа; имя автора допустимо, когда это часть самого события/расхождения. Абсолютный запрет любых имён авторов был бы лишним.
- Не принято предположение о маленьком story corpus и полном exact scan по всем stories. Exact search применяется к ограниченному indexed subset; общий путь должен выдерживать масштаб.
- `halfvec` — возможная измеряемая оптимизация после quality gates, не новая обязательная baseline. FP32 1024 остаётся исходным контрактом.
- Seal каждые 10–15 минут не принят как достаточная защита final accepted. Group commit имеет окно сбора не более 1 s; это не обещание cloud RTT ≤1 s. 15 минут относятся к отдельному control checkpoint, а не к разрешению терять принятые команды.
- PostgreSQL может предупреждать о collation mismatch: формулировка консультанта «ошибки не будет» слишком абсолютна. Риск несовместимых индексов реален; нужен rebuild затронутых объектов, а не только обновление номера версии.
- Не введены произвольные новые gates `cosine ≥0.9999` или точное равенство всех ANN top-K. Проверяются canonical hash и существующие защищённые retrieval-quality thresholds.
- Не предписывается непроверенная настройка Kaggle UI «pin to original environment». Требуется воспроизводимый runtime manifest и проверка совместимости восстановления.
- Не ослаблено restore-before-promotion до одного checksum каждые 4 часа. Один verified backup artifact используется для двух копий, чтобы не повторять дорогую работу без причины.
- HEAD с записанным клиентом SHA не доказывает целостность bytes. Требуется verified readback; provider-native checksum допустим только после проверки его фактической гарантии.
- Не принят запрет учитывать Kaggle как recovery copy из-за широкого account token. Проверен более узкий механизм: контроллер удерживает account credentials и version creation, notebook получает только signed byte-upload ticket. Реальный cross-host тест остаётся обязательным.
- Не создаётся новый benchmark tenant/IAM. Для масштаба достаточно изолированной benchmark DB/fixture с явной provenance.
- Channel gates не исчезают после core cutover. Они остаются частью полного задания; отсутствие их PASS нельзя назвать готовностью Slack/Mira/VK/публичных страниц.

## 5. Проверенные внешние основания и reuse

Root проверил первичные источники, которые консультант сам не открывал:

- [PostgreSQL 17 PREPARE](https://www.postgresql.org/docs/17/sql-prepare.html): выбор generic/custom plan после начальных custom executions требует проверки повторного запроса, не только первого EXPLAIN.
- [PostgreSQL 17 ALTER COLLATION](https://www.postgresql.org/docs/17/sql-altercollation.html): version mismatch требует исправления зависимых объектов; REFRESH VERSION не перестраивает их.
- [Yandex PutObject](https://yandex.cloud/en/docs/storage/s3/api-ref/object/upload): опубликованы conditional If-Match/If-None-Match. Рабочий credential/policy/CAS-race test всё ещё NOT RUN.
- [Kaggle CLI, pinned source](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/src/kaggle/api/kaggle_api_extended.py): `_upload_blob` получает `create_url` и token через `start_blob_upload`; byte upload и authenticated `dataset_create_version` являются разными стадиями. Это основание проектировать scoped upload, а не доказательство уже выполненного notebook upload.
- [pgvector](https://github.com/pgvector/pgvector), [PG FTS indexes](https://www.postgresql.org/docs/17/textsearch-indexes.html), [PostGIS spatial indexes](https://postgis.net/documentation/faq/spatial-indexes/), [PG basebackup](https://www.postgresql.org/docs/17/app-pgbasebackup.html), [autovacuum](https://www.postgresql.org/docs/17/routine-vacuuming.html): подтверждают выбранные механизмы; реальные query plans, latency и storage требуют собственных измерений.

В [обсуждении с Dot](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792478684820289) уточнены: Saved требует точного результата, а не только intent; окно batch не равно RTT; новый recovery должен получать свежий fence; существующий RKB S3 adapter — transport, а не готовый журнал. Переиспользуются ограниченные patterns из текущего RKB и MDH; отдельная система очередей/реестр/продукт не переносится. Existing requests/dependencies/consumer references остаются основой обработки поздних результатов.

## 6. Документация и пределы результата

После консультаций [владелец сообщил в Slack](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792480189328939) имена `TELEGRAM_RKB1` и `TELEGRAM_RKB2`, по одной session на Kaggle run; вторая пока пустая. Это внесено в handoff/configuration/archive integration: stage 0 проверяет реальные credentials, независимость и archive access; число одновременных runs ограничивается проверенным session pool и Kaggle capacity. Доступность не объявляется проверенной только по сообщению с именами. Runtime сессии в этом изменении не запускаются.

Активные architecture/storage/operations/import/story/case/identity/rights/GEO/channel/testing документы физически переписаны под один проект. Прежние самостоятельные prompts/handoffs сокращены до retired entrypoints с ссылкой на immutable history. Исторические отчёты и testing-journal сохранены как датированное свидетельство; они не подтверждают новую реализацию.

Девять protected requirement IDs и прежние численные thresholds сохранены. Смысловые уточнения владельца внесены через typed `requirements_update`, operation `op_4ad97289c9197039e8b67d7f`, digest `05de7e109e14b98b8648eefc38136263055e82aee09a261b8e09484a0f9c8d71`. ПМИ сохраняет прежние scenario IDs и добавляет проверки новых границ. Общая матрица приёмки синхронизирована с handoff.

Документационное изменение не выполняет миграцию, импорт, удаление старой БД, переключение Kaggle, выдачу прав, публикацию историй или live tests. Runtime blockers снимает кодовый агент по [плану приёмки](../testing/rkb-scalable-kaggle-acceptance-20261019.md), с exact heads, реальными receipts/readback и честным PASS/FAIL/BLOCKED/NOT RUN.

## Приложения: ответы консультанта на промежуточные версии

Далее сохранён текст трёх консультаций. Решения root в разделах 2–4 выше определяют, какие замечания приняты, уточнены или отклонены. Упоминания «сейчас», незакрытых вопросов и несовпадающих разделов относятся к версии, которую видел консультант, а не к окончательному handoff.


### Раунд 1: полный ответ

### Конкурентный critical audit: storytelling, retrieval и bounded storage

*Работаю только по evidence bundle. Измерений нет, книги не читались, provider-квоты не известны. Всё ниже касается механизмов и согласованности документов.*

#### Findings (по убыванию критичности)

**F1. Термин «Story» противоречит решению владельца.** В §8.2 написано: «Story — досье… не обязано содержать готовый рассказ». Владелец считает иначе: story без сохранённого рассказа не является story. Пока досье без body называется Story, попадает в «stories»-выдачу и в rankings, дефект «one synopsis, zero variants» будет воспроизводиться по определению.
→ Варианты: (а) переименовать досье в «материал/эпизод», а Story оставить только для записи с сохранённым narrative body; (б) сохранить сущность, но ввести инвариант: «story» в UI, API и rankings означает ровно `narrative_state ∈ {draft, needs_review, approved}` с непустым body. Рекомендую (б): меньше миграционного шума, и решение однозначно.

**F2. §15 и §8.2 не учитывают решения владельца от 2026-10-20 и не согласованы между собой.**
- Строка «какие stories/expert contributions/drafts входят в перенос» уже снята: переносится только GEO.
- Строка «кому разрешены scan/crop pixels» тоже снята: illustrations публичны, quote/crop доступны owner/demo/experts/local historians, полный PDF не публичен по умолчанию.
- §8.2 п.1 требует «сохранить existing IDs/revisions и API compatibility». Для non-geo контента это больше не нужно: assertions, stories и chunks импортируются заново. Схему assertions/stories можно спроектировать чисто, без shim'ов. Стабильность IDs сохраняется только для geo entities. Исключение одно: внешние потребители story-API, если они есть (это вопрос Q1).
- Исторические слои вроде «17 convenience records» и «заказаны две консультации» по требованию владельца не должны попадать в self-contained .md. Оставить нужно только механизм дефекта.

**F3. Висящий provenance у переносимых GEO-слоёв.** Georeferencing и provenance почти наверняка ссылаются на старые source revisions, chunks или receipts, а их не переносят. После fresh import эти ссылки повиснут.
- Нужно описание provenance без FK на старые chunks: библиографическая единица + locator + исходная цитата/хэш.
- После импорта нужен rebind к новым source revisions.
- Если rebind не найден, статус должен быть явным `legacy_unresolved`. Тихий null недопустим.

**F4. Readback и export не показывают body до publish_ready.** `story_export` отдаёт body только для publish_ready, `story_get` в compact-режиме отбрасывает variants, search режет `summary[:350]`, а `spoken_summary` формируется как attribution + `summary[:200]`. Даже правильно сохранённый рассказ владелец не увидит. Это нужно исправить вместе с моделью хранения, иначе fix хранения невидим.

**F5. Возможная утечка и медленный путь в `local_rankings`.** Функция на каждый запрос читает все документы, строит allowed-revision map и шлёт JSON в Supabase. Это O(corpus) на запрос плюс вывоз набора документов и разрешений во внешний сервис.
- Вопрос root (Q2): остаётся ли Supabase в целевой архитектуре при Kaggle PG? Если нет, удалить. Если да, отправлять только ID результата, без allowed-map.

**F6. Draft body и permissions.** «Body доступен до approval permitted readers», но роли для черновиков не названы. Есть два риска:
- story_search с excerpt из draft отдаст неодобренный текст анонимному пользователю;
- роль `demo` фактически полупублична: если demo-доступ раздаётся, crops становятся публичными.

Фильтр `narrative_state` + role должен работать в SQL до ранжирования, а не после hydrate.

**F7. Retention: hot window, удалённый tail и Yandex baseline.** Владелец хочет, чтобы Dev-журнал жил несколько часов. Предложение говорит «remote GC по min recoverable watermark». Но если старейшая точка восстановления — ежедневная копия Yandex, то удалённый tail обязан покрывать период от её watermark, то есть не меньше суток плюс время верификации. Нужно явно развести два журнала:

| Журнал | Где | Retention |
|---|---|---|
| Hot journal | Dev, локально | часы, по решению владельца |
| Remote tail | независимое хранилище | от min(verified Dataset seq, verified Yandex seq) |

Если tail лежит в том же Yandex, где и baseline, независимость двух точек восстановления ложная (Q5).

**F8. UNKNOWN send и retry после GC.** Если dedup-tombstone живёт меньше горизонта offline-retry клиента или его нет в PG/snapshot, возможен повтор команды. Два сценария:
- retry после локального GC приводит к повторному исполнению;
- restore из снапшота, сделанного до tombstone, тоже приводит к повторному исполнению.

Нужно три вещи:
1. Tombstone `(idempotency_key → payload_hash, result_ref, status)` хранится в PG и потому попадает в snapshot+tail. В одном локальном журнале его держать нельзя.
2. Ключ старше горизонта получает ответ `EXPIRED_QUERY_STATE`, а не исполнение.
3. Тот же ключ с другим hash получает `CONFLICT`.

**F9. Однослойный ACK скрывает RPO.** Пока cloud seal не завершён, отказ диска Dev теряет принятые команды. Предлагаю два уровня ACK: `accepted` (Dev) и `durable` (remote seal). Клиент хранит payload до `durable`. RESUME после restore сообщает серверный watermark ниже клиентского, и клиент делает replay. Выбор «ждать durable перед ответом пользователю» или «отвечать на accepted» — это tradeoff latency/RPO, и он за владельцем (Q6).

**F10. Shadow rebuild и peak-лимит хранения.** При реиндексе shadow HNSW временно удваивает место под индекс. Подготовка снапшота для Dataset может потребовать staging-копию. Оба события должны укладываться в peak 90%, иначе «бюджетный» реиндекс нарушит protected gate.
- Справочная арифметика, не измерение: 100k × 1024 × 4 B ≈ 410 MB только raw FP32, без HNSW-графа, TOAST и WAL.

#### Edge cases

**Title обещает «как», источник этого не сообщает** (тип «школа вопреки запрету 1337»).
- Title трактуется как утверждение: он привязан к assertion revisions и проходит тот же self-check.
- Если механизм не подтверждён, есть два варианта: (а) переформулировать title без обещания механизма; (б) body явно говорит «как именно — источники не сообщают» и ставит gap.
- Генерировать «как» запрещено. Embedding строится по body, а не по title, чтобы title не тянул нерелевантные запросы.

**Атрибуция и легенды.** Нужно разделить два понятия:
- Эпистемический маркер в тексте: короткий и общий («по преданию», «по одной из версий», «хронист утверждает»). Для легенды и спорной версии он обязателен.
- Библиографическая атрибуция: автор, издание, примечание, страница. Только в отдельном поле или на карточке, никогда в body или голосе.

Имя автора допустимо в тексте только тогда, когда без него непонятно само расхождение. Фраза «в примечании 17 автор говорит…» запрещена в начале. `spoken` = первые полные предложения body, без библиографии.

**Недостаток контекста.** Нужны явные исходы, а не пустая story:

| Состояние | Что хранится |
|---|---|
| `fact_only` | факты/observations, эпизода нет |
| `needs_context` | конкретный недостающий элемент + continuation pointer (range/locator) |
| `outcome_not_in_source` | рассказ есть, исход источником не сообщён |
| `draft` | связный рассказ с исходом или возвратом к вопросу |

Владелец ждёт «known outcome/return». Отсюда вопрос Q3: допустим ли `outcome_not_in_source` как story? Рекомендую допустить с явной финальной фразой, но не включать в ready-rankings.

**Длина.** «Часто 2–6 предложений» — мягкий ориентир. В §8.4 сейчас только «длина определяется материалом». Нужно добавить ориентир без hard cap. Однострочный пример с жюри в §8.5 допустим лишь как исключение.

**Narrow-filter ANN.** Если взять HNSW top-K и отфильтровать после (книга, роль, state, withdrawal), recall теряется и может вернуться меньше K результатов. Порядок:
1. Для узкого scope (одна книга или ревизия): B-tree по `document_revision` и точный расчёт расстояния по подмножеству.
2. Для широкого scope: HNSW с bounded iterative scan, если его поддерживает фактическая версия pgvector (проверить, не предполагать).
3. Story-корпус мал, поэтому точный scan, пока замеры не покажут иное. Partial HNSW per state не создавать: это дублирование индекса.

**Обновление.** Новые chunks индексируются инкрементально. Новая ревизия body перевычисляет embedding только этой story (cache по model + input hash). Полный re-embed нужен только при смене модели: shadow-колонка, затем swap. Withdrawal сразу фильтруется при hydrate, а dead tuples убирает autovacuum.

**Kaggle.**
- RAM теряется при завершении сессии, поэтому prewarm модели и tokenizer повторяется на каждом старте. Cold-start нужно мерить отдельно от warm p95.
- Writer fencing при истечении сессии обязателен.
- Лимиты сессии, размера и числа версий Dataset, а также возможность удалять старые версии — только из фактической документации или аккаунта (Q7). Не вписывать догадки.

**Illustration внутри page crop.** Класс артефакта назначается при создании и не выводится из содержимого. Публичная иллюстрация, вырезанная из скана, — отдельный ресурс. Crop вокруг цитаты остаётся restricted. Публичный CDN-URL не выдаётся для crop.

#### Recommendation (минимальное связное решение)

**Story-модель:**
- Assertion независима от Story (fresh schema, без compat).
- Story хранит `default_variant_ref` на существующий Variant с blocks, привязанными к `assertion@revision`.

**Поля readback** до publish_ready, которые отдаёт `story_get` по умолчанию, без LLM:

| Поле | Назначение / default |
|---|---|
| `id`, `revision` | идентичность |
| `title` | проверяемое утверждение |
| `narrative_body` | сохранённый рассказ |
| `narrative_state` | default = `draft` |
| `outcome_status` | known / not_in_source |
| `epistemic_status` | source / legend / memoir / disputed |
| `attribution_line` | краткая, отдельно от body |
| `bibliography[]` | работа, автор, издание, locator |
| `evidence[]` | `assertion@rev`; quote и crop_ref под role-gate |
| `gaps[]`, `case_refs[]` | пробелы и расхождения |
| `composed_by`, `approval` | кто собрал, состояние одобрения |
| `variant_ref` | ссылка на default variant |

- `summary` — одна строка карточки, выводится из той же ревизии body, иначе разъедется с ней.
- Search возвращает excerpt с меткой, обрезанный по границе предложения, и `body_ref`.
- Potential rating исключается из ranking ready-stories.

**Retrieval:** принять speed-предложение целиком, с тремя уточнениями.
1. Permission и state фильтруются в SQL до top-K.
2. Story ANN — точный scan до замеров.
3. Опционально, как tradeoff хранения: halfvec-индекс поверх FP32 raw (≈ ½ размера индекса). Включать только после повторного прохода Hit@5/10 и RU→DE gate.

**Удалить:**
- Python full-scan в `local_rankings`, catalog и `story_registry`;
- allowed-map JSON в Supabase;
- compat-требования для non-geo IDs;
- open choices «перенос stories/drafts» и «pixel audience»;
- исторические слои §8.1 и §15 в coding-agent .md;
- «Story = досье без рассказа».

**Storage:**
- Hot journal: N часов.
- Seal: каждые 10–15 мин или по порогу байт.
- Remote tail: до min-watermark всех считающихся recovery points, включая Yandex.
- Tombstones в PG с явным горизонтом.
- Два уровня ACK.
- Fail-closed, когда durable-хранилище недоступно и превышен лимит возраста или байт.
- Shadow rebuild и staging учитываются в peak 90%.

#### Вопросы, на которые root обязан ответить

1. Есть ли внешние потребители текущего story/MCP API? Если нет, compat для non-geo снимается.
2. Остаётся ли Supabase в целевом контуре и с какими данными?
3. Считается ли «story» рассказ без известного исхода? Входит ли он в ready-rankings?
4. Какие роли читают draft body? Что такое `demo`: отдельный закрытый аккаунт или раздаваемый доступ? Как выдаётся роль local historian?
5. В каком хранилище лежит remote tail, и независимо ли оно от Yandex baseline?
6. Подтверждает ли пользователь ответ на accepted-ACK (RPO > 0 при отказе диска Dev) или ждёт durable-ACK (больше latency)? Нужно решение владельца.
7. Каковы фактические ограничения Kaggle: сессия, размер и версии Dataset, удаление старых версий? Нужны документы или наблюдения, не оценки.
8. Каков горизонт offline-retry клиента, задающий retention tombstones?
9. Что делается со старой БД после переноса GEO: frozen read-only архив на срок X или удаление? «Не переносить» не значит «уничтожить», и это необратимый шаг.
10. Какая политика rebind для GEO provenance, не нашедшего новую source revision?

#### Confidence

- **Высокая:** противоречия документов (F1, F2), readback-дефект (F4), GC/tombstone/watermark-логика (F7, F8), фильтрация до top-K. Все они следуют непосредственно из evidence.
- **Средняя:** утечка в Supabase (F5) — видна только форма механизма, не содержимое payload; полезность halfvec; достаточность точного scan для stories (без замеров).
- **Не оценивал:** фактическую latency, лимиты Kaggle и Yandex, историческую достоверность примеров владельца.


### Раунд 2: полный ответ

### Консультация: повторный аудит после round1 (architecture)

#### Findings

Ниже пять правок, которые я считаю обязательными до coding handoff. Остальное либо уже корректно описано, либо относится к edge cases.

##### 1. CRITICAL — §4.1 / §6.5: физическая база не привязана к collation и окружению

**Проблема.** `pg_basebackup` переносит файлы B-tree как есть. Kaggle периодически обновляет docker image и вместе с ним glibc. Если новая сессия поднимет базу на другой версии glibc, порядок сортировки текста изменится. Индексы по `(title_sort, source_id)`, normalized alias и text keys тогда тихо испортятся: keyset cursor начнёт пропускать или дублировать строки, exact alias будет промахиваться. Ошибки при этом не будет.

Manifest в §6.5 сейчас перечисляет PG/extension/model/tokenizer, но не collation provider/version, Kaggle image digest и точные `.so` версии pgvector/PostGIS.

**Минимальное исправление.**
- Для всех keyed text columns использовать `COLLATE "C"`. Normalized alias и `title_sort` и так нормализованы в приложении.
- Человеческую сортировку, если она нужна, делать через pinned ICU.
- Добавить в manifest:
  - `datcollversion` / `pg_database_collation_actual_version()`;
  - ICU version;
  - PG minor;
  - точные версии pgvector/PostGIS;
  - image digest.
- В notebook включить «pin to original environment».
- При расхождении: BLOCKED или `REINDEX` text-индексов до READY.

**Различающий тест.** Восстановить base в образе с другой glibc. Readiness обязана отказать, либо `amcheck` (`bt_index_check`) после reindex проходит и keyset-пагинация каталога совпадает с эталоном.

##### 2. CRITICAL — §6.1 / §6.2: replay несовместим с недетерминированными derived outputs

**Проблема.** Порядок в §6.1 такой: exact command → apply → outcome hash → saved. Однако import и story-commands порождают BGE vectors. На CPU результат с другим числом threads, другой версией Torch или другим батчингом не совпадает побитно.

Если vectors или projections попадают в outcome hash, replay при restore получит hash mismatch. По правилу «неизвестную ошибку не пропускать» recovery остановится навсегда. Есть и второе следствие: restore+replay budget на деле определяется re-encode импортов, а не применением SQL.

**Минимальное исправление.**
- Разделить canonical domain outcome (IDs, revisions, body, assertions, evidence, cursor) и derived projections (vectors, tsvector, crops).
- В outcome hash входит только canonical outcome.
- Projections помечаются dirty и пересобираются фронтиром после replay.
- Readiness до догона dense-проекции возвращает degraded, но не блокируется.
- Оценку replay budget считать с учётом стоимости re-embed.

**Различающий тест.** Восстановить base без последнего import, сделать replay import с `torch threads=2` вместо 4. Outcome hash должен совпасть, cosine старых и новых vectors ≥ 0.9999, а search после догона фронтира должен вернуть тот же top-K.

##### 3. HIGH — §6.1 / §6.4: «UNKNOWN не истекает» + bounded admission = вечная блокировка

**Проблема.** У Telegram/Slack send нет надёжного native readback по operation key. UNKNOWN там может стать постоянным. Если такие состояния съедают глобальный admission budget, со временем DevCoveer перестанет принимать любые mutations, включая импорт. Это liveness-crash, растянутый во времени.

**Минимальное исправление.**
- Admission budget для UNKNOWN вести per provider/channel, а не глобально.
- Добавить явную команду разрешения через tail, по решению владельца или оператора:
  - `resolved_delivered`;
  - `resolved_not_delivered`;
  - `abandoned_no_resend`.
- Для провайдеров без readback зафиксировать политику at-most-once: auto-resend не делать.
- Слово «UNKNOWN не истекает» оставить, но рядом прописать, кто и как его закрывает.

**Различающий тест.** Инжектировать 50 UNKNOWN в Telegram. Import и story-save должны по-прежнему получать accepted/saved. После resolve-команды Telegram admission должен разблокироваться без resend.

##### 4. HIGH — §4.2 / §4.3: ANN-план ломается на prepared statements и mutable `active`

**Проблема 1 — generic plan.** После пяти выполнений prepared statement PostgreSQL может переключиться на generic plan. С параметризованными фильтрами он часто теряет HNSW или iterative scan. Поэтому EXPLAIN «при реальных bind values» на первом вызове ничего не доказывает.

**Проблема 2 — partial HNSW.** Если HNSW или partial index зависит от изменяемого флага `active`/visibility, активация revision превращается в массовые non-HOT updates с HNSW-вставками. Это противоречит «атомарной активации». Withdrawal без GC копит tombstones, и recall деградирует.

**Минимальное исправление.**
- Для ANN-запросов: `plan_cache_mode=force_custom_plan` или отказ от server-side prepare.
- `SET LOCAL` для `hnsw.ef_search`, `hnsw.iterative_scan` и `hnsw.max_scan_tuples` внутри транзакции. С pooler это обязательно.
- При `relaxed_order` делать пересортировку во внешнем запросе.
- Строки projection неизменяемы на revision. Активация — это смена маленького `active_revision` набора, к которому фильтр делает join по ограниченным кандидатам.
- Superseded/withdrawn строки удаляет bounded GC.
- `REINDEX CONCURRENTLY` HNSW запускать по измеренной доле dead tuples, а не по календарю. Это согласуется с §4.4.

**Различающий тест.**
- EXPLAIN (ANALYZE, BUFFERS) на 6-м и последующих выполнениях того же prepared statement должен показывать Index Scan по HNSW.
- Активация revision на 100k passages должна укладываться в секунды и не давать роста HNSW.
- Recall@50 после 30% superseded строк не должен падать ниже gate.

##### 5. HIGH — §6.1 / §6.3 / §6.5: fence на хранилищах и CAS manifest не доказаны

**Проблема.**
- CAS manifest в Yandex Object Storage требует conditional writes (If-Match/If-None-Match). Поддержка не проверена.
- Путь daily Yandex base не определён: многогигабайтная база не пройдёт через spool в 512 MiB.
- Если в notebook положить долгоживущий Yandex-ключ и Kaggle API token, старый writer после partition сможет писать или удалять объекты. `delete-old-versions` удалит все версии Dataset, включая pinned.

**Минимальное исправление.**
- Ключи сегментов и баз делать immutable, с префиксом epoch/seq: `.../e{epoch}/seq{from}-{to}`.
- Manifest продвигает только DevCoveer authority. Если conditional PUT не подтверждён, ставить BLOCKED, а не last-write-wins.
- Kaggle writer получает от DevCoveer per-object presigned PUT URL, привязанные к текущей generation и без права delete. Base загружается напрямую Kaggle → Yandex.
- Verify отвергает base, у которой `writer_generation` не совпадает с authority.
- `delete-old-versions` запретить в коде. Recovery-критичную копию считать живущей в Yandex: Kaggle token account-wide, и Kaggle-копия — только fast path.

**Различающий тест.** Partition: старый writer после выдачи нового epoch загружает base и Dataset version. Pointer не должен измениться, restore должен взять новую base, а попытка DELETE в Yandex должна получить 403.

##### Избыточная бюрократия (сократить без ослабления gates)

- **Одна base — две копии.** Делать один артефакт `pg_basebackup` + backup_manifest и проверять его один раз. Yandex daily — это копия того же verified артефакта, а не второй цикл restore.
- **Дешёвая проверка каждые 4 часа.** Для 4-часовых баз использовать `pg_verifybackup` + hash. Полный isolated restore + SQL/dense probe выполнять daily и перед handover. Для fast path рекомендую так, но решение за владельцем gate.
- **Readback без полного GET.** Вместо скачивания объекта после PUT проверять HEAD + провайдерский checksum (`x-amz-checksum-sha256`), если Yandex его поддерживает.
- **Укрупнение сегментов.** Group commit ≤1 s даёт до 86k мелких объектов в сутки. Нужна compaction sealed segments в часовые объекты, иначе GC и listing станут узким местом.

#### Edge cases

- **tsvector и языки.** Корпус многоязычный (RU/DE/LAT). Нужен stored tsvector с per-row regconfig, а запрос — OR по конфигурациям языков. Иначе lexical branch молча пустеет для немецких книг.
- **Read-after-write для историй.** Сразу после save рассказ находится лексически, но ещё не dense. Ответ должен показывать `projection_lag`, а не «не найдено».
- **Публичная иллюстрация на странице со сканом.** Crop должен быть отдельным derived artifact с bbox без соседнего текста. Публичный crop не должен давать доступ к proof-изображению страницы.
- **Смонтированный `/kaggle/input`.** Restore всегда берёт явную version через API download в working dir. Пик диска считать как archive + extracted + live PG.
- **UNKNOWN-концовка истории.** Prompt и §8.4 корректны. В golden-набор нужно добавить кейс, где честная граница знания — правильный ответ, а «needs_context» был бы ложным отказом.

#### Recommendation

Перед handover внести правки 1–5 как короткие дополнения к §4.2, §6.1, §6.4 и §6.5. Новые компоненты не нужны.

На уровне проекта я сам отвечаю на открытые вопросы так:
- **Collation:** C для ключей.
- **Хэш outcome:** только canonical domain, derived исключены.
- **UNKNOWN:** закрывается resolve-командой оператора.
- **Транспорт base:** presigned PUT из Kaggle напрямую в Yandex.
- **CAS:** проверить conditional writes Yandex как readiness task; при неуспехе — BLOCKED.

Story/body/prompt разделы (§8) считаю готовыми, правок, блокирующих handover, не вижу.

#### Confidence

- Пункты 1, 2 и 4 — высокая: это известное поведение PG/glibc/pgvector и CPU float.
- Пункт 3 — высокая.
- Пункт 5 — средняя: детали conditional writes и checksum у Yandex Object Storage нужно проверить на реальном аккаунте, я их не верифицировал.


### Раунд 3: полный ответ

### Third bounded audit: functional, rights, GEO, cases, roles, channels, acceptance

#### Findings

The five round-2 answers are closed adequately for handoff. Physical backup compatibility, UNKNOWN resolution, the ANN plan, conditional PUT through a narrow adapter, presigned orphans and NOT RUN status are all stated without false guarantees. I do not re-raise them. I also do not ask to bring back a per-image legal check or a quorum. That would contradict the owner's decisions and add bureaucracy.

All owner decisions are reflected consistently: ONLY GEO transfer, illustrations for everyone, proof for owner/demo/expert/local_historian, Story read vs private draft, stored body, and facts without a Story. I found no lost functions. §3 maps to §9–§12 and to the risk matrix.

I found five gaps that are worth fixing. Two are real contradictions; three are places that create a risk of false PASS or an impractical block.

| # | Severity | Section | Problem | Short fix |
|---|---|---|---|---|
| 1 | **High** | §12.2 p.2/p.4 ↔ §13 stages 2 and 4 | §12.2 says "first stable target layer/place IDs, then fresh books A+B". §13 puts the fresh import (stage 2) before the layers (stage 4). §12.2 p.4 also implies books are imported in parallel with the transfer. A geo-resolver on an empty target would either create duplicate places or lose the links. | Split the GEO work in two. **Identity manifest** (layer/place IDs, aliases, CRS) goes in stage 1–2, before any production book import. Bulk raster/geometry bytes stay in stage 4. Book references to not-yet-loaded layers stay `unresolved` in the geo queue and are resolved by the geo queue, never by reimport. Stage 2 before the manifest exists runs only on an isolated fixture DB. |
| 2 | **High** | §13 stage 7, §14.1, §15, §16 ("applicable gates") | It is undefined which gates block the **GEO/runtime cutover** and which block only the release of a **channel**. The agent could either hang the cutover on the physical handset, 90-minute voice session and VK test, or call the cutover complete with those channels unchecked. | Add one column to the §14 table: `cutover-blocking` vs `feature-release`. Cutover-blocking: DUR/FEN/WSS/REC, MIG, IMP, SRC/IDX, the PRF rights checks, ID-1, and one real non-owner personal MCP. Feature-release: CHN-2, VK, PUB, NEED, Mira provider. Feature-release gates may stay NOT RUN/BLOCKED after cutover, but the matching channel is not enabled for users and is not reported as working. |
| 3 | **Medium** | §14.1 "Scale", "Dense quality", IDX-1 | With ONLY GEO, the target starts with roughly two books. The 100 sources / 100k chunks gate has no defined corpus. The agent would either mass-reimport through the full story pipeline (expensive and slow) or quietly run the benchmark on old chunks with different chunking. | Use an explicit **isolated benchmark corpus**: real originals from the source archive, run only through the new chunk/BGE/lexical path (no story/case extraction). It gets a separate `benchmark` tenant, is never activated in production, and is excluded from public search. Old chunks/vectors are not allowed in the gate. Report the corpus manifest. |
| 4 | **Medium** | §11 table, §9.1, §10.1, PRF-4 | §11 says proof requires "category **and** current `scan_proof_read`". §9.1/§10.1 say specialization does not grant permissions. It is unclear whether category issues the right automatically or a separate grant is needed. Two wrong implementations are possible: an implicit category→pixels path, or a missing grant for demo/expert contrary to the owner's decision. | When a category becomes `owner_attested`/`independently_verified`, the system **materializes** a default `scan_proof_read` grant with an audit record. The grant is revocable separately; for demo it gets the demo term as expiry. The check at render time looks only at the grant, never at the category. A `claimed` category issues nothing. For combinations (student + expert), the verified expert category is enough. |
| 5 | **Medium** | §14.1 "Story", §16 blind comparison | "Blind source comparison" has no baseline and no judge. With no quotas, it is the easiest place to get a false PASS, for example the model judging its own output. | Baseline: the retired prompt from immutable history, run on the same fixture pages. The judge is the owner (or another designated human): a blind preference plus a list of lost facts/endings. A model judge may only pre-sort. Without a human verdict the gate is `NOT RUN`, not PASS. The mechanical invariants STO-1…8 are mandatory regardless. |

#### Edge cases

These do not need new design, only a line in implementation notes.

- **UNKNOWN in one's own dialog (§10.2, REC-6).** UNKNOWN has no TTL, so a person's dialog can stay blocked until an operator acts. Allow the user to continue explicitly in a new branch of the same dialog, with a marked context gap and no resend of the unknown result. Operator resolution stays as it is.
- **Astro (§9.4).** The site lives in someone else's repo/deploy (`events-bot-new/site`). Contract: RKB exports a versioned bundle and the existing site build picks it up. Readback is an HTTP GET of the published URL with a revision marker in the HTML/JSON. If there is no CDN API, "cache invalidation with verification" means a readback of the actual URL, not a promise to purge. If the deploy path is unavailable, the result is a single BLOCKED per §15.
- **Rollback (§12.2 p.5–6).** Once fresh books exist, the old schema cannot hold them, so in practice rollback means forward-fix plus a reference copy. The text says this honestly. Do not let the runbook call it "automatic rollback".
- **Ordering inside §14.2.** REC-7 sits after REC-10. This is cosmetic but can confuse traceability.

#### Recommendation

Apply fixes 1–5 as point edits to §12/§13/§14/§11. They need no new services and no new design round. After that, **the document can go to the coding agent without another design round**.

Runtime is unchanged, so every gate is NOT RUN. The agent's first deliverables are stage 0 and the GEO identity manifest.

#### Confidence

- **High** for #1 and #2: the contradictions are visible directly in the text.
- **Medium-high** for #3 and #4.
- **Medium** for #5: the original intent may already have been a human judge, but the text does not say so.

I cannot verify external facts (the Yandex If-Match behavior, Kaggle upload tickets, the VibePublish allowlist) from this bundle. I take them as stated, under the NOT RUN status.
