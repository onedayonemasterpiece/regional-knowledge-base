# Story Registry: факты, досье и сохранённые рассказы

**Целевой контракт от 20.10.2026; документация, не акт развёртывания.** Нормативный план и полная продуктовая матрица: [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md). Размещение и восстановление: [архитектура](designs/rkb-scalable-kaggle-architecture-20261019.md).

## Проверенная исходная точка и граница переноса

В [main a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/a8cf41b50b8ccb62409d046d009d53c09d74b1d7) есть Story/Assertion/Evidence, редакционные Variant и версии, оценки, межкнижное сопоставление, календарь и числовые наблюдения. Однако импорт создаёт title/summary и assertion без Variant, compact get отбрасывает варианты, search строит spoken_summary из атрибуции и summary, а export требует одобренного варианта. Это не готовый контракт рассказов, даже если карточка найдена и имеет высокий рейтинг. Код: [contracts](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_contracts.py), [ingestion](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_ingestion.py), [registry](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/src/regional_knowledge/story_registry.py).

PR #151 на `adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4` — отдельная незавершённая ветка, материал для повторного использования после ревью и тестов, не доказательство live PASS. Исторические schema v7–v13 и отчёты приёмки описывают свои SHA, а не целевой deployment.

В новый corpus переносятся **только исторические/картографические GEO-слои** по геоконтракту. Старые книги, Stories, ручные и личные черновики, вклады, дела и их очереди не мигрируют. Новые книги принимаются заново через MCP. Сохранность и история ниже относятся к данным, принятым в новом контуре; перенос прежних редакционных записей не является условием запуска.

## Один владелец данных и существующая поверхность

Каноническое взаимосвязанное состояние живёт в одном PostgreSQL/pgvector corpus на Kaggle. Один fenced writer фиксирует версии, evidence, права, audit, receipt и outbox атомарно. DevCoveer сохраняет небольшой durable control journal принятых вопросов/команд/результатов/ACK; исходящий WSS notebook → existing ASGI использует ту же admission authority/controller/BgeQueue. Durable question/effect подтверждается только после предусмотренных handoff exact remote intake/outcome; остановка notebook не теряет подтверждённый вклад. Обычный read_query берёт RAM permit, читает подходящий snapshot и возвращает status/freshness без durable row, fsync/seal, RESUME/resultSaved. Это не второй канонический реестр Story или отдельная read queue.

BGE — единственное embedding-пространство: 1024 FP32 baseline, конечный вход проверяет pinned BGE tokenizer, максимум 512 токенов. E5 и Supabase не входят в целевую архитектуру. Истории, дела и книги используют существующий индексатор, outbox и общую очередь; не добавляются LLM внутри MCP, локальный encoder, новый scheduler или постоянный судья качества. Производный индекс восстанавливается из текста, точной версии и material hash; он не владелец содержания.

Модель исследует и составляет текст через существующие book_ingest, story_*, graph/evidence и disagreement интерфейсы. MCP проверяет права, ссылки, типы, версии и запись. Dot→tools и Mira→tools — независимые smart routes. Mira — клиент Projects Hub; existing typed adapter v2 расширяется Stories/cases/proof, явными status/freshness/min-cursor/detail continuation и отдельно durable pending/request/result/native ACK для принятого вопроса/эффекта. PH provider обращается к existing Dev auth/admission и outbound notebook WSS без дополнительного Dot turn. Голосовой клиент читает сохранённый текст, не создаёт историю при каждом чтении. RKB не строит второго помощника и сам не отправляет публикации в TG/VK/MAX.

## Смысловые сущности и независимые состояния

| Сущность | Назначение и граница |
|---|---|
| Источниковый факт / assertion | Утверждение с exact source revision, автором/говорящим, модальностью и evidence. Существует **без Story**; дата, измерение и краткая запись не требуют искусственного сюжета. |
| Эпизод / связное объяснение | Связанный ход событий, практика или объяснимое явление. Не обязательны драма, герой, конфликт или известная развязка. |
| Story / досье | Стабильная редакционная рамка: assertions, источники, gaps, angle/audience value, связи, оценки и варианты. Неполное досье без текста не называется готовым рассказом. |
| VariantRevision | Сохранённый текст для аудитории/формата/канала. **Variant.body — единственная истина текста**; редакции и source-bound blocks сохраняются. |
| Дело разногласия | Самостоятельный объект с позициями, вкладами и заключениями, возможный без Story. Спор не равен запрету чтения или готовности к публикации. |
| Личный черновик | Частный рабочий текст автора. Явное внесение выбранной версии в общее дело/Story — отдельное действие. |
| Публикация | Наблюдаемый внешний выпуск одобренной редакции с provenance/receipt. Чтение рассказа не является публикацией. |

Story → StoryRevision → VariantRevision и Assertion → AssertionRevision → EvidenceLink/Assessment сохраняют смысл. Легенда, воспоминание, слух, гипотеза, историческое утверждение и художественная реконструкция различаются. «Автор сообщает X» и «X произошло» — разные assertions. Exact text match подтверждает присутствие цитаты, не её историческую истинность. Внешние свидетельства регистрируются как immutable source versions.

Contested, доступность body, редакционная готовность и результат публикации — разные свойства существующих объектов. Не вводить второй параллельный автомат состояний. Interest/rating помогает выбрать материал, но не доказывает наличие body, связность, истинность или publish_ready.

## Обязательный базовый рассказ при импорте

При первичном чтении модель сохраняет факты, людей, места, даты/календарь, наблюдения, позиции и достаточные эпизоды в **одном persisted research workflow**. Поиск и сравнение идут порциями с явным остатком; отдельное повторное чтение всей книги ради Stories или дел не требуется.

Если принятые материалы достаточны для связного эпизода, импорт **автоматически сохраняет базовый читаемый рассказ в existing Variant**, связывает его с exact assertions/evidence и выполняет readback. Обычная длина — 2–6 предложений: ориентир, не жёсткий лимит, минимум слов или квота на книгу. Нельзя закончить такой эпизод только title/summary, предложением «здесь можно рассказать» или оценкой потенциала. Повседневной практике и связному объяснению не нужен выдуманный драматический поворот.

Если есть лишь разрозненные факты или существенный разрыв, они остаются фактами/кандидатом с none либо needs_context и конкретным gap. Композиция не добавляет внешнее знание, причинность, мотив, сцену, героя, точную дату или исход без основания. Неизвестный исход допустим, если связная часть понятна и предел знания обозначен. Обязательный рассказ из достаточного материала не означает обязательного рассказа из любого фрагмента.

Базовый импортный вариант наследует **read scope Story**, даже если редакционно drafting. Он доступен читателю без publish_ready. Личные авторские черновики не становятся общими, default или search-excerpt без явного разрешённого действия.

### Сохранение и ссылка по умолчанию

- `default_variant_ref` указывает на **конкретные variant ID и revision**, не на «последний подходящий текст». Story не дублирует body в metadata/summary/отдельном ledger.
- Сохраняются язык, формат, audience/channel, body, existing blocks с assertion revisions, attributions и media. Недостающая привязка не заменяется вымышленным evidence.
- Число блоков и предложений не обязано совпадать. Существенные утверждения и выводы имеют основания; bibliography находится отдельно от рассказа. Существенный спорный рассказчик/автор версии называется в body там, где без этого изменится смысл.
- Existing idempotency key, expected Story/Variant revision и atomic receipt защищают создание/обновление. Retry или новый чат не создают новый Variant/Story и не меняют default. Заголовок/summary не служат ключом identity эпизода.
- После записи модель читает exact revision и сверяет body/hash, default ref, evidence и состояние. Pending/error не называется сохранённым рассказом по одному тексту в контексте модели.
- Поздняя композиция не перезаписывает ручную правку default: CAS возвращает конфликт и текущее состояние. Редактор выбирает дальнейшее действие.

### Композиционная инструкция

> По принятому evidence составь один связный рассказ, который уже можно прочитать или произнести. Начни с предмета или события, а не с «В примечании 17…», библиографического вступления или оценки занимательности. Дай понятную исходную ситуацию, последовательно свяжи относящиеся к ней действия/наблюдения и закончи известным результатом или пределом знания. Хронологическая последовательность не доказывает причинность. «Поэтому», «из-за» и мотив допустимы только с основанием; иначе изложи последовательность без вымышленной причины. Заголовок «Как…» должен получать ответ в body, иначе измени заголовок. Не добавляй героя, конфликт или финал ради формы. Обычно достаточно 2–6 предложений; сохрани существенные оговорки, даже если нужно больше. Библиографию передай отдельно; оставь в body имя рассказчика, если оно необходимо для понимания версии. Сохрани existing Variant, назначь exact default ref и прочитай сохранённый результат. При недостатке материала сохрани факты и конкретный gap, без фиктивного body.

Все канонические даты, имена, география, роли и measurements сохраняются независимо от короткого рассказа. Он выбирает релевантное, а не повторяет каждое поле досье. Полный рабочий prompt, синтетические отрицательные примеры и приёмка — в [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## Чтение, поиск, голос и export

| Операция | Целевой ответ |
|---|---|
| Обычный story_get | Реальный default Variant.body, exact ref/revision и состояние применимости. Title, summary, rating и bibliography отдельно. Длинный body имеет явное продолжение, без скрытого усечения. |
| story_search | Реальный excerpt из доступного saved body, явно помеченный excerpt, с exact body ref; короткий body допустим целиком. Metadata/interest/case warnings отдельно. Без body — none/needs_context, **без summary fallback**. |
| Голос / Mira / «расскажи» | Читает ту же сохранённую редакцию или получает её по ref. Не озвучивает рейтинг, описание потенциала и библиографическую преамбулу как рассказ. |
| Read-export | Доступные читателю body/blocks/provenance exact revision, включая editorial draft; без требования publisher. Не даёт права опубликовать. |
| Approved publication export | Exact publish_ready revision, текущие dependencies/fingerprint и право publisher. Внешний провайдер отвечает за отправку. |

Existing story_get view/cursor и story_export расширяются совместимо, без второго read API. Compact не должен выкидывать запрошенный рассказ. Spoken_summary не заменяет body. Чтение по умолчанию не вызывает LLM и не меняет текст.

Нет доступного default — вернуть none/needs_context либо действующее dependency revalidation с причиной. Нельзя скрыто подставить личный черновик, предыдущую невыбранную редакцию, summary или новый сгенерированный текст. Разрешённая историческая редакция явно помечается и не выдаётся текущей. Withdrawal/права проверяются для excerpt, кеша, export и отложенного ответа.

## Поиск и права

Текст и библиография **всех active accepted books доступны без per-book owner filter**. Все принятые иллюстрации публичны. Full PDF/DjVu остаётся частным. Страница/полоса proof выдаётся через existing `scan_proof_read` владельцу и категориям `demo`, `expert`, `local_historian` с проверкой действующего grant/expiry. Категория и вес не заменяют серверной capability. Личные drafts, непубличные сведения участников и ограниченные внешние материалы сохраняют права.

**Назначение proof по политике владельца:** категория demo/expert/local_historian, подтверждённая как owner_attested или independently_verified, атомарно материализует **существующий scan_proof_read grant** вместе с audit и source category revision; для demo фиксируется expiry. Claimed категория прав не создаёт. Render/read проверяет текущую capability и scope/expiry, без второго category bypass. Независимый revoke действует немедленно и не отменяется чтением или повтором прежнего события. Удаление категории отзывает только policy-derived grant/его основание; отдельные explicit grants не переписываются и не выдаются за категорийные. Повторное назначение после revoke требует нового разрешённого owner-policy действия. Это назначение существующего права, без нового IAM, pixel registry или ручного одобрения каждого изображения.

Story read scope, active/source withdrawal, exact revision и адресат проверяются до ранжирования и раскрытия. Нельзя снова вводить owner ACL на accepted book text под видом dependency check. Scoped policy delta проверяется по всем source/variant/grant dependencies item, включая cross-book narrative/summary и bibliography. При пересечении исключается весь affected item с его display/continuation, не только ссылка; оставшаяся выдача — degraded/evidence_withdrawn даже при нуле items, не empty ok. Неполный provenance, compacted delta history или недоказанный scope требуют fail closed для затронутого scope/result; свежий cursor не заменяет права. Незатронутые чтения продолжаются. Continuation/context refs не capability. Группа не наследует proof-права инициатора. Remote-durable barrier закрывает новую RKB-выдачу отдельно от ещё pending PG withdrawal/public purge; Saved требует соответствующих commit/readback по [handoff §§6, 10.1](prompts/rkb-scalable-kaggle-implementation-20261019.md).

Lexical ищет title/summary, текущие assertions и saved narratives. Semantic привязан к material hash/revision; story_bge, story_bge_partial, lexical_degraded честно показывают покрытие/fallback. Stale/архив не выдаются текущими. Пока hybrid использует только dense, это не фиктивное смешение. Renderer, title, рейтинг и производные поисковые представления не становятся evidence. Поиск использует bounded indexed PG selection и batched hydrate; полный Python либо native cosine/sort по всем Stories не является целевым ускорением. Read соблюдает deadline/generation/min_data_seq и current eligibility. Existing control index проверяет unresolved mutations по canonical person и typed resource/scope dependencies на старте/выдаче во всех Dot/Mira/MCP routes. Пересечение с pending/UNKNOWN изменением даёт prior_write_pending с известной freshness для всех затронутых читателей общего ресурса. Несвязанная UNKNOWN правка private draft не блокирует accepted-book search; недоказанную независимость считать зависимостью. Это отдельно от durable per-dialog order. Cancel/read reconnect не повторяет принятый effect и не обещает durable read resume; см. [handoff §§5.2, 10.2](prompts/rkb-scalable-kaggle-implementation-20261019.md).

## MCP-профили и гарантии записи

| Профиль | Методы | Полномочия |
|---|---|---|
| live | Existing knowledge_search; расширение чтения по handoff | Без редакционных мутаций; voice использует saved body/ref |
| story_reader | search/get/history/validate/job_get, corpus_read, entity_list, read-export | Story и допустимые material dependencies |
| story_contributor | Reader + create/edit/archive/source_register/extract | Раздельные contribute/research/edit, собственные private drafts |
| story_editor | Contributor + transition/merge/access/publication export/publication_record | Явные права редактора/управляющего/издателя |
| full | Прежние и Story tools | Видимость инструмента не разрешает действие |

Все мутации имеют idempotency key; изменяемые объекты — expected revision. Operations — ограниченный discriminated union. Retry возвращает receipt после reconnect; неизвестный исход разрешается чтением операции. Успех содержит commit_state, operation ID, resource IDs и committed/job revision; очередь без исполнителя не называется выполненной. Actor устанавливается сервером; user ID/reviewer_kind/assessor_kind из payload не доказывают права или человеческое авторство. Сохраняются verified principal/client и actual executor.

## Межкнижное исследование, связи и оценки

Одна цель импорта объединяет source-bound facts, graph/POI, dates/observations, episodes, cases и bounded composition. Existing reconcile/job/outbox используют общий research checkpoint, без отдельного полного прохода для каждой сущности. Уже принятую **в новом corpus** source revision можно дообогатить без OCR/rechunk/reimport. Новая книга не требует перечитать весь прежний corpus.

story_reconcile(next/start/enqueue/claim/stage/apply) сохраняет порционную работу. Модель ищет и Stories, и исходный корпус: карточка может ещё не существовать. Максимум 50 candidate refs на run — бюджет порции, не предел исследования/доказательство полноты. Lexical fallback не считается BGE-каналом. Awaiting_agent означает потребность во внешней модели.

Proposal содержит identity_relation, contribution kinds, independence с причиной, proposed effect, evidence обеих сторон, объяснение и неизвестности. Сервер проверяет exact revision/page/region/quote; ошибка не двигает cursor. Apply под editor проверяет proposal/version/права и добавляет evidence, attributed assertion, явную связь либо обоснованное отсутствие изменения. Переводы/перепечатки одной цепочки не считаются независимыми автоматически.

Фазы связываются направленным phase_of, не merge. Decision.phase_of_source=anchor|candidate задаёт направление независимо от UUID/дат. Неустановленное направление остаётся unresolved; fresh import не наследует старые связи. Story_merge — явная операция над проверенным дублем с сохранением assertions/evidence, contributors, редакций/вариантов и оценок, без наследования publication approval.

Optional evidence_relation/effective_assessment фиксируют support status, independence, semantic review, rationale и method version только для проверенной пары. Без assessment добавляется reports, не молчаливая поддержка. Corroborated требует отдельно заявленного independent evidence; backend не сертифицирует истину.

Все текущие assessments доступны через assessments_page: typed assessment и assertion revision. Preview первых шести сообщает assessments_has_more; остальные не теряются. Cursors привязаны к Story revision и assertion; чужой/устаревший отклоняется. Каждая ссылка проверяется заново.

Revise_relation сохраняет retract, restore, correct_direction: expected Story/relation revision, reason, before/after audit, verified actor/client/time. Restore повторно проверяет оба evidence; reconcile не обходит отзыв через INSERT OR IGNORE. Relations_page показывает действующие, relation_history_page — и отозванные, relation_reviews_page — все решения по связи с revision-bound cursor. Исправление связи не доказывает identity лиц/POI и инвалидирует только затронутые dependencies.

Pair decision сохраняется атомарно с apply: IDs, policy version, proposal/effect, fingerprints evidence обеих сторон. Title/summary не являются новым evidence. Anchor fingerprint фиксируется после дополнения. Неизменная пара возвращает skipped_decided_pairs; если все кандидаты уже сравнивались — already_compared_under_policy, не no_match_found. Cancelled/unapplied proposal не считается решением. Новое evidence/source/policy разрешает bounded run; скрытый повтор неизменных пар/merge запрещён. Completed_under_policy означает конечный срез и реальные каналы, не полноту всех будущих книг.

## Календарь, люди и числовые наблюдения

**Facts-first — необходимое расширение baseline:** record_event_date/record_observation сейчас требуют Story assertion. Нужно разрешить те же typed записи для принятого источникового assertion **без Story**, сохранив совместимость story-bound интерфейса. Нельзя запретить сухие Stories и потерять путь сохранения дат/измерений. Расширяется existing source/assertion/evidence модель, не новый универсальный fact registry.

Исторические person/organization/source author identities отделены от rkb_users участников. Одинаковое имя не объединяет людей; автор, редактор, переводчик, цитируемый рассказчик и современный комментатор различаются. Affiliations/roles, geography и valid-time связываются с evidence; canonical POI принадлежит Street Story. Angle/audience value дополняют рамку, не переписывают факт.

### Даты

Typed date хранит exact assertion revision, 1–8 evidence IDs, original_date_text, calendar, precision, date_role, соответствующие точности year/month/day, rationale и optional supersedes date ID. Исходное выражение буквально присутствует в evidence. Точности day, approximate_day, month, year, range, unknown. Год не превращается в 1 января, approximate — в exact.

Julian/Gregorian/unspecified сохраняются без скрытой конверсии; валидность даты учитывает календарь. Event valid-time, публикация и регистрация — разные роли. Конкурирующие незамещённые даты сохраняются; новая запись не выбирает истину. Event_dates_page/history_page возвращают текущие/замещённые записи; correction сохраняет историю и инвалидирует зависимые variants.

Story_calendar и совместимый facts-first путь выбирают для «этот день» только date_role=event AND precision=day с календарём, текущими assertions/source и cursor (year,date_id). Годы, approximate и публикации исключены. Ответ различает calendar_conversion=none, source-attributed evidence и точность выборки; бюджет страницы не маскируется под no_match. Связи event/entity и диапазоны сохраняют evidence без выдуманной точности.

### Наблюдения и измерения

Typed observation содержит assertion revision, 1–8 evidence IDs, original value text, decimal value **строкой без потери точности**, metric key, unit code, subject label/ref при доказанной identity, period text и подтверждённые bounds, method/note, precision, rationale и supersedes ref. Число, период и structured year bounds подтверждены исходной цитатой. Два показателя в цитате — две записи с разными metric/unit.

Observations_page/history_page читают текущие/замещённые версии; story_observation_search и facts-first чтение показывают действующие источник/assertion, не source_changed как свежие данные. Compare сохраняет диагностики different_metrics, different_units, unverified_subject_identity, incompatible_or_unknown_method, period_not_confirmed, manual_identity_and_semantic_review_required. Совпадение subject label не устанавливает person/org/POI identity. Нет автоматических conversion, difference/ratio, трендов и агрегации несопоставимых рядов. Допустимые compare/aggregate требуют проверенных subject, units, periods и methods.

## Разногласия, готовность и публикации

Story связывает exact case/conclusion revisions и source spans. Существенное изменение основания требует revalidation зависимого Variant; обычный комментарий не отменяет всё. Дело отдаёт current conclusion и last supported conclusion раздельно: человеческая поддержка относится к exact revision и не переносится. Читаемый рассказ может содержать ясно атрибутированные альтернативы.

Publish_ready ставится только перечисленным Variant revisions после редакторского решения, текущего dependency fingerprint и проверок формулировок, прав, атрибуции, критических gaps. Атрибутированная легенда может быть готова без доказательства чудесного события. Изменение существенных assertion/assessment/date/measurement/source/case требует целевого пересмотра. Базовый импортный рассказ не получает implicit approval.

Story_publication_record фиксирует внешний факт с editor_reported либо проверенным provider provenance; уровни не смешиваются. URL появляется только после реального ресурса/receipt. Read-export не публикует, approved export не отправляет. Private drafts не входят в общий поиск/VK/digest/case page без явного действия автора.

## Приёмка и сохранность

Достаточный эпизод после fresh import имеет saved body/default ref, одинаковый exact readback после restart и корректные get/search/read-export/voice. Сравниваются текст/hash, не слово «история». Retry не создаёт дополнительных Story/Variant; CAS сохраняет ручную правку. Title «Как…» без способа требует retitle. Факты без эпизода, сухая таблица, неполная цитата, чужая атрибуция, противоположные версии, неизвестный исход и недраматичная практика имеют отдельные ожидаемые результаты. Квота рассказов и постоянный второй LLM-judge не нужны.

Проверки сохраняют даты/календари, людей/роли, географию, units/method/period, альтернативы и private права. Basic draft читается в Story scope, private draft — только автором. Accepted text доступен независимо от uploader; illustrations публичны; proof проверяется для owner/demo/expert/local_historian; full PDF не выдаётся. Withdrawal закрывает stale кеш/excerpt/late delivery; public-page revocation не прячется за last-good.

**Слепой пилот качества рассказов — 15–20 примеров.** На одних и тех же свежих оригинальных фрагментах сравниваются pinned baseline prompt/contract из [архива a8cf](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/prompts/scalable-cross-book-story-enrichment-20261009.md) и новый контракт. Показываются именно фактически сохранённые body; если baseline/new run не сохранил body, это честно помечается «body отсутствует», summary не подставляется. Порядок/метки вариантов не раскрывают реализацию оценщику. Оценивает владелец или назначенный им человек: связность, понятное начало и мысль, известный исход/честная неопределённость, соответствие title, фактическая и атрибуционная точность, отсутствие выдуманной причинности, пригодность к чтению вслух. Самопохвала генерирующей модели не является PASS. Пока человеческий ответ не получен, human pilot имеет NOT_RUN. Это разовый gate качества изменения, не обязательное ручное одобрение каждой импортируемой книги.

Нагрузочная проверка реестра сохраняет сценарий 10 000 синтетических карточек, 10 читающих и 3 одновременно подающих запись клиента; это три caller, не три canonical PG writer. Реальные MCP tools/list, scope/revocation и post-deploy естественные вопросы проверяются отдельно от CI. Restore проверяет данные, audit/receipts/outbox, revisions и default body refs; подтверждённые команды переигрываются идемпотентно по handoff без второго writer. RPO/RTO и snapshot freshness подтверждаются измерением. Protected gates не ослаблены: backend p95≤1000 ms; MCP p95≤1500 ms/hard 2000 ms; BGE Hit@5≥85%/@10≥90%, RU→DE@10≥85%, ≥30 held-out families; ≥100 sources/100000 chunks; storage≤80% steady/≤90% peak. Исторический CI/report не является PASS redesign.

Сравнение потребления рассказов Mira/Dot следует [handoff §14.4](prompts/rkb-scalable-kaggle-implementation-20261019.md) после actual PH provider: общий текст queries/corpus/rights/tool budget оценивает ответы, отдельный реальный ASR/voice/Slack route — пользовательское время первого полезного/полного ответа. Это не слепой import-body pilot выше и не основание включать future «Мира, хорошо подумай»/Dot delegation в base implementation.

Связанные контракты: [дела](design/rkb-disagreement-journal-design-20261018.md), [участники и POI](design/rkb-participants-poi-fact-bridge-20261018.md), [ПМИ](testing/rkb-disagreement-pmi-20261018.md). [Историческая версия](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/story-registry.md) сохраняет датированные детали v7–v13 и прежнюю приёмку; её SQLite/Supabase deployment и миграционные инструкции не целевые требования.
