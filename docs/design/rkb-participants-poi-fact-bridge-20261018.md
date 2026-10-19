# Участники и доказательные POI-факты — дополнение РКБ v3.2

**Уточнение владельца 19.10.2026 — [единый доступ эксперта](../requirements/unified-expert-access-20261019.md):** личный MCP из совместимого клиента (включая предполагаемые ChatGPT/Grok) и Slack → dot используют один существующий MCP-ресурс РКБ, canonical person и серверные права. «Ограниченный экспертный маршрут» означает контекст/capabilities, а не отдельный MCP-сервер или `/expert/mcp`. Прямая работа не зависит от dot; совместимость и не-owner OAuth требуют собственной фактической приёмки.

**18 октября 2026 года. Статус: проект, не внедрение.** Часть проверок основана на чтении текущего кода, часть — на критическом разборе dot. Обязательная приёмка перечислена в [ПМИ](../testing/rkb-disagreement-pmi-20261018.md); основной [проект разногласий v3.1](rkb-disagreement-journal-design-20261018.md).

## 1. Два независимых результата

**Один человек — одна карточка участника, много подключённых аккаунтов и явно назначенных прав.** Владелец регистрирует человека через свой MCP. Slack, VK, Projects Hub, Mira и будущие приложения подтверждают владение внешним аккаунтом и привязываются к той же карточке. Участники не знают о dot и не обращаются к нему напрямую. В их Slack-пространстве приложение dot может вообще отсутствовать: отдельный Kaliningrad Base получает сообщение, РКБ сохраняет подтверждённого инициатора, внутреннее MCP-событие будит dot, Kaliningrad Base отправляет результат от своего имени. Экспертный маршрут не умеет импортировать документы, включая запрос, который через него послал владелец.

**Книга → POI → атомарный факт с оригиналом и автором.** Street Story остаётся единственным владельцем POI, локального регистра накопленных фактов и предметного арбитража. РКБ владеет PDF/DjVu, OCR, принятыми редакциями, страницами, цитатами, авторством, source rights и исследовательскими делами. Никакой второй POI registry, полной копии корпуса или универсального графа исторических фактов.

## 2. Что действительно существует

- В РКБ уже есть пользовательский UUID в таблице **rkb_users**, действующие workspace/document grants и привязка SQL actor (sql/006_application_identity.sql). Не требуется новая каноническая таблица пользователей.
- Целевое единое first-party identity описано в Projects Hub, но нынешний сервер identity.kenigevents.ru и OAuth РКБ пока ориентированы на владельца. РКБ OAuth approve() назначает owner_subject, а текущий Projects Hub helper выдаёт invite платформенного владельца. **Их нельзя использовать как уже готовую выдачу доступа другому человеку.** Projects Hub actor имеет формат usr_...; RKB subject — UUID. Нужен явный подтверждённый mapping, не вставка PH actor как RKB principal.
- Во время импорта РКБ принимает typed POI fact candidate и формирует **poi.fact_evidence.v1** с документом, ревизией, page/region, source-family, проверенным авторским authority и иной provenance. Activation фиксирует его в rkb_integration_outbox в одной транзакции. **poi_registry** уже умеет отдельно связывать source entity с каноническим POI Street Story, **poi_context** возвращает источник/соседние сущности/истории.
- В Street Story написаны **poi_external.ingest_poi_evidence**, **poi_claims**, **poi_claim_evidence**, semantic review и конфликтный журнал. Но в проверенных RKB source-модулях не найден действующий outbox-delivery worker, а в Street Story текущий внутренний HTTP-route **/v1/internal/rkb-poi** обслуживает identity-link, не факт-evidence intake. **Наличие linked POI и outbox row не означает доставленный и принятый POI-факт.**
- Существующий source evidence URI, например **knowledge://evidence/<candidate_uuid>**, не продемонстрирован как разрешимый обычным fetch/source_proof. Нужен проверяемый переход от candidate_id к активной/исторической source revision и оригинальным page/region.
- Street Story хранит исходные source title/year/ref/revision в событии, но текущая пользовательская проекция местами выводит URI вместо человеческого заголовка; авторы конкретных статей/глав не гарантируются одним author_profile_ref.
- При переобработке и отзыве исходной редакции существующая public-проекция фильтрует scope и claim status, но не все состояния исходного события и актуальность источника. Старое свидетельство может остаться используемым без актуального основания. Это потенциальный дефект по статическому чтению, не утверждение о найденной порче production.
- Выполненный read-only вызов RKB poi_context для собора на Кнайпхофе вернул source place, страницу/цитату и story при **complete_extraction_claimed=false**. Это подтверждает navigational POI-link, не завершение конвейера фактов.

## 3. Создание и сопровождение пользователей — без админ-панели

### 3.1. Каноническая карточка

РКБ использует существующий **rkb_users.id** как person UUID и небольшой профиль в metadata/узкой таблице. Карточка содержит имя, статус, audit record создавшего, необязательное описание; **множество категорий**: demo, student, research_staff, expert, guide, local_historian. Человек может одновременно быть гидом и исследователем.

Для научного сотрудника хранить необязательные регалии: степень, звание, учреждение, должность, специализация, период/география, языки, библиографические сведения и подтверждающее evidence. Каждая регалия имеет **claimed / owner_attested / independently_verified** с проверявшим и временем. Частные документы о человеке не становятся общедоступными из-за наличия профиля.

В существующем реестре **rkb_author_profiles** записаны **авторы публикаций**, это не аккаунты пользователей. Если эксперт также является автором статьи, отдельную подтверждённую связь оформляет владелец; одинаковая фамилия не устанавливает identity. Библиографический авторитет может помогать оценивать конкретный текст, но не давать право редактировать дела или публиковать.

**Категория, capability, ACL и предметный вес — четыре разные вещи.** Шаблон demo может дать ограниченный временем публичный поиск; student — право предложить аргумент; research_staff/expert — право подготовить профессиональный вывод и просматривать специально разрешённые сканы; guide/local_historian — поиск для своей работы и собственные свидетельства. Это предлагаемые defaults, а не неотменяемое правило. Ни регалии, ни вес автоматически не дают доступ к частной книге или право на импорт.

Выдаваемые права: public_search, case_read, case_contribute, scan_proof_read, personal_conclusion, support_exact_revision, maintain_shared_summary, discussion_request, discussion_approve, participants_manage, а также узкие document/workspace grants. **Импорт** — отдельная owner-only capability и другой route. Временные права и демо-доступ имеют expires_at. Предметный вес хранится с областью, основанием, назначившим и историей.

### 3.2. Управление через MCP владельца

Минимальный типизированный интерфейс — развитие уже спроектированного **disagreement_participants**: upsert/get/list/disable; categories/credentials; grant/revoke; assign_domain_weight; link_account_begin/confirm/unlink; audit/readback. Физических MCP-методов может быть меньше. Все изменения имеют operation key, expected revision и серверную проверку owner. Не строить новый административный сайт или IAM.

Пример естественного поручения владельца: «Создай участника N, научный сотрудник, область — архитектура Восточной Пруссии, степень заявлена, пока не подтверждена; доступ к открытым материалам и конкретной разрешённой коллекции; может вносить аргументы и предлагать заключения, но не менять общую сводку и не импортировать; добавь pending Slack workspace T/user U». Результат MCP должен показать **точные** роли/гранты/профиль и person UUID; модель не назначает дополнительные права по смыслу звания.

### 3.3. Подтверждённые внешние аккаунты

Небольшая таблица **rkb_external_identities**: person UUID, provider, tenant/workspace, provider_subject, pending/verified/revoked, proof method, confirmed_by/at, revision и история. Уникальный действующий ключ (provider,tenant,subject) нельзя связать сразу с двумя людьми. **Никаких OAuth-токенов внешних сервисов в таблице связи.**

| Канал | Минимальный способ |
|---|---|
| Slack | Владелец добавляет точные workspace/team ID и user ID как pending. Проверенный Slack DM от этого аккаунта и короткая одноразовая проверка/owner-confirmed binding переводят в verified. Отображаемое имя и email не нужны для сопоставления. Подпись HTTP Events или проверенный Socket Mode transport подтверждает источник события. |
| VK | Только точный числовой user ID и доказательство владения через допустимую user-auth процедуру либо одноразовое сообщение из этого аккаунта, подтверждённое провайдером. Отрицательный from_id сообщества — не человек. До подтверждения связь pending и не переносит экспертный вес. |
| Projects Hub | Текущий usr_... — **внешний PH actor ID**, а не RKB UUID и не доказательство multi-user OAuth РКБ. Привязка по действительной PH-сессии и ограниченному challenge; future first-party OIDC делегирует доступ к отдельному Knowledge resource с правильными audience/scope. Не использовать существующий owner invite для учёного. |
| Mira и будущий клиент | Та же персональная карточка; после готовности многопользовательского identity.kenigevents.ru связывается подтверждённый issuer+sub, без изменения исторических ID дела или POI. |

Не сопоставлять людей по имени, фамилии, степени, email, VK-аватару, одинаковому слову в комментариях. При неверной связи владелец отзывает её с историей; исторические мнения не исчезают, ошибочно приписанные поддержки переходят в attribution_review_required. Отзыв ресурса/роли перепроверяется перед чтением и асинхронной отправкой ответов.

**Никто в пользовательском Slack-пространстве не видит dot:** Kaliningrad Base — отдельное приложение. Внутреннее MCP-событие/очередь не требует Slack-упоминания dot и не даёт его владельческий авторизационный контекст экспертам.

### 3.4. Владелец как испытатель Slack-маршрута (19.10.2026)

Для тестирования поиска и работы с делами владелец использует **тот же отдельный Kaliningrad Base**, что и эксперты: DM либо упоминание бота в разрешённом канале. По подтверждённым метаданным исходного поручения его точный внешний ключ: **provider=slack, team_id=T0C82495TK5, user_id=U0C8C1QKWAG**. Упоминание **U0C8A9GM76D** в тексте адресовано dot; это не user ID автора и не аккаунт для owner binding.

Целевое сопоставление: этот проверенный Slack-ключ → **существующий канонический владелец в rkb_users.id**, тот же UUID, которому уже принадлежат workspace/documents. Значение UUID берётся из доверенной текущей RKB identity-конфигурации и подтверждается readback при настройке; здесь оно не выдумывается. Не создавать второго владельца, не менять владельцев корпуса и не назначать owner из имени, упоминания, параметра actor_id/owner=true или любого неподтверждённого payload. Поля team/user берутся только из проверенного провайдером события. Применяется обычная процедура verified binding из §3.3 с audit/ревизией; при отсутствующем, pending, неоднозначном или revoked mapping владелец не распознаётся и owner fallback запрещён.

Это дополнительный **исследовательский маршрут существующего владельца**, не grant всем пользователям workspace и не владелец-сервис для чужих запросов. В сохранённом request инициатором остаётся канонический человек O и его подтверждённый Slack-ключ; service principal Kaliningrad Base и исполнитель dot записываются отдельно. Полномочия вычисляются как пересечение действующих прав O, разрешений expert route и ACL ресурса/аудитории. Поиск, чтение дела, личный черновик, явный вклад/поддержка точной версии и подписка идут через обычные операции и receipts; чужие запросы не наследуют O. Администрирование участников, новые grants и импорт этим дополнением не открываются. Импорт/reprocess/массовый scan по-прежнему недоступны через бота даже O.

**Автоматизированный инициатор:** уточнение владельца от 19.10.2026 требует использовать эту идентичность для автотестирования. Harness может инициировать собственные разрешённые операции O только через поддерживаемый owner-authored connector/transport, который реально отправляет сообщение от аутентифицированного аккаунта O. Сервер проверяет обычное Slack-событие и действующий binding; переданный harness actor_id, сообщение service-бота и тестовая подмена event не доказывают авторство. Наличие проверенного mapping не доказывает существование такого транспорта. До подтверждения user-sender автоматизации именно Slack E2E-8 — BLOCKED/implementation gap, без fallback к impersonation или личному owner-токену dot; component/integration автотесты операций и отката выполняются независимо от Slack-транспорта. Существующие права O не расширяются; необходимые grant/transport setup не выполняются этой документационной правкой.

**Тестовые изменения обратимы:** один test_run_id, baseline/receipts и существующие domain operations; по умолчанию изолированная копия дела с неизменяемыми реальными источниками. Тестовые мнения, веса и поддержки не влияют на реальные экспертные заключения/уведомления/VK. Для канонического дела допустима только адресная CAS-компенсация своих эффектов, не стирающая поздний вклад эксперта; conflict останавливает принудительный откат. Повтор и crash/resume безопасны, state/generation fence блокирует поздние эффекты. Чистоту search/projections/outbox и исходное ожидание экспертов подтверждает readback. Подробный минимум без новой подсистемы: [Kaliningrad Base, §4.1](rkb-kaliningrad-base-collaboration-20261018.md#41-безопасный-откат-автоматизированных-проверок).

**Статус: planned acceptance, не live binding и не выданный доступ.** Эта правка не меняет production/ACL, не доказывает установку бота и не отмечает испытания PASS. Проверки поиска, дел, неправильного team/user, подмены actor, отзыва, безопасной выдачи и обратимости автотестов: [ПМИ KB-41…48 и E2E-8](../testing/rkb-disagreement-pmi-20261018.md).

## 4. POI: два рабочих маршрута, один факт-регистр

### 4.1. При импорте книги

1. Модель выявляет упоминание физического/памятного POI и **атомарные факты**: дату, авторство постройки, назначение, перестройку, исторический эпизод. Структура сохраняет говорящего, время, условность, автора конкретной главы/статьи и точные page+region.
2. Для объекта модель использует существующий **poi_registry(search → select/create candidate)**. Исторический остров/район не превращается автоматически в отдельный объект здания; похожие имена — кандидаты на identity review, не повод соединить два объекта. Та же source entity связывается с каноническим Street Story POI.
3. Для каждого факта РКБ создаёт отдельный принятой ревизии **poi.fact_evidence.v1** и outbox receipt. После активации источника обязательная фаза 2 может обнаружить **дополнительные** факты — нужна узкая write-операция submit_poi_evidence для той же активной source revision с exact page/region, а не повторный импорт всей книги.
4. Существующий outbox worker доставляет авторизованный event в **отдельный narrow intake Street Story** (не identity route /v1/internal/rkb-poi и не Android device token). Действующий **ingest_poi_evidence_with_reviews** используется повторно. Public-service sender и private approved resource-grant различаются. Если private grant отсутствует — pending_authorization, не тихая публикация.
5. Street Story возвращает event ID, canonical POI ID, claim ID, evidence ID, candidate/semantic review state и receipt. Входящее свидетельство сначала является **кандидатом**, пока модель не квалифицирует конкуренцию и не примет решение по POI факту. Текущее disagreement дело РКБ может ссылаться на предметный POI conflict, но не перезаписывает его самостоятельно.
6. Импорт завершает OCR + обязательный scan независимо от внешнего сбоя. Но отчёт **отдельно** показывает POI identities linked/unresolved, extracted evidence, outbox pending/authorized/delivered, claims candidate/accepted/contested; фраза «POI обогащён» допустима только после подтверждённой owner receipt. Видимый незавершённый POI-sync без ложного completed.

### 4.2. Когда Street Story ищет материалы по POI

Street Story знает свой canonical poi_id и уже принятые факты. Модель обращается через ограниченный сервисный запрос RKB **poi_evidence_research(poi_ref, question, scope, cursor)**: сначала точные связанные source entities, затем дополнительные совпадения имён и смыслов из разрешённого корпуса. Связь с новым объектом модель подтверждает отдельно. RKB возвращает только *кандидаты* и их исходные локаторы/библиографию; разные варианты включены, даже если у одного ниже evidence score.

Модель сравнивает кандидат с накопленными POI facts: новый независимый факт; дополнительное свидетельство к уже существующему; вопрос идентичности; возможный конфликт; неподходящий источник. Для подходящего факта использует **тот же intake и claim/evidence регистр**, не создаёт второй «факты из поиска». Результат Street Story содержит явный флаг origin=regional_knowledge и source document+revision. **poi_context** сейчас возвращает one-hop источник/истории, не полную выборку атомарных фактов; его нельзя выдавать за новый исследовательский поиск.

## 5. Читабельная и открываемая доказательная цепочка

У каждого POI факта, пришедшего из РКБ, нужен минимальный provenance pack:
- Street Story canonical poi_id, claim_id, статус candidate/accepted/contested; RKB document_ref и source_revision;
- название **конкретной** книги/статьи/главы, год, автор и роль (отдельно от редактора/переводчика), при известности — verified author profile;
- физическая и печатная страница, page_id/region_id/chunk_id, точная цитата, original proof с видимым выделением;
- source family и повторные независимые/зависимые свидетельства, их visibility/ACL;
- состояние current/superseded/retracted/unavailable/reanchor_required и ссылка на связанное дело о разногласии, если оно есть.

**Сегодня формат v1 строгий:** source содержит document ref, revision, title, year; evidence содержит page/region и возможные author_profile_refs, но не обязательно имя автора отдельной статьи. Для MVP авторизованный reader РКБ разрешает по исходному source ref актуальную библиографию компонента и атрибуцию автора; исходное событие v1 менять молча нельзя. Если позднее потребуется автономный contributor snapshot в Street Story, расширить producer/consumer **согласованной** версионированной схемой и backward-compatible tests.

**Устранить ложный evidence URI:** текущий knowledge://evidence/<candidate_id> должен иметь RKB resolver до действительной комбинации document, revision, page, region, chunk и origin byte/scan; расширить существующие fetch/source_proof, а не создавать новое хранилище сканов. Пока resolver не готов — возвращать честное unavailable, не кликабельную фиктивную цитату. Совпадение OCR, визуальная локализация и человеческое подтверждение — три разных свойства.

### 5.1. Новая редакция и отзыв источника

Сохранённая история событий не удаляется, но:
- после reprocess старый evidence сначала становится superseded/reanchor_required до проверки новой source revision;
- публичная выборка Street Story проверяет **текущее состояние event, действующий источник и права**, а не только public и claim status;
- при отзыве книги закрыть исходное свидетельство, не уничтожая другие независимые доказательства того же claim;
- если private/workspace право не подтверждено, не отправлять байты, названия закрытых книг, private текст или их пересказы в public POI/Slack;
- конфликтный факт остаётся contested, даже если источник имеет высокий score; количество ссылок, повторяющих одного автора, не голосует за истину;
- недоступность Street Story оставляет outbox pending/retry_wait, исходный импорт не откатывает, а повторный event не создаёт дубли.

## 6. Минимум операций MCP

Не создавать отдельный инструмент для каждого UI-действия. Расширить существующие группы:
- **disagreement_participants** — owner upsert, credentials, profile categories, grant/revoke, domain weight, get/list/status, link external account begin/confirm/revoke, audit.
- **poi_registry/poi_context** — оставить источниковую POI identity boundary; linked не равно accepted fact.
- **poi_evidence_submit** — late accepted-source fact+proof и outbox; idempotent по document/revision/evidence.
- **poi_evidence_research** — канонический POI ref, вопрос, subject/time filters, cursor; evidence candidates, source completeness, independent families.
- **fetch/source_proof** — разрешающий evidence candidate ID до конкретной цитаты и оригинала, по actor ACL.
- **poi_sync_status** — полный receipt delivery/identity/semantic review/provenance/revision; можно совместить со статусом исходного outbox.
- **disagreement_get/export** — взаимные stable ссылки на Street Story conflict и RKB source disagreement без двойного арбитража.

Детерминистика отвечает за право, версии, ссылки, idempotency и доставку. Модель отвечает за смысл факта, выбор POI, различение контекста/свидетельств, конфликт и вывод. Существующие SQL/memory и BGE остаются; PostgreSQL не нужен ради этого дополнения.

## 7. Проверяемый первый результат

**Участники:** владелец одним MCP-вызовом создаёт человека с несколькими категориями, конкретными grants и незаверенной учёной степенью. Затем Slack и PH/VK accounts подтверждённо связываются с тем же RKB UUID, у другого человека с тем же именем свой UUID. Экспертный Slack-бот не показывает dot и не даёт импорт, включая текст «владелец разрешил».

**Книга → факт:** на реальном разрешённом источнике модель подтверждает POI identity, извлекает один атомарный факт с book/author/page, пишет existing outbox и получает фактический Street Story receipt. Street Story readback показывает отдельную claim+evidence, а source proof открывается на **оригинале**. Повтор event не удваивает доказательство.

**POI → новый источник:** поиск по тому же poi_id находит вторую независимую книгу, модель привязывает её как новое свидетельство или противоположный вариант, не как второй POI. Рецензия и дело РКБ доступны по ссылке. Reprocess/private revoke сохраняет историю, но не выдаёт отозванную опору как действительную.

**Пилот допускается на уже импортированной книге**, где подтверждены страницы и POI identity; не ждать многопользовательского расширения OIDC для первых owner-managed Slack связок. Для входа PH/mobile вне существующего owner-only OAuth потребуются отдельные подтверждённые resource grants; нельзя присвоить PH actor ID или owner_token вместо них.

## 8. Основания и честные границы

Код: [RKB stage_service](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/src/regional_knowledge/stage_service.py), [RKB POI outbox](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/sql/004_poi_outbox.sql), [RKB rkb_users](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/sql/006_application_identity.sql), [Street Story poi_external](https://github.com/onedayonemasterpiece/street-story/blob/main/backend/street_story/poi_external.py), [Street Story POI memory](https://github.com/onedayonemasterpiece/street-story/blob/main/backend/street_story/poi_memory.py), [Projects Hub identity target](https://github.com/onedayonemasterpiece/projects-hub/blob/main/docs/product/17-unified-identity-and-self-development.md).

[OAuth security best current practice (RFC 9700)](https://www.rfc-editor.org/info/rfc9700/); [Slack event identity / signatures](https://docs.slack.dev/authentication/verifying-requests-from-slack); [Slack Sign in identity](https://docs.slack.dev/reference/methods/users.identity/).

Dot по прочитанному коду отметил 5 рисков: owner-only PH invite, несовпадающие PH/RKB actor IDs, не соединённый POI fact intake, неоткрываемый candidate evidence и stale revocation в публичной проекции. Эти замечания учтены. Новую консультацию Claude Opus 5.5 именно по этому дополнению **не удалось вызвать**: оба подключённых клиента consult_model по-прежнему отвергают alias claude_opus_5_5. Предыдущее подтверждённое заключение Opus относилось к v3, а не к нынешним решениям. Не заявлять о состоявшемся тройном консилиуме или production-приёмке.

**Статус реализации всех добавлений: planned.** Отдельная ПМИ фиксирует критерии PASS/FAIL; её выполнение ещё впереди.
