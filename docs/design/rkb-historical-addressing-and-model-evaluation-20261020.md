# RKB: сквозная историческая адресация — единое решение

**23 октября 2026. Статус:** source-backed **проект решения для RKB-Продукт и Opus**. Реализация/тест исторического соответствия/live — **NOT RUN**. Продолжение **того же draft PR158**, а не новая параллельная концепция. [Полное исследование 20.10 (immutable original @80e1b0d)](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/80e1b0d0b1b79a843096090b33a7c8e4c473fb6b/docs/design/rkb-historical-addressing-and-model-evaluation-20261020.md) содержит исследовательские R0/P1/P2/P3 и 28 потенциальных проверок; **они не являются реализованным API или обязательным новым компонентом**. Этот документ — единственный действующий предлагаемый design в PR158.

Baseline сравнения: RKB `main@c35ad8903c159c894369b5e5d121f809f6566d61` (содержит PR320), Street Story `main@de75352038effe9320cc6ab5ccb4ef05cd59977b`, Regional Cartography `main@b4b65abf9dca495fac3450b88a40845650d63b8b`. [Одобренная предметная постановка](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792744773999769). Текущий runtime/provider, установленный клиент и действительный корпус этим чтением исходников не инспектировались.

## 1. Выбор без дополнительных подсистем

**Принять baseline PR158:** одна логическая операция разрешения исторического места **внутри RKB рядом с существующим PG/Kaggle**, а не отдельный сервис. Использовать source-place/graph, typed source evidence, индексируемые historic/current aliases и source-time, при необходимости `pg_trgm`, PostGIS и уже имеющийся BGE-M3. Это *предлагаемая логика для существующих tool/read/mutation контуров*, **не новый публичный MCP method и не новая физическая схема в этом docs PR**. GPU/new NN/ranker/fine-tune P2/P3 **не нужны по умолчанию**: допустимы лишь по измеренным category errors и преимуществу на независимых source/address/epoch families.

**Ровно один владелец каждого объекта:**

| Объект | Authority | Степень внедрения по source |
| --- | --- | --- |
| Исходное упоминание, RKB source-place ID, aliases, датированная адресная связь, роли material → place, Case/evidence | RKB target | `poi_ref` и `occurred_at`/`located_in` уже typed, но cross-source историко-современный resolver **не подтверждён** |
| Растры/карты, GCP, современная/историческая геометрия, CRS, локальная ошибка и версия пространственного утверждения | **Regional Cartography** | `cartography.resolve.v1` существует, accepted house geometry на весь регион **не доказана** |
| Идентичность физического POI, его жизненный цикл и receipts | **Street Story** | Существующий `streetstory://poi/…`; имя/координаты RKB не вправе автоматически создать или слить POI |

RKB хранит **внешние owner refs**, не копирует POI/geometry как вторую власть. Улица, район, площадь, дом, участок, поколение здания, населённый пункт и POI — не взаимозаменяемые identities; смена таблички не доказывает прежнюю трассу или сохранившийся дом. Стабильная place identity не должна зависеть от seed-книги, но *видимые* labels/assertions всегда проверяют действующую eligibility и provenance. GEO-only legacy anchors не восстанавливают старый book corpus.

## 2. Один логический контракт input → output

**Вход:** exact original place spelling/адрес (включая строковый домовой номер с литерой), surrounding source text + source_id/revision/page/region/quote; язык, settlement/admin context, role (mention/event location/publication place/residence/etc.), уточнённое `event_time`, *отдельное* `naming_time` и `publication_time`, требуемый уровень и current actor/audience. Modern-reference date и already-known RKB ID/Street Story POI optional. Неустановленное время остаётся `unknown`/`null`, а не заполняется годом книги.

**Ответ:** ограниченные current-authorized candidates и одно итоговое решение только при наличии основания:

| Поле/смысл | Контракт |
| --- | --- |
| `status`/`precision` | `resolved` до **settlement/district/square/street/street-segment/house/building** с отдельно названным уровнем; `ambiguous`, `unresolved`, `not_found_in_coverage` или `not_a_place`. `partial/unavailable` для недоступной части системы **не** выдавать за not found |
| `historical` | existing RKB source-place ID, исходный historical name/address для конкретной эпохи, source/owner provenance, подтверждённый тип (улица ≠ район ≠ дом); несколько обоснованных candidates — не «первый выиграл» |
| `modern` | Современное имя/адрес/компоненты **только** при evidence-backed correspondence. Может оставаться null; распознанная улица без номера → точность до улицы, не до дома |
| `geometry` | Nullable `cartography://…` owner ref, revision, CRS, type, observed epoch, method/error/uncertainty; centroid/pin не является historical door/footprint. Нет геометрии — не придумывать координаты |
| `related` | Датированные и проверенные связанные район, площадь, улица/segment, дом/участок, населённый пункт и отдельно подтверждённый `streetstory://poi/…`; neighbor/containment не выводить только по близости точек |
| `proof`/`coverage` | Точный source locator, assertion/owner revision, temporal basis, coverage period/area/gaps, current auth, reasons и possible next_action; не раскрывать private source/Case/scan через alias/ranking/cache |

**Время и адресный смысл:** «как называлась эта улица в 1910?» проверяет contemporaneous naming, а «в 1910 на нынешней улице X» сначала разрешает нынешнюю reference, затем историческую территорию/segment 1910. Издание 1935 не делает каждое описанное событие «событием 1935 года». Переименование ≠ перенумерация ≠ новый дом на старом участке. Matching name, house number, map proximity, BGE cosine и pg_trgm — кандидатообразование, **не** доказанный `same_as`.

## 3. Один model page pass, без блокировки текста GEO

Модель на странице в обычном `start → book_pages` (target MCP default **1**, max request **4**, renderer может вернуть одну) определяет source place, actual evidence и семантическую роль. Если есть достойные кандидаты, **один bounded логический lookup на упоминание** выбирает существующий place ID в рамках existing permitted tooling, после чего link с ролью/evidence передаётся **в том же `book_ingest(stage)`** через существующие `entity_candidates`/`poi_facts` и связанные поля, если текущая typed schema их допускает. Не вызывать вторую обязательную модель в backend, не запускать второй проход или поиск по всей книге. `entity_list` сейчас требует ровно один source ID — полноценный cross-book resolver ещё не реализован; это честная future integration boundary, а не утверждение, что tool уже поддерживает весь input.

Если alias/адрес не сопоставился — сохранить *дословное* имя, контекст, source locator, uncertainty и pending decision; **не** создавать выдуманный entity ID/POI/geometry. При добавлении проверенного исторического alias, карты или owner POI **досопоставить сохранённое упоминание**, точно обновив соответствующую link/projection через имеющуюся authorisation/idempotency и revision dependency, **без повторного OCR, перечитывания книги, нового backfill/reimport или полного re-embedding**.

PR320 перевёл target на **accepted stage packet → source text searchable без whole-book seal**. Поиск первой страницы Гаузе через MCP **не ждёт** географической интерпретации, картографического owner, story/image enrichment, согласования экспертов или реестра улиц. Отдельная ошибка enrichment не откатывает подтверждённый текст; потеря ответа по эффекту/UNKNOWN требует того же exact receipt/readback, а не новой команды. Заявление о live Saved остаётся отдельной проверкой.

## 4. Read по именам, адресу и POI — источник, рассказ, событие, дело

Historic/current name, адрес или exact Street Story POI → тот же bounded actor-eligible graph/PG lookup по датам/aliases/refs → source fragments с literal evidence, связанный story `body`, dates/events/facts и Case (только если actor имеет Case grant), при наличии Cartography geometry owner ref. `mentions` не расширяется в `occurred_at` без доказательства. Место издания источника не назначается местом каждого события. На update/withdrawal/actor revocation исключить stale labels, закрытые Case, сканы и derived read projections.

**Найденное по исходникам, а не по живым запросам:**

- RKB [`kaggle_source_graph_tools.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/c35ad8903c159c894369b5e5d121f809f6566d61/src/regional_knowledge/kaggle_source_graph_tools.py): `entity_list` ограничен одним exact document ID, `graph_fetch` читает по известному ID; это **не** глобальный gazetteer. [`kaggle_geo_backend.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/c35ad8903c159c894369b5e5d121f809f6566d61/src/regional_knowledge/kaggle_geo_backend.py) отдаёт retained private GEO owner refs, не полный accepted-source corpus.
- RKB [`server.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/c35ad8903c159c894369b5e5d121f809f6566d61/src/regional_knowledge/server.py) в существующем `GET /internal/street-story/poi-context` после service bearer делает `if not actor or not hasattr(backend,"corpus"): 503 scope_unavailable` и затем legacy `PoiRegistry(backend).context`. Target [`KaggleSourceBackend`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/c35ad8903c159c894369b5e5d121f809f6566d61/src/regional_knowledge/kaggle_public_backend.py) **не объявляет `corpus`**. Это source-backed **условная причина 503 при настроенном bridge и valid bearer**, но **не** live HTTP evidence. Другой 503 — `bridge_unavailable`, чужой bearer — 401; лечить заменой target read adapter с existing service grant, а не удалением auth или новым endpoint.
- [Street Story `rkb-poi-link.md`](https://github.com/onedayonemasterpiece/street-story/blob/de75352038effe9320cc6ab5ccb4ef05cd59977b/docs/rkb-poi-link.md): `GET /v1/pois/{poi_id}/knowledge` и исходный внутренний RKB bridge предусмотрены; device token не пересылается в RKB; Street Story владеет POI, связанный RKB fragment не размножается по POI name.
- [Regional Cartography README](https://github.com/onedayonemasterpiece/regional-cartography/blob/b4b65abf9dca495fac3450b88a40845650d63b8b/README.md) и [`cartography.resolve.v1`](https://github.com/onedayonemasterpiece/regional-cartography/blob/b4b65abf9dca495fac3450b88a40845650d63b8b/contracts/cartography.resolve.v1.schema.json): карта/геометрия и её provenance остаются у автора карты.

**Следующее integration, не текущая реализация:** сохранить действующий bridge URL, owner service permission и Street Story device auth; переключить только target handler от legacy corpus к правомочному source/PG reader с current ACL и exact POI owner binding. Accepted source fragments/real stories/facts/events/cases требуют отдельного source-level E2E и не возникают автоматически из GEO-only refs.

## 5. Реальные, проверяемые исторические кейсы

| Case | Исходное свидетельство | Минимальная проверяемая семантика |
| --- | --- | --- |
| **H1. Königstraße → улица Фрунзе** | [Муниципальная топонимия](https://www.klgd.ru/city/history/gubin/toponim.php), таблица района 1: `Штрассе «Дер СА» (бывшая Кёнигштрассе) → Фрунзе`. | Сопоставление **исторической и современной улицы** как source claim; дом/геометрия/точная длина совпадающего segment не подтверждены |
| **H2. Paradeplatz → Университетская** | Та же официально опубликованная историческая таблица, в ней есть промежуточные названия. | Тип площади, датированные names, не подменять физическую историческую геометрию |
| **H3. Steindamm → Житомирская и часть Ленинского проспекта** | Та же таблица; современная колонка содержит **два соответствия**, не одно. | Partial/split; не делать один `same_as` всей улицы |
| **H4. Перенумерация Кёнигштрассе/Фрунзе** | Оригинальный [MOB Königsberg 1935](https://digital.martin-opitz-bibliothek.de/resolver/0010201-83-1935) и [картографическое сравнение 1912/1935 Ahnenspuren](https://www.ahnen-spuren.de/ostpreussen/kirchspiele/koenigsberg-stadt/stadtplaene/1935) с номерами на перекрёстках. | **Реальный проверяемый вопрос, но ни одна конкретная пара old→new № не подтверждена просмотром двух первичных страниц в этом раунде**. До проверки — `house_number_unknown` или street-only; одинаковый номер не подтверждает тот же дом |
| **H5. «В 1910 на нынешней улице Фрунзе»** | H1 — реальное название; предложение — **контролируемый тестовый запрос, не цитата из книги**. | Найти современную reference, затем отдельно источник для состояния 1910; modern name не отсекается по event year |
| **H6. Место издания ≠ событие** | У MOB 1935 каталогизирован `Erscheinungsort=Königsberg`. Для фактического события в другом городе требуется точно выбранный текст/страница, **пока НЕ проверено**. | Не создавать `occurred_at(Königsberg)` из publication metadata книги |
| **H7. Пробелы источника** | [MOB Tilsit 1912](https://digital.martin-opitz-bibliothek.de/resolver/0028679-29-1912): исторический digital record, gaps отмечены в исследовании PR158. | Отсутствующий лист/частичный OCR → `coverage_gap`, а не утверждение, что места не было |

Честность evidence: **H1–H3** source-backed *топонимические* соответствия; **H4–H7** проверяемые реальные архивы и заранее объявленные позитивные/негативные сценарии, но house-level identity, точная геометрия, время события и historical source gold ещё **NOT RUN**. Никакая synthetic fixture или OCR-сниппет не признаётся историческим доказательством без сверки original page + owner геометрии.

## 6. Раздельная приёмка, без раздувания P0

| Этап | Проверить | Что НЕ блокирует |
| --- | --- | --- |
| **P0 Гаузе** | Принятая первая страница этой же книги → обычный authenticated MCP source search/fetch, точный source/page/receipt, нет whole-book seal, UNKNOWN не выдан за success. Live доказательство отдельно | GEO, POI, карта, house numbers, ML, Story/Case enrichment |
| **GEO-A post-P0** | H1–H6 на проверенных исторических/современных refs, 0/1/N кандидатов, улица-only, null geometry, late linking из source text без re-read; current ACL/withdrawal | Массовый геокодер каждого дома, миграция old books |
| **GEO-B post-P0** | Street Story existing POI knowledge endpoint → actor-scoped target PG source fragments, story bodies, events, authorized Case; 503 причины устранены при unchanged service grant | Новый HTTP endpoint, новый владелец POI, открытие private Case |
| **GEO-C post-P0** | Independent owner Cartography geometry, typed precision/epoch/split/renumber, traceable evidence и ambiguous cases | Псевдоточность одного растра или скана |
| **P2/P3 условно** | Held-out source/address/epoch family failures и observed latency/CPU/quality vs baseline exact aliases/optional `pg_trgm`/BGE; тест нового ranking/model только по доказанной категории miss | Автоматическая fine-tune/специализированная нейромодель |

**Protected** v4 readiness gates не ослабляются; Stage-P0 принимается отдельно от полной book-quality/restore/scale readiness. В этой docs работе нет PG schema/benchmark, source/SQL changes, P0 deployment, Kaggle/provider call, new services, backfill/reimport или historical E2E.

## 7. Решения для review и дальнейшей постановки

1. **RKB-Продукт + Opus:** подтвердить единственную owner model / input-output / street-only/uncertainty, разделение P0/GEO/ML, актуализацию `7/`12/`13 ТЗ. Review не означает, что resolver уже installed.
2. После review — отдельный bounded implementation по (A) existing PG GEO/source alias and evidence join, (B) reverse read historic/modern/POI → accepted source fragments/stories/events/cases, (C) старый authenticated Street Story bridge → target PG вместо legacy `corpus`, (D) экспертиза нескольких real historic links owner Cartography/POI Street Story; не новый общий проект.
3. **Шкипер** — единственный runtime/SQL/provider/deploy owner и сейчас занят P0. Ему не назначать работу по адресам до отдельного решения; P0 page→search не ждёт этой задачи. Изменения SQL/PG/схемы, внешних API, credential и provider в текущем PR отсутствуют.

**Evidence status:** точные исходники/договоры и исторические каталоги **прочитаны**; реализация resolver, проверка текущего реального Street Story 503, поиск по современному номеру, Gause live, trained model и качественные метрики — **NOT RUN**. Этот текст — единое решение, не акт приёмки функции.
