# GEO MVP → native PostgreSQL: перенос существующей очереди, без нового проекта

**23.10.2026 · RKB-Удаление · DOCS ONLY · implementation/acceptance NOT RUN.**  
Source pin: `main@a2339b60c40461b6874ff75646e56b8dc89a15b4`. Основание: [постановка RKB-Продукт](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792773036167399), [GEO-A/GEO-B решение Контура — PR158](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/158) `@23d53f514470dfbf7b97d1f6ad54afa2d3721d28` (на дату сверки **OPEN DRAFT**, не merged). Нормативные owner pins `bd138b2449f68cabb2cbfa3df552df8471f40c78`: [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), [owner alignment](../designs/rkb-owner-alignment-20261021.md), [acceptance](../testing/rkb-scalable-kaggle-acceptance-20261019.md).

Это **исполнительский план переноса уже реализованного GEO MVP**, не замена PR158, [описания старой очереди](../geo-resolution-queue.md) или [GEO-only cutover](../migrations/rkb-layers-only-cutover-20261019.md). Библиотеку/SQLite/backups/старые receipts не изменять и не удалять; не выполнять live PG, Kaggle, Street Story, Wiki/OSM, расписания, source implementation либо выдачу прав.

## 1. Код как есть: чёткая граница legacy и target

| Путь/операция | Реально существует | Недостаёт в текущем target |
|---|---|---|
| **Legacy очередь** | [`geo_resolution.py`](../../src/regional_knowledge/geo_resolution.py), `GEO_SCHEMA`: SQLite `rkb_geo_intents`, `rkb_geo_aliases`, `rkb_geo_attempts`, `rkb_geo_receipts`, `rkb_geo_watermarks`; `enqueue_mention` / `geo_revision_activated` в source/graph transaction. | Аналогичное *операционное состояние* в PG для новых принятых источников. SQLite не становится target corpus или второй authority. |
| **Legacy MCP** | [`server.py:964–1064`](../../src/regional_knowledge/server.py) содержит `geo_request`, `geo_claim` (не более **5**, lease **15–180 с**), `geo_stage`, `geo_apply`, `geo_status`, `geo_receipt`, `geo_recheck`. Источник — `GeoQueue(backend.corpus)`, не PG. | Target `KnowledgeMCPServer.call_tool` эти write/status tools в `kaggle_source` не допускает; наличие legacy tool definition не делает target callable. |
| **GEO-only PG чтение** | [`kaggle_geo_read.py`](../../src/regional_knowledge/kaggle_geo_read.py), [`kaggle_geo_backend.py`](../../src/regional_knowledge/kaggle_geo_backend.py), [`geo_read_gate.py`](../../src/regional_knowledge/geo_read_gate.py): существующие `GEO_READ`/WSS/read ACL и `geo_lookup/poi_context` по immutable private legacy manifest. | Нет claim/stage/apply новых source mentions. Backend честно возвращает `accepted_target_source_evidence=false`, это **не** полный новый `poi_context` с chunks/Story. |
| **Новые source places** | [`sql/kaggle/011_geo_identity_manifest.sql:314–375`](../../sql/kaggle/011_geo_identity_manifest.sql): `source_graph_documents`, `source_entity_ids` (`poi_ref`), `source_entity_representations`, `source_entity_mentions`, `source_entity_aliases`, `source_entity_references`, `source_entity_relations`, `source_poi_hypotheses`, `source_graph_receipts`. [`SQL026:731–762`](../../sql/kaggle/026_source_batch_activation.sql) проектирует их из принятого packet; [`source_graph_native.py`](../../src/regional_knowledge/source_graph_native.py) сохраняет page/region/accepted passage links. | Нет автоматического создания **GEO intent из accepted source mention** и source-scoped backlog на PG. Никакого повторного OCR/перечитывания книги не требуется. |
| **Retained GEO manifest** | PG `geo_manifest_heads/items/lookup_names/poi_links/receipts` в [SQL011](../../sql/kaggle/011_geo_identity_manifest.sql)/[SQL012](../../sql/kaggle/012_geo_dependency_bound.sql), immutable owner refs/legacy-unanchored provenance. | Это не writable очередь новых книг и не геометрическая/веб-верификация. Не смешивать с `source_entity_mentions`. |
| **Street Story POI owner** | [`poi_registry_bridge.py`](../../src/regional_knowledge/poi_registry_bridge.py): `search/get` → явный model `select/create` через существующий фиксированный Dev loopback `POST /v1/internal/rkb-poi`, owner receipt/idempotency и тот же RKB entity ID. [Street Story contract](https://github.com/onedayonemasterpiece/street-story/blob/main/docs/rkb-poi-link.md). | Его `GraphService(backend)`/`rkb_entities` — legacy, не target `source_entity_*`. Target allowlist не включает `poi_registry`. Не выдумывать новую Street Story POI таблицу. |
| **Обратный POI read** | Есть `GET /internal/street-story/poi-context`, отдельно авторизованный service bearer, и native [`KaggleSourceBackend.source_graph_fetch/story_get/fetch`](../../src/regional_knowledge/kaggle_public_backend.py). | У target нет `backend.corpus` для legacy `PoiRegistry.context`; `KaggleGeoBackend.poi_context` отдаёт лишь GEO manifest, не принятые passages/Story. Нужна сборка из *существующих* PG-read adapters за *существующим* endpoint. |

Реальная [однообъектная приёмка Cathedral/Кнайпхоф 10.10](../poi-model-link.md) относится **только к прежнему release/SQLite** и не доказывает PG target. PR158 определяет историческую адресацию GEO-A/B, но находится в открытом draft. В main нет специально выделенного PG-MVP queue-transfer плана; этот файл не правит решения PR158.

## 2. Исходное упоминание и отложенная GEO-работа

Нормальный `book_ingest(start → book_pages → stage)` в одном модельном проходе сохраняет дословное `exact_source_spelling`, тип/контекст места, `entity_id` и accepted `passage_id/chunk_id` + физическую страницу/регион. `source_poi_hypotheses` хранит POI-факты книги независимо от наличия Story. `source_entity_references`/aliases сохраняют точные явные ссылки, а не backend name matching.

**После фактического accepted text/graph apply** производный `geo_request` для принятого `poi_ref`/mention ставит/пробуждает intent по стабильному source+revision+mention+policy key. Pending/rejected stage, сырое OCR, vector-only shortlist и чужой private source **не** eligible. Withdrawal или superseded revision лишает старый intent права на новую мутацию, но не удаляет старые receipts. Geo enrichment не блокирует ни одну принятую страницу, обычный `search/fetch`, последующее `finalize` или сохранённый Story. Дополнительный graph/owner link приходит **позже**, без reimport/re-embed.

Модель (ChatGPT/dot) по **существующему** разрешённому расписанию получает не более пяти доступных claims, проверяет реальные Wikipedia/Wikidata/OSM/иные источники, сопоставляет тип здания/площадь/остров, эпоху, источник, соседний контекст и внешние ID. `poi_registry(search)` лишь даёт shortlist; модель выбирает существующий POI через `select` или отдельно мотивирует `create` candidate. Имя, расстояние и совпадение Wikidata-слова сами по себе не сливают RKB entity/Street Story POI и не создают точных координат.

## 3. Минимальное состояние и действующий протокол для будущего source/SQL PR

**Не добавлять доменные сущности**, новые RKB place identities, POI registry, OAuth, роли, сервис, WSS frame, очередь планировщика или самостоятельный воркер. Нужен **один additive versioned SQL migration** с операционными PG-аналогами уже существующих SQLite `geo_intents/aliases/attempts/receipts`, а `geo_watermarks` — только если подтверждена реальная необходимость в текущем `recheck/poll` (не добавлять автоматический poller). Номер миграции назначать после проверки чужих SQL-веток; SQL001–029 и старые accepted payload/receipt bytes неизменны.

| Legacy таблица | Минимальные PG поля/проверки (не второй доменный реестр) |
|---|---|
| `rkb_geo_intents` | actor/source_id/revision/entity_id/mention_id, original spelling, current visibility/source hash и immutable input/policy hash; FK/выборка из **существующих** `source_entity_ids/mentions/graph_receipts` и eligibility. |
| `rkb_geo_aliases` | нормализованное имя/явный owner external-ID *для поиска кандидатов*, индекс exact lookup; не choose/merge по совпадению. |
| `rkb_geo_attempts` | dependency kind/ref/revision, state/priority/available_at, bounded attempts, `lease_token/fence/until`, proposal + hash, final status; source-scoped fairness. |
| `rkb_geo_receipts` | actor/route/operation_key/payload hash/exact outcome, связанный с существующими `rkb_v4.applied_ops`/cursor/accepted replay; тот же ключ при потере ответа. |

**Adapter reuse:** на Dev действуют текущие `TargetServiceStartup`/OAuth/`ReadAuthority`, `ControlSpool`/`AcceptedSourceDispatch`/`HandlerRegistry` и Street Story owner bridge; на Kaggle — нынешние WSS native dispatch/PG lease/`kaggle_apply.py` и существующие source/geo read gates. `geo_claim`/lease меняет лишь операционное состояние в *короткой* PG-транзакции (limit<=5, lease 15–180 с, monotonically increasing fence, maximum 5 lease attempts, fairness **одна задача на документ в пачке**). Не создавать новый wire kind. **Прежде чем реализовывать, Opus/Контур должны проверить**, какой именно current native command/lease route поддерживает эту короткую mutation. Если такого compatible маршрута нет — точный technical blocker; не подменять его локальной SQLite state или новым transport.

`geo_stage` фиксирует typed proposal как `staged`, **не применяет** link. `geo_apply` повторно проверяет current OAuth actor/owner-source ACL, accepted revision, source withdrawal, mention/passage evidence, lease/fence/time, proposal hash и current Street Story owner ID/revision; только после этого атомарно связывает **тот же** RKB UUID с owner POI и сохраняет PG receipt. Внешняя проверка/`poi_registry` остаётся на Dev с существующим service credential; в Kaggle не отправляются token, arbitrary URL или клиентский «owner proof». Если PG-apply пока не может потребить current проверенный owner result через имеющийся accepted-command transport — это blocker для Opus/Контура, не повод добавлять issuer/manager.

**Точное восстановление:** timeout после Street Story `create` или PG apply = UNKNOWN до чтения текущего `poi_registry` owner receipt / `geo_receipt`. Использовать **тот же idempotency_key/operation_key и exact payload**, не создавать второго POI по новому key. Изменённый payload под старым key конфликтует. Сохранять traceable states/receipts через existing Dataset+ordered replay/snapshot protocol; не объявлять Saved по одному transport ACK. Краш между accepted intent, PG commit и delivery outcome не удваивает действие.

**Legacy GEO и миграция:** сохранённые `rkb_geo_*` SQLite, backup, PG immutable `geo_manifest_*`, подтверждённые stable Street Story/Cartography owner keys не удалять/переписывать. Старые очереди с old-book `chunk_id` не переигрывать как accepted target commands; переносится только whitelisted GEO identity и минимальная provenance. Связь с новой книгой — explicit reanchor по edition/source SHA и проверенным page/region, иначе `legacy_unanchored`. Старые stories, corpus, vectors, cases, private drafts, delivery attempts не переносить под видом GEO.

## 4. Web URL + дата отдельно от книжной цитаты (конкретный контрактный gap)

[`entity_graph.py::ResearchSource`](../../src/regional_knowledge/entity_graph.py) уже имеет `url/note`, в то время как `GraphEvidence` сохраняет **book** `chunk_id/page_id/region_id`. В [`geo_resolution.py::GeoProposal`](../../src/regional_knowledge/geo_resolution.py) разрешены лишь `status/reason/canonical_poi_ref/relation/layer_ref/geometry_ref`: URL и дата посещения **отсутствуют**. Поэтому future narrow source PR должен дописать только *версионированное поле существующего geo proposal/attempt/receipt* с bounded external evidence: `url` + фактическая `checked_at_utc` + note/утверждённый external ID, а `source_published_at`/historical epoch только когда известны. Не создавать ещё один provenance registry и не изменять старые accepted JSON. Отдельное время проверки веб-страницы **не** является датой события, названием 1910 года, датой книги или доказательством координат. Никаких реальных веб-вызовов в данном PR.

**Неполное существующее перечисление статусов:** `GeoProposal.status` = `candidate|unresolved|ambiguous|outside_coverage|rejected`; отдельного `not_found` нет. Если поиск не дал подходящего POI, оставить `unresolved` + конкретные проверенные URL/date/coverage и reason «не найдено среди проверенных источников», а не глобальное «объекта нет». Несколько совпадений → `ambiguous` (не select); непокрытая территория/эпоха → `outside_coverage`; owner outage/partial/read unavailable → retry/pending **не** `not_found`.

## 5. Сохраняемая семантика статусов, безопасности и наблюдаемости

- `geo_request(entity_id,cursor,limit<=20)` — только уже принятые владельцем дословные source mentions; не backfill всей книги и не выгрузка чужого private PDF.
- `geo_claim(limit<=5)` — fenced lease с actor/source scope, одинаковый timestamp не отменяет queue fairness; expired `leased/staged` возвращаются строго по доказанному expiry, исчерпание → видимый `retry_exhausted`.
- `geo_stage` — `candidate` с **реально** существующим owner ID и rationale либо `unresolved/ambiguous/outside_coverage/rejected`; spatial relation не объявляется historic owner-verified из POI shortlist.
- `geo_apply` — current source/revision/actor/owner binding, один exact durable receipt; `stale_source`, `blocked_access`, `owner_revision_changed` не дают успешной привязки.
- `geo_status(request_id/entity_id)` — actor-only bounded items + state_counts/oldest_pending, pending/ambiguous/no match, попытки и last reason. `geo_receipt(operation_key)` — exact actor-bound read-after-lost-reply. `geo_recheck(name, owner_poi|cartography_layer, ref, revision)` — только новое проверенное dependency и exact normalized name, не беспредметный повтор.
- Current source ACL/withdrawal/revision и OAuth/`read_authority` проверяются перед выдачей и после late result. Public source passage не раскрывает original scans/private Case/other actor queue; `scan_proof_read` и оригиналы — независимые права. Street Story bridge использует только свой existing scoped service key, **не** `STREET_STORY_DEVICE_TOKEN`, и не притворяется пользовательским OAuth.
- Историческая точная геометрия — Cartography owner; физический POI и реальные нынешние координаты — Street Story owner. Candidate POI может иметь `latitude/longitude=null`. Не превращать ближайшую OSM pin в координату исторического собора, замка или утраченной кирхи.

## 6. GEO-B: тот же `poi_context`, но source-aware PG

В [`kaggle_geo_backend.py::poi_context`](../../src/regional_knowledge/kaggle_geo_backend.py) сейчас только retained GEO manifest; в [`poi_registry_bridge.py::context`](../../src/regional_knowledge/poi_registry_bridge.py) — legacy `StoryRegistry(backend.corpus)`. Переиспользовать текущий target `SOURCE_GRAPH_READ`/`source_graph_fetch`, source mentions/accepted passage refs и уже реализованные `story_get`/`fetch`; не выдавать результаты из одного immutable GEO manifest за source-grounded story.

Оставить **тот же** Dev `GET /internal/street-story/poi-context` и Street Story `GET /v1/pois/{poi_id}/knowledge`. В Dev endpoint заменить target-incompatible проверку `backend.corpus`/legacy graph access на существующий авторизованный native PG source read, не менять bearer/URL/API и не добавлять WSS frame. Результат: RKB entity UUID, exact `passage_id/chunk_id` (обычный `fetch`), связанное сохранённое Story ID/body (обычный `story_get`), facts/relations по current scope; только разрешённые компоненты. Отсутствие Story не удаляет факт/POI mention.

**Зависимая незакрытая read-приёмка:** Шкипер сообщил в основном треде 1792773763, что после PR346 `story_get(editorial)` для реального Gause5–7 дал `internal_error` из-за PG role access к `source_stage_pages`. Ремонт принадлежит владельцу read-slice/Шкиперу, **не** этому GEO PR. До исправления нельзя объявлять будущий `poi_context→Story` приёмочно готовым.

## 7. Предлагаемый порядок отдельных будущих source-пакетов

1. **GEO-A1, очередь:** одна additive native migration + exact `source_entity_mentions → geo_intents` после APPLIED, scoped PG claim/stage/receipt/read tests с owner/fence/source revoke. Проверить WSS native mutation capability до записи. Без другого планировщика и без import gate.
2. **GEO-A2, управляющий MCP:** те же `geo_request/claim/stage/apply/status/receipt/recheck` в `kaggle_source/full` через текущий controller/auth/source ACL, тот же operation-key/recovery. Никаких новых сервисов.
3. **GEO-B1, POI:** model `poi_registry(search/get/select/create)` через уже настроенный Dev→Street Story bridge; перевести source lookup и post-owner receipt RKB link с legacy `rkb_entities` на принятый `source_entity_*`, не менять owner POI semantics.
4. **GEO-B2, контекст:** тот же `poi_context` и internal bridge на current source graph/chunk/story. Read actor/source grants и "не найдено" явно различают incomplete/not_ready.
5. Не входит: GEO-C (historic building geometry/house renumbering), новый gazetteer/ML, mass backfill, новая расписанная задача, новый owner registry, импорт старого non-GEO корпуса.

## 8. Будущая operator acceptance — **NOT RUN**

| Case | Проверяемое поведение |
|---|---|
| **G1: Гаузе** | Принятые страницы с реальными объектами (замок, собор, кирхи) сохраняют дословный RKB `poi_ref` mention, current page/region/accepted chunk и независимые source facts; доступен обычный `search/fetch` без GEO wait. Не подменять их синтетическими landmarks. |
| **G2: очередь** | `geo_status` показывает все нужные current owner intents и неразобранные attempts; `geo_claim<=5`, lease/fence, fairness между книгами, pending age и retries; other actor private backlog недоступен. |
| **G3: проверка web/POI** | Реальные Wikipedia/Wikidata/OSM URL с датой проверки отдельно от книжного evidence; модель выбирает 1 owner POI или `create(candidate)` под текущим owner key с объяснением. 0/N → `unresolved/ambiguous`, не авто merge. |
| **G4: POI + координаты** | Где Street Story/Cartography **действительно подтвердили** owner coordinate/precision, выбранный landmark имеет канонический POI + координаты; для lost/unsupported geometry `null` с причиной, не выдуманная pin. Реальная геометрия/эпоха — отдельное подтверждение. |
| **G5: `poi_context`** | По выбранному `streetstory://poi/<id>` возвращаются только current-authorized Gause chunks/passages и связанные **Saved** Stories; `fetch` читает точный passage, `story_get` — body; Street Story knowledge endpoint использует свой existing grant. |
| **G6: отрицательные** | Non-owner/withdrawn/stale/lease-expired не читают/не применяют чужие задачи; N name matches не привязываются; owner timeout/replay не создаёт второй POI; 5 attempts → visible exhausted; changed dependency → bounded recheck. |
| **G7: восстановление** | Exact PG accepted receipts/lease/fence и старые SQLite/GEO-only data/backup сохранены; crash после owner create / PG effect восстанавливается по тому же key, duplicate replay не создаёт эффект; independent search остаётся рабочим. |

Нет настоящих новых web searches, POI create/link/coordinates, `geo_stage/apply`, расписаний, SQL/PG/provider/live mutations или новых performance измерений в этом doc PR; **не заявлять live PASS**. Приёмка следует **после** отдельного source/SQL решения и отдельного разрешения Шкиперу.

## 9. Вопросы только для технического review и ownership

- **Контур (PR158) + Opus:** подтвердить один existing native command/lease путь для PG queue, однократную additive migration и минимальное web `URL+checked_at` поле в *существующем* proposal/receipt; при несовместимости назвать точный технический blocker вместо нового RPC/service. Это уточнение PR158 GEO-A/B, не конкурентный дизайн.
- **dot:** future source ownership по срезам выше; внешнюю модель запускать исключительно существующим механизмом после разрешения, **без нового scheduler**. Не переносить/продолжать unknown jobs Контура.
- **Шкипер:** sole runtime/PG/provider/Street Story live оператор. Проверяет реальные G1–G7 после соответствующего source PR и readiness; полный импорт Гаузе/статьи не ждёт GEO.
- **RKB-Удаление:** только эта документальная инвентаризация/план; никаких production hunks, SQL, повторных GEO/POI эффектов или merge.

**Evidence/source pins:** main `a2339b60c40461b6874ff75646e56b8dc89a15b4`, PR158 `23d53f514470dfbf7b97d1f6ad54afa2d3721d28` (draft), original owner requirements `bd138b2449f68cabb2cbfa3df552df8471f40c78`. Исторический Oct-10 POI успех — legacy, не PG acceptance.
