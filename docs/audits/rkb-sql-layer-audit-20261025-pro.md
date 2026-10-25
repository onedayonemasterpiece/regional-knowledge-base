# Аудит слоя PostgreSQL RKB — 25 октября 2026

**Аудитор SQL (ChatGPT Pro, заказ RKB-Opus).** База аудита: свежий `main`, коммит `401f308e553e1b834f3526130595f0a68bdbb449`. Статический аудит; изменения продукта, установки и испытания не выполнялись.

## Итог и границы доказательства

Найдено **восемь новых пунктов**: четыре конкретных отказа или неверных результата на достижимых путях, два обхода растущей истории, запрет исправить ещё не применённую SQL-миграцию и лишнее преобразование native-векторов через JSON. Ниже «достижимый» означает последовательность, выведенную из текущего producer → SQL → reader/controller. Это **не сообщение о наблюдавшемся инциденте**. Задержки, p95, страницы/час и планы PostgreSQL не измерялись.

Самые прямые вычитания: убрать source-wide revision equality из exact passage fetch; убрать два сравнения полного набора источников из чтения личного черновика; снять online replay Case; заменить first-write-wins иллюстрации уже существующей обновляемой проекцией. Исправление migration barrier ограничено **доказанным окончательным отказом до DDL**. Частично применённую миграцию и UNKNOWN этим правилом освобождать нельзя.

Рамки владельца сохранены: Kaggle / PostgreSQL 17 / pgvector / BGE-M3; versioned Dataset как основное хранение, S3 как недельная копия; один поток импорта; книги переимпортируемы; подтверждённая работа людей и exact outcomes сохраняются. Предложений новых подсистем, режимов, очередей или проверок SHA нет. Чтение книг доступно всей закрытой группе; приватность авторских черновиков остаётся.

### Что действительно прочитано

| Область | Покрытие |
|---|---|
| SQL | **Все 37 `sql/kaggle/*.sql` целиком**, 9 881 строка. Для каждой функции учитывались последующие `CREATE OR REPLACE`, `RENAME` и `DROP`; ранняя definition не выдаётся за текущую. |
| Обязательные документы | Целиком `AGENTS.md`, `docs/architecture.md`, `docs/storage.md`, `docs/designs/rkb-owner-alignment-20261021.md`, `docs/review/simplification-review-rules.md`; локальные Kaggle skill/profile/migrations; документ `docs/audits/rkb-simplification-tasks-20261025-opus.md` из ветки `docs/rkb-simplification-tasks-20261025`. |
| Координация | Issue [#481](https://github.com/onedayonemasterpiece/regional-knowledge-base/issues/481), состояния связанных PR; две последние страницы общего [Slack-треда](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792868771439369). Вся более старая переписка не прочитана. |
| Предыдущий аудит | Прочитаны сводка, все ID/заголовки и относящиеся к SQL/search/import/recovery разделы [предыдущего Pro-аудита](rkb-simplification-audit-20261025-pro.md). **Не заявляется полное повторное чтение всех 829 строк.** |
| Python целиком | `kaggle_query`, `kaggle_search`, `kaggle_worker_query`, `kaggle_owned_pg`, `kaggle_source_commands`, `kaggle_source_materializer`, `kaggle_source_batch`, `kaggle_domain_bindings`, `kaggle_native_source_bindings`, `kaggle_current_registry`, `kaggle_schema_owner`, `kaggle_schema_live`, `kaggle_case_read`, `kaggle_cases_backend`, `kaggle_editorial_read`, `kaggle_story_editorial_backend`, `disagreement_native`, `disagreement_contracts`, `story_editorial`, `geo_manifest_commands`, `source_stage`, `source_batch_selection`, `source_batch_context`, `kaggle_source_read`, `kaggle_story_read`, `kaggle_passage_illustrations`, `kaggle_public_illustration_native_read`, `kaggle_illustration_preview` — имена файлов `.py` в `src/regional_knowledge/`. |
| Python по вызывающим путям | Проверены relevant ranges `kaggle_apply`, `kaggle_domain_schema`, `kaggle_schema_migrate`, `kaggle_pg_snapshot_native`, `pg_snapshot_recovery`, `control_spool._validate`, recovery dispatch/phases, service startup, native intake, public backend, source/case read gates, contracts, stage graph, manifest/activation, importer. Это не полное чтение всех больших файлов. |
| Полнота поиска символов | Получены все 233 файла `src` и полный git tree; поиск `pg_proc`, `fingerprint`, `PgFunction`, `HandlerSpec`, `schema_revision`, `function_revision`, `executor_revision`, имён функций и writers проводился по этому снимку. Отрицательные ответы GitHub code search не использовались как доказательство отсутствия. |
| Scripts и тесты | Все 86 `.py/.sh` под `scripts/` покрыты поиском символов; целиком прочитаны migration-request, warm-read-builder, BGE worker/transport и два коротких control scripts. Manifest JSON не включались. Тестовые assertions прочитаны выборочно; карта ниже. **Ни один тест или продуктовый script не запускался.** |
| Не покрыто | Живые PG catalog/grants, EXPLAIN, runtime/log timings, реальные Dataset bases/tails, содержимое текущего пользовательского журнала и число команд каждой старой схемы. Нельзя вывести эти числа из наличия decoder в Python. |

Статический инвентарь: **213** конструкций `CREATE [OR REPLACE] FUNCTION`, из них **60** с именем `check_*`; **5** переименований и **2** удаления функций. После последовательного свёртывания файлов остаётся **179 различных имён функций**, включая **43 имени `check_*`**. Это подсчёт исходников, не live `pg_proc` и не число перегрузок. **86** лексических `CREATE TRIGGER` также не означают 86 установленных triggers: есть шаблоны внутри циклов и поздние удаления. Список текущих `check_*` приведён в приложении.

## Сводная таблица

Уверенность относится отдельно к механизму и к ожидаемой эксплуатационной величине. P1 — прямой отказ/неверное чтение либо работа, растущая с накопленной историей, на критическом пути; P2 — более узкое препятствие или ограниченный лишний объём. Нумерация `SQLA-*` не заменяет прежние `SQL-*`, `I4`, `I5`, `RT03`.

| ID | где (file:line) | элемент | какая цель задета и на каждый ли запрос/пакет | реальный сбой | чем уже покрыт | вердикт (УДАЛИТЬ / ЗАМЕНИТЬ на … / ОСТАВИТЬ с тестом) | выигрыш (вызовы, строки, работа на запрос) | нужна ли миграция/переустановка | риск | уверенность | копии в слоях |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **SQLA-01 / P1** | `kaggle_query.py:308–319`; `004_read_fences.sql:54–82` | Source-wide eligibility на exact passage fetch | Надёжность fetch; когда между search и fetch добавлен новый passage той же активной книги | Неизменный фрагмент получает `evidence_withdrawn` | Exact passage/source/revision, active source, accepted/not-withdrawn, database generation | **УДАЛИТЬ** equality, лишний SELECT column и JOIN на этом пути | Убирает ложный отказ и один JOIN; ms не измерены | SQL migration нет; установить Python read handler | Не снять настоящую withdrawal/revision/DB проверку | Высокая | SQL trigger → locator Python → SQL read → gate; частичные тесты continuation |
| **SQLA-02 / P1** | `kaggle_schema_owner.py:121–151`; `kaggle_schema_live.py:120–123` | Barrier после окончательного отказа до DDL | Импорт/writes и обычный checkpoint; только при неверном неприменённом плане | Сохранённый reject повторяется, `done=0` блокирует исправленный plan | Durable phase receipt, неизменный applied prefix, existing lifecycle | **ЗАМЕНИТЬ на** завершение доказанно pre-DDL rejected request с сохранением receipt | Конечный отказ перестаёт бессрочно держать intake/write barrier | Python controller; SQL/data migration нет | Нельзя освобождать partial/UNKNOWN | Высокая для узкого сценария | Controller SQLite → dispatch receipt → live validation; checkpoint test покрывает другой случай |
| **SQLA-03 / P1** | `026_source_batch_activation.sql:822–824`; `kaggle_passage_illustrations.py:8–25` | First-write-wins mutable geometry | Корректность illustration fetch; каждое принятое исправление той же figure identity | Новые 90° приняты, публичный crop остаётся 0° | Checked current page/figure, immutable publications, exact-once apply | **ЗАМЕНИТЬ на** existing serving projection UPSERT, latest accepted repair | Исправление становится читаемым без полного переимпорта | Новая SQL migration + установка; repair существующей projection | Сохранить old publications/locators и порядок | Высокая; restore test не выполнен | Pydantic/compiler → SQL media → apply → renderer; старый immutable trigger уже удалён |
| **SQLA-04 / P1** | `010_disagreement_cases.sql:419–432`; `031_case_packet_evidence_provenance.sql:21–27` | Draft dependencies должны равняться всему нынешнему Case inventory | Надёжность fetch сохранённой работы; после добавления evidence из новой книги | Автор теряет draft в get/list, хотя текст/роль/исходная книга прежние | Current Case role/source checks, draft author, exact version | **УДАЛИТЬ** только два read-предиката | Возвращает сохранённое тело без переписывания истории | Новая SQL read-function migration + установка | Publish predicate отдельно: **BLOCKER retained replay** | Высокая | SQL read и write; Python private selector сохранён; старый fixture не покрывает рост inventory |
| **SQLA-05 / P1** | `kaggle_case_read.py:90–105`; `031_case_packet_evidence_provenance.sql:108–243` | Полный replay Case перед bounded read | Case search/fetch и mutation preflight; каждый соответствующий read | История и full-book manifests занимают бюджет bounded read, verifier false закрывает чтение | Atomic apply/receipt, PK/FK, current actor/source checks; не эквивалент произвольной anti-corruption сверки | **УДАЛИТЬ** два online вызова и boolean gates | Минус один verifier SQL и его timeout setter, replay всей истории каждого выбранного Case | Python reader; SQL function можно пока оставить | Ошибка supported apply могла бы остаться незамеченной; нужен различающий тест | Высокая в работе; задержка не измерена | Online Python → SQL replay → source manifests; обычный editorial reader этого не делает |
| **SQLA-06 / P1** | `036_source_batch_media_scoped_lookup.sql:12–40,124–126` | Повтор истории stage для current media links | Скорость импорта; один новый semantic prepare с непустыми links | Работа растёт со всеми прежними packets книги при фиксированном текущем пакете | Exact accepted packet + current native compiler и DB membership | **УДАЛИТЬ** history helper call и зависящие raw-link дубли | Минус H декодирований прежних payloads; для N growing linked packets суммарно O(N²) | Новая SQL migration + установка | Сохранить A→B→A semantics; direct prepare boundary | Высокая в механизме, средняя в безопасном объёме вычета до теста | Compiler и SQL; есть тест буквального равенства validator implementation |
| **SQLA-07 / P2** | `kaggle_schema_live.py:101–105,115–138` | Неприменённый SQL candidate объявляется immutable | Повтор/исправление миграции; после ошибки первого pending файла | PostgreSQL откатил файл, worker всё равно отвергает исправленные bytes | Ledger уже фиксирует только committed prefix, SQL+ledger атомарны по файлу | **УДАЛИТЬ** защиту candidate bytes до принятия ledger; оставить applied-prefix guard | Снят лишний запрет исправления непринятого файла; controller/job reconciliation отдельно | Python worker; SQL migration нет | Ранние файлы bundle могли commit; их менять нельзя | Высокая | In-memory candidate pin + PG ledger; barrier SQLA-02 отдельный слой |
| **SQLA-08 / P2** | `026_source_batch_activation.sql:773,789,812,846–860` | Vector → real[] → JSONB → vector | Скорость импорта; каждый новый text/semantic apply | Лишняя сериализация под writer lock, в том числе до конфликтующих INSERT | Existing vector PK/type/norm/input binding | **ЗАМЕНИТЬ на** прямой lookup native vector и удалить один helper | Минус 1024 float encodes/parses на каждый passage/default Story vector | Новая SQL migration + установка; reembedding нет | Не перепутать component и first-fragment fallback | Высокая в преобразованиях; ms не измерены | Python install → typed PG vector → JSON helper → typed PG vector |

### Что уже учтено в прежнем реестре

На момент проверки [PR488](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/488) и [PR489](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/489) оставались открыты. Формулировка задания «уже исправленное PR488» поэтому трактуется как **занятая задача**, а не как доказательство merge. SQL-7 / `source_passage_metadata_v1` и first-packet provenance здесь повторно не назначаются. Worker terminal/error handling PR489 также не пересоздаётся: SQLA-02 — отдельный persisted controller lifecycle при уже имеющемся definitive receipt, SQLA-07 — in-memory SQL candidate.

PR486, 487, 490, 491, 492, 493 входят в проверенный main. SQL-1…SQL-6, I4/I5 и RT03 остаются под прежними ID: scoped dense scan, runtime catalog/FTS, повтор source reads, timeout setters, QUERY serialization, fragment hashes, contract/inner-check duplicates, counters и полный applied_ops capture соответственно. Здесь уточняются реальные текущие callers и кратность; новый ID за то же действие не выдаётся.

## A. Форма JSON в SQL и настоящие предусловия состояния

### Граница доверия и прямые вызовы

`rkb_writer` имеет table DML и EXECUTE всех функций; `PUBLIC` отозван, `rkb_reader` получает ограниченный набор SELECT/EXECUTE. Это видно в [kaggle_owned_pg.py:74–84](../../src/regional_knowledge/kaggle_owned_pg.py#L74-L84), [118–145](../../src/regional_knowledge/kaggle_owned_pg.py#L118-L145). Следовательно, технически writer может обойти Python. Но это доверенный собственный исполнитель, способный также непосредственно писать таблицы; роль не является sandbox от собственного повреждённого кода. Обосновывать все дубли одной возможностью ручного privileged SQL нельзя.

Обычный command проходит Python validation перед acceptance: [control_spool.py:499–583](../../src/regional_knowledge/control_spool.py#L499-L583), [kaggle_native_source_bindings.py:21–39](../../src/regional_knowledge/kaggle_native_source_bindings.py#L21-L39), затем общий apply. **Есть настоящая отдельная граница:** `prepare_source_batch_v1` и `install_source_batch_vector_v1` вызываются прямо из [kaggle_source_batch.py:31–91](../../src/regional_knowledge/kaggle_source_batch.py#L31-L91), до canonical apply, своими транзакциями. Их writer/database-generation/accepted-stage guards нельзя удалить со ссылкой на внешний `apply_accepted`. Retained whole-book prepare/install также вызываются прямо в `kaggle_source_materializer.py:700–703,761–764`; их writer guards принадлежат той же отдельной границе, хотя это не обычный current packet path.

По всем загруженным `src` и 86 scripts найдены эти direct prepare/install callers, registry-dispatched applies и legacy utility code. Иного production producer current semantic media помимо native compiler не найдено. Тесты, вручную меняющие compiled JSON и вызывающие SQL, существуют; это нужно отделять от продуктовой входной границы. Живые расширенные GRANT/ручные операторы не обследовались.

### Карта проверок: удалять чистую форму, оставлять состояние

Таблица группирует все 43 текущих `check_*`; точные имена и final definitions перечислены в приложении. «Дубль» здесь не значит разрешение удалить функцию целиком. Pure grammar отделена от атомарной проверки существующих строк, версии, владельца, CAS и exact accepted identity. Частота legacy-функций не приравнивается к каждому новому пакету.

| Семейство и final SQL | Что повторяет форму/вычисление входа; Python counterpart | Что остаётся в PostgreSQL; достижимость |
|---|---|---|
| Stage v1/v2/packet, seal v1/v2 — SQL013:54,229; SQL014:110; SQL026:988–1179 | Envelope/keys/UUID/base64/model shape повторяют `source_stage.py:112–197,200–244`; current intake `kaggle_native_source_intake.py:533–559` | Session/ingestion identity, accepted source, exact retry, predecessor/order — состояние. Current binding `kaggle_native_source_bindings.py:43–49` выбирает v2. Полный packet check выполняется **трижды** на новое apply: уточнение I4 ниже. |
| Batch base — **SQL029:4–53** | Codec/model/source, непустой текст, language/token bounds, локальная уникальность/order; `_typed` и Pydantic `kaggle_source_materializer.py:49–62,443–447,492–517` | SQL029 уже сняла вторую полную closed schema из026/027. Не предлагать повторное удаление старого тела. Persistence bounds/BGE space оставлять. |
| Batch semantic; passage evidence; facts/quoted facts — **SQL028:4–68**, renamed SQL008:46–123 | Внутренние ссылки, evidence indexes, passage/region присутствие; materializer `_semantic:529–623`, особенно555–569,597–599; legacy `source_import_v3.py:12–38` | Stronger manifest association в graph checker не тождественна простой ссылке. Legacy original_excerpt branch остаётся транзитивным. Два fact checks в prepare и третий при projection — уточнение I4/F, не новая задача. |
| Batch/source/image manifests и passage checks — SQL009:310–408; **SQL025:2–146**; SQL026:415–488 | Keys, region/metadata grammar и reconstruction body; `source_manifest.py:96–213`, `source_activation.py:102–229`; current materializer515–517 | Actual stage session/page/rights/source association остаётся. SQL025 уже заменила старые repeated scans014 на maps. Text compile использует другой validate flag; нельзя механически объявить оба пути одинаковыми. |
| Batch/source/image graph и evidence — SQL011:379–501; SQL014:396–532; **SQL032:7–109** | Внутренние entity/relation kinds и refs; materializer462,485–489,518–520; `source_batch_context.py:29–36` | Actual persisted entity owner/kind/reuse в SQL032:23–43 остаётся: compile и prepare не одна транзакция. Оптимизация старых nested loops уже есть в032. |
| Batch media / legacy illustrations — **SQL036:45–176**, SQL020:18–137 | Closed schema/figure/link/crop projection; `contracts.py:221–290`, `stage_graph.py:244–273`, materializer215–295 | Current page/figure/source membership сохраняется. Историческая reconstruction отдельно SQLA-06. Не доказано полное один-к-одному соответствие всех legacy020 predicates одному Python validator; blanket deletion не обоснован. |
| Batch apply / finish — SQL026:862–882,943–952 | Небольшой command envelope; `source_batch.py:14–42` и native bindings | Prepared bytes/identity, conflicts, vector completeness/hash, source/session/receipt — DB state. Batch check дважды: I4. Finish production `kaggle_native_source_intake.py:594–616` сначала закрывает pending tails; SQL body сам по себе не доказывает early-finish bug. |
| Activation base/semantic/materialize/activate/wrapper — SQL014:132–215,711–736; **SQL023:2–148**, **SQL018:244–282**, SQL020:161–169 | Command envelope — `source_activation.py:58–79,233–255`; manifest —102–194; native knowledge — `kaggle_source_materializer.py:443–488,529–623` (legacy compile caller689–690). `source_import_v2.py:45–134` — исторический аналог, не вызов native compiler | Candidate/materialization/accepted receipt/order/counts и текущие persisted region/story IDs остаются. Routes в registry сохранены; новый packet producer не делает это работой каждого пакета. |
| Import v1/v2/v3/v4/graph — SQL006:33;007:96;008:194;009:525;011:503 | Вложенная grammar `source_import.py:47–115`, `source_import_v2.py:45–143`, `source_import_v3.py:12–38`, `source_manifest.py:96–213`; actual graph boundary `source_graph.py:129–150` из domain binding25–34 | Current registry всё ещё содержит `source.import.v4` и transitive graph→v4→v3→v2→v1. Доказанные source/origin/current identity checks не являются формой. Отсутствие нового producer не доказывает отсутствие retained tail. |
| Story edit/access — SQL009:93–161,204–229 | Envelope, action grammar, block/enums/vector shape; `story_editorial.py:35–93`, backend204–219 | Editor/manager role, Story/source/current variant/assertion revision, CAS, global ID uniqueness — SQL009:105–110,134–153,215–219. Оба current routes живы. |
| Case — SQL010:107–254, current apply SQL031:15–27 | `case_exact_keys`, `case_command_types`, body shape/enums повторяют `disagreement_native.py:44–106`, `disagreement_contracts.py:8–124` | Current role/source/CAS, evidence exact quote/identity, contribution/reply/draft author/version/span — состояние. Draft historical-to-current equality отдельно SQLA-04; его write-изменение не смешивается с чистой grammar. |
| Story reconcile — SQL016:173–259 | Смешанная grammar intent с DB lookup; Python public `story_reconcile` относится к legacy SQLite, `server.py:1183–1196` | Native registry caller SQL016 не найден. Installed trigger seed всё ещё есть016:26–35. Без demonstrated current path не обещать ускорение и не объявлять migration целиком мёртвой. |
| GEO install v1/v2, resolve — SQL011:67; SQL012:4–128; SQL035:67–89 | Header/item/codec/arrays grammar: `geo_manifest_commands.py:65–128`; resolve command shape: `kaggle_source_geo_mvp.py:82–101`, binding124–130 | Current native install выбирает **v2**; сохраняются installed staging/index, accepted bytes/digest, owner/dependency completeness, one-active state. Resolve проверяет actual mention/source/revision. Это не обычный book-search guard. |
| Withdraw / catalog — SQL005:4–43; **SQL034:11–33** | Withdraw envelope уже `control_spool.py:571–583`; catalog codec/shape уже `source_catalog_command.py:26–57`, как явно реализует SQL034:24–25 | Withdrawal expected revision/current state остаётся на mutation. Catalog SQL уже сокращён до current source + accepted first stage; новое blanket удаление не требуется. |

Ссылки на защищённые Python файлы в этой карте — **доказательства существующей границы**, не предложение править их. Все восемь новых рекомендаций обходятся без изменения 12 fingerprint inputs. Если исполнитель обнаружит необходимость их менять, это **BLOCKER fingerprint**, отдельная остановленная часть, а не разрешение расширить PR.

## B. Проверка установленных функций, revisions и миграционный lifecycle

### I4: что действительно осталось на каждом apply

В [kaggle_apply.py:330–340](../../src/regional_knowledge/kaggle_apply.py#L330-L340) `_function_contract` читает `pg_proc`; на новое apply вызывается для check и apply в [408–409](../../src/regional_knowledge/kaggle_apply.py#L408-L409): **два catalog SELECT**. Проверяются ABI/свойства функции, не SHA её SQL body. Duplicate resolving branch [385–398](../../src/regional_knowledge/kaggle_apply.py#L385-L398) проходит раньше и не повторяет эффект.

Поля `definition_sha256` названы исторически: текущая сборка [kaggle_current_registry.py:13–23](../../src/regional_knowledge/kaggle_current_registry.py#L13-L23) получает их от **фиксированной signature**, а не `pg_get_functiondef`. `HandlerSpec.fingerprint` [kaggle_apply.py:172–177](../../src/regional_knowledge/kaggle_apply.py#L172-L177) собирает небольшой descriptor; сохранённый `handler_sha256` участвует в canonical receipt [430–443](../../src/regional_knowledge/kaggle_apply.py#L430-L443). Утверждение «на каждом apply хешируется тело SQL» для этого main неверно.

`HandlerRegistry.resolve` [194–198](../../src/regional_knowledge/kaggle_apply.py#L194-L198) выбирает **route**, а не историческую SQL definition по tuple revisions. `before/after` сейчас берутся из audit revisions команды [402–403,421](../../src/regional_knowledge/kaggle_apply.py#L402-L421). Это ещё не делает сохранённые поля мусором: их читают historical receipt/command paths, например [kaggle_source_commands.py:151–161,409–410](../../src/regional_knowledge/kaggle_source_commands.py#L151-L161). SQL017:44–48 уже удалила migration-transition functions и старые schema/function/executor columns из recovery state. Native database probe не перечисляет весь catalog; возвращает пустой capability payload и ledger state ([kaggle_database_probe.py:43–87](../../src/regional_knowledge/kaggle_database_probe.py#L43-L87)).

**Вердикт по существующей I4:** удалить per-command catalog reproof и внутренние повторные checks там, где единственный supported caller — common apply в той же транзакции. Ordered migrations/ledger и нормальный function invocation уже владеют установкой и исполнением. Не добавлять новый fingerprint cache или другой gate. Самый сильный контраргумент — реальная неправильная SECURITY/volatility/ABI replacement при той же установленной migration. Один различающий тест: поддержанная migration создаёт такую несовместимость, common call/transaction её не предотвращают, а `_function_contract` единственный предотвращает конкретный неверный effect/outcome. Пока такого supported сценария не показано, extra catalog query не оправдан; failed SQL invocation откатит транзакцию. Этот тест **предложен, не выполнен**.

Уточнение кратности I4: `source.stage.v1` вызывает packet checker **3 раза**: outer → `check_source_stage_v2` (SQL026:1165), inner `apply_source_stage_v2`→v2 (1176), затем `apply_source_stage_packet_v1`→packet (1141). Current v2 binding — `kaggle_native_source_bindings.py:43–49`. `source.batch` — **2 раза** (outer и SQL026:769). Media validator туда не входит: он выполнялся один раз при prepare. Нельзя заявлять тройной media replay на apply. Direct prepare/install guards из A остаются.

### SQLA-02. Окончательный отказ до DDL оставляет вечный migration barrier

**Цель:** импорт/writes и продвижение обычного checkpoint; частота — только ошибочный migration plan. Чтения могут продолжаться: это не полная остановка search.

1. [kaggle_schema_owner.py:21–47](../../src/regional_knowledge/kaggle_schema_owner.py#L21-L47) принимает shape-valid target/bundle digest, сохраняет request в SQLite. Это ещё не проверка actual bundle. Same key возвращается; любой другой plan при `done=0` получает `migration_existing_request_pending` (43–45).
2. `advance` после drain выставляет `barrier=1` [121–129](../../src/regional_knowledge/kaggle_schema_owner.py#L121-L129). `schema_migrate` с неверным target/digest получает `MigrationDenied('migration_version_changed')` в [kaggle_schema_live.py:120–123](../../src/regional_knowledge/kaggle_schema_live.py#L120-L123), **до acquire/migrate/DDL**.
3. [kaggle_recovery_phases.py:297–310](../../src/regional_knowledge/kaggle_recovery_phases.py#L297-L310) возвращает `rejected`, `retryable=False`. Dispatch сохраняет exact result; повтор не создаёт retry-key для terminal rejection ([kaggle_recovery_dispatch.py:102–114,216–232](../../src/regional_knowledge/kaggle_recovery_dispatch.py#L102-L114)).
4. Owner [143–145](../../src/regional_knowledge/kaggle_schema_owner.py#L143-L145) бросает исключение, а цикл [101–104](../../src/regional_knowledge/kaggle_schema_owner.py#L101-L104) только логирует. Единственное `done=1,barrier=0` — после **успешного** schema checkpoint [148–151](../../src/regional_knowledge/kaggle_schema_owner.py#L148-L151). Повтор возвращает тот же reject; исправленный plan запрещён.

`pending/intake_allowed/writes_allowed` [56–64](../../src/regional_knowledge/kaggle_schema_owner.py#L56-L64) сохраняют блокировку. Startup подключает их к intake и authorisation ([kaggle_service_startup.py:308–310,337–345](../../src/regional_knowledge/kaggle_service_startup.py#L308-L310)); refresh с pending возвращается перед обычным checkpoint path [518–523](../../src/regional_knowledge/kaggle_service_startup.py#L518-L523). Restart сохраняет SQLite row. `resume_attachment` [66–96](../../src/regional_knowledge/kaggle_schema_owner.py#L66-L96) не освобождает barrier и исключает уже done rejected job.

**Минимальная замена:** по существующему definitive receipt, однозначно означающему отказ **до запуска DDL**, завершать это намерение существующими `done=1,barrier=0`, сохраняя job/result/operation key. Исправленный plan получает новый key. Никакого нового rollback manager, статуса, таймера или ledger. Уже committed migration prefix принадлежит PostgreSQL ledger ([kaggle_schema_migrate.py:150–175](../../src/regional_knowledge/kaggle_schema_migrate.py#L150-L175)).

**Сильнейший контраргумент:** часть bundle могла commit, ACK мог потеряться, runtime/schema могли разойтись. Поэтому blanket unlock по любому `rejected`/exception **не предлагается**. Если existing result не доказывает pre-DDL/unchanged prefix, вычет для этого случая блокируется. Обычная SQL syntax error здесь не доказательство: `schema_migrate.py:169,177–179` её пробрасывает; phase ловит только перечисленные исключения, generic DDL error не становится этим retained definitive receipt. Не подменять этим PR489.

**Копии и поиск выхода:** barrier существует только в `kaggle_schema_owner.py` среди всех `src`; по 86 scripts нет cancel/release/supersede этой таблицы. `scripts/production/request_schema_migration.py:18–24,39–41` только создаёт request. Transport `CANCEL_QUERY`, `RESOURCE_RELEASED`, `STOP` не являются schema release. Это статический поиск по полному inventory исходников, не утверждение об отсутствии вне репозитория ручной процедуры.

**Один различающий тест, NOT RUN:** настоящий owner + durable dispatch, prefix N, shape-valid неправильный bundle digest → definitive pre-DDL reject; PG ledger остаётся N, receipt сохраняется, restart не возвращает barrier, исправленный plan и обычная запись допускаются. В параметрах этого же scenario partial prefix N+1/UNKNOWN должны продолжить блокироваться. Existing checkpoint-loss test покрывает успешный DDL, а не этот отказ; карта тестов ниже.

**Установка/restore:** Python controller change, без изменения SQL/body/book data; в строгом pre-DDL случае новой schema base не возникло. Нельзя выдавать его за решение partial migration или снятие необходимости checkpoint после настоящего DDL.

### SQLA-07. Ещё не принятый SQL candidate защищается как применённый файл

**Цель:** повтор/исправление schema migration после ошибки; не стоимость каждого поиска. В [kaggle_schema_live.py:115–116](../../src/regional_knowledge/kaggle_schema_live.py#L115-L116) inline candidate нельзя изменить, если имя уже есть в `self.files`. При этом `self.files=candidate` присваивается **до** `await migrate` [137–138](../../src/regional_knowledge/kaggle_schema_live.py#L137-L138). Artifact-stage делает ту же проверку всех cached files [101–105](../../src/regional_knowledge/kaggle_schema_live.py#L101-L105).

Достижимая последовательность: передан первый новый SQL-файл; его DDL ошибся и транзакция файла откатилась; ledger его не принял, но `self.files` уже содержит неудачные bytes. Исправленный body того же имени получает `migration_file_changed`. Идентичность **кандидата** стала сильнее идентичности принятой migration. Ошибка в later file bundle сложнее: ранние файлы могли уже commit, поскольку [kaggle_schema_migrate.py:162–175](../../src/regional_knowledge/kaggle_schema_migrate.py#L162-L175) выполняет транзакцию на файл, не на весь bundle.

**Минимальный вычет:** убрать запрет смены непринятых candidate bytes в этих двух предварительных сравнениях; сохранить authentication/bundle shape/resource bounds и **PG applied-prefix comparison** [150–160](../../src/regional_knowledge/kaggle_schema_migrate.py#L150-L160) до нового DDL. Именно ledger запрещает менять уже принятый filename/digest, а SQL и запись ledger commit атомарно. Это не предложение редактировать принятую numbered migration.

**Сильнейший контраргумент:** ответ потерялся после commit; наивная замена cached bytes может переписать accepted prefix. Existing ledger comparison обязан остаться и разрешать этот случай по факту PostgreSQL, не по памяти worker. SQLA-02 и зависший worker job — отдельные препятствия: снятие candidate check само по себе не обещает разблокировать controller.

**Один различающий тест, NOT RUN:** первый pending файл падает до commit → corrected bytes того же filename успешно проходят; вариант с уже committed файлом отвергает изменённый digest, а точный неизменённый повтор после потерянного ACK разрешается по ledger без повторного эффекта. Проверяются фактические ledger rows, не только mocked `self.files`.

**Установка/restore:** Python worker migration owner; новая SQL migration не нужна. Existing snapshots и accepted prefix не меняются. Защита in-memory candidate не нужна для сохранности Dataset, защита ledger нужна. Выигрыш — снятие лишнего запрета на исправление непринятого файла; завершение controller/job reconciliation остаётся отдельным условием, ускорение не измерялось.

## C. Неизменяемость, исправления и сохранённая работа

### Какие запреты действительно действуют

`reject_revision_update` не является сам по себе недостатком. Нужно различать immutable **историческую запись** и mutable **текущую проекцию**. Обычный retry уже возвращает сохранённый outcome до effect ([kaggle_apply.py:385–398](../../src/regional_knowledge/kaggle_apply.py#L385-L398)); exact prepared retry возвращается до checks ([SQL026:143–148](../../sql/kaggle/026_source_batch_activation.sql#L143-L148)); повтор vector install по прежнему input hash сохраняет первый принятый vector ([SQL026:181–184](../../sql/kaggle/026_source_batch_activation.sql#L181-L184)). Удаление всех immutable triggers ради повторов ухудшит сохранность, а не устранит доказанный тупик.

| Таблицы / запрет | Где действует после всех migrations | Что предотвращает; мешает ли correction/retry | Вердикт |
|---|---|---|---|
| `applied_ops` | SQL002:19–39, UPDATE/DELETE запрещены | Перезапись canonical outcome/command и двойной effect после lost ACK. Exact duplicate разрешён без UPDATE. | **ОСТАВИТЬ с тестом** lost-ACK/duplicate; это первичный факт применения. |
| `sources.source_id`, `revisions`, `passage_search` | SQL001:40–56; source identity guard и BEFORE UPDATE guards | Подмена уже выданного exact evidence/identity. Это не общий запрет DELETE: исходный trigger001 — UPDATE. | **ОСТАВИТЬ с тестом** старого exact locator после нового импорта; ложная source-wide ревизия отдельно SQLA-01. |
| Source import origin; source regions, Story/assertion/variant/version/search/history; dates/observations | SQL006:30–31; SQL007:77–90; SQL008:23–26 | Подмена принятого текста/доказательства под старым ID. Current editorial correction добавляет версии; полная переимпортированная книга получает новую source identity. | **ОСТАВИТЬ с тестом** append новой версии при сохранении старого evidence. Не переписывать историю UPDATE. |
| Editorial variant/story versions, receipts, search projection | SQL009:61–68 | Сохранённые редакторские тексты и соответствующий versioned search. `apply_story_edit_v1:163–202` вставляет новую версию и двигает head. | **ОСТАВИТЬ с тестом** старой и новой версии; гипотеза «trigger запрещает Story edit» опровергнута. |
| Source manifest, legacy catalog, passage provenance | SQL009:269–288 | Identity/provenance опубликованной версии. Catalog updates теперь append в `source_catalog_corrections`, SQL034:35–61. | **ОСТАВИТЬ с тестом** latest catalog correction при сохранении origin; новый prohibition на catalog не найден. |
| Case versions/evidence/contributions/conclusions/events/draft versions/publications/receipts; Case/draft identity | SQL010:373–381,483–494 | История подтверждённой работы и авторство. SQL031:21–27 appends draft versions; head mutable. | **ОСТАВИТЬ с тестом** correction/private/publication; лишнее условие чтения отдельно SQLA-04. |
| GEO items/names/POI links/receipts; head state | SQL011:46–65 | Принятый GEO inventory и переход staging→active, не arbitrary one-shot boolean. | **ОСТАВИТЬ с тестом** retained install/replay; текущий product blocker не доказан. |
| Десять graph tables от `source_graph_documents` до `source_graph_receipts` | SQL011:371–376, dynamic trigger loop | Перезапись accepted entity/evidence graph под прежним source. Наличие loop объясняет, почему lexical trigger count не live count. | **ОСТАВИТЬ с тестом** повторного graph acceptance; no new correction failure proven. |
| Stage sessions/batches/candidates; stage pages | SQL013:45–52; page guard **заменён SQL026:5–16** | Accepted raw batch и binding неизменны; page identity сохраняется, **model_page разрешено обновлять**. Exact retry проверяется до effect. | **ОСТАВИТЬ с тестом** same identity/new interpretation; не удалять уже снятый полный page freeze. |
| Activation candidates/image origins/vectors/materialized | SQL014:24–27,668–671 | Retained whole-book prepared effects и deterministic input binding. Не обычная current packet-frequency. | **ОСТАВИТЬ с тестом** retained restore, пока inventory не разрешил удаление исполнителя. |
| Illustration candidates / current `source_illustrations` / origins | SQL015:31–36, **два первых triggers удалены SQL020:15–16** | Origins исторические; serving data уже mutable. Current packet writer всё ещё игнорирует accepted replacement. | Origins оставить; **SQLA-03** исправляет `DO NOTHING`, не удаляет trigger повторно. |
| Batch prepared/vectors/publications/receipts, read completions | SQL026:55–62,941–942 | Exact accepted artifacts и завершение; повтор возвращает прежнюю запись. Новое исправление — следующий batch, latest publication. | **ОСТАВИТЬ с тестом** duplicate + next-batch correction; no demonstrated one-shot retry block. |

Тесты в последнем столбце — критерии конкретных сохраняемых гарантий, **не запущенная suite**. Микроудаление дешёвого PK/CHECK ради сокращения числа условий здесь не предлагается. Text correction под тем же region/passage ID конфликтует в SQL026:82–89, но это exact evidence identity. Новый полный импорт получает новые source/ingestion IDs ([kaggle_importer.py:291–299](../../src/regional_knowledge/kaggle_importer.py#L291-L299)); resume того же file_id использует прежнюю identity [275–284](../../src/regional_knowledge/kaggle_importer.py#L275-L284). Поэтому утверждение «immutable SQL запрещает любой переимпорт» неверно.

### SQLA-03. Принятое исправление иллюстрации не обновляет публичный crop

**Цель:** надёжность illustration fetch; каждое исправление уже опубликованной figure с прежним `illustration_key`. Достаточно изменить только поворот 0°→90° при прежних тексте, region/chunk keys и issued image.

Вход допускает поворот ([contracts.py:221–234](../../src/regional_knowledge/contracts.py#L221-L234)); UUID фигуры зависит от page и stable key, а geometry берётся заново ([stage_graph.py:189–203,251–266](../../src/regional_knowledge/stage_graph.py#L251-L266)). Native intake создаёт следующий batch для изменённого payload, exact repeat только для identical input ([kaggle_native_source_intake.py:533–559](../../src/regional_knowledge/kaggle_native_source_intake.py#L533-L559)). SQL026:1106–1113 принимает новую интерпретацию прежнего изображения; :1150–1154 обновляет `model_page`. Current compiler строит новый bbox/rotation/crop binding ([kaggle_source_materializer.py:215–295,443–454,485–526](../../src/regional_knowledge/kaggle_source_materializer.py#L215-L295)), а final media checker проверяет его против новой страницы ([SQL036:74–85,103–107](../../sql/kaggle/036_source_batch_media_scoped_lookup.sql#L74-L107)).

Дальше [SQL026:822–824](../../sql/kaggle/026_source_batch_activation.sql#L822-L824) делает `INSERT source_illustrations ... ON CONFLICT DO NOTHING`. Прежняя `data` остаётся, хотя applied receipt и **новая** immutable media publication уже сохраняются в [897–899](../../sql/kaggle/026_source_batch_activation.sql#L897-L899). `source_batch_conflicts_v1:80–99` не запрещает новый figure rotation; при неизменном тексте остальные сравнения проходят.

Это достигает реального reader: [kaggle_passage_illustrations.py:8–25](../../src/regional_knowledge/kaggle_passage_illustrations.py#L8-L25) читает `i.data`, от stage page берёт только physical index. [kaggle_illustration_preview.py:14–19](../../src/regional_knowledge/kaggle_illustration_preview.py#L14-L19) использует из этой `data` bbox/rotation. Composition: [kaggle_public_backend.py:207–209,565](../../src/regional_knowledge/kaggle_public_backend.py#L207-L209) → [kaggle_source_read.py:181–183](../../src/regional_knowledge/kaggle_source_read.py#L181-L183) → renderer; configure в [server.py:1744–1745](../../src/regional_knowledge/server.py#L1744-L1745). Историческая гипотеза про неиспользуемый legacy CAS reader к этому доказательству не относится.

**Минимальная замена:** current packet apply обновляет `source_illustrations.data` на conflict `(source_id,illustration_id)` из уже проверенной media. Это existing mutable serving projection; whole-book path уже делает такой UPDATE в SQL020:153–157. Новые таблицы/generations/сверки не нужны. Для уже затронутых строк — repair из **latest accepted semantic publication отдельно для каждого `(source_id, illustration_id)`**, содержащей этот ID в `figure_ids`, по batch index, с выбором соответствующего элемента `media.illustrations`; existing latest convention есть в SQL026:1249–1251. Не брать произвольный latest `model_page`: stage мог быть принят, а semantic ещё нет.

**Почему старый retry не откатит исправление:** producer сначала разрешает pending ([kaggle_native_source_intake.py:516–518](../../src/regional_knowledge/kaggle_native_source_intake.py#L516-L518)), новый индекс — `state.batch_count` (:546), `_packet_progress:565–585` ведёт один текущий index, text перед semantic. Common apply держит writer row и exact sequence, duplicate возвращается до effect. Не добавлять новый out-of-order режим ради этого изменения.

**Сильнейший контраргумент:** старый delivered artifact должен сохранить прежний crop. Поэтому сохраняются immutable publication/receipts и уже выданные locator/bytes. Из этого не следует first-write-wins для **нынешнего** renderer. Снятые SQL020 triggers повторно удалять не требуется.

**Один различающий тест, NOT RUN:** base с figure0°; exact accepted tail со stage/text/semantic correction90° при тех же ID; также corrected publication, уже входящая в вариант base. Восстановить base, поставить latest migration **до tail**, воспроизвести exact tail: public fetch/renderer используют90°, repair берёт accepted publication, старый duplicate не возвращает0°, `outcome_bytes/outcome_sha256` и immutable `manifest/media/figure_ids` равны прежним. First-publication test не проверяет эту коррекцию.

**Установка/restore:** новая ordered SQL migration и установка; старые SQL015/020/026 не переписывать. Конкретная замена сохраняет canonical outcome: SQL026:837–843 возвращает IDs/counts/frontier/hashes/time, не geometry; после020 UPDATE geometry не имеет visibility/frontier trigger; capture897–899 использует прежний prepared row. Packet origin verifier [SQL026:1193–1205,1220–1225](../../sql/kaggle/026_source_batch_activation.sql#L1193-L1225) проверяет accepted command/prepared bindings, не первый crop. Это статический аргумент совместимости, не выполненная restore-приёмка. **BLOCKER fingerprint не требуется.**

### SQLA-04. Новая книга в Case скрывает сохранённый личный черновик

**Цель:** надёжность fetch подтверждённой авторской работы. Сценарий: Case содержит evidence из A → автор сохраняет draft D → разрешённый участник добавляет evidence из B → D исчезает у того же автора. A не менялась, текст/версия/права D не менялись.

`case_sources_v1` возвращает весь current source inventory ([SQL010:75–88](../../sql/kaggle/010_disagreement_cases.sql#L75-L88)). Current draft apply сохраняет source dependencies на момент записи ([SQL031:21–27](../../sql/kaggle/031_case_packet_evidence_provenance.sql#L21-L27)). Backend перед mutation получает current edit context ([kaggle_cases_backend.py:506–513](../../src/regional_knowledge/kaggle_cases_backend.py#L506-L513)). Поэтому D имеет `[A]`, а Case теперь `[A,B]`. Read-функция осталась из010: `draft_get` требует equality в [423](../../sql/kaggle/010_disagreement_cases.sql#L419-L423), `draft_list` — в [432](../../sql/kaggle/010_disagreement_cases.sql#L429-L432). NULL превращается в `access_denied` ([kaggle_case_read.py:113–114](../../src/regional_knowledge/kaggle_case_read.py#L113-L114)). Ещё проще — draft при пустом Case, затем первое evidence.

**Минимальный вычет:** удалить только historical dependencies == весь current Case inventory из двух **read** branches; при get оставить проверку наличия exact draft version. Сохранить исторические dependencies как provenance, Case role/source checks SQL010:400–402, author419–420/431, exact version421–422, private Python gates ([case_read_gate.py:164–205](../../src/regional_knowledge/case_read_gate.py#L164-L205), [kaggle_case_contracts.py:176–178,195–197](../../src/regional_knowledge/kaggle_case_contracts.py#L176-L178)). Evidence append-only, исходная A остаётся в current Case; при реальном withdrawal A `case_sources_v1` всё ещё закрывает read. Новая B сама по себе не отзывает авторство D.

**Сильнейший контраргумент:** draft мог зависеть от устаревшего/отозванного evidence. Текущие source checks сохраняются; полное равенство всего набора источников не является необходимым условием доступа автора к сохранённому тексту. Переписывать старые draft bytes или молча пересохранять их не требуется.

**Похожая write-проверка — отдельно:** SQL010:245 блокирует и `draft_publish`. Автоматическое её снятие здесь **BLOCKER retained replay**: base обновляется до latest до replay, прежняя accepted-but-rejected publish-команда может стать applied. Наличие новой ordered migration само по себе не сохраняет прежний outcome. Первое безопасное изменение возвращает get/list; явный новый `draft_save` может создать новую версию с текущими refs для нового publish, сохраняя прежнюю. Это не объявляется исправлением publish semantics.

**Один различающий тест, NOT RUN:** `[A] → draft_save → add_evidence(B) → draft_get/list` через native/public путь возвращает прежний body тому же автору; другой автор не получает D (get отказывает, list не содержит D), а настоящий withdrawal A закрывает чтение по существующим правилам. Старые версии/publications/receipts неизменны. Existing small draft privacy/publish test не проверяет рост источников.

**Установка/restore:** новая migration заменяет только `read_disagreement_v1`; live function установить штатно. Accepted mutations и outcomes не меняются, base/tail не переписываются; восстановленная base после migration сразу читает сохранённый draft. Изменение `check_disagreement_v1` в этот пакет не включать.

## D. Версии функций и фактический executor восстановления

### Current registry: 17 routes, 14 разных пар check/apply

Основание: [kaggle_domain_bindings.py:13–16,54–74](../../src/regional_knowledge/kaggle_domain_bindings.py#L13-L16), [kaggle_native_source_bindings.py:13,43–68](../../src/regional_knowledge/kaggle_native_source_bindings.py#L43-L68), [kaggle_current_registry.py:13–23](../../src/regional_knowledge/kaggle_current_registry.py#L13-L23). В таблице суффикс означает два имени `check_<suffix>` и `apply_<suffix>`.

| Current route(s) | Пара SQL functions | Конкретный binding |
|---|---|---|
| `source.import.v4` | `source_import_graph_v1` | `kaggle_domain_bindings.py:57–61` |
| `source.withdraw.v1` | `source_withdraw_v1` | `kaggle_domain_bindings.py:72–74` |
| `story.edit.v1` | `story_edit_v1` | `story_editorial.py:127–134` |
| `story.access.v1` | `story_access_v1` | `story_editorial.py:127–134` |
| `disagreement.edit.v1`, `disagreement.access.v1`, `disagreement.draft.v1`, `disagreement.publish.v1` | Одна общая `disagreement_v1` | `disagreement_native.py:142–145` |
| `geo.manifest.install.v1` | `geo_manifest_install_v2` | `kaggle_native_source_bindings.py:62–65`, `geo_manifest_commands.py:181–186` |
| `source.stage.v1` | `source_stage_v2` | `kaggle_native_source_bindings.py:43–49` |
| `source.stage.seal.v1` | `source_stage_seal_v2` | Там же |
| `source.activate.v1` | `source_activate_with_illustrations_v1` | `source_activation.py:95–99` |
| `source.materialize.v1` | `source_materialize_v1` | `source_activation.py:271–275` |
| `source.batch.v1` | `source_batch_v1` | `source_batch.py:59–65` |
| `source.finish.v1` | `source_finish_v1` | Там же |
| `source.catalog.v1` | `source_catalog_v1` | `source_catalog_command.py:74–79` |
| `geo.resolve.v1` | `geo_resolve_v1` | `kaggle_source_geo_mvp.py:124–130` |

Это **28 фиксированных имён** entry functions, не 28 всех транзитивных helpers. Наличие route означает возможность dispatch, а не доказанный текущий traffic. `KaggleSourceImporter` действительно composed в [kaggle_service_startup.py:267–269](../../src/regional_knowledge/kaggle_service_startup.py#L267-L269); current registry заставляет его native intake ([kaggle_importer.py:117–120,135–137](../../src/regional_knowledge/kaggle_importer.py#L117-L137)). Неверно говорить, что весь importer не используется. Legacy choices ещё встречаются в101–105, но не являются новым packet producer.

### Старые названия не равны старому executor

| Слой | Что установлено кодом | Чего из этого не следует |
|---|---|---|
| Схемы команды `rkb-v4-sql001-011-v1`, `-012-v1`, `-014-v1` | Все три декодируются: `kaggle_source_commands.py:47–52`, `control_spool.py:505–519`. Fresh011 code содержит девять domain routes, **не только GEO**, хотя retained перенос владельца ограничен GEO. | Нельзя посчитать число команд этих схем в настоящем журнале или свести011 к одной route по названию. Журнал не читался. |
| Старые source.import v1/v2/v3 | Decoders остаются в `kaggle_source_commands.py:53–62`; historical contract helper136–148 сохраняет dependency view. | Decoder не устанавливает старое SQL body; не доказано, что все эти executors нужны текущему Dataset tail. |
| Транзитивный import v4 | SQL011:503 вызывает v4; SQL009:525 вызывает v3+manifest, затем старшие wrappers вниз. | Эти checks нельзя объявить мёртвыми только по suffix. Частота нового packet path от них не следует. |
| Пять `RENAME ... before_* / quoted_*` | SQL026:1206,1213,1220,1233 создаёт retained branches; wrappers1209–1225 и1240 вызывают их. SQL028:24 переименовывает fact validator, новый branch30 вызывает quoted version. | Все пять имеют реальные SQL callers. «Удалить все before_*» без учёта retained sources ошибочно. |
| Два DROP FUNCTION | SQL017:44–45 уже удаляет `apply/check_geo_dependencies_v2_transition`. | Не предлагать удалить их повторно и не считать final functions. |
| SQL016 reconcile и legacy origin readers | Current registry entry для SQL016 не найден; older declarations/readers существуют. `kaggle_source_history_native.py:255–267` всё ещё выбирает verifiers разных import versions в historical paths. | Нельзя доказать «ни откуда» для внешних retained artifacts только current producer grep. Нет доказанного product выигрыша от косметической зачистки. |

**Фактический порядок восстановления принципиален:** [kaggle_pg_snapshot_native.py:519–545](../../src/regional_knowledge/kaggle_pg_snapshot_native.py#L519-L545) восстанавливает base и выполняет `_upgrade_restored` / `migrate` до создания replay executor; затем [pg_snapshot_recovery.py:162–184](../../src/regional_knowledge/pg_snapshot_recovery.py#L162-L184) воспроизводит exact accepted tail. Registry выбирает current handler по route. Автоматического «каждую старую команду запускаем старым телом SQL» в этом пути нет.

Следовательно, **retained execution нужно обосновывать outcome equivalence**, а не количеством `_v1…_v4` и не самим наличием ordered migration. Команды уже в base разрешаются по `applied_ops` без повторного эффекта; отсутствующие в base tail-команды снова проходят текущий checker. Расширение допустимых mutations способно изменить прежний rejected outcome. Именно поэтому read-часть SQLA-04 допустима как независимое вычитание, а publish-часть заблокирована до inventory retained outcomes.

Реальные counts текущего журнала по011/012/014, retained bases/tails и dead executors **не получены**: live доступ запрещён заданием. Статическая альтернатива в этом отчёте — exact current route map, final definition inventory, transitive callers и явно отмеченные unknowns. Ноль текущих новых producers не выдается за ноль сохранённых команд.

## E. Горячий путь: ложный fetch и работа, растущая с историей

### SQLA-01. Append той же книги отзывает обычный search → fetch ID

**Цель:** надёжный fetch найденного фрагмента. Частота — добавление действительно нового passage уже активного source между search и fetch; не каждый первый пакет, не exact duplicate и не другая книга.

Обычный MCP search создаёт `PassageContinuation` из dependency и помещает его в result ID ([kaggle_public_backend.py:291–317](../../src/regional_knowledge/kaggle_public_backend.py#L291-L317), search687–690). Public fetch декодирует этот ID [704–709](../../src/regional_knowledge/kaggle_public_backend.py#L704-L709), `_search:503–515` делает PassageReadRequest. Gate обновляет actor/min_data_seq, но не переписывает locator ([source_read_gate.py:212–218](../../src/regional_knowledge/source_read_gate.py#L212-L218)); worker вызывает continuation reader ([kaggle_worker_query.py:187–190](../../src/regional_knowledge/kaggle_worker_query.py#L187-L190), [kaggle_query.py:259–263](../../src/regional_knowledge/kaggle_query.py#L259-L263)). Старую ссылку не нужно изобретать вручную.

Reader правильно выбирает **точные** passage/source/content revision, требует active revision и accepted/not-withdrawn ([kaggle_query.py:308–315](../../src/regional_knowledge/kaggle_query.py#L308-L315)). Но [317–319](../../src/regional_knowledge/kaggle_query.py#L317-L319) дополнительно требует equality whole-source `eligibility_revision` из locator с текущим счётчиком.

Этот счётчик повышает trigger на **каждый INSERT нового passage в обслуживаемую active_revision принятого, неотозванного source** ([SQL004:54–82](../../sql/kaggle/004_read_fences.sql#L54-L82), особенно67–75). Current packet apply вставляет passages [SQL026:784–790](../../sql/kaggle/026_source_batch_activation.sql#L784-L790), active revision остаётся1 (:830). Старый passage не изменялся; UPDATE запрещён SQL001:55–56. Значит normal sequence `search(A) → append(B к той же книге) → fetch(A)` даёт ложное `evidence_withdrawn`.

**Минимальный вычет:** удалить `row[6] != locator.source.eligibility_revision`, соответствующий SELECT column и JOIN `source_visibility` **только из exact passage reader**. Сохранить DB generation comparison, точные IDs/revision, active/accepted/not-withdrawn predicates и source identity в ответе `_item:342–344`; не переписывать locator ABI. Сохранить withdrawal CAS на write-path: там expected eligibility защищает настоящую mutation от гонки, здесь old source-wide counter необоснованно отзывает immutable фрагмент.

**Сильнейший контраргумент:** старый locator не должен читать действительно отозванный/заменённый источник. Это уже исключают остальные SQL predicates и generation. Удаление equality не требует делать чужие private drafts читаемыми и не меняет Case ACL. Book visibility для закрытой группы не является дополнительной причиной отклонить неизменный фрагмент.

**Один различающий тест, NOT RUN:** real public search выдаёт ID для A; current producer принимает новый text packet B того же source/revision; fetch исходного ID возвращает тот же exact body. Параметры этого же сценария с настоящим withdraw, active revision switch и другой database generation должны по-прежнему отказывать. Existing query tests покрывают другую книгу, revision switch и withdraw, но не same-source same-revision append.

**Установка/restore:** только Python read handler, SQL migration не нужна. Accepted commands, vectors, Dataset bytes и current schema не меняются. На restored fork старый DB locator остаётся недействителен. Выигрыш — снятие конкретного ложного отказа и одного ненужного JOIN; не заявляются миллисекунды. Предыдущий PR491 снял другую Dev-side book barrier; native equality, показанное здесь, остаётся самостоятельным текущим местом.

### SQLA-05. Перед bounded Case read выполняется полный replay Case

**Цель:** скорость/надёжность Case search, fetch, operation, draft, edit_context; последний также вызывается перед не-create mutations ([kaggle_cases_backend.py:506–513](../../src/regional_knowledge/kaggle_cases_backend.py#L506-L513)). Это не обычный книжный semantic search и не всякий editorial read.

Production worker идёт в `execute_case_read` ([kaggle_worker_query.py:171–173](../../src/regional_knowledge/kaggle_worker_query.py#L171-L173)). Reader ставит максимум2сек, REPEATABLE READ READ ONLY и bounded output ([kaggle_case_read.py:35–57](../../src/regional_knowledge/kaggle_case_read.py#L35-L57)), но для непустого search вызывает verifier для всех возвращаемых IDs [90–94](../../src/regional_knowledge/kaggle_case_read.py#L90-L94), для остальных sections — перед собственно read [104–112](../../src/regional_knowledge/kaggle_case_read.py#L104-L112).

Текущее тело — **SQL031:108–243**, не первоначальное010:

- [108–130](../../sql/kaggle/031_case_packet_evidence_provenance.sql#L108-L130): для каждого Case фильтруются `applied_ops` через JSON route/case ID, декодируются payload/base64, считаются hashes. В SQL002:19–30 индексы key/sequence; позднего индекса под этот expression в37 migrations не найдено. Это не выдаётся за снятый EXPLAIN.
- [137–143](../../sql/kaggle/031_case_packet_evidence_provenance.sql#L137-L143): пересобираются все private draft versions даже для чтения публичного заголовка. Это лишняя работа, не утверждение об утечке клиенту.
- [157–171](../../sql/kaggle/031_case_packet_evidence_provenance.sql#L157-L171): для каждого исторического add_evidence берётся полный folded `read_source_manifest_v1`, разворачиваются страницы/регионы книги ради известных IDs.
- [218–239](../../sql/kaggle/031_case_packet_evidence_provenance.sql#L218-L239): агрегируются все heads, versions, grants, evidence, contributions, conclusions, events, drafts, publications, receipts.

Лимит результата ограничивает **число Case**, но не общую applied history, историю выбранного Case и размеры использованных книг. Verifier false становится `not_ready`; deadline даёт отказ ([kaggle_case_read.py:94,105,133–138](../../src/regional_knowledge/kaggle_case_read.py#L133-L138)). Фактический timeout в live не измерен.

**Что уже гарантируется:** one transaction writer fence/sequence/CAS/effect/receipt/cursor ([kaggle_apply.py:375–449](../../src/regional_knowledge/kaggle_apply.py#L375-L449)); exact duplicate; PK/UNIQUE и immutable `applied_ops`; current Case head lock и domain checker (SQL031:15–25); capture receipt в той же транзакции (SQL010:354–371); history/body immutability; current role/source/author checks (SQL010:400–402,419–420); один MVCC snapshot и private gates. Это защита поддержанного apply пути, **не доказательство логической безошибочности любого SQL body**. Не утверждается, что FK целиком заменяет provenance verifier.

**Минимальный вычет:** удалить два online verifier calls и их boolean gates, сохранить настоящий bounded reader, actor/source/private/freshness checks и deadline. Минус один verifier SQL **и его отдельный timeout setter** на такой read; search делает один query, но внутри replay по каждому выбранному ID. Саму SQL function можно пока оставить: для снятия вызова migration не требуется. Иных исполняющих src callers этого verifier не найдено; `kaggle_domain_schema.py:14` и `kaggle_owned_pg.py:110` — inventory/grant. Нельзя обещать «перенести его в уже существующий restore вызов»: такой вызов не найден. Новые cache/background proof не предлагаются.

**Сильнейший контраргумент и один тест, NOT RUN:** поддержанная ошибка apply или допустимая migration после accepted create/edit/draft/publish и base+tail restore оставляет committed ledger, но неправильный current body/ACL; обычный reader показывает неверный текст/чужой draft, а verifier — единственная оставшаяся защита. Если такой fixture воспроизводится, удаление блокируется до ремонта конкретного writer/recovery нарушения. Arbitrary privileged UPDATE в обход поддержанных путей сам по себе не оправдывает replay всей истории на каждом read. В прочитанном коде конкретного supported corruption scenario не доказано.

**Установка/restore:** Python reader rollout, без изменения accepted mutation outcomes или base/tail. Окончательное удаление SQL object/grant, если понадобится позже, отдельная ordered migration после inventory; для ускорения этого read не нужно. С SQL-7/PR488 не дублируется: здесь другой caller и повтор всех Case-команд, а не metadata одного passage.

### SQLA-06. SQL036 читает всю stage-историю ради current semantic media links

**Цель:** скорость импорта; **один вызов на новый semantic prepare с непустыми links**. Ни text component, ни exact prepared retry, ни batch apply эту полную media проверку повторно не выполняют: [SQL026:143–158](../../sql/kaggle/026_source_batch_activation.sql#L143-L158), [SQL036:121–126](../../sql/kaggle/036_source_batch_media_scoped_lookup.sql#L121-L126).

Final `source_batch_media_stage_links_v1` [SQL036:12–20](../../sql/kaggle/036_source_batch_media_scoped_lookup.sql#L12-L20) читает **все** `source_stage_batches` одного source по batch index и декодирует каждый payload/page. Wanted chunk/media keys в [32–38](../../sql/kaggle/036_source_batch_media_scoped_lookup.sql#L32-L38) ограничивают только сохраняемые maps; не количество прочитанных/декодированных packets. Actual caller checker124–126, raw comparisons128–171. Native producer делает direct prepare с5сек statement budget ([kaggle_source_batch.py:40–60](../../src/regional_knowledge/kaggle_source_batch.py#L40-L60)); это бюджет кода, не наблюдавшееся время.

При H предыдущих stage packets нынешний небольшой linked packet снова оплачивает H decodes и page comparisons. Если N последовательно растущих linked packets каждый проходит этот путь, суммарно1+…+N = O(N²) batch decodes; отдельные payload sizes добавляют свой множитель. Пустые links обходят helper, поэтому нельзя приписывать эту стоимость любому пакету.

**Существующая граница:** accepted raw packet извлекается/проверяется в [source_stage.py:260–280](../../src/regional_knowledge/source_stage.py#L260-L280); explicit context выбирается в [source_batch_context.py:15–16,47–86](../../src/regional_knowledge/source_batch_context.py#L47-L86); current compiler [kaggle_source_materializer.py:388–406,443–454,485–489](../../src/regional_knowledge/kaggle_source_materializer.py#L388-L406), graph refs [stage_graph.py:355–363,393–403,447–473](../../src/regional_knowledge/stage_graph.py#L447-L473), media projection [kaggle_source_materializer.py:272–291](../../src/regional_knowledge/kaggle_source_materializer.py#L272-L291) уже разрешают ссылки current packet. Между этим compiled dict и prepare не найден иной production author media links. Direct writer технически может вызывать SQL, как отмечено в A; guards prepare для accepted stage/source/writer от этого не исчезают.

**Минимальный вычет:** SQL-only удаление history helper call и **зависящих от него повторных staged/raw-link сравнений**, с сохранением current source/page/figure membership, accepted stage, ownership/conflicts и остальных нужных SQL guards. Protected compiler не переписывать; новый inverse index/table не вводить. Это снятие второй реализации history resolution, а не перенос логики в ещё один сервис.

**Сильнейший контраргумент:** SQL036 правильно сохраняет A→B→A invalidation: возврат прежней страницы не должен воскресить link, который не был явно задан заново. Нельзя заменить history простым сравнением только final page hash. Нужен current compile path, уже работающий с accepted explicit refs, а не урезанный SQL036 без понимания этой семантики. Если реальный поддержанный producer обходит compile и создаёт иной результат, вычитание блокируется этим примером.

**Один различающий тест, NOT RUN:** correction sequence A→B→A, vanished old refs и explicit same-batch replacement через **current compile_current → prepare**. Removed/nonexistent figure/link отвергается на действующей входной границе; явно заданный допустимый current link проходит без чтения предыдущих stage payloads; prepared exact retry и результат публикации прежние. В restore-варианте того же correction scenario восстановить base, установить latest migration до exact tail и сравнить прежние canonical outcomes/publications. Нужна проверка результата/доступа, не benchmark suite. Existing helper-vs-helper test и literal SQL-body equality не доказывают эту границу.

**Установка/restore:** новая migration заменяет checker; старые033/036 не редактировать. Already prepared rows/accepted commands не перекодируются. На base→latest→tail необходимо подтвердить сохранение прежнего accepted input/outcome; в этом аудите это не проверялось; снятие проверки произвольно испорченного post-compiler JSON не должно незаметно расширить production boundary. **BLOCKER fingerprint не нужен**, пока изменение ограничено SQL; уверенность в объёме безопасного удаления средняя до указанного контрпримера.

### SQLA-08. Typed vectors проходят через JSON внутри writer

**Цель:** скорость импорта; каждый новый text/semantic source.batch apply. Это ограниченная packet-size работа, не scan всей истории и не прежняя SQL-5 про QUERY serialization.

`source_batch_vectors.embedding` уже имеет тип `public.vector(1024)`, PK `(source_id,batch_index,component,input_id)`, FK и norm CHECK ([SQL026:35–40](../../sql/kaggle/026_source_batch_activation.sql#L35-L40)). Direct installer167–187 проверяет writer/input hash/tokens; caller [kaggle_source_batch.py:69–91](../../src/regional_knowledge/kaggle_source_batch.py#L69-L91). Но apply [767–773](../../sql/kaggle/026_source_batch_activation.sql#L767-L773) вызывает `source_batch_doc_v1` [846–860](../../sql/kaggle/026_source_batch_activation.sql#L846-L860): vector→real[]→JSONB floats для passages и default Story variants. INSERT затем снова парсит JSON text в `public.vector` [789](../../sql/kaggle/026_source_batch_activation.sql#L784-L790), [812](../../sql/kaggle/026_source_batch_activation.sql#L808-L813).

Semantic component обычно повторно содержит passages text component; большие arrays создаются и для будущих `ON CONFLICT DO NOTHING`. Один compiler output получает лишние1024 float encodes/parses на passage/default variant внутри writer transaction. Ни model inference, ни математическое преобразование вектора для этой промежуточной копии не нужны.

**Минимальная замена:** брать `source_batch_prepared.knowledge` напрямую, а embedding двух INSERT — из существующей typed vector table по **всем четырём PK fields**. Сохранить Story выбор `fragments[0].fragment_id` с fallback на variant_id из SQL026:857–858. После замены единственного SQL caller773 удалить `source_batch_doc_v1`. Input completeness/hash checker878–880, installer/type/norm/FK и exact-once boundary оставить. Никаких новых envelopes/tables или BGE изменений.

**Сильнейший контраргумент:** неполный JOIN может взять vector другого component или не первого fragment, отсутствие строки может тихо потеряться. Поэтому existing completeness check и exact key сохраняются. Helper сам по себе не даёт новой гарантии правильности embedding, лишь сериализует его.

**Один различающий тест, NOT RUN:** base с prepared/vector artifacts и exact text+semantic tail, разные заранее заданные vectors для passages и двух Story fragments. Base→latest→тот же tail даёт те же DB embeddings, first-fragment choice, найденные IDs, canonical outcomes и immutable publications; duplicate даёт один effect. Existing encoder-call test подтверждает отсутствие повторного BGE на exact retry, но не стоимость этого SQL JSON roundtrip.

**Установка/restore:** новая SQL migration заменяет apply и удаляет helper после caller inventory; reembedding/backfill нет. SQL026:837–843 outcome не содержит vector; knowledge hash берётся из прежнего prepared, variant data и раньше сохранялась без временного embedding (:807), publication capture898–899 тоже берёт prepared. Same ABI не меняет signature-derived handler fingerprint. При сохранении key/fragment selection canonical outcome остаётся прежним; это требует proposed restore comparison, не объявления уже проведённой приёмки. Protected files не меняются.

## F. Дубли гарантий и копии в слоях

### Что является дублем, а что самостоятельной гарантией

| Условие | SQL / Python / Dev / тестовые копии | Решение |
|---|---|---|
| Command grammar и installed function ABI | Intake codecs/typed contracts; outer check; inner apply check; два `pg_proc` lookups; schema installation/ledger | Existing **I4**, кратность уточнена в B. Чистую форму не проверять заново на каждом внутреннем шаге; current DB/CAS условия оставить один раз в transaction. |
| Fact selected evidence indexes | `check_source_batch_semantic_v1` SQL028:52 → `check_source_facts_v1`; затем prepare026:159–162 повторяет; projection trigger008:129 вызывает третий раз, затем извлекает proofs133,139 | Второй prepare loop — кандидат вычета в I4 family. Projection trigger обслуживает и legacy callers; удалять его целиком только из-за current duplicate нельзя. Нет нового ID. |
| Exact vector binding | Python model/token binding → installer026:172–180 → vector type/norm/FK/PK → apply completeness878–880 → JSON helper | **SQLA-08** вычитает только сериализацию. Type/norm/FK и check фактического input не эквивалентны Python grammar и остаются. |
| Source identity/freshness и whole-source counter | Dev/public gate → native continuation→SQL active source плюс equality; current counters обновляются trigger004 | **SQLA-01**: exact fragment validity уже проверена точными SQL predicates; historical source-wide counter не обязателен на read. Mutation CAS сохраняется. |
| Media links | Typed stage/graph refs → current compiler projection → current page checks → full raw stage replay SQL036 → literal SQL test | **SQLA-06** убирает вторую history resolution; нужный membership и direct prepare guards сохраняются. |
| Accepted illustration correction | Typed input/new stage → checker036 принимает new geometry → immutable publication сохраняет её → mutable serving table оставляет старую | **SQLA-03**: это не полезный второй proof, а расходящиеся current projections. Existing UPSERT устраняет first-write-wins. |
| Case current authority и исторический source inventory draft | SQL current role/source+author → SQL historical equality get/list/publish; Python private selectors | **SQLA-04** снимает только read equality. Private author check не дублирует book-read policy и сохраняется. Publish outcome boundary отдельно. |
| Current Case state и весь accepted replay | Atomic effect/receipt → immutable versions/PK/FK → current actor/source reader → SQL031 online reconstruction | **SQLA-05** снимает reconstruction с bounded reads. Атомарность не объявляется доказательством безошибочности произвольной migration. |
| Migration identity | Authenticated bundle + current plan → in-memory `self.files` → PG immutable applied ledger; controller barrier поверх durable phase receipt | **SQLA-07** ограничивает immutability ledger; **SQLA-02** завершает только доказанно неприменённое failed intent. Никакого blanket reset. |

Dev/Kaggle сравнение выполнено по **реально composed paths**, а не по совпадению имён. Старые SQLite services и legacy illustration reader не объявлены новым native guard. Общее правило «все читают все книги» не превращается в разрешение показывать чужой private draft. Общие SQL counts/cheap CHECK без measurable path work не получают самостоятельных задач.

### Что уже закрепляют просмотренные тесты

Это статическое чтение assertions **шести файлов**; наличие теста не означает, что он сейчас проходит. Пробелы относятся к этим сценариям, не ко всей suite. Imported fixtures из других файлов дополнительно не исследовались; schema не выводится из имени fixture.

| Тест / строки | Существующая гарантия | Что не различает / копия реализации |
|---|---|---|
| `tests/test_kaggle_query.py:301–327,332–341,422–450` | Другая книга сохраняет locator; own active revision switch, DB fork и настоящий withdraw отказывают | Same-source append той же revision и public opaque search ID→fetch отсутствуют. Fixture147–156 ставит003–009, не current026. SQLA-01 сохраняет указанные контрпримеры. |
| `tests/test_kaggle_schema_live.py:31–45,108–142` | Malformed inline input не открывает PG; retired writer не применяет DDL; exact repeat после успеха идемпотентен | Не соединяет definitive pre-DDL refusal с persisted controller barrier. |
| `tests/test_kaggle_schema_live.py:164–207` | Успешный DDL + потеря checkpoint удерживает barrier; restart завершает checkpoint без повторного DDL | Fake dispatch всегда успешен; это нужная защита partial lifecycle, не оправдание блокировки после отказа **до** DDL. |
| `tests/test_kaggle_schema_migrate.py:50–144` | Ledger/no-op, old SQL change rejection, failed additive DDL rollback, baseline schema equivalence, command/outcome preservation | Не SQLA-02 controller refusal и не SQLA-07 corrected cached candidate; не live Dataset restore запуск. |
| `tests/test_source_batch_media_history_scale_pg.py:159–254` | Один current linked packet после113 accepted stages, три link modes, задан threshold5сек; malformed media напрямую отклоняется SQL | Одна точка H не доказывает отсутствие full scan. Неверный JSON меняется **после compiler**; это другой boundary. Текст threshold не является нашим измерением. |
| `tests/test_source_batch_media_history_scale_pg.py:260–350` | A→B→A, identical reissue, explicit replacement, same-batch reinsertion | Сравнивает scoped helper с filtered legacy helper, не current compile→prepare без history replay. |
| `tests/test_source_batch_media_history_scale_pg.py:125–137` | **Буквальное равенство validator body036 и033 после одной string replacement** | Закрепляет implementation duplicate. При согласованном SQLA-06 заменить осмысленной проверкой boundary, а не сохранять копию кода как продуктовый контракт. |
| `tests/test_source_batch_media_history_scale_pg.py:28–119` | Empty-link prepare после90 stages имеет assertion <5сек послеSQL033 | Это другой, уже сокращённый путь; SQLA-06 относится только к непустым links036. |
| `tests/test_source_batch_pg.py:93–107,141–162` | Exact retry одного packet не запускает encoder заново; первая figure и Story появляются после text | Не same-ID geometry correction и не many-input native vector/JSON cost. |
| `tests/test_disagreement_cases_native.py:152–170` | Private draft v2, чужой actor не читает, selected publish не раскрывает private prefix/suffix, duplicate не создаёт contribution дважды | Draft helper82–86 вызывает SQL напрямую; нет `[A]→[A,B]`, native response cost и growing history. |
| `tests/test_disagreement_cases_native.py:114–129,174–218` | Exact quote, grants/withdraw, native Case search scope | Малые fixtures SQL010, без current031 packet override и большой history. |

Ни предложенные discriminating scenarios, ни существующие тесты в этом аудите **не запускались**. Не предлагается новый performance harness или массовая перезапись тестов: исправляется только тест, который закрепляет удаляемую реализацию, и добавляется один различающий сценарий на согласованную задачу.

## Сложность без влияния на продукт (не трогать сейчас)

- Legacy SQL016 Story reconcile: current native route не найдена, seed trigger016:26–35 существует; cosmetic deletion без retained inventory не обещает ускорение.
- `before_*` и quoted fact names: у всех пяти renames есть current SQL callers (SQL026:1206–1251, SQL028:24–30); длина имён не проблема продукта.
- Legacy illustration-origin/CAS reader: текущая composition идёт через SOURCE_READ/renderer (`kaggle_public_backend.py:207–209,565`); целиком сломанным public gallery этот неиспользуемый путь не объявляется.
- Old base closed schema026/027, manifest nested scans014 и graph loops до032: заменены029/025/032; повторный аудит старого тела не новая задача.
- Source batch limit2500 (`source_batch.py:32–35`, `source_stage.py:132`): correction packets расходуют limit, но конкретного current product failure не доказано; изменение защищённого контракта — **BLOCKER fingerprint**.
- `read_passage_illustrations` может проверять links по множеству figures (`kaggle_passage_illustrations.py:38–60`); без EXPLAIN/доказанной existing exact alternative не предлагается новая inverse table или новый performance ID.
- One-active GEO manifest (SQL011:13,55–65): текущий resolution — отдельный `geo.resolve.v1`; препятствие реальному correction workflow не доказано.
- Число проверок простых scalar types, длина JSON shape code и повторные declaration inventories сами по себе не дают product gain; meaningful current repeats уже отнесены к I4.

## Что точно оставить и почему

1. **`applied_ops` как окончательный факт применения и точные исходные bytes/outcome.** PK/UNIQUE, immutable receipt, exact duplicate до эффекта и последовательность transaction сохраняются: [SQL002:19–39](../../sql/kaggle/002_recovery.sql#L19-L39), [kaggle_apply.py:375–449](../../src/regional_knowledge/kaggle_apply.py#L375-L449). Lost ACK/UNKNOWN разрешается lookup, не новой записью эффекта; rollback до commit не превращается в успех.
2. **Один writer fence, expected revisions/CAS и DB current membership.** Python preflight не заменяет state в той транзакции, где меняется база. Это source/session/ingestion, row identity, editor/author, vector input binding; direct prepare/install имеют собственную границу (A).
3. **История подтверждённой работы и append новых версий.** Story/Case/draft/publication receipts не удаляются и не переписываются ради простоты; mutable heads/current projections могут двигаться на новую принятую версию. SQLA-03 меняет serving projection, SQLA-04 только read predicate.
4. **Точные PK/FK и типы данных.** Unique operation/sequence, foreign keys к accepted source/prepared, vector dimension/norm, identity constraints ловят реальные противоречия persisted state, а не только повторяют JSON grammar (SQL001/002/026).
5. **Настоящие withdrawal, active content revision и database generation.** Source-wide book visibility equality на immutable fetch лишняя; факт настоящего отзыва/замены/новой DB остаётся. Чужие private drafts также остаются закрытыми, SQL010:419–432 и Python actor selectors.
6. **Immutable applied migration prefix и контролируемое продолжение partial DDL.** Ledger comparison и per-file SQL+ledger transaction ([kaggle_schema_migrate.py:150–175](../../src/regional_knowledge/kaggle_schema_migrate.py#L150-L175)) нельзя заменить только памятью worker. Authenticated SQL delivery и bounded input сохраняются. Barrier нужен после реального DDL до checkpoint, не после доказанно pre-DDL rejection.
7. **Exact base+tail recovery с сохранением canonical outcomes.** Восстановление устанавливает latest schema до replay; изменение mutation semantics требует проверить те же accepted bytes/outcomes. Смена имени/тела SQL не даёт разрешения потерять отвергнутую или подтверждённую человеческую работу.

## Влияние на восстановление и уже сохранённые версии Dataset

**Ничего в этом аудите не установлено и не мигрировано.** Таблица — требования к будущим независимым fixes. Старые numbered SQL не редактируются: новая migration следует текущему ordered ledger, применяется к новой базе и восстановленной базе перед tail. Reinstall отдельного Python reader/controller не требует заново импортировать книги.

| Изменение | Уже сохранённая base | Exact tail после latest schema | Необходимое действие |
|---|---|---|---|
| SQLA-01 exact fetch | Строки/receipts остаются прежними | Mutation effects не меняются; новый DB generation по-прежнему закрывает старый locator | Обновить native Python reader; SQL migration нет |
| SQLA-02 pre-DDL barrier | Строгий rejected plan не менял PG schema/base | Старые receipt bytes остаются; исправленный plan новый, UNKNOWN не объявляется resolved | Controller lifecycle; только existing proof неприменения |
| SQLA-03 figure latest projection | Repair из latest **accepted** semantic publication для каждой `(source_id, illustration_id)`, содержащей ID в `figure_ids`, и соответствующего `media.illustrations`; не raw newest page | UPSERT same ABI сохраняет result counts/frontier/hash и capture bytes; latest state правильный | Новая SQL migration/function install + узкий repair; proposed base→latest→same-tail outcome comparison |
| SQLA-04 draft read | Сразу возвращает уже сохранённый draft после замены read function | Checks/applies mutations прежние; write publish predicate не меняется | Новая SQL read migration; **publish снятие отдельно BLOCKER retained replay** |
| SQLA-05 Case online replay | Данные не меняются | Accepted effects/outcomes не меняются | Python read rollout; SQL verifier object пока может остаться |
| SQLA-06 media history duplicate | Prepared/accepted artifacts не перекодируются | Требуется подтвердить прежние projection/outcome; нельзя без доказательства расширять production producer boundary | Новая SQL checker migration; один current-path correction scenario; old033/036 неизменны |
| SQLA-07 candidate immutability | Applied prefix остаётся immutable в PG | Изменить можно только ещё не принятый файл; partial prefix/commit ACK решает ledger | Python worker; independent controller/job reconciliation сохраняется |
| SQLA-08 vector JSON | Existing typed vectors остаются без reembedding | Exact key/first-fragment выбор сохраняет result/publications и signature-derived receipt fingerprint | Новая SQL apply migration, затем DROP единственного лишнего helper; proposed exact-tail comparison |
| I4 refinement | Receipt fields и declared ABI пока сохраняются | Удаление второго same-transaction check не должно менять result/rejection; direct prepare/install отдельны | Следовать прежней I4 задаче, не открывать дубликат; Python changes и SQL migrations по фактическому вычету |

**Сохранение handler identity для SQLA-03/08:** `kaggle_current_registry.py:19–22` считает SHA signature, а не body; unchanged signature/route/dependency оставляет `handler_sha256`. `before/after` берутся из исходной команды. Поэтому эти SQL-only replacements не требуют нового function-pin ритуала и не дают права переписать старые outcome bytes.

**Две явные блокировки:** (1) любой fix, требующий изменения 12 materializer fingerprint inputs, — **BLOCKER fingerprint**; таких необходимых edits в восьми пакетах не выявлено; (2) изменение `draft_publish` rejection semantics или удаление retained executor без base/tail/outcome inventory — **BLOCKER retained replay**. Изменять архитектуру хранения, добавлять phase или отказываться от saved user work для обхода blocker не предлагается.

## Вопросы владельцу

**Новых вопросов для принятия этого аудита нет.** Архитектура, один поток, переимпорт книг, private drafts и сохранность подтверждённой работы уже заданы. Недостающие числа retained bases/tails и старых rejected commands — факты для проверки RKB-Opus перед отдельным mutation/compatibility fix, а не повод заново спрашивать владельца об этих решениях. Настоящий журнал в этом read-only source audit не подменяется предположением.

## Порядок исправлений и независимые пакеты

Порядок ниже — рекомендация RKB-Opus после проверки находок; это не authorization на установку в рамках настоящего аудита. Каждый пакет должен иметь один описанный выше различающий scenario, сохранённые scope и rollback границы. Никакие runtime acceptance gates не считаются выполненными.

1. **SQLA-01:** узкий Python exact-fetch вычет. Независим от SQL migration и импортера; сохранить genuine withdrawal/revision/fork regression. Соседние metadata изменения PR488 не дублировать.
2. **SQLA-02:** controller release только definitive pre-DDL refusal. Рассматривать вместе с owner будущих migrations, но не смешивать с worker PR489 или generic exception unlock.
3. **SQLA-03:** SQL current figure UPSERT + repair existing accepted projection. Отдельный SQL пакет с unchanged canonical receipt proof; materializer не менять.
4. **SQLA-04:** только draft get/list read-function. Отдельный SQL пакет; `draft_publish` остаётся вне diff до retained outcome inventory.
5. **SQLA-05:** Python Case verifier callers. Снять полный replay с bounded read; не добавлять replacement cache/registry. Не менять Case mutations/ACL.
6. **SQLA-06:** SQL media history duplicate. Сначала различить existing current compiler boundary и тест прямой подмены после compiler; сохранить A→B→A. Осмысленно заменить literal SQL equality assertion.
7. **SQLA-07:** убрать candidate-file pin, оставить PG applied prefix. Самостоятельный worker-level fix, эксплуатационное завершение зависит от корректного reconciliation owner/job; не обещать, что один diff освобождает всё.
8. **SQLA-08:** typed vector lookup без JSON; отдельная bounded оптимизация после correctness/liveness. Выигрыш измерять по removed conversions, без выдуманного p95.

I4/I5/SQL-1…SQL-7/RT03 остаются в существующих пакетах реестра. Новая независимая задача не создаётся только ради ещё одного места известной гарантии. Перед каждым реальным исправлением сверить актуальный main и занятые PR; описанные здесь source findings относятся к pinned baseline в шапке.

## Приложение. Final definitions всех 43 `check_*`

Имена получены последовательным свёртыванием37 исходных migrations с учётом rename/drop; это reference map для A, не live catalog query. Переименованный `check_source_quoted_facts_v1` имеет body из008 и имя из028. В следующей таблице каждая ссылка ведёт к исходному файлу с определением.

| Имя | Final body / rename | Группа A |
|---|---|---|
| `check_disagreement_v1` | [010_disagreement_cases.sql:140](../../sql/kaggle/010_disagreement_cases.sql#L140) | Case |
| `check_geo_manifest_install_v1` | [011_geo_identity_manifest.sql:67](../../sql/kaggle/011_geo_identity_manifest.sql#L67) | GEO |
| `check_geo_manifest_install_v2` | [012_geo_dependency_bound.sql:4](../../sql/kaggle/012_geo_dependency_bound.sql#L4) | GEO |
| `check_geo_resolve_v1` | [035_geo_links.sql:67](../../sql/kaggle/035_geo_links.sql#L67) | GEO |
| `check_source_activate_v1` | [018_story_embedding_fragments.sql:244](../../sql/kaggle/018_story_embedding_fragments.sql#L244) | Activation |
| `check_source_activate_with_illustrations_v1` | [020_media_identity.sql:161](../../sql/kaggle/020_media_identity.sql#L161) | Activation |
| `check_source_activation_base_v1` | [014_source_stage_activation.sql:132](../../sql/kaggle/014_source_stage_activation.sql#L132) | Activation |
| `check_source_activation_semantic_v1` | [023_semantic_finding_alias.sql:2](../../sql/kaggle/023_semantic_finding_alias.sql#L2) | Activation |
| `check_source_batch_base_v1` | [029_source_batch_base_contract.sql:4](../../sql/kaggle/029_source_batch_base_contract.sql#L4) | Batch base |
| `check_source_batch_graph_v1` | [032_source_batch_validator_perf.sql:7](../../sql/kaggle/032_source_batch_validator_perf.sql#L7) | Graph |
| `check_source_batch_manifest_v1` | [026_source_batch_activation.sql:415](../../sql/kaggle/026_source_batch_activation.sql#L415) | Manifest/passage |
| `check_source_batch_media_v1` | [036_source_batch_media_scoped_lookup.sql:45](../../sql/kaggle/036_source_batch_media_scoped_lookup.sql#L45) | Media |
| `check_source_batch_semantic_v1` | [028_source_passage_evidence.sql:42](../../sql/kaggle/028_source_passage_evidence.sql#L42) | Facts/semantic/evidence |
| `check_source_batch_v1` | [026_source_batch_activation.sql:862](../../sql/kaggle/026_source_batch_activation.sql#L862) | Batch/finish |
| `check_source_catalog_v1` | [034_source_catalog_corrections.sql:11](../../sql/kaggle/034_source_catalog_corrections.sql#L11) | Catalog |
| `check_source_facts_v1` | [028_source_passage_evidence.sql:25](../../sql/kaggle/028_source_passage_evidence.sql#L25) | Facts/semantic/evidence |
| `check_source_finish_v1` | [026_source_batch_activation.sql:943](../../sql/kaggle/026_source_batch_activation.sql#L943) | Batch/finish |
| `check_source_graph_evidence_v1` | [011_geo_identity_manifest.sql:379](../../sql/kaggle/011_geo_identity_manifest.sql#L379) | Graph |
| `check_source_graph_v1` | [011_geo_identity_manifest.sql:407](../../sql/kaggle/011_geo_identity_manifest.sql#L407) | Graph |
| `check_source_illustrations_v1` | [020_media_identity.sql:18](../../sql/kaggle/020_media_identity.sql#L18) | Media |
| `check_source_image_graph_evidence_v1` | [014_source_stage_activation.sql:396](../../sql/kaggle/014_source_stage_activation.sql#L396) | Graph |
| `check_source_image_graph_v1` | [014_source_stage_activation.sql:417](../../sql/kaggle/014_source_stage_activation.sql#L417) | Graph |
| `check_source_image_manifest_v1` | [025_source_passage_lookups.sql:71](../../sql/kaggle/025_source_passage_lookups.sql#L71) | Manifest/passage |
| `check_source_image_passages_v1` | [025_source_passage_lookups.sql:2](../../sql/kaggle/025_source_passage_lookups.sql#L2) | Manifest/passage |
| `check_source_import_graph_v1` | [011_geo_identity_manifest.sql:503](../../sql/kaggle/011_geo_identity_manifest.sql#L503) | Graph |
| `check_source_import_v1` | [006_source_import.sql:33](../../sql/kaggle/006_source_import.sql#L33) | Import |
| `check_source_import_v2` | [007_source_stories.sql:96](../../sql/kaggle/007_source_stories.sql#L96) | Import |
| `check_source_import_v3` | [008_source_facts.sql:194](../../sql/kaggle/008_source_facts.sql#L194) | Import |
| `check_source_import_v4` | [009_story_editorial.sql:525](../../sql/kaggle/009_story_editorial.sql#L525) | Import |
| `check_source_manifest_v1` | [009_story_editorial.sql:372](../../sql/kaggle/009_story_editorial.sql#L372) | Manifest/passage |
| `check_source_materialize_v1` | [014_source_stage_activation.sql:711](../../sql/kaggle/014_source_stage_activation.sql#L711) | Activation |
| `check_source_passage_evidence_v1` | [028_source_passage_evidence.sql:4](../../sql/kaggle/028_source_passage_evidence.sql#L4) | Facts/semantic/evidence |
| `check_source_passage_manifest_v1` | [009_story_editorial.sql:310](../../sql/kaggle/009_story_editorial.sql#L310) | Manifest/passage |
| `check_source_quoted_facts_v1` | [008_source_facts.sql:46](../../sql/kaggle/008_source_facts.sql#L46); rename [SQL028:24](../../sql/kaggle/028_source_passage_evidence.sql#L24) | Facts/semantic/evidence |
| `check_source_stage_packet_v1` | [026_source_batch_activation.sql:988](../../sql/kaggle/026_source_batch_activation.sql#L988) | Stage/seal |
| `check_source_stage_seal_v1` | [013_source_stage.sql:229](../../sql/kaggle/013_source_stage.sql#L229) | Stage/seal |
| `check_source_stage_seal_v2` | [014_source_stage_activation.sql:110](../../sql/kaggle/014_source_stage_activation.sql#L110) | Stage/seal |
| `check_source_stage_v1` | [013_source_stage.sql:54](../../sql/kaggle/013_source_stage.sql#L54) | Stage/seal |
| `check_source_stage_v2` | [026_source_batch_activation.sql:1161](../../sql/kaggle/026_source_batch_activation.sql#L1161) | Stage/seal |
| `check_source_withdraw_v1` | [005_source_withdraw.sql:4](../../sql/kaggle/005_source_withdraw.sql#L4) | Withdraw |
| `check_story_access_v1` | [009_story_editorial.sql:204](../../sql/kaggle/009_story_editorial.sql#L204) | Story editorial |
| `check_story_edit_v1` | [009_story_editorial.sql:93](../../sql/kaggle/009_story_editorial.sql#L93) | Story editorial |
| `check_story_reconcile_v1` | [016_story_reconcile.sql:173](../../sql/kaggle/016_story_reconcile.sql#L173) | Story reconcile |
