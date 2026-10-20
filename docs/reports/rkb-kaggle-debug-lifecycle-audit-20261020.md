# RKB: критический аудит стоимости отладки и жизненного цикла Kaggle

Дата: 2026-10-20. Результат — **проектирование по исходникам и документации**, не выполненная реализация. Kaggle runs, installs, inference, imports, stop/delete и benchmarks в этой задаче не запускались. Новые runtime критерии имеют статус **NOT RUN**.

Нормативный результат — [один полный handoff v4](../prompts/rkb-scalable-kaggle-implementation-20261019.md), прежде всего §5.4, §6.6 и §14.5. Этот отчёт хранит основания и разбор критики; он не второй план внедрения. База изменения — main `3e39046fe32aaf14cfa088d65d15ff0082425309` после PR155.

## 1. Принятое решение

Обычная итерация должна выглядеть так: небольшой exact artifact → apply в уже работающем owned run → фактически загруженная revision и полезный result readback. Новый notebook, переустановка всех dependencies, повторная загрузка BGE и новая Dataset version не являются шагами каждой правки.

Notebook **slug стабилен**; версии, run/operation IDs и launch nonce различаются. Ранний минимальный WSS/HELLO предшествует тяжёлым install/restore/model stages. READY приходит после model/SQL/query probes. Один существующий controller управляет запуском, attachment, maintenance, состоянием и остановкой; второго scheduler/poller/remote-shell framework нет.

| Что меняется | Достаточное действие |
|---|---|
| Совместимый SQL/function | Exact migration к живому PG; транзакционный ledger/readback, без model reload |
| Stateless read handler | Import/self-test candidate, drain, одна замена ссылки, полезный probe; rollback на прежний handler при ошибке |
| Mutation executor / несовместимая функция | Ordered revision barrier, reconciliation старых UNKNOWN и сохранение replay semantics |
| Transport/model-owning context/deps | Controlled process restart; повторный model warmup, если он действительно нужен, показывается честно |
| Крупная новая DB base | Exact download и конечное PG maintenance окно внутри той же notebook session; active routing отдельно от recovery pointer |
| Bootstrap/image/завершившаяся session | Один новый разрешённый run с restore; прежний UNKNOWN сначала reconciled |

Baseline языка из v3 сохранён: Python glue, один resident PyTorch BGE, native PG/pgvector/PostGIS; Go/Rust — обоснованная замена конкретного компонента, без обязательного rewrite. SQL-функции используют существующий pool, не требуют нового HTTP RPC сервиса. Второй PG cluster/catch-up — возможная оптимизация измеренной паузы, не обязательная часть первого debug цикла.

## 2. Что найдено в наших проектах

### RKB: есть reusable SQL path, но warm-update границы ещё нет

[RKB worker на 3e39046](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/3e39046fe32aaf14cfa088d65d15ff0082425309/scripts/production/bge_kaggle_worker.py) создаёт model/tokenizer и transport/encode loops внутри `main`. Существующая модель загружается один раз, но безопасной смены handler, раннего WSS и нового supervisor protocol там нет. Прежняя heartbeat/claim polling схема не должна называться уже реализованным bidirectional runtime. Failure до загрузки модели также не покрывается одним только summary в завершающем блоке рабочего loop.

[PostgresDataClient](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/3e39046fe32aaf14cfa088d65d15ff0082425309/src/regional_knowledge/postgres_backend.py) вызывает parameterized SELECT через AsyncConnectionPool. Его `_rpc` не является лишним HTTP hop. Reuse pool/parameter binding/atomic mutation pattern оправдан; legacy scope enumeration всех документов и прежние E5/auth assumptions переносить нельзя.

### My Data Hub: полезные identity/readback guards, недостаточный stop proof

Аудирован source head `a26797759fe30718fc6a12206c344968dcdc80a9`. [Kaggle adapter](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/providers/kaggle/adapter.py) различает обычный и attested master путь. Обычный путь сверяет latest private notebook identity/source/version; attested output связывает результат с run/attempt/epoch/executed source. Это полезная защита от подмены результата новой версией, но не доказательство состояния старой compute session.

`terminate_attested_master_run` проверяет ожидаемую identity, вызывает `kernels_delete` и ждёт отсутствия notebook resource. Такой receipt означает наблюдаемое отсутствие ресурса, **не подтверждённое завершение конкретной execution**. Переносить его в RKB как освобождение Telegram slot нельзя. SDK pin `kaggle==2.2.4` и исторические wheel сведения также не доказывают текущую установленную capability.

### Events Bot: хороший unknown-launch pattern, опасная для RKB нормализация cancel

Аудирован head `d431b9f86b87999cf27f74b99857d23aedc28f16`. [Registry](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/kaggle_registry.py) сохраняет launch intent и уникальные config/input references. При неизвестном исходе push сверяет metadata/input identity; lookup error остаётся indeterminate, advanced remote revision не разрешает слепой resubmit. Это подходящий reuse pattern, но уникальные inputs/nonce **не требуют менять notebook slug**.

[Poller](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/video_announce/poller.py) объединяет `cancel_requested`, `cancel_acknowledged` и `cancelled/canceled` в ветвь обработки failure/reconciliation. Для строгого RKB compute/TG lifecycle эти состояния необходимо различать. Нельзя объявить процесс остановленным по запросу или подтверждению принятия cancel. Declared `kaggle>=1.8.2` не фиксирует реально установленную версию и не подтверждает точную pagination/status semantics wrapper.

Эти выводы относятся к проверенным source paths, не означают, что все executions двух проектов завершались неверно. Установленные версии, действующие provider sessions и реальная отмена в этом аудите не испытывались. Полезные primitives переиспользуются узко; полный перенос чужого оркестратора не нужен.

## 3. Внешняя документация и исправление устаревших выводов

Официальный [Kaggle CLI source 6894864](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/src/kaggle/api/kaggle_api_extended.py) содержит `version_label` в status/output и штатный обход output pages, когда caller не ограничивает page token. [Issue1045](https://github.com/Kaggle/kaggle-cli/issues/1045) о пропущенных страницах закрыт [PR1046](https://github.com/Kaggle/kaggle-cli/pull/1046). Следовательно, неверно утверждать, что нынешний официальный CLI всегда скачивает лишь первую страницу, или писать поверх него ещё один paginator. Проверить нужно выбранный installed SDK/adapter и expected inventory.

Открытый [issue1169](https://github.com/Kaggle/kaggle-cli/issues/1169) описывает отсутствие public CLI cancel и доступного session ID в проверке автора на 2.2.4. Это конкретный report о границе API, не договор Kaggle и не доказательство невозможности всех способов отмены. В проверенных наших adapters готового exact execution cancel не найдено. Stage0 должен показать фактический поддержанный stop path и terminal readback; наличие метода с подходящим названием недостаточно. Cooperative stop остаётся полезным, но hung supervisor требует честного UNKNOWN до доказательства завершения.

[Kaggle kernels documentation](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/docs/kernels.md) и [Kagglehub README](https://github.com/Kaggle/kagglehub) помогают различать новую version/run, сохранение без запуска, exact version download и подключение resource в работающую session. Из них не следует, что уже mounted input или загруженный Python module автоматически обновится. Dataset используется для checkpoints/крупных artifacts; частый SQL/code patch идёт отдельным небольшим артефактом.

Поиск также охватил Kaggle discussions о [Session Persistence](https://www.kaggle.com/discussions/product-feedback/355440) и [потере working directory](https://www.kaggle.com/discussions/product-feedback/427717). Доступный web extract этих страниц не содержит полного thread. Они используются как указатели классов отказов, **не** как доказательство provider SLA, гарантированной persistence или конкретного сегодняшнего лимита. Recovery проектируется по проверяемым artifacts/receipts, а не обещанию сохранения notebook RAM/files.

Технические ограничения сверены с первичными источниками: [PG17 CREATE FUNCTION](https://www.postgresql.org/docs/17/sql-createfunction.html), [CREATE INDEX](https://www.postgresql.org/docs/17/sql-createindex.html), [PREPARE](https://www.postgresql.org/docs/17/sql-prepare.html), [filesystem backup](https://www.postgresql.org/docs/17/backup-file.html), [Python importlib.reload](https://docs.python.org/3/library/importlib.html#importlib.reload). Их следствия в проекте: совместимость signature/return shape, контролируемые grants/search_path, транзакционный migration ledger, отдельный нетранзакционный concurrent index step при необходимости, actual custom/generic plans, запрет подмены live PGDATA и произвольного reload всей import graph. Это проектные решения, не измеренный прирост производительности.

## 4. Два новых раунда actual Opus 5.5

Обе консультации — DevCoveer `provided_only`, read-only, provider Anthropic, **actual model claude-opus-5-5**, без fallback. Переданы owner requests, source evidence, draft и ответы root/Dot. Консультант не запускал workload и не изменял документы.

| Раунд | Task | SHA-256 сохранённого Markdown | Суть |
|---|---|---|---|
| 1 | `dvt_0ccc65088b074973aa02c191256809c8` | `3b6e885266975f1a3224db194745add69a6420c41643d352b62b2a08253ccfda` | Упростить disposable debug, отделить update/restart, уточнить identity/stop/completeness |
| 2 | `dvt_e643e0fd434b46ad83c9678808fc6887` | `1c8ca801d722c48ae8aec9efdf037b295574640a9ce6a651c868e2e85990999b` | Adversarial review exact §5.4/6.6: семь конкретных must-fix и замечание об объёме |

### Принятые замечания и конкретные ответы

| Критический вопрос | Решение в handoff |
|---|---|
| C принята при function v1, ACK потерян; установлена v2 — как повторить C? | Barrier закрывает новую v1 фиксацию и завершает всю accepted/sealed v1 queue, включая undispatched; UNKNOWN сверяется в исходной `applied_ops`/DB generation. Commands/transitions сохраняют порядок и semantics; replay не использует latest функции для старой C. |
| Handler заменён, import/probe упал или mutation всё ещё исполняется? | Hot path только stateless reads, candidate self-test до switch, прежний handler сохраняется для rollback; durable executor меняется через drain/reconcile barrier. |
| Nonce меняет source hash, как отличить code reuse и новый run? | Code hash отдельно от launch-config и exact executed-source hash. Bootstrap incarnation ID отличает два исполнения одной version/nonce, controlled worker restart — отдельная generation. |
| Что именно доказывает остановку? | Свежий provider terminal status конкретной execution с проверенной семантикой. Delete/ACK/TTL/child exit/latest-by-slug не годятся. Telegram revocation освобождает только session-use, не compute. |
| Supervisor тоже завис или credential уже прочитан? | Supervisor best-effort; controller fence проверяется на durable sinks. Telegram не имеет такого sink fence: прежний credential нельзя считать отозванным по epoch. Secret-at-start явно кооперативен; preferred credential delivery после HELLO. |
| Manifest не создан либо producer объявил пустой success? | Controller до операции задаёт expected required kinds/IDs и completeness rule. Run manifest сверяется с ними; отсутствие required data/manifest — partial/unknown. |
| Разработчик пометил реальную БД disposable и reset удалил Saved? | Immutable fixture classification, отдельные test bindings, запрет product routing/real ACK; reset отклоняется при настоящих GEO/accepted/Saved/user refs или прошлой product designation. |

### Что принято с поправками или отклонено

- В раунде1 TTL/self-exit grace предлагались как практическая граница reuse. **Не принято как proof:** SIGSTOP/зависший parent оставляет живой процесс и прочитанный Telegram credential. Таймер остаётся защитой, UNKNOWN — состоянием, требующим reconciliation.
- Наличие одного manifest **не** completeness. Проверяются требуемые artifacts и их bytes/hash; ожидаемый inventory нельзя брать только из завершившегося run.
- Не добавлена обязательная новая debug Telegram session. RKB2 ещё не готова, прежний verified session/quota cap сохранён.
- Не введены обязательные model sidecar, новая WSS file-transfer система, remote shell/IDE, multi-cluster catch-up и отдельный scheduler. Переиспользуются current authority и existing artifact transport.
- Формальное предложение отклонять fixture reset при **любом** `applied_ops` уточнено: synthetic test ledger нужен для idempotency/migration tests. Запрет относится к реальным product refs/ACK/routing; тестовые записи не повышаются до production.
- Числовые polling defaults оставлены настройками, не SLA/новыми hard gates. Conditional index/plan checks относятся только к применимому SQL change; не требуется реализовать все optional optimizations до первого run.
- Заключение консультанта о передаче было условным; root внёс исправления после раунда2. Финальная независимая сверка дополнительно закрыла undispatched accepted v1 queue: одного drain in-flight/UNKNOWN было недостаточно. Отдельного третьего Opus approval и runtime PASS нет. Запусков в этой документационной задаче нет; дальнейшая реализация действует в рамках реально выданного задания и бюджета, без придуманного требования заново согласовывать каждую уже разрешённую итерацию.

## 5. Обсуждение с Dot и владельцем

Владелец поставил задачи [живой отладки/SQL](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792486703770319), [снижения цены итерации](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792486782748519) и предоставил [Grok guide как спорный input](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792487877523119). Дополнительное [решение о стабильном slug](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792490139433929) закреплено без нового вопроса владельцу.

Dot подтвердил [semantic replay migration case](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792489860546829), [границу delete/absence в MDH](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792489893676429), [installed SDK/attestation/registry facts](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792489949080839), [уточнение current CLI pagination](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792490076972819). В [финальном stop ответе](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792490447774779) готовый verified exact execution cancel в проверенных двух проектах не найден. Это ограниченный результат code/evidence audit, не заявление о невозможностях Kaggle вообще.

Root проверил ключевые исходники самостоятельно, не перенёс мнения консультантов в статус установленного runtime факта. Прямое противоречие старого runbook «restore → model load → WSS» исправлено на ранний control/HELLO; recovery pointer отделён от active DB routing.

## 6. Что полезно и что избыточно в Grok guide

Приняты воспроизводимые modules/dependency pins, небольшие осмысленные fixtures, явные stage diagnostics, checkpoints там, где они действительно восстанавливают работу, и bounded supervisor. Это помогает RKB.

Не приняты как общий метод `Restart Kernel & Run All` на каждую правку, произвольные 5% данных как доказательство качества, обязательная схема CPU trial→GPU final, Polars/ML-training checkpoints как ответ на PG/WSS/recovery проблему и повтор всего `main` после каждого hot update. Seed не делает историческое извлечение или внешний provider effect автоматически воспроизводимым. Точная принятая модельная работа сохраняется как payload/receipt; source-aware fixture проверяет конкретный риск.

Стоимость цикла измеряется через edit-ready→apply→first useful result, install/restore/encode wait, starts/process restarts/model loads и повторно скачанные bytes. Cold bootstrap, совместимый warm patch и несовместимое обновление отчётны отдельно. Экономия времени/денег в этом проектировании не измерена; нет выдуманной цифры «ускорение в N раз».

## 7. Приёмка, хранение и границы завершения

Новая DBG-1–10 матрица сначала требует дешёвые deterministic fixtures: invalid handler, lost ACK до/после migration, reset с реальными refs, output pagination/hash/identity и unknown push/stop. Один bounded разрешённый Stage0 run проверяет реальные provider boundaries, WSS и useful readback. Не запускать второй notebook только ради mock-сценария; не повторять весь corpus benchmark при каждой SQL правке. Ordered migration/recovery работают до первого настоящего accepted/Saved, включая GEO.

Физическое хранение также ограничено: hash-pinned artifacts и cache reuse, без полного Dev checkout/OS image/HF cache в snapshot; PGDATA/models/secrets вне auto-published output; staging и old/candidate overlap входят в peak budget; reserve нужен для WAL/control/shutdown. Dataset не растёт новой версией от каждой правки. Unreferenced artifacts очищаются по refs/ownership, accepted/UNKNOWN — не по возрасту. Эти меры сокращают повторные downloads/install/reindex и не отнимают индекс/cache, который доказанно улучшает скорость.

Предыдущие функции сохранены: свежий MCP import; настоящий saved Variant.body и отдельная provenance; даты/люди/organizations/events; GEO/toponyms/historical layers; disagreement registry/cases; роли; personal MCP, Slack, Mira/PH. GEO-only перенос не расширен. Девять protected requirement IDs/critical flags и количественные gates в `.devcoveer/requirements.json` **не меняются** этим дополнением.

Документы архитектуры/configuration/storage/operations/cutover/index maintenance/AGENTS и acceptance согласованы с v4; старые отчёты сохраняют историческое содержание. Документ завершает проектные решения. Установленные SDK, exact session/stop semantics, private Dataset attach, writable-volume footprint и реальные latency/restore/iteration timings остаются обязательной проверкой реализации, а не скрытой предпосылкой или уже выполненным обещанием «швейцарских часов».

