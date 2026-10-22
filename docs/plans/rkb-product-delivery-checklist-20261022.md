# RKB — продуктовый план и общий чеклист поставки

**Project manager: dot.** Версия 1.1: назначения/блокеры обновлены 22.10.2026, 10:47 UTC; технический evidence-срез §2 — 10:29–10:31 UTC. Это рабочий план достижения полного результата, **не новый аудит и не текстовый пилот**. dot поддерживает приоритеты, владельцев, блокеры и доказательства; исполнители предлагают поправки по своим вехам, не останавливая независимую разработку.

**Сейчас принято реальных книг: 0.** Работающий Kaggle и зелёные component tests — важные предпосылки, но не приёмка импорта, изображений, рассказов или продукта. Процент готовности не рассчитываем по числу PR/тестов.

## 1. Что пользователь должен получить

Владелец импортирует реальный PDF/DjVu, модель читает источник и сохраняет текст, иллюстрации, факты и при достаточном основании настоящий рассказ. После подтверждённого **Saved** другой разрешённый пользователь находит основание, читает сохранённый body, получает допустимые изображения, продолжает исследование из нового чата и после восстановления сервиса. Разногласия, экспертная работа, Slack/личный MCP/Мира, GEO, публичные страницы и внешние обсуждения сохраняют функции исходного продукта на новой архитектуре.

**Ближайший критический путь:**

`совместимый integration + recovery/GEO prerequisites → модель читает одобренный оригинал → source-bound facts + body при достаточном эпизоде ИЛИ justified needs_context + reachable frontier → atomic source activation → durable Saved → ordinary search/fetch/body/actual illustrations readback → post-acceptance continuation → restart/recovery readback → quality/latency/scale`

**Совместная приёмка M2–M4:** persisted/Saved означает доказанную сохранность конкретного payload/outcome, а не автоматическую accepted retrieval readiness. Источник становится accepted/retrieval-ready только после source/index gates и natural/paraphrased/relevant cross-language per-book проверки точной candidate/active revision. Facts/body либо justified frontier создаются в том же чтении и activation, а M3/M4 после Saved проверяют обычную выдачу. Это не последовательная очередь «сначала принять пустую книгу, потом добавить знания» и не цикл зависимостей.

Recovery-предпосылки проверяются **до** первого Saved; восстановление именно принятой книги — **после** него. Каналы/экспертные и публичные поверхности разрабатываются параллельно, но не исчезают из полного результата.

### Три обязательных документа для КАЖДОГО назначения

Передаются вместе с задачей, exact scope и критериями; фраза «сделать импорт» не заменяет их:

1. [Полное задание v4.1](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md).
2. [Уточнения владельца](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/designs/rkb-owner-alignment-20261021.md).
3. [Полная приёмка](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/testing/rkb-scalable-kaggle-acceptance-20261019.md).

Pin требований: `bd138b2449f68cabb2cbfa3df552df8471f40c78`. Этот план группирует, а не ослабляет контракт. При разночтении старого daily-S3 расписания и owner alignment действует **Dataset primary + weekly independent Yandex backup**. Непроверенное удалённое покрытие означает pending, не Saved.

### Как читать статусы

- `not_started`: веха не начата как сквозной результат; наличие старого кода не доказательство.
- `in_progress`: есть активный исполнитель и следующий ограниченный результат.
- `blocked`: назван конкретный недостающий вход/владелец разблокировки; независимая работа продолжается.
- `verified`: выполнена собственная приёмка вехи на exact candidate/runtime/corpus, приложено evidence.

Чекбокс вехи закрывает **продуктовый результат**, не существование модуля. В evidence отдельно пишем source/CI synthetic, real-class fixture, actual PG fixture, installed/live, real-book acceptance. PASS старого SHA, fake provider или skipped SQL нельзя повышать до live PASS.

## 2. Где мы действительно находимся

Каноническая ветка — `integration/rkb-native-import-20261022`; исходный SHA `4e6a7b7ab0c90a9ff41effa37f25f92930041ccf`, SHA базы этого doc-only branch **`1c6cf0e66ed3c75b3677cb034e87ed1b316e99f8`** после PR229. При создании PR238 GitHub уже показал target **`b932c15ef133d668a0eaa3570cc895e750876c77`**; историческая база не названа текущим HEAD. Ветки исполнителей направляются PR в integration, **не main**. Только Opus сливает и управляет live.

| Наблюдение / вход | Что доказано | Что НЕ доказано / ближайшая зависимость |
|---|---|---|
| v106 погиб при resize/reboot Dev, reconnect исчерпался примерно за 30 s | Причина установлена; [PR228](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/228) вводит требуемый grace 600 s | Это не тест сохранности принятой книги |
| Opus: v107 run `883f0e5d`, READY с 10:01; readback 10:02, Dataset2, applied/projection20, schema SQL001–011 | Восстановлен живой базовый сервис; release4e6a7b7, READY за217 s | GEO repair12 и автоматический successor ещё нужны; SQL013–016/media не считаются installed |
| dot: actual search 10:02, ready, seq20/20, results=[], backend336.109 ms; позднее Шкипер735.315 ms | Реальный MCP read на текущем пустом текстовом корпусе | Ни meaningful source retrieval, ни p95, ни приёмка книги. Opus признал degraded/bounded_retrieval вводящим в заблуждение success label — исправить без сокрытия настоящих ограничений |
| [PR223 packaging](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/223), head3df8575 | GitHub39762610380 SUCCESS15PASS и full39762616526 SUCCESS обе Python; artifact12419470365:103 packages/3532files, safe extraction+recursive ELF closure PASS | Ранний39760622775 RED исправлен. Native execution/actual target ABI NOT RUN; permanent bundle/manifest/pyaes placement receipt и prepared-file budget≈165+ ещё нужны |
| Frozen first-context77 paths5f63; compatible baseline01a1c71 / candidate317a539 | Шкипер: byte-identical source, CI39759312648 9PASS; [PR233](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/233) содержит только2 test/doc paths | Полный source ещё не в integration. Синтетические logical barriers5/6 — НЕ реальные Dataset versions |
| [PR229 health](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/229) merged; [PR234](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/234) head1db82e6, focused/full green | Read-only adapter; follow-up sanitizes public DTO и отвергает неполную SQL readiness | Реальный native SQL request kind/startup hook ещё не подключён. Health eager-imports non-native MCP/read-authority graph: просто включить файл в bundle недостаточно |
| [PR231 media](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/231), head04e2b1d | Пять собственных файлов Рифа; CI39761245340:2499PASS/42SKIP на3.12/3.13, genuine authority classes |10 skips требуют настоящие SQL013–016. Это не live archive/pixels; старый PG17 PASS PR222 не заменяет combined candidate |
| [PR236 Quartz](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/236), head655a2a | GitHub39762654094 SUCCESS:2461PASS/32SKIP обе Python; real Dataset-bound writer/signed ingress classes | Старый39761936577 RED на da1b511 исправлен только fixture signed retry order, не runtime bug. Native/provider boundaries synthetic, не live Dataset adoption; PR226 отдельный leaf |
| [PR230 Linza](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/230), current3a417be4; [PR232](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/232),87269af | First-book gold/workload и reconnect regression; PR230 full39761688016 SUCCESS на current3a417be4; PR232 сохраняет отдельный RED READY race, не продуктовый PASS | Gold/live quality не пройдены; latency source fix — отдельная активная обязанность, не закрытая этими тестами |

Текущий координационный источник: [общий Slack thread](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792577182627799), прочитан ограниченный свежий диапазон с1792663059.059128 до конца доступных сообщений. Статусы меняются: при следующем handoff перепроверить exact HEAD/checks, не копировать эту таблицу как вечный live report.

**Первый оригинал уже одобрен:** Гаузе,174 страницы,12 752 172 bytes, SHA256 `10328207e2664aabef78201fb997e18f3ea8e48f8d7d56a98f3d6832b21ddb65`. Не просить повторную загрузку без установленной потери/истечения реального ref. **В10:44 provider readback подтвердил доступность того же полного оригинала и download SHA; повторная загрузка от владельца не нужна.** Пустой target book_find в10:36 означает «ещё не импортировано», не потерю оригинала. До модельного чтения dot получает этот проверенный оригинал по поддержанному private client path; наличие provider объекта не равно уже просмотренным страницам. Сохранять тот же import operation; книга, private resource IDs/URLs и её частный текст не публикуются в GitHub.

## 3. Одиннадцать продуктовых вех

### [ ] M1. Можно безопасно начать настоящий импорт
**Статус:** `in_progress`. **Ответственный:** Opus. **Исполнение:** Шкипер — packaging/health в своём scope; остальные передают совместимые leaves.

**Следующий результат:** один проверенный combined integration candidate с точным dependency manifest, рабочим native import path и честной readiness. Подтянуть согласованные77 source paths, ordered SQL и независимые PR без whole-file overwrite; PR233 — только2 test/doc files, не носитель77path source, а PR236 — combined source+PR215, не простой однофайловый leaf: Opus выбирает exact paths/порядок и проверяет совместимость, не сливает чужой stack вслепую; закончить actual103 assembly, pyaes hosting receipt и native-safe health hook. Исправить misleading success label.

**Зависимости/передача:** Шкипер отдаёт artifact/hash/ELF receipt и sanitized DTO; Opus проверяет shared protocol/runtime/SQL, exact installed versions. До production import нужны GEO identity manifest и доказанный Dataset intent/outcome coverage, fencing и ordered migration recovery; полная scale-приёмка не требуется для каждого component test.

**Принимаем по evidence:** actual private transfer/SDK/stop capabilities; PG17/pinned pgvector/PostGIS+BGE, SQL/BGE/readiness отдельно от Telegram readiness; одна verified authority, одна model instance; DBG-1–9/11–13 применительно к включаемому пути, DUR/FEN/REC. До unresolved coverage — pending, никаких ложных accepted/Saved или provider effects с незакреплённым intent. Runtime skill install/read receipt обязателен.

### [ ] M2. Одобренная книга принята целиком, а Saved можно восстановить
**Статус:** `blocked` M1. **Ответственный:** Opus — интеграция и разрешённый live; **Модельное чтение/первый MCP импорт:** dot; **приёмка:** Линза.

**Следующий результат:** первый end-to-end импорт уже одобренного Гаузе: source identity/edition → model reading → pages/stage/validate/finalize → точный durable receipt. Не называть первые страницы полной принятой книгой. Затем отдельный настоящий DjVu подтверждает сохранение этого формата.

**Исполнительские границы (10:47):** **dot назначен модельным reader/import runner первой уже одобренной книги через MCP. Кварцу назначен actual CODE пакет native stage/tokenize/embed/materialize + точный import callsite/composition patch с reuse77 paths; статус — assigned/awaiting ACK, исполнение blocked доставкой задания.** Launcher не смог ввести prompt; новый import-worker также не стартовал. Queued/попытка запуска не означают ACK или активного исполнителя. dot устраняет delivery blocker и получает exact scope ACK, не объявляет работу начатой. Кварц готовит отдельный PR→integration, только import-specific patch, не whole-file replacement; занятые lifecycle Opus, media Рифа и latency Линзы не затрагиваются. Shared composition конфликтующие hunks и merge остаются у Opus. Это не назначение Opus всей реализации.

**Зависимости/передача:** поддержанный client fileParams/direct bytes без Dev bulk proxy; reader модель получает реальные страницы из клиента либо book_pages по необходимости. Не повторять backend extraction/render, когда pixels уже даны; не добавлять backend OCR/VLM/LLM. Оригинал/archive locator и required checkpoint должны быть доступны.

**Принимаем по evidence:** все печатные материалы, tables/captions/footnotes, semantic spans/cross-page continuation и source-review coverage сохранены; около256 tokens/passages и strict512 полного BGE input,1024 FP32 без truncation. Интегрированный source diff и actual readback доказывают Kaggle-local stage/semantic graph/tokenize/embed/materialize: **Dev-bound query/document vector bytes=0**, Dev не хранит рабочий semantic JSON/corpus. На exact integration1c6cf0e независимый code review указал kaggle_importer.py::_vectors (516–549), _semantic/_save, composition local tokenizer и book_ingest action dispatch: vectors собираются из Dev queue в resolved payload. Этот finding относится только к1c6cf0e; поздний77path/другой candidate по этому finding не обследован и не объявлен неработающим. Наличие importer/Story fields не закрывает проверку replacement wiring. MCP action dispatch должен реально включать reprocess/rechunk по совместимому контракту, а не только start/stage/validate/finalize/status. Один owner-only start/resume/reprocess lease; второй start не параллелен, уже принятые книги читаются. Retry stage/finalize не дублирует source/facts/story; revision/dedup/catalog/reprocess/rechunk работают. Article-scoped retrieval проверяет две статьи на одной странице, cross-page continuation/semantic scope, parent union и stale revision, без примеси соседней статьи. Final Saved имеет exact verified Dataset version/cursor, recoverable input+outcome, не только local applied. IMP-1–3, VIS-1, DUR-1–4, REC-5/7, DBG-11/13.

### [ ] M3. Книгу можно найти, прочитать и увидеть её иллюстрации
**Статус:** `in_progress`: wiring передан Рифу, live acceptance ещё blocked M1/M2/SQL dependencies. **Ответственный за media-result:** Риф; **shared/live интеграция:** Opus. **Приёмка чтения:** Линза.

**Следующий результат:** обычный MCP search/fetch возвращает релевантный exact source evidence и настоящий ImageContent связанной иллюстрации, а не только описание. После wiring Риф выполняет отдельно порученную владельцем **реальную ChatGPT E2E цепочку: модель видит страницу и делает crop → crop сохраняется в Telegram archive → повторный MCP fetch извлекает сохранённые pixels, модель их действительно видит**. Нужны actual archive locator/receipt и повторный ImageContent readback, не fixture или только metadata; без нового notebook restart и без новых credentials. Существующие grants и live координация сохраняются. Opus назначил Рифу server.py illustration_fetch + backend adapter + read_issuance wiring на b932c15 (Slack1792664965.397179); задача возобновлена10:30, job6fda07b9-d71b-4aa0-98cf-7c081883809b. Этот план отражает назначение, не самостоятельно расширяет его.

**Зависимости/передача:** PR231 + реальные SQL013–016/narrow SELECT grants; native dispatch/WSS installed reader, server illustration_fetch, backend adapter, compatible read_issuance и actual NativeTelegramIllustrations.observe/authorized artifact issuer. Назначенный shared media участок — Риф; перед записью он ACK exact scope/base. Native protocol/grants остаются у Opus, их назначение Рифу не подразумевается.

**Принимаем по evidence:** hybrid BGE+lexical/alias внутри Kaggle, batched hydrate и catalog keyset; vectors не делают круг через Dev. Natural/paraphrased и relevant cross-language per-book queries; stale/min_data_seq/withdrawal не превращаются в false not-found. Chunk↔figure, printed caption, labelled model description и pixels сохранены; search не ждёт Telegram. Прямой Kaggle↔Telethon archive read/write, один uploader/pacing/FloodWait, expected photo/document доступен по locator — без обязательного post-upload custom SHA download. Photo default; high-res только по фактической нужде. Иллюстрации всем; scan/stripe только scope+scan_proof_read для owner/demo/expert/local_historian, оригинал с отдельным grant. Отдельный proof request: PDF/DjVu page → model bbox → deterministic stripe; current actor/source/audience recheck до final issuance, no stale pixels. Partial items/MIME/preview binding/cache grants проверены. SRC/IDX, PRF-1–5, ARC-1, FAST-2/5/6.

### [ ] M4. Из источника сохранены знания и настоящий рассказ
**Статус:** `in_progress` совместно с M2; integrated acceptance ещё blocked M1/непереданным composition scope. Разработка M4 НЕ ждёт принятия книги. **Ответственный:** dot за продуктовую приёмку; **код/интеграция:** Opus в текущем first-context candidate; свободный последующий domain leaf назначается отдельно, не подразумевается.

**Следующий результат:** ordinary facts/calendar/observation/entity/Story get/search читает сохранённые значения; достаточный эпизод имеет фактический Variant.body, exact default revision и evidence map. Summary, «потенциал истории» и variants=[] не засчитываются.

**Зависимости/передача:** exact accepted model payload, target facts/story registry и dirty projections; source refs создаются при M2, не ждут готового media M3. Facts/body либо обоснованный needs_context+reachable frontier входят в atomic activation M2; после Saved проверяем readback, а не впервые придумываем рассказ. Модель в том же чтении сохраняет claims, dates, measurements, entities, places и body либо конкретный gap, без второго полного corpus scan.

**Принимаем по evidence:** факты существуют независимо от story, включая честный zero-story; original calendar/precision/date_role, decimal/unit/subject/period/method, language/time aliases, автор/рассказчик/персонаж и организация/здание различаются. Body обычно2–6 предложений, но нет квоты/минимальной длины; нет выдуманных причин/мотивов/современных объектов. Atomic body/default ref/evidence/receipt/projection; retry и get/search/read-export возвращают тот же body/hash, compact не подменяет summary. Human approved versions не переписываются новым источником; edit/merge/archive/restore/history/variants/assessment и publication_record сохраняются. STO-1–8, MET-1, source fidelity; human gold в M7.

### [ ] M5. Исследование продолжается после принятия книги и нового чата
**Статус:** `blocked` M2/M4. **Ответственный:** dot за результат и очередь; **shared frontier интеграция:** Opus, последующее bounded implementation назначение ещё открыто.

**Следующий результат:** закрыть чат после Saved, вернуться к сохранённому gap/frontier и дочитать/сопоставить без повторной загрузки книги и изменения immutable принятого payload. Затем две свежие книги A→B и B→A приводят к воспроизводимым связям/предложениям дел.

**Зависимости/передача:** атомарные activation+reachable research frontier, exact source revision/min_data_seq и M4 findings. Достаточный рассказ не ждёт будущего экспертного консенсуса; принятие книги не ждёт карты/POI.

**Принимаем по evidence:** один research workflow объединяет episodes/reconcile/case proposals/editorial compose и адресные gaps; не N² scan и не два независимых сравнения корпуса. Покрытие ranges/candidates/comparisons/deferred gaps различимо. Reextract не меняет chunks/vectors без explicit reprocess. Смена session/worker не повторяет inference для принятого payload. Source needs связываются с независимо импортированной работой внутри сборника после semantic check: downloaded/imported/task done ≠ fulfilled. STO-1/3/7, IMP-1/2, NEED-1.

### [ ] M6. Сохранённая книга, права и исследования переживают остановку/сбой
**Статус:** `in_progress`, окончательное real-book доказательство зависит от M2–M5. **Ответственный:** Opus; **bounded regressions:** Кварц/Линза в своих leaves.

**Следующий результат:** проверить на одном согласованном сценарии restore exact Dataset chain и readback книги/body/frontier; закрыть automatic cold-demand successor и GEO12. PR228/232 reconnect — предпосылка, не замена recovery drill.

**Зависимости/передача:** Dataset primary, weekly Yandex independent copy с age/version и проверенным restore/SQL+dense proof точных bytes;  exact commands/model outcomes/IDs, applied_ops, schema transitions и control state. Нельзя silently оставить online S3 primary или приписать weekly backup RPO0.

**Принимаем по evidence:** crash before/after apply/ACK, lost finalize/verify, corrupted snapshot, stale writer/socket, UNKNOWN provider effect и Dev control-disk loss имеют проверенный outcome. Нет двойной записи/отправки или потери confirmed data; UNKNOWN сначала reconcile, never blind resend/TTL erase. Existing controller запускает/переиспользует один cold startup, сразу возвращает pending/continuation до launch/restore work, не удерживает synchronous search; после READY клиент делает новый read с current authorization/generation, без replay старого ephemeral search; stable slug,600s control-loss grace и30min useful idle не выдаются за terminal proof. TG session exclusive lease не переиспользуется по одному expiry; exact provider termination/revocation границы честны. Warm SQL/handler patch сохраняет PG/BGE, readback loaded hashes; ordered migrations без live PGDATA overwrite, rollback после writes сохраняет tail. Control spool4h/512MiB и reference-aware GC/backpressure; temporary Dataset не удаляется при active/UNKNOWN refs. DUR/FEN/WSS/REC, DBG-1–13 с реальными соответствующими границами.

### [ ] M7. Качество, скорость и масштаб подтверждены измерениями
**Статус:** `in_progress` — workload/latency implementation; реальные gates `not_started` до corpus. **Ответственный:** Линза; **human verdict:** владелец либо назначенный им человек, организацию ведёт dot.

**Следующий результат:** использовать уже предоставленный Opus sanitized same-trace receipt (1792664965.397179): auth111, binding49, dispatchgate56, admission226, dispatch-result535(native199), finishgate220, resultterminal375, total1362ms; интервалы могут перекрываться, не суммировать их как независимые. Telemetry input blocker снят. Linza retry3e1a64f7 возобновлён10:28; source fix только подтверждённого bottleneck с before/after, не speculative guard changes. Исходный trace Opus:1362ms, predispatch448, WSS≈336, native199(encode184), postresult378; выяснить конкретные ожидания, не объявлять Python причиной. Далее первый source-grounded read slot и human gold.

**Зависимости/передача:** M2–M6 для live полного результата; PR230/232 — reusable evidence, не новые бесконечные fixtures. Exact changed paths перед записью, shared conflicts согласуются с Opus. Gold остаётся owner-private, в repo только допустимые refs/метрики.

**Принимаем по evidence:**
- ≥100 book-equivalent sources/100000 active chunks в isolated benchmark, не публичный test corpus; synthetic scale отдельно от реальных source-grounded quality families.
- ≥30 held-out fact families: BGE-only Hit@5≥85%, Hit@10≥90%, critical RU→DE Hit@10≥85%; lexical-only/fusion ablations, ANN vs exact.
- Warm backend p95≤1000ms; public MCP p95≤1500ms; evidence deadline2000ms. p50/p95, весь queue/tool-lock wait, concurrency/import/snapshot, warm/cold/cache, failures/pending denominator отдельно.
- Storage steady≤80%, peak≤90% включая WAL/index/snapshot/reimport/cache; реальные budgets, не рекламные квоты. Read/write concurrency: один import не блокирует representative reads, search reserve/fairness/cancel/maintenance доказаны.
-15–20 freshly reviewed original fragments, часть held-out, blind old/new human comparison; facts/uncertainty/source spans сохранены, body проверен после restart и ordinary reads. До человеческого вердикта qualitative PASS отсутствует.
- NATIVE-DECISION закрыт actual trace: измеренно оставляем baseline либо один оправданный Go/Rust candidate с полным IPC/lifecycle/ABI cost, без переписи ради языка. FAST/IDX/NAT и protected §14.1.

### [ ] M8. Эксперт и обычный пользователь работают через одну проверенную личность
**Статус:** `in_progress`. **Ответственный:** Кварц — signed ingress/identity acceptance; **shared/live:** Opus; **PM:** dot.

**Следующий результат:** передать уже зелёный exact PR236655a2a combined receipt и закрыть integration handoff; подключить и проверить actual non-owner personal MCP и настоящий signed Slack event/result/hash ACK.

**Зависимости/передача:** PR226 отдельно от PR236, Dataset durable child coverage и owner-assigned combined source. Прежний denial не обходится; authorised candidate не разрешает править frozen source. Ingress/route/registration wiring вне уже принятого scope требует явной передачи, не считается сделанным по fixture.

**Принимаем по evidence:** canonical person по verified provider/tenant/subject, capabilities∩case/artifact∩audience∩service fence; нет email/name guessing или owner fallback. Owner personal import/participant admin не доступны expert bot. Admin grant/revoke/link/unlink/credentials/domain weight и audit сохраняются; mistaken binding даёт attribution_review_required, не стирает историю. Verified demo/expert/local_historian атомарно materialize policy-derived scan_proof_read; claimed категория не даёт grant, expiry/removal снимают derived grant, manual revoke не отменяется чтением/replay. Private drafts/history принадлежат автору, selective publish не открывает остальное. Slack Kaliningrad Base остаётся внешним bot, Dot executor; current revoke после await, coalesced ETA>10s, fair canonical-person admission, ordered durable dialogs и UNKNOWN/no resend. Независимые reads не ждут durable log. ID-1, QUE-1–3, CHN-1, FAST-1/3–5; real sender E2E без impersonation.

### [ ] M9. Разногласия становятся живыми исследовательскими делами
**Статус:** `not_started` как сквозная приёмка; отдельные source components существуют. **Ответственный за план/назначение:** dot. **Исполнитель связки Case/research:** **не назначен**; выбрать свободного ChatGPT исполнителя после bounded handoff, не складывать всё на Opus.

**Следующий результат:** передать свободный Case leaf с точным scope, затем показать третьим источником дополненное то же дело, contribution/receipt и новую версию вывода.

**Зависимости/передача:** M4/M5/M8, reuse PR151 pinned adf78a5 participant/expert/research/Slack mechanisms через target PG authority. Не переносить legacy SQLite corpus.

**Принимаем по evidence:** identity Case = предмет/субъект/время/место, pair key лишь dedup proposal; aliases/merge/history и точные evidence revisions. Авторское мнение, модельная сводка и human support exact-version различаются; current vs last-supported conclusion, based_on boundary и новые нерассмотренные аргументы видимы. Third source не создаёт лишнее дело, silence не согласие. Сохраняются disagreement_participants/edit(operation_status), expert_request_delivery/expert_research/research_request, lost create response→same case_id solely by operation_key **после GC Dev spool**, delegated Slack ACK/reconciliation без дубля. Reviews сохраняют review_of и автора; research needs/PH tasks различают найдено/импортировано/fulfilled. CAS-1/2 и дополнительные CAS-F8–F10 из связанной migration parity; NEED-1.

### [ ] M10. GEO перенесено без подмены источников, Мира и Dot читают один продукт
**Статус:** `in_progress` GEO repair; PH/Mira live wiring `not_started`. **Ответственный GEO/cutover:** Opus. **Ответственный межпродуктового назначения PH:** dot; **PH implementation owner ещё не назначен**.

**Следующий результат:** GEO12 lookup/readback на сохранённых identity/provenance; отдельно назначить PH provider+normalizer+Live adapter scope компетентному свободному исполнителю, не создавать приложение/второй IAM.

**Зависимости/передача:** GEO identities до M2, bulk layers можно позже; PH actual resource-bound provider, compatible response version, current permissions и M3/M4. Реализация в других репозиториях — отдельное согласованное handoff, не подразумеваемое разрешение этого документа.

**Принимаем по evidence:** GEO-only manifest/counts/IDs/CRS/axes/units/geometry/GCP/transform/hashes/time/aliases/provenance; Regional Cartography geometry и Street Story POI ownership не дублируются. Legacy anchors остаются unanchored до exact edition/SHA/page/region; target writes переживают rollback. Identities/grants/config сохранены, non-GEO old books/stories/drafts/cases не мигрируют. Bulk freeze/delta и unresolved geo queue прозрачны. Проверены обе стороны bridge: **book→POI fact и POI→new evidence**, фактический Street Story intake receipt/readback, рабочий proof resolver (не фиктивный knowledge://evidence), current ACL/withdrawal/reanchor. Привязка POI требует model-selected resolution/provenance, similarity не identity; map/POI enrichment остаётся продолжением, не gate принятия книги.
Два независимых smart routes Dot→RKB и Mira→RKB, не Mira→Dot по умолчанию; progressive facts/story/calendar/observations/entities/GEO/cases/drafts/needs tools, source cards и тот же saved body. Реальные pending/result/hash ACK, context refs/reconnect либо explicit context gap, no stale audio/barge-in, measured tool-lock wait. Физический телефон,90min voice и4 users; paired Mira/Dot comparison accuracy/source fidelity/useful first+full answer/cost, не красноречие. MIG-1–3, PH-1–3, CHN-1/2, §14.4. «Хорошо подумай» delegation — не baseline release.

### [ ] M11. Публичные поверхности и итоговая эксплуатация закончены
**Статус:** `not_started`. **Ответственный за назначение/общий sign-off:** dot. **Реализация Astro/VK/PH bridge:** владельцы соответствующих интеграций, конкретные исполнители **не назначены**. **Финальный integration/live release:** Opus.

**Следующий результат:** после появления реального Case и свободной ёмкости назначить bounded public-bundle→existing Astro и VibePublish provider tasks; затем единый applicable-gates readout/runbook/cutover.

**Зависимости/передача:** M8/M9 права/Case, existing events-bot-new/site и VibePublish — не новые клиенты/token stores. Не публиковать тестовые мнения, посты, приглашения или подписки под видом приёмки.

**Принимаем по evidence:** постоянная public case page со stable random slug, библиотографией/допустимыми illustrations, current/last-supported выводами и ограничениями; mobile без login/JS requirement. URL существует только после actual HTTP revision readback; last-good сохраняется при failed update, late build fenced, revocation/tombstone/cache HTML/JSON/OG/assets проверены, нет private drafts/proof. VK editorial queue/schedule/move/cancel/comments/moderation — через VibePublish, native permissions/readback, cancellation_unknown не освобождает slot, pagination не выдаётся за полноту. Voluntary subscriptions/digests/expert invitations dedup/current grants; PH task done не fulfilled. Publication_record ≠ отправка поста.
Итоговый runbook: warm/cold/status/reconnect/import resume/failed command/restore/pending delivery/rollback; exact deployed source/schema/model/config/corpus и все applicable evidence. Старый runtime перестаёт writer только после gates; rollback сохраняет новые target writes, existing backup retention стартово7 дней, без удаления сейчас. PUB-1/2, VK-1/2, NEED-1 и все не закрытые acceptance families. **Core cutover не равен завершению проекта; незавершённый канал остаётся в плане.**

## 4. Кто делает следующий ход и где есть свободная ёмкость

Это очередь результатов, не выдача права на новые shared файлы. Обычные уже разрешённые шаги выполняются без постоянного запроса владельцу. Блокировка одной зависимости не повод замолчать или писать бесконечные тесты вместо интеграции.

| Владелец | Сейчас / ёмкость | Следующий исполнимый результат | После него / кто разблокирует |
|---|---|---|---|
| **dot, PM** | Активно: приоритеты/общий чеклист/межоконная координация | Собрать по M1–M11 ACK или exact correction от всех; зафиксировать gaps и очередь, не ждать общего «согласны» перед coding | Назначить свободные Case/PH/public leaves; получить releases/ACK, организовать first-book/human gold. Не заменять исполнителей ещё одним аудитом |
| **Opus** | Перегруженное узкое место: sole merge/live + сложные lifecycle/restore | Принять совместимые candidates, выдать свободный media wiring hunk, решить native-safe health contract; GEO12/automatic successor и recovery prerequisite | Combined CI → разрешённая live проверка → первый оригинал. Делегировать механический wiring, не повторять latency audit или чужие fixture fixes |
| **Шкипер** | Actual103 packaging готов; health hook блокирован; разрешена помощь telemetry | Передать green3df8575 artifact/ELF receipt, prepared-file budget и permanent placement inputs; извлечь разрешённый sanitized stage receipt для Линзы | Opus назначил health phase2 через native-safe kaggle_database_probe; исполнить свой leaf по этому контракту. PR2332paths ждёт77path source от Opus. Не держать работника на одном ожидании сборки |
| **Риф** | PR231 готов; Opus передал shared media wiring1792664965.397179, возобновлён10:30 | ACK b932c15 и exact server.py illustration_fetch/backend/read_issuance scope; реализовать wiring PR→integration, не новый standalone test круг | SQL013–016/native protocol/grants остаются зависимостью Opus; затем actual image issuer/archive/pixels E2E |
| **Кварц** | PR236655a2a GREEN; новый native import CODE пакет assigned/awaiting ACK, delivery blocked | После успешной доставки подтвердить exact reuse77paths/stage/tokenize/embed/materialize/import-callsite scope; подготовить source PR→integration, не только fixture | До ACK не active. Не трогать Opus lifecycle, Риф media, Линза latency; shared composition conflicts только через Opus, no whole-file replacement |
| **Линза** | latency task возобновлена10:28; sanitized stage receipt уже предоставлен Opus | Подтвердить bottleneck по готовым stage intervals, назвать minimal paths и выполнить evidence-backed source fix+before/after; без speculative guard changes | M2 corpus-ready + private gold + read slot от Opus → live first-book/quality/latency gates; PR230/232 не заменяют этот результат |
| **Первый import: dot / Кварц** | dot — model reader/MCP runner; Кварц — native materialization/import composition, awaiting ACK и blocked delivery | dot получает доступные private original bytes для реального чтения и устраняет доставку implementation задания; Кварц после ACK делает exact import-only patch | До совместных M1–M4 gates нет принятой книги. Zero Dev vector/corpus roundtrip, reprocess/rechunk/source fidelity/Saved readback; queued launcher не прогресс |
| **Не назначено: Case/PH/public glue** | Явный planning gap, не скрытая нагрузка Opus | dot выбирает исполнителя по завершённому ближайшему leaf и компетенции; не выдаёт один файл двум окнам | Exact target/scope/dependencies/three-doc packet + release старого owner + ACK нового |

### Передача и перераспределение задачи

1. Исполнитель сообщает: веха, полученный результат, exact branch/base/HEAD/paths, tests и их класс, оставшиеся dependencies, следующий результат; если blocked — exact action/target/reason и что можно делать независимо.
2. dot меняет приоритет/владельца результата. Если затронут занятый файл, **старый owner явно releases конкретные paths/hunks**, сохраняет commit/handoff и прекращает запись; новый **ACK exact scope/base** до начала. Пока ACK нет, задача не считается принятой.
3. Один scoped PR для передаваемого пакета в **integration**, с обязательными тремя документами, source dependencies и acceptance. Не stacking чужих старых whole-file changes, не competing main release, не второй integration owner. Opus resolves merge conflicts/rebase на shared integration; чужие branches не переписываются без передачи.
4. ChatGPT-окна **тоже пишут код**. Готовый fixture handoff переводится в следующий свободный implementation leaf, а не постоянную роль наблюдателя. Opus получает hardest lifecycle/restore/authority decisions и итоговый review, экономим его лимит.
5. Достаточно короткого milestone update при новом доказательстве/блокере/handoff; unchanged polling не прогресс. dot обновляет этот файл после значимых изменений. Milestone review — ACK либо «M#, что неверно, exact evidence, предлагаемый owner/next result»; развитие не ставится на паузу ради всех ответов.
6. Scope не расширяется сам собой: новый provider effect/grant/destructive action или чужой проект требуют соответствующей authority. Denial не обходить другим tool/executor; uncertain effect сначала readback. Не запрашивать повторно уже одобренный оригинал/обычный разрешённый шаг.

## 5. Что прикладываем к закрытию вехи

Короткий receipt: `M# / owner / status / UTC / exact source+runtime SHA / corpus+revision / expected→observed result / CI vs live / gate IDs / evidence URL / remaining boundary / next owner`.

Для записи отдельно input accepted, domain applied, external coverage verified, Saved и delivery ACK; для чтения — источник/версия/права/cursor, не Saved. Для runtime — actual provider version/run/schema и loaded hashes; для quality — corpus/model/config, sample/denominator/human verdict. Секреты, signed URLs и частный текст оригиналов не включаются.

**Финальное определение готовности:** все11 вех verified по собственной применимой приёмке; все §3 функции полного задания сохранены; текущий run/readiness, восстановление confirmed данных, полный source→facts/story→Case→каналы→public путь и защищённые численные gates доказаны. Ни зелёный CI, ни одна книга, ни core cutover по отдельности не закрывают весь проект.

## 6. Независимая проверка плана

Отдельный reviewer сверяет нормативные документы, актуальную рабочую структуру и source/PR зависимости; редактор этого файла один, PM остаётся dot. В v1 уже внесены его замечания: явные три незакрытых owner handoff критического импорта; Kaggle-local materialization и реальный reprocess/rechunk dispatch; bidirectional POI bridge с receipt/proof resolver; operation_key recovery после Dev spool GC; различение leaf PR233 и combined PR236. Независимый reviewer после второго прохода подтвердил полноту scope v1 при внесённых sequencing/ownership/coverage поправках; это approval плана, **не product-ready**. На момент review execution blocker был отсутствующий ACK владельцев model reader/native materializer/composition import; актуальное распределение и delivery blocker теперь явно записаны в M2/§4. Последующие milestone corrections фиксируются в PR/следующем обновлении. Обсуждение не приостанавливает независимую разработку.

### Карта покрытия действующих документов

Навигация — [docs/README](../README.md); ниже привязка контрактов к вехам, а не новый список сотен задач. Historical reports дают dated evidence, не отменяют v4.1/owner alignment.

| Контракт | Вехи и проверяемый результат |
|---|---|
| [Ingestion](../ingestion.md), [semantic passages](../operations/source-backed-semantic-passages.md), [accepted repair](../operations/accepted-import-repair.md), [catalog correction](../operations/import-finalization-corrections.md), [article scope](../operations/article-search-scope.md) | M2–M5: fidelity, article boundaries, correction/reprocess/rechunk, atomic activation и continuation |
| [Story registry](../story-registry.md), [organization graph](../organization-graph.md) | M4/M5/M7: факты отдельно от narrative, exact revisions, entities, human quality |
| [Participant/POI bridge](../design/rkb-participants-poi-fact-bridge-20261018.md), [POI integration](../poi-integration.md), [model link](../poi-model-link.md), [GEO cutover](../migrations/rkb-layers-only-cutover-20261019.md) | M1/M2 prerequisites, M10 bidirectional receipt/proof и geometry/provenance |
| [Migration parity](../testing/rkb-migration-parity-20261021.md), [Case design](../design/rkb-disagreement-journal-design-20261018.md), [collaboration](../design/rkb-kaliningrad-base-collaboration-20261018.md), [Case PMI](../testing/rkb-disagreement-pmi-20261018.md) | M8/M9: полный OLD→NEW functional parity, CAS-F8–F10/PR151, source needs и expert workflow |
| [Rights](../rights.md), [identity](../platform-identity.md), [MCP](../mcp.md), [integrations](../integrations.md), [load management](../user-request-load-management.md) | M3/M7/M8/M10: exact grants/derived rights, actual clients, fair concurrency/ACK, Mira |
| [Illustrations](../operations/illustration-pipeline.md), [archive](../operations/telegram-archive.md), [archive continuation](../operations/archive-read-continuation.md) | M2/M3/M6: actual pixels/locators/current grants/direct Telethon/recovery |
| [Storage](../storage.md), [operations](../operations.md), [readiness](../mass-ingestion-readiness.md), [static page](../requirements/disagreement-case-static-pages-20261019.md) | M1/M6/M7/M11: target durability, cutover and full public delivery; owner weekly-backup override сохраняется |

**Оперативное дополнение10:31 UTC:** Opus подтвердил integration b932c15/PR237 health phase1; PR239 исправляет реальную READY_ACK race (старый код3/3RED). PR223/232 требуют повторной проверки после её merge/rebase, несмотря на отдельный packaging green. Media wiring и health phase2 уже переданы как указано выше; на10:31 model reader/native materializer/composition import ещё оставались незакрытыми назначениями (обновлены в M2 на10:47). На ближайшие2 часа Opus экономит лимит: сверка примерно каждые15min и только critical/merge/live, не синхронное ожидание его ответа на каждую независимую правку. dot распределяет implementation работу и агрегирует review inputs.

**Обновление PM10:47 UTC:** план передан владельцу и команде; запрошен один ACK/поправки каждого владельца без остановки разработки. dot принял model-reading/первый MCP import. Native implementation назначена Кварцу, но сообщение ещё не доставлено в рабочее окно и ACK отсутствует; новый import-worker не создан. Блокер — доставка задания, не отсутствие одобренной книги: тот же original подтверждён доступным10:44. Следующее изменение статуса implementation — только после реального ACK/результата, не по очередному queued запуску. Остальные владельцы не переназначены.
