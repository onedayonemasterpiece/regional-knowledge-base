# Готовность к массовому импорту

Обновлено 20.10.2026. Это readable companion к protected requirements, не отчёт о выполненной приёмке. Все **девять стабильных ID и численные gates** сохраняются. Историческое имя CORE-SUPABASE-CAPACITY означает measured storage capacity и не требует Supabase.

Target: PostgreSQL 17/pinned pgvector и BGE в Kaggle; existing DevCoveer MCP/BgeQueue/controller, direct outbound WSS, короткий raw spool и compact control state; existing Yandex exact recovery tail и verified PG snapshots. Только geo переносится; всё негеографическое создаётся fresh. E5/Supabase target не включает.

[Единый handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [полная acceptance matrix](testing/rkb-scalable-kaggle-acceptance-20261019.md), [import/retrieval checks](testing.md), [testing journal](testing-journal.md).

## Численные gates

| Gate | Обязательный результат |
|---|---|
| Scale | ≥100 book-equivalent sources и ≥100000 новых active passages через originals/new chunk/BGE path в isolated benchmark DB; benchmark готовности, не product limit. Старые chunks/vectors не используются. |
| Storage | ≤80% steady, ≤90% largest-import/snapshot/restore/cutover peak каждого applicable budget. Учитываются heap/TOAST/vectors/indexes/WAL/temp/staging и old+candidate overlap; отдельно spool/recovery/caches. |
| Dense | ≥30 held-out source-grounded fact families; BGE-only Hit@5≥85%, Hit@10≥90%; critical RU→DE Hit@10≥85%. |
| Ablations | BGE-only, lexical-only и configured fusion отдельно. Lexical не prefilter dense candidates; E5 history не target gate. |
| Latency | Warm backend p95≤1000 ms; public MCP p95≤1500 ms; hard evidence deadline2000 ms. В отчёте все attempts/errors/degraded outcomes, не только успешные быстрые calls. |
| Source fidelity | Coherent passages около 256 pinned BGE tokens; exact full input с prefixes≤512, без silent truncation. Printed text/captions/notes/provenance сохранены. |
| Per-book | Естественные/перефразированные и нужные cross-language evidence queries на exact candidate/active revision; finalize/vector counts недостаточны. |
| Recovery | Remote exact intent/payload до accepted; domain commit + remote exact outcome/receipt до Saved; verified base/replay/fences и actual SQL+dense readiness. |
| Features | Facts/dates/observations/entities/geo сохраняются при zero-story input; stories/variants/cases/private drafts, роли, Мирa/Slack/request/delivery работают по полным контрактам. |

## CORE-MASS-BOOK-INGESTION

РКБ предназначена для регулярного массового импорта, а не единичного demo. Готовность требует одновременного прохождения production-like scale, retrieval quality, latency, import fidelity и recovery gates.

Scale fixture живёт в отдельной isolated benchmark DB, без нового tenant/IAM и без публикации fixture в public corpus. Он использует исходные PDF/DjVu и новый chunk/BGE path; старые chunks/vectors запрещены. Heavy Story/case extraction для load fixture не обязательна. Controlled synthetic amplification допустима только как доказательство масштаба, не качества: dense held-out gate опирается на реальные source facts и новые passages. Это не снимает функциональные и per-book gates production import.

Импорты resumable/idempotent, с bounded queues/backpressure, не зависят от открытого чата. Failed/superseded revision не блокирует другие книги и не уничтожает prior active revision. Книга не считается accepted только потому, что записаны text/vectors.

Exact accepted source-analysis payloads и человеческие вклады долговечны до эффекта. Restart/replay сохраняет выбранные IDs, timestamps, preconditions и значения; повторная model inference не заменяет accepted extraction.

## CORE-SUPABASE-CAPACITY

Working corpus находится в Kaggle PostgreSQL 17/pgvector. Full sources, scans и BGE weights не входят в Dev control БД. Measured capacity включает PG heap/index/TOAST/WAL/temp, system overhead, imports, snapshots/restore overlap, Dataset versions и отдельные recovery/spool/cache budgets.

Gates 80%/90% применяются к фактическим provider/mount budgets. Raw-vector bytes или row counts не являются измерением всей системы. Нельзя молча сокращать corpus completeness/provenance/quality ради quota.

Dev raw spool defaults 4h/hard 512 MiB измеряются до cutover; compact UNKNOWN/request/receipt metadata живут до reconciliation. Local GC после remotely sealed payloads/verified metadata не ждёт oldest daily base. Remote-tail GC сохраняет цепочки designated retained verified baselines и unfinished consumers. Bounded sealed-tail compaction под existing controller/CAS ограничивает object count/bytes, сохраняя logical IDs/hashes/seq. Generic S3 transport не доказывает immutable seal/CAS; capability проверяется при реализации. Большие base bytes идут direct Kaggle→recovery object по scoped key+epoch+expiry, не через 512 MiB spool; old-writer upload не promotable даже при ещё действующем presigned URL. Pressure ограничивает admission до подтверждения; подтверждённая работа не удаляется.

## CORE-DENSE-RETRIEVAL-QUALITY

Dense retrieval самостоятельно находит evidence. Held-out benchmark≥30 distinct fact families измеряет known-evidence Hit@5/10 и language slices; metrics не называются generated-answer accuracy.

BGE-only Hit@5≥85%, Hit@10≥90%, каждый critical language slice включая RU queries→DE sources Hit@10≥85%. BGE-only/lexical-only/fusion — разные отчёты. ANN scoped/rare-alias/withdrawal cases и рост corpus входят в актуальную проверку; bounded refill не маскирует failed recall. E5 удалён из target, старые измерения остаются историческими.

## CORE-AGENT-SEARCH-LATENCY

Warm ranked-candidate backend p95≤1000 ms при representative concurrency. Public MCP p95≤1500 ms, hard evidence-search deadline2000 ms. Ни branch, ни повтор alias, ни скрытое ожидание encoder не получают дополнительные секунды поверх общего deadline.

Queue wait полного human question и time to final answer измеряются отдельно. Ephemeral read_query не создаёт durable queue row и не ждёт fsync/remote recovery journal/ordinary telemetry. Один fused query выполняет BGE→SQL/fusion/hydrate в Kaggle, без vector roundtrip через Dev; готовый query не ждёт заполнения batch. Быстрый pending response не является завершённым semantic query; percentiles/errors/degradation публикуются честно.

## CORE-BOOK-IMPORT-QUALITY

Scanner/service material отделяется от печатного содержания; document→page→region provenance, captions и footnotes сохраняются. Семантический passage около 256 tokens не обязан совпадать с physical page/transport batch. Final BGE input с фактическими prefixes считается exact pinned tokenizer и не превышает 512; смена limit требует measured change gate.

Fresh PDF/DjVu import начинает source analysis заново. Старые non-geo corpus/chunks/vectors/receipts, inferred facts/stories/cases/drafts не подменяют его. Переносится только geo и минимальные необходимые ему source refs; прежнее состояние остаётся отдельным reference, а не active searchable target corpus. GEO identity manifest со stable layer/place IDs, aliases и CRS фиксируется до production book imports. Bulk raster transfer может идти в phase 4; stage 2 без manifest ограничен isolated fixture.

Activation и persisted research frontier атомарны. Expert consensus, карта и Street Story downstream не блокируют accepted книгу. Facts/assertions/evidence, people/roles, organizations, dates/calendar/precision, exact observations/units, historical toponyms и disagreements сохраняются без обязательной Story.

Story extraction различает facts, episode, dossier и publication Variant; продолжает эпизоды через page/windows, допускает zero stories и не выдумывает мотивы/диалоги/причины. Сохранены angle/gaps/history/variants. Небольшое source-grounded blind comparison проверяет narrative usefulness, false acceptance/refusal и фактическую сохранность; квоты историй и второй model judge не требуются.

## CORE-RETRIEVAL-CHANGE-GATE

Изменение chunking, model/space/precision, vector indexes, lexical formulation, fusion, scope/authorization или storage placement повторяет применимые capacity/dense-quality/latency gates до rollout. Green unit suite и ready-vector count недостаточны.

Отчёт фиксирует exact corpus/revision/model/tokenizer/search config, actual executing SHA и resources. Target должен убрать full-catalog allowed map, N+1 hydration, Python all-story similarity; plans и round trips показывают это на scale fixture. Prepared ANN plans проверяются после 6+ executions и на selectivity extremes; SET LOCAL knobs pool-safe. Immutable revision projections + small active pointer исключают массовую activation rewrite; FTS per-row RU/DE/simple с bounded query configs без alias loop. Нельзя объявить ускорение из несопоставимых benchmarks.

## CORE-REQUIREMENTS-GUARD

Physical-base manifest фиксирует PG minor/extensions/libs/platform/collation provider/version; C collation для deterministic normalized keys и affected-index correction до READY. Canonical outcome hash сохраняет exact IDs/body/evidence, не заново вычисленные BGE floats/FTS/crops. Derived dirty/rebuild не блокирует canonical replay; delivered artifact bytes/hash/receipt остаются exact.

Protected requirements автоматически доступны implementation/review tasks. Если реализация не проходит gate, конфликт показывается, не превращается в пониженный target. Намеренное изменение выполняется typed requirements_update с явной owner authorization; эта Markdown правка сама protected manifest не меняет.

Новые эксплуатационные defaults 1 s group window/15 min control checkpoint/4 h changed base/4h512 MiB raw spool не ослабляют quality, latency и durability gates. До cutover они проверяются на реальном workload и failure boundaries.

## CORE-UNIFIED-EXPERT-ACCESS

Один existing RKB MCP resource для personal compatible AI clients, Slack и Миры. Verified provider/tenant/subject связывает canonical person; service executor дополнительно ограничен request/generation/scope и никогда не наследует unrelated owner/Dot bearer.

Text/bibliography активных accepted книг без per-book owner ACL; illustrations доступны всем. Stripe quote scans — owner/demo/expert/local_historian. Full originals/full-page pixels/private drafts и writes имеют отдельный scope. Current capability, source withdrawal и delivery audience проверяются после cache/queue waits.

Import/admin остаются owner personal route. Owner/case manager не читают чужой draft автоматически. Приёмка включает реального non-owner, two routes, denial/revocation, delayed result и запрет прямого вызова запрещённого tool; документация/OAuth endpoint не доказывают совместимость клиента.

Сохраняются story CRUD/history/variants, dates/calendar, entities/relations, observations/comparison, geo queue/POI refs, independent cases/contributions/conclusions и private drafts. Fresh scope не разрешает уничтожить прежнюю reference работу или дублировать Cartography/Street Story authority.

Dot и Мира остаются двумя самостоятельными интеллектуальными маршрутами к тем же инструментам. Мира в Projects Hub расширяет actual provider/typed adapter/shared live-interaction: delegated identity, fast read v2 со status/freshness/generation, progressive research и durable pending/result/receipt для принятых долгих задач/изменений, voice reconnect и screen bibliography/evidence. PH input-utterance checkpoint не является lossy telemetry. Search-only protocol/test fake не full acceptance. После реального подключения выполнить парную ПМИ Mira/Dot (§14.4 handoff); «хорошо подумай»/делегирование Dot остаётся будущей опцией.

## CORE-REQUEST-LOAD-MANAGEMENT

Одна admission authority сохраняет canonical-person fairness и measured stage permits. Ephemeral read_query использует RAM permits без второго durable turn; durable questions/mutations сохраняют per-dialog order/context, exact result/ACK и arithmetic ETA. Normal load ставит accepted вопрос в ожидание, не отбрасывает его.

Все claim/wake/resume/explicit-ID paths используют один guard. UNKNOWN predecessor блокирует зависимое durable продолжение своего dialog, остальные продолжаются. Проверочное/независимое read может вернуть prior_write_pending/context gap и observed cursor, но не выдать старое значение за последнее; min_data_seq обязателен. UNKNOWN admission/reconciliation bounded per provider/route, не весь import; operation resolve verified_delivered/not_delivered/abandoned_no_resend требует evidence/operator authority. Restart восстанавливает точный result и delivery outcome; result_ready не запускает новую генерацию. TTL raw spool и terminal receipt horizon не удаляют unfinished requests.

ETA≤10 s не вызывает notice; >10 s и существенные увеличения имеют durable coalesced notices только ближайшего request человека. Final имеет приоритет. ETA использует measured duration/fair sequence, не LLM и не выдуманное число concurrency.

Stage permits учитывают co-located PG/BGE/proof/import/maintenance и Dev control; resize адаптирует допуск. Search reserve минимум для одного supported query сохраняется во время import, proof/import получают bounded aging. Capacity wait не расходует outage retries; proven external unavailable даёт один честный итог, poison stage остаётся recoverable.

Outbound duplex WSS использует existing queue/controller, claims/hashes и writer/connection fence. Reconnect живой session сохраняет model; после session end новый run restore/warmup. Application ACK, socket Pong, Saved и Delivered различаются. QUERY_RESULT не имеет Saved/RESUME: reconnect/новый turn отменяет его прежнюю generation. Async read telemetry ограничена и не держит queue write lock; drops видны. Durable effects и audit не теряют гарантий.

Перед выпуском проверить cross-client/dialog fairness, actual second-client result/ACK, restart/generation/remote seal/CAS gaps, notification thresholds, real resize/import+search, held-out ETA error. Terminal receipts 30 days допустимы лишь для verifiable server-issued IDs: expired never-new-execution; arbitrary legacy keys и UNKNOWN не GC blindly.

Go/Rust допускаются как scoped component replacement для подтверждённой задержки/ресурсов либо воспроизводимой проблемы памяти/control/lifecycle. Baseline — thin Python и один resident native PyTorch/PG. IPC, threads, ABI, model-space и recovery включаются в сравнение. Не требовать полный rewrite, не приписывать старым backend/connector числам текущий voice E2E. Измерять PH tool-lock wait и время первого полезного/полного слышимого ответа; все прежние gates неизменны.

## Историческое evidence и текущий статус

[Production stress 08.10](reports/expanded-retrieval-stress-20261008.md) на прежнем corpus дал BGE-only 25/32 Hit@5 и 27/32 Hit@10: ниже защищённых 85%/90%. Экспериментальный t256 результат не являлся acceptance той production revision. Полный [исторический readiness документ](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/mass-ingestion-readiness.md) сохраняет numbers/context.

Core/GEO runtime cutover и feature-release Миры, VK, Astro/case pages и research needs имеют раздельные gates. Все функции обязательны для whole-project completion: NOT RUN не означает working или PASS. Регрессия уже включённого канала блокирует switch; молчаливое отключение канала не является обходом gate.

Новая PG 17/WSS/recovery/import pipeline требует собственной applicable acceptance. Документация, source review, установленные indexes и ready vectors не являются новым PASS. Массовое unattended filling не объявляется готовым до связанных проверенных gates.
