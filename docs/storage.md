# Хранение, восстановление и ограничение объёма

Уточнение владельца 21.10.2026: [alignment](designs/rkb-owner-alignment-20261021.md), [аудит](reports/rkb-requirements-code-audit-20261021.md). **Основное постоянное хранение — versioned Kaggle Dataset; Yandex S3 — независимый weekly backup.** Ниже сохраняются snapshot/replay/ACK invariants, но старый online-S3 binding не переутверждается автоматически. До доказанного внешнего покрытия соответствующие новые writes остаются pending, старые accepted recovery records не удаляются. Telegram media verification — только доступность по locator, не PG snapshot integrity.

Обновлено 20.10.2026. Целевой контракт для [единого handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md); новая реализация не развёрнута этой документационной работой. [Исходная SQLite/Supabase схема](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/storage.md) сохранена как immutable historical record.

## Один рабочий corpus

PostgreSQL 17 с pinned pgvector на Kaggle хранит exact accepted text, bibliography, source/page/region graph, факты, entities/relations, stories/variants, dates/observations, cases, drafts и BGE/FTS serving projections. E5 и Supabase в target отсутствуют. Короткая DevCoveer control БД не содержит постоянной второй копии corpus, vectors, PDF или BGE weights.

Небольшой временный PG17/pgvector/PostGIS fixture на DevCoveer допустим для SQL/schema/permissions/migration checks при измеренном CPU/RAM/disk headroom, конечном deadline и cleanup owner; предпочтительно существующее CI/окружение. Только малые разрешённые/synthetic данные и готовые test vectors, без полного corpus и BGE/E5 weights/inference. Изолированный fixture не публикуется в общий поиск и не становится target БД; подробный контракт — [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), §6.6.

Originals и illustrations сохраняются напрямую из Kaggle в Telegram через Telethon, без VibePublish/Dev bulk proxy. Private Kaggle Dataset — основной versioned persistence согласованного PG состояния; run восстанавливается из exact confirmed version и сохраняет туда изменения. Yandex S3 получает независимую backup-копию раз в неделю. Source media archive, working PG, Dataset recovery state и временные caches имеют разные lifecycle/budgets; старый SQLite archive не является target PG snapshot.

Exact source_text и детерминированный search_material имеют собственные hashes и material version. Passages ссылаются на точные source spans; не размножать полную страницу ради каждого embedding. Model/human accepted decisions сохраняются точно: PDF SHA и повторная модель не воспроизводят прежний accepted result. Старые derived vectors можно убрать после безопасной смены revision; provenance и принятые значения нельзя подменить пересчётом.

## Прямой bulk и временные datasets

[Handoff§5.5](prompts/rkb-scalable-kaggle-implementation-20261019.md) — полный transfer/GC contract. Existing storage принимает private attempt-specific objects; Dev хранит control/refs, не проксирует большие bytes. Client fileParams reference отличается от controller ticket; expiry чужой ссылки требует reattach, reprocess читает archived original. Exact provider version/size и full-hash verification предшествуют first-verified binding; незавершённый upload не accepted/Saved. Native ImageContent остаётся в bounded MCP responses.

Temporary Dataset создаётся лишь для необходимого provider input и регистрируется с owner/consumers/budget. GC и выдача leases используют общий guard: запрет новых refs, reconciliation активных mounts/transfers/UNKNOWN, отсутствие accepted/recovery dependencies, затем exact delete/readback. Expiry не доказывает consumer completion. Confirmed native absence закрывает ownership; quota проверяется если доступна, отсутствие delete API — видимый blocked cleanup. Private DB base/pinned recovery versions не являются disposable staging. Signed URLs/private originals/PGDATA не попадают в notebook source, logs или automatically committed outputs; actual private visibility проверяется до передачи private bytes.

## ACK и короткий raw spool

Эти durable границы относятся к принятым долгим вопросам, mutations и внешним effects. Обычный `read_query` использует ту же authority допуска, но имеет отдельную ephemeral семантику, описанную ниже; transport response не называется accepted/Saved.

| Граница | Значение |
|---|---|
| Local intake receipt | pending: известны request/idempotency key и временно сохранён input; финальное accepted ещё не обещано. |
| Remotely persisted intent | Точный input/intent внешне закреплён выбранным Dataset/checkpoint/continuation протоколом; coverage подтверждено. До этого received/pending. Существующий online S3 seal не подменяет выбранный Dataset primary молча. |
| Applied / Saved | PG transaction сохранила domain effects, applied_ops и непрерывный cursor; exact outcome/receipt также remotely verified. Cloud intake сам по себе не applied. |
| Delivered | Единственный sender подтвердил native receipt/readback конкретного result hash. Unknown outcome остаётся UNKNOWN до reconciliation. |

Accepted mutations и durable user turns группируются в bounded group commit: готовая группа ждёт flush не более 1 секунды. Это верхняя граница накопления, не обязательный sleep и не обещание provider latency: при свободном uploader готовый singleton можно отправить сразу. Ошибка remote commit оставляет pending и не превращается в Saved. Обычный read-only поиск не ставится в очередь remote journal и не требует S3 round trip.

NEW уже реализует части S3 immutable create/seal/CAS; это существующий recovery binding, а не доказательство Dataset-primary публикации. Переиспользовать общие semantics и явно проверить выбранный Dataset/continuation adapter; не вводить скрытую online-S3 зависимость только потому, что этот код уже есть. [Yandex PutObject](https://yandex.cloud/en/docs/storage/s3/api-ref/object/upload) документирует If-Match/If-None-Match; runtime probe проверяет конкретный SDK/bucket и competing-writer race, а не существование headers в спецификации. Segment содержит sequence range/count, payload hashes и previous hash. Manifest принимает только verified continuous chain; orphan upload не accepted.

Стартовые implementation defaults для DevCoveer raw spool — **4 часа и hard cap 512 MiB**; до cutover измерить их достаточность под accepted payload sizes и write bursts. Числа не относятся к сроку жизни диалога, UNKNOWN send, private draft или unresolved case. Компактные request/receipt/UNKNOWN metadata сохраняются до reconciliation и переносятся в control checkpoint. Terminal receipt horizon 30 days допустим только для verifiable server-issued request IDs: expired ID не становится новой execution. Arbitrary legacy keys не GC вслепую. Pending callbacks используют durable artifact refs, а не удерживают месяцы raw body на DevCoveer.

Local GC удаляет raw bytes после remotely sealed payload + verified metadata. Он **не ждёт самой старой weekly backup** после доказанного внешнего покрытия; это не разрешает удалять ещё непокрытый payload. Нельзя объявлять неподтверждённые bytes защищёнными по успешному socket send или upload request. Age/byte pressure заранее ограничивает новые admissions; при неисправном recovery backend нет финального accepted ACK. Ни возраст, ни заполненный диск не разрешают удалить единственную копию уже подтверждённого результата.

## Base, tail и checkpoint

1. PostgreSQL base — consistent physical backup с нужным WAL и indexes. Обычный cp работающего data directory запрещён. Logical dump применяется для малого fixture либо обоснованного version/schema upgrade с отдельным измерением restore/index rebuild.
2. Exact command/control/receipt continuation должен быть восстановим в рамках выбранного Dataset-primary протокола. В NEW используется online S3 tail: его смена или дополнительная роль требует явного решения, а не подмены weekly backup. Resolved payload включает выбранные IDs, logical timestamps, preconditions и codec/schema version; модель не вызывается заново для принятого решения. До доказанного покрытия — pending; прежние accepted bytes сохраняются.
3. Control checkpoint создаётся каждые **15 минут**. Accepted turns, grants/revocations и effects не ждут этого интервала: их изменения защищает remote tail до финального ACK.
4. Changed PG base создаётся каждые **4 часа**, а также раньше при превышении replay-time/byte budget или перед planned handover. Без изменений полную копию не создавать. Index rebuild и reembedding не являются частью каждого checkpoint.
5. Раз в неделю сохранить в Yandex независимую копию подтверждённой Dataset version с manifest/cursor и датой. Временно нужны old + candidate; прежняя копия удаляется только после годной замены и проверки retention dependencies. Weekly backup не содержит автоматически весь более свежий хвост; показывать фактическую version/age и гарантии отдельно.
6. Remote-tail GC сохраняет все интервалы, нужные **designated retained verified baselines** и unfinished consumers. Отставшая копия либо остаётся с полным recovery chain, либо явно выводится из набора recoverable baselines после годной замены. Нельзя удалить её tail и продолжать показывать её как восстановимую.
7. Readers pin exact snapshot version, не Dataset latest; их leases и восстановительные зависимости учитываются до version GC.

Manifest связывает snapshot ID, exact Dataset version, PG major/minor, extension/library builds, platform и collation provider/version, schema/codec, восстановленный внутренний applied cursor, hashes/sizes, WAL range, model/space и verification receipt. Cursor не вычисляется произвольным MAX(seq) в другой БД. Для deterministic keyed normalized text используется C collation; несовместимость collation/version требует корректного rebuild затронутых indexes до READY. Не предполагать непроверенный pin окружения через Kaggle UI.

Большие base bytes передаются напрямую Kaggle→Dataset/recovery object, не через 512 MiB Dev spool. Notebook не получает long-lived delete/global-bucket credentials: только scoped upload authorization на конкретный key+epoch с expiry. Presigned URL не отзывается автоматически при epoch change; expired writer может оставить orphan upload, но внешний promotion fence не разрешит его выбрать. DevCoveer владеет manifest/CAS/GC authority.

Для Dataset source-backed primitive есть в [Kaggle CLI 6894864](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/src/kaggle/api/kaggle_api_extended.py): `_upload_blob` вызывает `start_blob_upload(ApiBlobType.DATASET)`, получает `create_url` и token; `upload_complete` передаёт bytes напрямую, а `dataset_create_version` использует `UploadFile.token` в `body.files`. Узкий target adapter разделяет это: Dev controller хранит account key/blob token, создаёт upload и затем version, делает readback/promotion; notebook получает только ограниченный signed URL/session и отправляет bytes. Это подтверждённая кодом возможность адаптации, не готовый adapter: runtime probe проверяет expiry, direct upload, finalize и восстановление exact version.

Sealed-tail compaction ограничивает object count/bytes/replay time. Один existing controller собирает verified sealed segments в более крупные immutable bundles, сохраняет logical IDs/hashes/sequence proofs и переключает refs через CAS. Старые objects удаляются лишь после проверки replacement и учёта readers/consumers; не строить новый compaction service.

Проверка candidate: provider status → скачать exact version → hashes/size → restore с нужными extensions/WAL → SQL invariants/cursor → replay непрерывного tail → actual SQL+dense probes. Canonical pointer переключается внешним fenced CAS только после этих проверок. Crash между apply и ACK возвращает тот же результат через applied_ops, не повторяет внешнюю отправку.

Один consistent base artifact можно отправить в Kaggle и Yandex с независимым transport readback, без второго pg_basebackup и одинакового restore тех же bytes/runtime. Если уже разрешённый reader/planned successor доступен, проверять candidate на нём и сохранять восстановленный instance для serving после replay/readiness/fencing. Не выбрасывать его ради повторного скачивания и restore. Weekly recovery drill можно совместить с candidate verification только при выполнении всех его SQL/dense/control-receipt/recovery checks. Если свободного разрешённого run нет, verification использует ограниченный maintenance CPU/I/O/memory budget с search reserve; дополнительный Kaggle run в обход Telegram-session cap не создаётся. Hash-only verification не заменяет restore proof, а откладывание maintenance не нарушает replay/tail budgets.

## SQL-изменения и редкая смена базы

Обычная SQL/code итерация не требует новой Dataset version или notebook restart. Exact migration ID/hash/predecessor и schema/function/executor transitions входят в тот же ordered tail, что commands, пока retained bases/tails требуют их artifacts/decoders. Incompatible v2 закрывает новую v1 фиксацию и ждёт outcomes всех прежних accepted/sealed v1 commands, включая undispatched queue; UNKNOWN сверяется по applied_ops исходной DB generation. Replay воспроизводит исходный порядок, не устанавливает все latest migrations до старых команд; SQL patch переживает потерю session без нового snapshot. Эти гарантии требуются до первого реального accepted/Saved. Только immutable test fixture без product routing/реальных refs можно пересобрать из малого seed; synthetic test applied_ops не выдаёт product ACK. GEO, accepted input, Saved и вклад пользователя не становятся disposable от слова «отладка».

Default для редкой DB replacement — конечное maintenance окно в той же notebook session: проверить candidate → прекратить новые applies и сверить in-flight → подтвердить recovery boundary → остановить нужный PG instance → restore/replay до фиксированного cursor → SQL/ACL/ANN readback → открыть routing нового generation. Accepted mutations ждут в прежней bounded queue и не получают ложный applied. Файлы, каталог или symlink живого PGDATA не подменяются; смена БД сама по себе не перезагружает BGE. Exact Dataset version можно загрузить в живую session через проверенный SDK path, но старый mounted input не обновляется автоматически.

Old verified remote base и её нужный tail сохраняются до подтверждения замены. Два одновременных PG clusters/catch-up нужны лишь при измеренно неприемлемом maintenance окне и достаточном peak budget; они не обязательны для первой отладки и не создают второго writer. Recovery pointer и active routing различаются. После первой новой записи возврат к старой БД требует совместимого tail catch-up либо roll-forward, а не простого возврата каталога. Полный порядок, compatibility/drain и memory/storage gates — [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), §5.4 и §6.6.

## Failure domains и быстрые reads

Потеря Kaggle session: восстановить verified base, replay exact remotely accepted tail, подтвердить новые readiness probes. Canonical outcome hash фиксирует exact domain IDs/body/evidence, исключая заново вычисленные BGE floats, derived FTS и crops. Derived projections могут стать dirty и rebuild асинхронно, accepted BGE artifacts reuse; dense lag не задерживает canonical replay. Уже delivered artifact bytes/hash/receipt остаются exact и не подменяются regenerated изображением. Потеря DevCoveer disk: восстановить compact control checkpoint и remote command/control/receipt tail; не воскресить старую writer authority или отозванный grant. Потеря Yandex требует отдельного анализа остаточного Dataset recovery point: две копии в одном Yandex не доказывают RPO=0.

Ephemeral `read_query` использует ту же person fairness/resource admission authority и прогретый serving PG/BGE. Его bounded in-flight correlation/context generation не создаёт durable user-turn автоматически: ни fsync, ни remote seal, ни durable RESULT_ACK/RESUME не стоят перед dispatch/result. После потери соединения такой read можно выполнить заново с актуальным контекстом; это не replay принятого domain effect. Уже managed request передаёт request/generation context без второго lifecycle; его собственная durable история/результат сохраняются по прежнему контракту. `min_data_seq` обеспечивает read-after-write; ordinary reader сообщает `data_as_of`.

Read telemetry/history references отправляются асинхронно в bounded sink вне queue/control SQLite write path: они не захватывают её write locks и не используют durable job queue. Переполнение coalesces/drops telemetry с видимым счётчиком, а не задерживает поиск и не теряет принятые mutations. Это не обещание durable хранения каждого быстрого read. Не журналировать private query bodies по умолчанию.

Ingress проверяет resource-bound identity и передаёт компактный actor/audience/request/policy context, без перечисления всех доступных книг. PG применяет scope predicates; gateway перед output/cache reuse/delivery выполняет текущую batched проверку dependencies top-K. Current withdrawal/capability barrier сохраняется даже при отставшем reader; cached token/identity не заменяет проверку отзыва.

`text_search_ready`, `archive_read_ready` и `proof_ready` независимы внутри разрешённого run. Telegram reconnect/FloodWait или pending crop не отключают готовые PG/BGE и не удерживают текстовый ответ. Сам лимит simultaneous runs по независимо авторизованным Telegram sessions остаётся в силе.

## Ёмкость и скорость

Измерять отдельно:

- Kaggle writable mount: PG heap/TOAST/indexes, WAL, temp, staging, restored data, candidate backup и download/render cache;
- private Dataset quota: retained base versions и upload overlap;
- Yandex: weekly backup versions и overlap; текущий legacy online tail учитывать отдельно до явного решения о replacement, не скрывать его расход;
- DevCoveer: raw spool, compact metadata/checkpoints и bounded network buffers.

Gates: steady usage **≤80%**, крупнейший import/restore/snapshot/cutover peak **≤90%** каждого applicable budget. Minimum benchmark — **100 book-equivalent sources и 100000 active chunks**; это не product limit. Raw-vector arithmetic не заменяет measured index/volume usage.

Приоритет владельца — полезное уменьшение E2E latency, в том числе на 0.5 s. Полезные bounded caches, FP32 vectors и indexed serving projections допустимы в этом budget; экономия bytes сама по себе не оправдывает дополнительные roundtrips, recompute или распаковку на каждом read. Выигрыш подтверждается сопоставимым измерением, а не объявляется по выбору Go/Rust/native реализации.

Пределы Dataset/private storage, числа files, writable notebook space и session lifetime проверяются по [Kaggle Dataset docs](https://www.kaggle.com/docs/datasets), [Notebook docs](https://www.kaggle.com/docs/notebooks) и actual account/mounts непосредственно перед внедрением. Прежние числовые оценки не считаются текущей квотой аккаунта. Dataset capacity не равна рабочему диску PG; фактическое expanded/stored accounting и overlap должны быть измерены.

[CLI](https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md) создаёт versions асинхронно и предоставляет status/delete-old-versions. Не предполагать транзакционный append, дешёвую incremental upload или selective version GC. Current + candidate и reader pins входят в budget; при необходимости использовать ограниченную ротацию целых generation datasets после verified replacement, не Dataset на каждую книгу. Сжатие проверяется по фактическому stored/uncompressed accounting.

## RAM и обслуживание индексов

Один loaded BGE process обслуживает queries/documents. Budget включает model/activations, PG shared buffers, все одновременные query workspaces, maintenance и OS reserve. work_mem умножается по операциям, sessions и parallel workers; отдельно ограничить maintenance/autovacuum memory и temp files. См. [PostgreSQL resource consumption](https://www.postgresql.org/docs/current/runtime-config-resource.html).

Bulk imports используют bounded batches и готовое staging. GIN/HNSW initial build идёт после начальной bulk load; activation поддерживает active revision, затем targeted ANALYZE. Autovacuum включён. Один controller выдаёт permit тяжёлой maintenance работе и учитывает serving reserve; нельзя выполнять hourly/daily full REINDEX, VACUUM FULL или reembedding по таймеру.

CREATE INDEX CONCURRENTLY применять к serving table только при необходимости сохранить конкурентные writes; на неактивном staging обычный build проще и дешевле. Concurrent build — отдельный нетранзакционный шаг с конечным budget и проверкой native catalog после ошибки/lost ACK: invalid index и несовпадающее определение не считаются готовым результатом, IF NOT EXISTS этого не проверяет. Короткий lock_timeout ограничивает ожидание DDL; никакой полной переиндексации после каждого SQL patch. Подробности миграций — [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), §6.6.

REINDEX допустим при доказанном bloat/повреждении/upgrade и наличии места для replacement index. VACUUM FULL требует exclusive lock и дополнительную копию; это отдельный ремонт, не обычный GC. Старые derived revisions удаляются по retention/provenance contracts с последующим обычным vacuum. [PostgreSQL vacuum](https://www.postgresql.org/docs/current/routine-vacuuming.html), [pgvector maintenance](https://github.com/pgvector/pgvector).

## Originals, caches и безопасный доступ

Принятый текст и библиография доступны всем; иллюстрации также доступны всем через соответствующий artifact. Stripe quote scans разрешены owner/demo/expert/local_historian. Full PDF/DjVu, full-page previews и private draft history остаются private. Region/figure grant не разрешает страницу целиком или чужие annotations. До preview, render, callback и delivery проверяются текущая роль, audience, source revision и artifact scope.

Exact hashes используются для snapshot/code/accepted-command integrity и исходной edition identity, не как публичный object key и не как обязательный Telegram upload/download proof. Доступность ожидаемого media по locator достаточна для архивной операции. В MCP возвращаются opaque source/artifact IDs и authorized retrieval; raw storage keys, credentials и signed URLs не попадают в public logs. Recovery snapshots закрыты: public text projection не делает всю БД public.

Сохраняются bounded streaming, incremental hash, MIME/signature/container validation и SSRF guard загрузки: HTTPS/443, без credentials/redirects/IP-literal/localhost/private DNS targets; approved DNS pins, без proxy/cookie inheritance и неожиданной content decompression. Существующий source size default 128 MiB и operator ceiling 512 MiB проверяются до и во время download. Это размер входного файла, не spool budget.

Original cache и proof cache имеют свои отдельные byte/idle caps и cross-process leases. Exact preview/hash/rotation/transform сохраняется до terminal locator callback; утрата preview требует новой generation и повторной локализации. Подтверждённый callback/artifact ref позволяет освободить временные bytes. Сессии Telegram независимы и имеют exclusive lease; account FLOOD_WAIT остаётся общим пределом.

Concurrent reads одного exact source SHA могут разделять одну in-flight загрузку и verified immutable cache object под lease. Для Telegram source/media проверяется зарегистрированный доступный locator; custom download/hash verification не вводится. Local generation/source binding отличает cache entries; warm read неизменённого объекта не требует повторного скачивания/repacking/reembedding. Snapshot/code restore имеет отдельную обязательную integrity verification. Каждый вывод всё равно проверяет текущие права. Cache keys/refs различают source/render generation, actual geometry, artifact class и scope; public illustration не переиспользует restricted page/quote rendition как публичный ответ.

Source deletion/withdrawal сначала отзывает видимость и delivery grants, затем отменяет разрешённые downstream projections и выполняет checked lifecycle. Незавершённый consumer, legal retention и историческое evidence не стираются случайным cache GC. Не менять исходный чужой archive/POI/map owner.

## Cutover

Переносятся только geo layers/places/geometry/CRS/temporal aliases/provenance и минимальные source reference stubs, необходимые геоданным. Остальные старые данные, включая негеографические facts/stories/cases/drafts и corpus, не импортируются как accepted target state. Fresh import создаёт новые проверенные source-bound records. Legacy geo anchor остаётся явно unanchored до exact original/edition/locator verification; название и номер страницы недостаточны. См. [план переноса](migrations/rkb-layers-only-cutover-20261019.md).
