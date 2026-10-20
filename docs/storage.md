# Хранение, восстановление и ограничение объёма

Обновлено 20.10.2026. Целевой контракт для [единого handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md); новая реализация не развёрнута этой документационной работой. [Исходная SQLite/Supabase схема](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/storage.md) сохранена как immutable historical record.

## Один рабочий corpus

PostgreSQL 17 с pinned pgvector на Kaggle хранит exact accepted text, bibliography, source/page/region graph, факты, entities/relations, stories/variants, dates/observations, cases, drafts и BGE/FTS serving projections. E5 и Supabase в target отсутствуют. Короткая DevCoveer control БД не содержит постоянной второй копии corpus, vectors, PDF или BGE weights.

Originals и illustrations сохраняются существующим VibePublish/Telegram archive owner. Private Dataset хранит согласованные PG snapshots; существующий Yandex recovery storage — immutable command/control/receipt tail, accepted payloads и проверенную daily base. Source binary archive, recovery storage, working DB и temporary caches имеют разные lifecycle и budgets.

Exact source_text и детерминированный search_material имеют собственные hashes и material version. Passages ссылаются на точные source spans; не размножать полную страницу ради каждого embedding. Model/human accepted decisions сохраняются точно: PDF SHA и повторная модель не воспроизводят прежний accepted result. Старые derived vectors можно убрать после безопасной смены revision; provenance и принятые значения нельзя подменить пересчётом.

## ACK и короткий raw spool

| Граница | Значение |
|---|---|
| Local intake receipt | pending: известны request/idempotency key и временно сохранён input; финальное accepted ещё не обещано. |
| Remotely sealed intent | Точный immutable payload и resolved intent доступны в Yandex recovery storage, hashes/size/version подтверждены readback; команда может получить accepted. |
| Applied / Saved | PG transaction сохранила domain effects, applied_ops и непрерывный cursor; exact outcome/receipt также remotely verified. Cloud intake сам по себе не applied. |
| Delivered | Единственный sender подтвердил native receipt/readback конкретного result hash. Unknown outcome остаётся UNKNOWN до reconciliation. |

Accepted mutations и реальные user turns группируются в bounded group commit: готовая группа ждёт flush не более 1 секунды. Это предел batching delay, а не обещание provider latency. Ошибка remote commit оставляет pending и не превращается в Saved. Обычный read-only поиск не ставится в очередь remote journal и не требует S3 round trip.

Existing S3ObjectStore — transport put/get/delete. Immutable create/seal, exact verification и conditional manifest CAS ещё реализуются поверх него. [Yandex PutObject](https://yandex.cloud/en/docs/storage/s3/api-ref/object/upload) документирует If-Match/If-None-Match; runtime probe проверяет конкретный SDK/bucket и competing-writer race, а не существование headers в спецификации. Segment содержит sequence range/count, payload hashes и previous hash. Manifest принимает только verified continuous chain; orphan upload не accepted.

Стартовые implementation defaults для DevCoveer raw spool — **4 часа и hard cap 512 MiB**; до cutover измерить их достаточность под accepted payload sizes и write bursts. Числа не относятся к сроку жизни диалога, UNKNOWN send, private draft или unresolved case. Компактные request/receipt/UNKNOWN metadata сохраняются до reconciliation и переносятся в control checkpoint. Terminal receipt horizon 30 days допустим только для verifiable server-issued request IDs: expired ID не становится новой execution. Arbitrary legacy keys не GC вслепую. Pending callbacks используют durable artifact refs, а не удерживают месяцы raw body на DevCoveer.

Local GC удаляет raw bytes после remotely sealed payload + verified metadata. Он **не ждёт самой старой daily base**. Нельзя объявлять неподтверждённые bytes защищёнными по успешному socket send или upload request. Age/byte pressure заранее ограничивает новые admissions; при неисправном recovery backend нет финального accepted ACK. Ни возраст, ни заполненный диск не разрешают удалить единственную копию уже подтверждённого результата.

## Base, tail и checkpoint

1. PostgreSQL base — consistent physical backup с нужным WAL и indexes. Обычный cp работающего data directory запрещён. Logical dump применяется для обоснованного version/schema upgrade с отдельным измерением restore/index rebuild.
2. Exact command/control/receipt tail хранится immutable в существующем Yandex recovery storage. Resolved payload включает выбранные IDs, logical timestamps, preconditions и codec/schema version. Результаты модели не генерируются заново при replay.
3. Control checkpoint создаётся каждые **15 минут**. Accepted turns, grants/revocations и effects не ждут этого интервала: их изменения защищает remote tail до финального ACK.
4. Changed PG base создаётся каждые **4 часа**, а также раньше при превышении replay-time/byte budget или перед planned handover. Без изменений полную копию не создавать. Index rebuild и reembedding не являются частью каждого checkpoint.
5. Примерно раз в сутки сохранить одну проверенную независимую от Kaggle PG base в Yandex. Временно нужны old + candidate; удалить old можно после проверки новой копии. Yandex base и Yandex tail находятся в одном failure domain и не считаются независимыми резервами друг друга.
6. Remote-tail GC сохраняет все интервалы, нужные **designated retained verified baselines** и unfinished consumers. Отставшая копия либо остаётся с полным recovery chain, либо явно выводится из набора recoverable baselines после годной замены. Нельзя удалить её tail и продолжать показывать её как восстановимую.
7. Readers pin exact snapshot version, не Dataset latest; их leases и восстановительные зависимости учитываются до version GC.

Manifest связывает snapshot ID, exact Dataset version, PG major/minor, extension/library builds, platform и collation provider/version, schema/codec, восстановленный внутренний applied cursor, hashes/sizes, WAL range, model/space и verification receipt. Cursor не вычисляется произвольным MAX(seq) в другой БД. Для deterministic keyed normalized text используется C collation; несовместимость collation/version требует корректного rebuild затронутых indexes до READY. Не предполагать непроверенный pin окружения через Kaggle UI.

Большие base bytes передаются напрямую Kaggle→Dataset/recovery object, не через 512 MiB Dev spool. Notebook не получает long-lived delete/global-bucket credentials: только scoped upload authorization на конкретный key+epoch с expiry. Presigned URL не отзывается автоматически при epoch change; expired writer может оставить orphan upload, но внешний promotion fence не разрешит его выбрать. DevCoveer владеет manifest/CAS/GC authority.

Для Dataset source-backed primitive есть в [Kaggle CLI 6894864](https://github.com/Kaggle/kaggle-cli/blob/6894864ff4095cf98e9e36d3a694ff5fc9ba3818/src/kaggle/api/kaggle_api_extended.py): `_upload_blob` вызывает `start_blob_upload(ApiBlobType.DATASET)`, получает `create_url` и token; `upload_complete` передаёт bytes напрямую, а `dataset_create_version` использует `UploadFile.token` в `body.files`. Узкий target adapter разделяет это: Dev controller хранит account key/blob token, создаёт upload и затем version, делает readback/promotion; notebook получает только ограниченный signed URL/session и отправляет bytes. Это подтверждённая кодом возможность адаптации, не готовый adapter: runtime probe проверяет expiry, direct upload, finalize и восстановление exact version.

Sealed-tail compaction ограничивает object count/bytes/replay time. Один existing controller собирает verified sealed segments в более крупные immutable bundles, сохраняет logical IDs/hashes/sequence proofs и переключает refs через CAS. Старые objects удаляются лишь после проверки replacement и учёта readers/consumers; не строить новый compaction service.

Проверка candidate: provider status → скачать exact version → hashes/size → restore с нужными extensions/WAL → SQL invariants/cursor → replay непрерывного tail → actual SQL+dense probes. Canonical pointer переключается внешним fenced CAS только после этих проверок. Crash между apply и ACK возвращает тот же результат через applied_ops, не повторяет внешнюю отправку.

## Failure domains и быстрые reads

Потеря Kaggle session: восстановить verified base, replay exact remotely accepted tail, подтвердить новые readiness probes. Canonical outcome hash фиксирует exact domain IDs/body/evidence, исключая заново вычисленные BGE floats, derived FTS и crops. Derived projections могут стать dirty и rebuild асинхронно, accepted BGE artifacts reuse; dense lag не задерживает canonical replay. Уже delivered artifact bytes/hash/receipt остаются exact и не подменяются regenerated изображением. Потеря DevCoveer disk: восстановить compact control checkpoint и remote command/control/receipt tail; не воскресить старую writer authority или отозванный grant. Потеря Yandex требует отдельного анализа остаточного Dataset recovery point: две копии в одном Yandex не доказывают RPO=0.

Read-only evidence search использует прогретый serving PG/BGE, не создаёт durable user-turn запись автоматически. Уже managed request передаёт request/generation context без второго lifecycle. min_data_seq обеспечивает read-after-write; ordinary reader сообщает data_as_of. Current withdrawal/capability barriers проверяются gateway перед output/cache reuse/delivery, даже если reader snapshot отстаёт.

## Ёмкость и экономичность

Измерять отдельно:
- Kaggle writable mount: PG heap/TOAST/indexes, WAL, temp, staging, restored data, candidate backup и download/render cache;
- private Dataset quota: retained base versions и upload overlap;
- Yandex: designated bases, immutable tail/payloads, pending artifacts и overlap;
- DevCoveer: raw spool, compact metadata/checkpoints и bounded network buffers.

Gates: steady usage **≤80%**, крупнейший import/restore/snapshot/cutover peak **≤90%** каждого applicable budget. Minimum benchmark — **100 book-equivalent sources и 100000 active chunks**; это не product limit. Raw-vector arithmetic не заменяет measured index/volume usage.

По текущей [Kaggle Dataset документации](https://www.kaggle.com/docs/datasets): 200 GB на Dataset, 200 GB суммарно private datasets, максимум 50 top-level files. Archives распаковываются; zip не обходит лимит. [Notebook docs](https://www.kaggle.com/docs/notebooks) отдельно указывают 20 GB autosaved working space и конечную session lifetime. Реальный аккаунт, mounts и свободные quotas проверяются перед cutover; Dataset capacity не равна рабочему диску PG.

[CLI](https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md) создаёт versions асинхронно и предоставляет status/delete-old-versions. Не предполагать транзакционный append, дешёвую incremental upload или selective version GC. Current + candidate и reader pins входят в budget; при необходимости использовать ограниченную ротацию целых generation datasets после verified replacement, не Dataset на каждую книгу. Сжатие проверяется по фактическому stored/uncompressed accounting.

## RAM и обслуживание индексов

Один loaded BGE process обслуживает queries/documents. Budget включает model/activations, PG shared buffers, все одновременные query workspaces, maintenance и OS reserve. work_mem умножается по операциям, sessions и parallel workers; отдельно ограничить maintenance/autovacuum memory и temp files. См. [PostgreSQL resource consumption](https://www.postgresql.org/docs/current/runtime-config-resource.html).

Bulk imports используют bounded batches и готовое staging. GIN/HNSW initial build идёт после начальной bulk load; activation поддерживает active revision, затем targeted ANALYZE. Autovacuum включён. Один controller выдаёт permit тяжёлой maintenance работе и учитывает serving reserve; нельзя выполнять hourly/daily full REINDEX, VACUUM FULL или reembedding по таймеру.

REINDEX допустим при доказанном bloat/повреждении/upgrade и наличии места для replacement index. VACUUM FULL требует exclusive lock и дополнительную копию; это отдельный ремонт, не обычный GC. Старые derived revisions удаляются по retention/provenance contracts с последующим обычным vacuum. [PostgreSQL vacuum](https://www.postgresql.org/docs/current/routine-vacuuming.html), [pgvector maintenance](https://github.com/pgvector/pgvector).

## Originals, caches и безопасный доступ

Принятый текст и библиография доступны всем; иллюстрации также доступны всем через соответствующий artifact. Stripe quote scans разрешены owner/demo/expert/local_historian. Full PDF/DjVu, full-page previews и private draft history остаются private. Region/figure grant не разрешает страницу целиком или чужие annotations. До preview, render, callback и delivery проверяются текущая роль, audience, source revision и artifact scope.

Exact hashes используются для integrity/dedup, не как угадываемый публичный object key. В MCP возвращаются opaque source/artifact IDs и authorized retrieval; raw storage keys, credentials и signed URLs не попадают в public logs. Recovery snapshots закрыты: public text projection не делает всю БД public.

Сохраняются bounded streaming, incremental hash, MIME/signature/container validation и SSRF guard загрузки: HTTPS/443, без credentials/redirects/IP-literal/localhost/private DNS targets; approved DNS pins, без proxy/cookie inheritance и неожиданной content decompression. Существующий source size default 128 MiB и operator ceiling 512 MiB проверяются до и во время download. Это размер входного файла, не spool budget.

Original cache и proof cache имеют свои отдельные byte/idle caps и cross-process leases. Exact preview/hash/rotation/transform сохраняется до terminal locator callback; утрата preview требует новой generation и повторной локализации. Подтверждённый callback/artifact ref позволяет освободить временные bytes. Сессии Telegram независимы и имеют exclusive lease; account FLOOD_WAIT остаётся общим пределом.

Source deletion/withdrawal сначала отзывает видимость и delivery grants, затем отменяет разрешённые downstream projections и выполняет checked lifecycle. Незавершённый consumer, legal retention и историческое evidence не стираются случайным cache GC. Не менять исходный чужой archive/POI/map owner.

## Cutover

Переносятся только geo layers/places/geometry/CRS/temporal aliases/provenance и минимальные source reference stubs, необходимые геоданным. Остальные старые данные, включая негеографические facts/stories/cases/drafts и corpus, не импортируются как accepted target state. Fresh import создаёт новые проверенные source-bound records. Legacy geo anchor остаётся явно unanchored до exact original/edition/locator verification; название и номер страницы недостаточны. См. [план переноса](migrations/rkb-layers-only-cutover-20261019.md).
