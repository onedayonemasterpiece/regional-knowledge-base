# Ресурсный допуск поиска, импорта и maintenance

Обновлено 20.10.2026. План целевого Kaggle PG 17/BGE runtime. [Handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), [технический request lifecycle](../designs/user-request-load-management-20261019.md), [storage](../storage.md).

## Совместный envelope

На Kaggle конкурируют PostgreSQL connections/transactions/buffers, BGE inference/tokenizer, render/proof, source I/O, import и snapshot/index maintenance. Разные API не создают независимые CPU или RAM. На DevCoveer учитывать event loop, compact control state, raw spool и bounded WSS buffers; BGE inference и E5 там отсутствуют.

Один BGE model process; pool/parallelism учитываются суммарно по всем PG consumers. Измерять current/peak RAM, CPU quota/pressure, event-loop lag, PG lock/temp/WAL/I/O, stage duration и useful query latency. work_mem/parallel workers и maintenance memory умножаются по одновременным операциям; не отдавать один и тот же available memory reserve двум claimers.

Existing controller атомарно выдаёт короткий permit следующей стадии. Для durable jobs сохраняет generation/checkpoint; ephemeral `read_query` использует ту же admission/fairness authority и bounded in-flight context без создания durable job. Другие clients/producers не заводят собственные schedulers. Permit не удерживает SQL transaction во время внешнего ожидания; реально освобождённый ресурс возвращается, но память незавершённого этапа не объявляется свободной.

Один готовый read dispatches немедленно при доступном permit. Query batch объединяет уже ожидающие запросы, не ждёт заполнения. Ни fsync/remote seal, ни durable RESULT_ACK/RESUME, ни запись read telemetry не стоят на critical path; telemetry имеет bounded async sink вне queue SQLite/write locks. Durable questions/mutations сохраняют прежний ACK/recovery contract. Потерянный ephemeral read можно повторить с актуальным context, но это не разрешает повторять UNKNOWN effects.

## Поиск во время импорта

Поддерживать измеренный резерв как минимум одного supported search, включая query encode, PG candidates/hydration и control response. Резерв нужен и при пустой пользовательской очереди: иначе новый запрос встретит уже запущенный чрезмерный batch.

Query-first в BgeQueue не прерывает текущий bounded encode. Новый document/story batch, render или index build запускается только если помещается вместе с резервом. На pressure не брать следующий safe batch; commit/rollback текущей transaction завершается нормально. SIGSTOP/kill и ожидание permit под writer lock не используются.

Proof и import получают положительное продвижение через bounded aging: если oldest runnable stage постоянно не помещается, не начинать конкурирующие новые этапы, дать им закончиться и собрать её рабочий объём, сохраняя serving reserve. Если минимальный batch плюс резерв физически не помещаются, уменьшить checkpointed batch или признать capacity failure. Бесконечная starvation не считается выполненной гарантией.

Resource resize перечитывается. После уменьшения прекращаются новые тяжёлые starts; принятые запросы и checkpoints остаются. Повышение concurrency разрешено только в подтверждённом envelope. Missing/stale telemetry не даёт автоматически расширять допуск; hysteresis предотвращает дрожание.

## Deadline и query work

Warm backend p95≤1000 ms, public MCP p95≤1500 ms, hard evidence deadline2000 ms. Queue wait до полного human answer измеряется отдельно. Внутри короткого search не появляется скрытый многосекундный sleep ради пользовательской очереди.

Одна операция существующего Kaggle worker выполняет query encode, независимые lexical/alias branches, dense/fusion и batched hydration рядом с PG. Один BGE process и общий PG pool не превращаются в отдельный RPC chain. Query vector/промежуточные candidates не возвращаются в Dev для следующего запроса в Kaggle. Ответ — компактный typed evidence pack: default3/max5 для Live, нужные exact fragments/labelled excerpts, attribution/uncertainty, refs и continuation. Полный dossier/history/изображения не добавляются без запроса; actual saved story body и доступ к полному тексту через continuation сохраняются. Relay не делает JSON-inside-text parse/repack на каждом hop.

Проверенная ingress identity передаётся как компактный actor/audience/request/scope context; public book search не требует O(all books) allowlist. SQL проверяет eligibility, output gateway — текущие права и withdrawal всех top-K dependencies одной batched проверкой. Cached identity не отменяет current revocation barrier. Dot→tools и Mira→tools — оба рабочие smart routes; дополнительный агент для поиска не требуется.

Все lexical/alias/dense branches делят общий deadline; до 20 последовательных alias calls с собственным полным timeout недопустимы. Broad и scoped SQL имеют проверенные планы, включая actual prepared ANN после 6+ executions и крайние selectivity. Custom-plan/disableprepare применяется только к ANN при доказанном generic-plan regression; SET LOCAL knobs не протекают между pool clients; HNSW candidate limit стоит до полной hydration. Дедупликация/RRF и batch read top-K не обходят весь corpus. FTS per-row RU/DE/simple использует bounded набор query configs; alias lookup не запускает цикл отдельного full FTS на каждое имя. Immutable revision projections и маленький atomic active pointer исключают массовый activation UPDATE; inactive versions имеют bounded GC и проверенный iterative/exact filter path.

Scoped ANN имеет bounded iterative scan/refill. Deadline exhaustion или недостаток допущенных candidates маркируется partial/degraded, а не доказанным отсутствием фактов. Seq Scan маленькой таблицы допустим; O(N) full-catalog map, JSON dossier scan и Python sort всех story vectors исключаются.

`text_search_ready` не зависит от `archive_read_ready`/`proof_ready`: Telegram/FloodWait, cache miss оригинала и долгий proof не занимают text-query deadline. Уже готовые evidence возвращаются, artifact items продолжаются отдельно. Это не меняет owner cap: один genuine exclusive Telegram session lease на каждый разрешённый Kaggle run.

## Ограниченное расширение source context

Сохранить существующий bounded post-pass как дополнительный сигнал: для top 2 hits можно проверить не более двух соседних source-position chunks в каждую сторону, с тем же original document, active revision, article/component и physical page. Второй hop требует допустимого промежуточного chunk; illustration-bearing content не пересекается. Scope проверяется до fetch.

Добавляется максимум один contextual source result, top 2 ranked evidence защищены. Immediate grammatical continuation имеет приоритет; альтернативный context требует минимум трёх question cues и большего нормализованного overlap, чем anchor. Direction/hop/same_page_context маркируются отдельно от primary BGE rank.

Не добавлять hardcoded user titles, locations или entity-specific rewrites. Dense-only gate измеряется без этого post-pass; supplement не превращает слабый dense recall в PASS. Смена этих правил требует source-grounded quality gate.

## Maintenance и измерения

Autovacuum включён; targeted ANALYZE после значимой bulk activation. Один maintenance slot в existing controller; крупный snapshot/build/reindex учитывает disk/memory overlap и serving reserve. Никаких hourly/daily full REINDEX, VACUUM FULL или полного reembed.

При возможности candidate restore выполняется на уже разрешённом planned reader/successor, который затем используется после полной verification/replay/fencing. Weekly drill может использовать тот же restore только при выполнении всех recovery/control checks. Без такого run maintenance остаётся bounded по CPU/I/O/RAM и не отнимает search reserve; новый notebook ради проверки не обходит session cap. Snapshot/restore contention входит в latency gate. Полезные bounded caches, FP32 и projections сохраняются ради скорости при 80/90% capacity headroom; warm reads не вызывают повторный full hashing/repacking/reembedding неизменённых verified objects.

Различать encoding time, queue time, SQL candidates, hydration, source I/O, e2e и native delivery. Сохранять ошибки/timeout/degraded attempts, resource/pin/config и corpus revision; не отчитываться лучшим одиночным call. Исторические systemd CPUWeight/Quota не являются измерением новой Kaggle мощности.

[Прежние CPU measurements и deployed context rules](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/retrieval-cpu-scheduling.md) сохранены как immutable baseline. Текущая документация не запускала новый benchmark.
