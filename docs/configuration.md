# Конфигурация целевого runtime

Обновлено 20.10.2026. Документ описывает конфигурационный контракт реализации; новая конфигурация ещё не принята в production. Единое [задание](prompts/rkb-scalable-kaggle-implementation-20261019.md), [architecture](architecture.md), [storage](storage.md) и [acceptance](testing/rkb-scalable-kaggle-acceptance-20261019.md) определяют поведение. [Прежние variables/setup](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/configuration.md) — immutable baseline до cutover.

## Version manifest

| Параметр | Целевой контракт |
|---|---|
| PostgreSQL | Major **17**, выбранный patch/build закреплён в release manifest; проверены non-root startup, fsync, extensions и physical restore. Manifest фиксирует minor, libs/platform и collation provider/version; affected-index rebuild при несовместимости предшествует READY. Не брать случайный latest. |
| pgvector | Exact version/build pin, совместимый с PG 17. Для iterative scans требуется поддерживающая их версия; pin и query plans проверяются вместе. |
| BGE-M3 | Существующий revision **5617a9f61b028005a4858fdac845db406aefb181**, 1024 dimensions, FP32; inference только в Kaggle. |
| Tokenizer | SHA-pinned BGE tokenizer, точный final input ≤512 tokens; ориентир passage около 256. Смена path не меняет hash/version. |
| Schema / function / executor / command codec | Exact revisions и migration ID/hash/predecessor в commands, snapshots и ordered recovery tail; несовместимый restore/replay не получает readiness. |
| Application release | Immutable merged commit и совпадающие actual executing revisions; Git main не доказывает запущенный код. |
| Notebook identity | Стабильный owned slug; отдельные provider version/execution, logical run/nonce, bootstrap instance и worker/connection generation. Code hash отдельно от launch config и executed source. |

E5, его vector indexes, runtime/fallback и обязательные token counts удаляются из target. Supabase не является target dependency. Независимые shared Python consumers и pinned BGE tokenizer сохраняются; исторический каталог с именем fast-e5 сам по себе не основание удалить нужный tokenizer. Generic 768-dimensional embedding provider не подставляется вместо BGE.

## Bootstrap и optional native bundle

Default: Python bootstrap/QUERY handler и существующая authority, один resident PyTorch FP32 model, PostgreSQL native kernels. Manifest фиксирует Python/packages, actual tokenizer backend и model/tokenizer hashes. Go/Rust optional: замена измеренного transport/hot function либо компонента с воспроизводимой проблемой памяти/lifecycle/backpressure. Для нового компонента эти эксплуатационные требования допустимы и без доказанного CPU hot loop; проверенный native reuse не требует полного Python дубля. Go на DevCoveer заменяет существующий компонент, а не добавляет proxy/authority. Sidecar не является default; полный rewrite и второй model/controller не нужны. Local IPC допустим при подтверждённом net benefit полезной задержки/ресурсов/отказов с учётом copies, serialization, Python BGE child и lifecycle, без регрессии quality/прав/recovery; заменяемый путь удаляется.

Precompiled artifact закреплён source SHA/toolchain/dependency lock/hash, protocol version, Linux architecture, minimum CPU ISA и libc/shared-library ABI. Не собирать с неподтверждённым target-cpu=native и не предполагать одинаковый host после restart. Bundle проверяется до execute из writable runtime directory; no per-request spawn/compile/model load. Supervisor использует явные process groups/exit codes, bounded log drains и PG shutdown; session kill не даёт guaranteed finally/cleanup. Shared libs/CA assets входят в совместимость; CGO_ENABLED=0 или musl не делают любой inference stack автоматически статическим.

## Обязательные настройки реализации

Ниже имена контрактов, а не утверждение о наличии уже реализованных новых env flags. Агент фиксирует реальные имена/defaults в коде и readback до выпуска.

| Область | Начальная настройка / проверка |
|---|---|
| Raw DevCoveer spool | Горизонт **4 h**, hard cap **512 MiB**; отдельно compact request/UNKNOWN/receipt state до reconciliation. |
| Remote accepted commit | Existing Yandex private recovery storage; exact payload/intent/hash/version readback перед accepted. Saved дополнительно требует domain commit и remote exact outcome/receipt. Group-commit batching window **≤1 s**, не cloud RTT. |
| Control checkpoint | **15 min**; новые accepted mutations, реальные turns и revocations защищает tail до ACK. |
| PG base cadence | **4 h при изменениях**, раньше по replay budget/byte pressure/planned handover; без изменений skip. |
| Independent-from-Kaggle backup | Подтверждённая Dataset version копируется в Yandex S3 раз в неделю; exact version/date и old+candidate overlap до проверки. Старый online tail не подменяет Dataset primary. |
| Useful idle | **30 min** от полезного спроса; socket Ping/Pong не продлевает этот таймер. |
| Lifecycle budgets | Конечный per-run wall budget и stage deadlines; начальный control-loss grace 10 min. Stop/readback не заменяется TTL. |
| Provider reconciliation | Один controller; начальные start/stop polls ≥30 s, healthy 5 min, error backoff до2 min с jitter/Retry-After. Это настраиваемые defaults, не provider SLA; WSS progress приходит сразу. |
| Writer / claim / connection | Разные short leases/generations, heartbeat и deadlines; численные lease limits калибруются под bounded stages, не под 30 min idle. |
| Memory / concurrency | Один BGE process, bounded PG pool и parallelism, measured stage permits, минимум один search reserve во время import. |
| Storage headroom | ≤80% steady, ≤90% peak; отдельные volume/Dataset/recovery/spool/cache budgets. |
| Query latency | Backend p95≤1000 ms, MCP p95≤1500 ms, hard evidence deadline2000 ms. Каждый branch укладывается в общий deadline. |

Cadence не является TTL: нельзя удалять неподтверждённый payload или UNKNOWN outcome ради таймера. Terminal receipt horizon 30 days разрешён только для verifiable server-issued IDs: expired ID never-new-execution; arbitrary legacy keys не GC blindly. UNKNOWN admission/reconciliation ограничивается per provider/route, не всем import. Read-only search не вызывает remote commit. Account quota, доступная RAM/CPU, writable mount, disk headroom, source sizes и restore time измеряются до cutover; старый Supabase pool или quota не переносятся как Kaggle limits.

## File transport capabilities

Зафиксировать поддержанный client fileParams/export/direct-PUT contract, actual URL lifetime/queue behavior, выбранный artifact provider, exact immutable version/checksum readback, HTTPS redirect allowlist, byte/extraction limits и private visibility. Не хранить live signed URLs в release/source manifest. Controller tickets имеют actor/class/source/attempt binding и выдаются при готовности transfer; чужой client URL не получает фиктивный refresh. Provider-enforced upload cap, delete/version capabilities и quota readback обозначаются как verified либо unavailable, не предполагаются. Использовать existing control registry/guard для temporary consumers/GC; отдельный service или framework не нужен. Полный contract и acceptance — handoff§5.5/DBG-11–13.

## WSS и event loop

Notebook использует единственное outbound authenticated WSS attachment к existing ASGI/HTTPS service; public Kaggle listener отсутствует. Два самостоятельных клиента Dot → RKB и Mira → RKB используют ту же auth/admission authority; Dev relay не добавляет reasoning hop. Проверить реальный reverse-proxy upgrade path. Задать handshake/send deadlines, bounded frame/total-buffer bytes, credits, Ping/Pong, jittered reconnect и resume window. Large artifacts — по existing byte transport и immutable refs.

Run credentials scoped к конкретному run/generation; user bearer не передаётся notebook. Reconnect не вызывает kernels_push и не выгружает BGE, пока текущая Kaggle-сессия жива. После её завершения новый run восстанавливает PG и модель. Late old-socket results fenced. Durable jobs: same-ID same-hash replay idempotent, different-hash conflict. Ephemeral QUERY: request/turn/connection generation + общий deadline/min_data_seq; без durable replay/RESUME, late/cancelled answer не выдаётся в новой реплике.

Минимальный WSS/HELLO поднимается до тяжёлого restore/model load; READY после probes. Bootstrap/watchdog получает actual controller lease через bounded local record и не продлевает его локальной активностью. Runtime paths, expected outputs, installed SDK/version-specific status semantics/pagination/stop path фиксируются Stage0 receipt. Current CLI docs не доказывают installed adapter capabilities; delete notebook и cancel ACK не освобождают execution.

Warm changes используют exact artifact refs по existing byte transport: SQL к живому PG, только совместимый stateless read handler с rollback; прочие process/context changes — controlled restart. Deps/model caches hash-pinned, без pip upgrade/reinstall на каждом patch. Полный lifecycle и fixture/migration barriers — [handoff §5.4/6.6](prompts/rkb-scalable-kaggle-implementation-20261019.md).

Blocking encode, PG backup, render, file I/O и external download не выполняются в ASGI event loop. Read polling/control views не открывают BEGIN IMMEDIATE. Wakeup после commit поддерживает межпроцессный путь; process-local Event не заменяет durable work и bounded reconciliation.

QUERY целиком выполняет в Kaggle encode + dense/FTS/alias SQL + RRF + batched hydrate. На Dev передаются bounded bibliography/evidence, mode/freshness, а не vector.tolist() для обратной отправки в PG. При encode lexical/alias SQL может выполняться параллельно. One resident model и один общий admission/search reserve сохраняются для read и import jobs.

Read-only audit асинхронен и не ожидается перед request/response: bounded buffer refs/hashes/timings, без raw private body. Потери audit отражаются отдельно, не превращаются в blocking queue и не объявляются durable receipt. Remote accepted/Saved относится к mutations и явно выбранному durable request, не любому read-turn.

## SQL и query budgets

Pool size считается суммарно по всем notebook processes, including maintenance; не множить произвольный default на каждый процесс. PostgreSQL socket остаётся локальным notebook service. Roles имеют минимальные права, authenticated application actor устанавливается transaction-locally; identity не берётся из Supabase auth.uid/JWKS.

У query paths явные statement/lock timeouts, общий request deadline, bounded candidate depth, alias expansion и hydration window. ANN ef/search scan/memory параметры устанавливаются через SET LOCAL внутри transaction, имеют конечные максимумы и не протекают через pool. Actual prepared ANN plan проверяется после 6+ executions и на крайних selectivity; custom plan/disableprepare только для ANN при доказанном regression. FTS per-row RU/DE/simple и bounded query configurations исключают alias loop. Narrow scope и broad ANN — отдельные проверенные SQL shapes.

work_mem лимитирует отдельные sort/hash операции и умножается на concurrency; maintenance_work_mem/autovacuum_work_mem/shared_buffers/parallel workers/temp_file_limit выбираются совместно с BGE RSS и OS reserve. Torch intra/inter-op threads, PG parallel workers, render/import и возможный native runtime используют единый измеренный CPU budget; Python worker не размножается вместе с model. Исходные 4 Torch threads/1 interop не являются гарантированной доступной capacity. Autovacuum не выключается ради краткого benchmark. Один controller ограничивает тяжёлые index builds/snapshots; фоновый task не создаёт собственный независимый scheduler.

## Application identity и credentials

Сохранить существующие auth-plane settings:
- RKB_RESOURCE_URL — exact публичный MCP resource;
- RKB_AUTH_ISSUER и RKB_AUTH_JWKS_URL — application/platform issuer и signing keys;
- RKB_DEV_NOAUTH — только локальные deterministic tests, не production.

Provider/tenant/subject связываются с canonical person UUID. Не передавать end-user bearer другому MCP/worker/data plane; service grant ограничен request/generation/scope. Private drafts, mutations, import/admin и pixel artifacts имеют server-side capabilities, которые перепроверяются после ожидания и перед delivery.

Existing S3ObjectStore пока предоставляет put/get/delete transport. Immutable sealing, exact segment sequence/count/hash/previous и continuous manifest CAS реализуются узко поверх него. If-Match/If-None-Match документированы в [Yandex PutObject](https://yandex.cloud/en/docs/storage/s3/api-ref/object/upload); runtime probe подтверждает фактический SDK/bucket CAS и race behavior. Orphan upload не accepted. Bounded sealed-tail compaction под одним controller сохраняет logical IDs/hashes/seq и CAS refs.

Большие base bytes идут direct Kaggle→Dataset/recovery object, не через Dev 512 MiB spool. Notebook получает scoped upload key+epoch+expiry без long-lived delete/global-bucket credentials. Presigned URL сам не отзывается при epoch change: внешний promotion fence не выбирает late old-writer upload. DevCoveer сохраняет manifest/GC authority. Для Dataset controller также удерживает Kaggle account key и blob token, выполняет start_blob_upload/version create/readback/promotion; notebook получает только bounded signed upload URL/session. Source-backed primitive и обязательная runtime проверка описаны в [storage](storage.md).

Yandex/archive credentials и Telegram sessions выдаются раздельно с минимальными правами. Existing RKB_S3_* имена требуют проверки фактического bucket назначения: временный ingestion bucket не становится recovery archive по совпадению prefix. Не наследовать случайные OPENAI/GOOGLE/GEMINI keys или общие owner credentials.

Secrets/configs с секретами размещаются вне архивируемого PG data directory, Git, logs, public notebook outputs и snapshots. Physical backup содержит cluster files, поэтому отсутствие секретов проверяется на итоговом archive. Владелец указал `TELEGRAM_RKB1` и `TELEGRAM_RKB2` (вторая пока пустая). До запуска проверить их фактическую независимость и archive access; одна session выдаётся exclusive lease одному Kaggle run. Число одновременных runs не превышает числа проверенных sessions и доступной Kaggle capacity. Имена здесь — ссылки на защищённую конфигурацию, не секретные значения; StringSession не клонируется между процессами.

Telegram use grant только после verified HELLO/lease. Предпочтительна scoped выдача credential после gate; если protected secret binding доступен с запуска, запрет раннего чтения считается кооперативным и фиксируется как ограничение. Старый credential не отзывается сменой epoch. TG slot переиспользуется лишь после exact execution terminal proof либо доказанной ревокации authorization с проверкой новой session; последнее не подтверждает остановку compute. Дополнительная debug session не предполагается.

Изменение Go/Rust glue не меняет embedding contract. ONNX/GPU/quantization или другой tokenizer backend с иными token IDs/numerical behavior — отдельный quality/latency track; прежний space нельзя молча использовать для несовместимых vectors. Дополнительные первичные sources и границы выводов: [runtime assessment](designs/kaggle-regional-knowledge-service-assessment-20261019.md).

## Rights и readiness

Active accepted book text/bibliography и illustrations доступны всем. Stripe quote proof — owner/demo/expert/local_historian; полный PDF/DjVu и full-page pixels private. Публичная иллюстрация не открывает исходную страницу целиком. Current grants и audience применяются также к warm caches, preview, delayed callback и final delivery.

Serving readiness требует actual SQL query + BGE query probe в закреплённом model space, exact active revision и compatibility cursor. Separately показываются socket health, writer lease, vector coverage, model readiness, snapshot/replay status и delivery status. HTTP200, ready-vector counts или принятый enqueue не равны успешной выдаче evidence.

Первичные справочники: [PostgreSQL resources](https://www.postgresql.org/docs/current/runtime-config-resource.html), [pgvector](https://github.com/pgvector/pgvector), [Kaggle notebooks](https://www.kaggle.com/docs/notebooks), [Kaggle Datasets](https://www.kaggle.com/docs/datasets). Их текущие limits сверяются с реальным account/environment, не превращаются в обещание SLA.
