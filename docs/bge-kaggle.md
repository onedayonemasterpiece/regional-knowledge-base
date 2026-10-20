# BGE-M3 в Kaggle: модель и serving

Обновлено 20.10.2026. Целевой контракт, не подтверждение внедрения. Полное [задание агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md), [Kaggle operations](operations/bge-kaggle.md), [placement boundary](operations/bge-kaggle-only.md).

## Неизменяемый baseline

BGE-M3 query и document inference выполняются только в Kaggle. Existing model revision — **5617a9f61b028005a4858fdac845db406aefb181**, dense dimension **1024**, **FP32**. Используется SHA-pinned BGE tokenizer; точный final input с фактическими prefixes не превышает **512 tokens**, coherent passage ориентировочно около **256**. Silent truncation не является допустимой обработкой oversized input.

Один resident PyTorch FP32 model process используется QUERY serving и existing BgeQueue import/story jobs через общие permits. Сохраняется baseline no-prefix CLS pooling + L2; model load на запрос запрещён. Book и story producers используют тот же embedding space и очередь; у stories своя типизированная PG search projection. Увеличение dimensionality, скрытая quantization или иной model под прежним space запрещены. Изменение representation требует нового material/model contract и quality gate.

Target — PostgreSQL **17** с pinned pgvector рядом с BGE. E5 и Supabase из target исключены. Локальный BGE fallback на DevCoveer не появляется при cold start или outage. Нужный tokenizer-only asset можно использовать на ingestion validation стороне; это не разрешение загружать model weights.

## Запрос и очередь

Dot → RKB и Mira → RKB остаются независимыми маршрутами; Mira не вызывает Dot для retrieval. Notebook держит authenticated outbound WSS к existing DevCoveer ASGI, который служит тонким auth/admission relay. Public notebook listener не требуется. Ephemeral QUERY использует общий read permit/search reserve, но не durable row/claim или mutation RESUME; durable import/story jobs сохраняют existing queue/claims. Reconnect не создаёт второго dispatcher.

QUERY целиком исполняется в Kaggle: query encode + local PG dense/FTS/alias SQL + bounded RRF + batch hydrate bibliography/evidence. Lexical/alias branches идут параллельно encode. Промежуточный query vector не возвращается на DevCoveer; наружу идёт ограниченный result DTO. Query-first действует перед следующей порцией import; already running bounded encode завершается, следующий document batch допускается при сохранении search reserve.

Ключ embedding cache включает model/space и точный normalized input. Дедупликация encoder job не объединяет разные пользовательские диалоги и не отменяет current scope. Accepted source decisions не воспроизводятся повторным вызовом модели при replay.

Вопрос передаётся без unrelated book titles и длинных instruction wrappers. Book/geo/time restrictions передаются отдельными filters. BGE — retrieval encoder, а source analysis и проверяемое извлечение facts остаются работой вызывающей модели через import contract.

## Готовность

Раздельные состояния: model loaded, DB compatible/restored, vector coverage, actual query readiness, socket health, request result и final delivery. Serving ready требует SQL query и dense query probe с useful evidence; число записанных vectors или heartbeat ready не заменяют probe.

Temporary WSS failure сохраняет тёплый PG/BGE process, пока сама Kaggle session жива. После завершения session необходим новый run, restore и warmup. Useful-idle 30 минут не равен task lease и не продлевается Pong.

Если BGE недоступна, ephemeral QUERY возвращает явный degraded lexical result либо unavailable в общем evidence deadline, без скрытого durable resume. Pending continuation применяется только к явно выбранному durable request. Лексический результат не засчитывается как dense PASS. Canonical command outcome сохраняет exact domain IDs/body/evidence, но не зависит от заново вычисленных BGE floats/FTS/crops. При replay projection может стать dirty; её rebuild и actual dense readiness идут отдельно от canonical cursor, accepted BGE artifacts reuse. Уже delivered artifact bytes/hash/receipt не подменяются новыми bytes.

Read-only QUERY не ждёт журналирования ни в одном направлении: bounded async audit refs/hashes/timings не является обещанием durable receipt. Mutations и явно принятые durable requests сохраняют accepted/Saved/replay contract; человеческий read-turn сам по себе в него не переводится. Late QUERY_RESULT от отменённой turn/connection generation отбрасывается.

## Python default и native optimization

Python остаётся glue-кодом над resident PyTorch и PG/native indexes. Общий CPU budget учитывает Torch intra/inter-op, SQL/maintenance/import и event-loop reserve; бесконтрольно умножать model copies/threads нельзя. Fused QUERY не spawn-ит subprocess и не компилирует код на каждый запрос.

Precompiled Go/Rust binary осуществим как ограниченная замена: Go на DevCoveer заменяет конкретный transport/serialization component, Rust — дорогую функцию либо компонент с воспроизводимой проблемой памяти/lifecycle. Default остаётся Python; новый proxy, второй model/authority не добавляются, sidecar не является baseline. Память, deadlines, backpressure и control-loop isolation допустимы как основания native выбора для нового компонента без предварительного CPU bottleneck; проверенный совместимый native компонент можно переиспользовать без полного Python дубля. Принимается один узкий вариант с подтверждённым выигрышем полезной задержки/ресурсов/отказов, без регрессии quality, прав и recovery, с удалением заменяемого пути. Local IPC не запрещён: учитывать его copies/serialization/backpressure, Python BGE child и lifecycle в полном сравнении; обязательного порога 10% нет. Actual tokenizer backend нужно установить явно; Python AutoTokenizer не доказывает Python tokenization. ONNX/GPU/quantization, изменение pooling/prefix/batch/numerical path проверяются отдельными model/material и quality/latency gates; похожие floats не доказывают прежний recall. Exact token IDs и accepted domain payload не подменяются.

Immutable bundle, architecture/ISA/libc/shared-library ABI и supervisor lifecycle описаны в [configuration](configuration.md) и [runbook](operations/bge-kaggle.md). Compilers не являются production cold-start dependency.

## Проверка качества и времени

BGE-only Hit@5 ≥85%, Hit@10 ≥90%, RU→DE Hit@10 ≥85% на ≥30 held-out fact families. BGE-only, lexical-only и configured fusion проверяются отдельно; lexical не prefilter dense candidates. Minimum scale ≥100 book-equivalent sources/100000 active chunks. Warm backend p95≤1 s, MCP p95≤1.5 s, hard evidence deadline2 s.

[Исходный worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/scripts/production/bge_kaggle_worker.py) использует один FP32 CPU model, четыре Torch threads и один interop thread. Это code baseline, не измеренная достаточность совместного PG+BGE envelope. [Прежний документ](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/bge-kaggle.md) сохранён в immutable history.
