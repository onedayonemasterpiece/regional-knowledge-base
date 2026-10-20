# BGE-M3 в Kaggle: модель и serving

Обновлено 20.10.2026. Целевой контракт, не подтверждение внедрения. Полное [задание агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md), [Kaggle operations](operations/bge-kaggle.md), [placement boundary](operations/bge-kaggle-only.md).

## Неизменяемый baseline

BGE-M3 query и document inference выполняются только в Kaggle. Existing model revision — **5617a9f61b028005a4858fdac845db406aefb181**, dense dimension **1024**, **FP32**. Используется SHA-pinned BGE tokenizer; точный final input с фактическими prefixes не превышает **512 tokens**, coherent passage ориентировочно около **256**. Silent truncation не является допустимой обработкой oversized input.

Один загруженный model process обслуживает existing BgeQueue. Book и story producers используют тот же embedding space и очередь; у stories своя типизированная PG search projection. Увеличение dimensionality, скрытая quantization или иной model под прежним space запрещены. Изменение representation требует нового material/model contract и quality gate.

Target — PostgreSQL **17** с pinned pgvector рядом с BGE. E5 и Supabase из target исключены. Локальный BGE fallback на DevCoveer не появляется при cold start или outage. Нужный tokenizer-only asset можно использовать на ingestion validation стороне; это не разрешение загружать model weights.

## Запрос и очередь

Notebook держит direct authenticated outbound WSS к existing ASGI broker. Queue/job/claim/result identity одна, transport reconnect не порождает второй dispatcher. Query jobs имеют приоритет перед document jobs; already running bounded encode завершается, следующий document batch получает permit только при сохранении serving reserve.

Ключ embedding cache включает model/space и точный normalized input. Дедупликация encoder job не объединяет разные пользовательские диалоги и не отменяет current scope. Accepted source decisions не воспроизводятся повторным вызовом модели при replay.

Вопрос передаётся без unrelated book titles и длинных instruction wrappers. Book/geo/time restrictions передаются отдельными filters. BGE — retrieval encoder, а source analysis и проверяемое извлечение facts остаются работой вызывающей модели через import contract.

## Готовность

Раздельные состояния: model loaded, DB compatible/restored, vector coverage, actual query readiness, socket health, request result и final delivery. Serving ready требует SQL query и dense query probe с useful evidence; число записанных vectors или heartbeat ready не заменяют probe.

Temporary WSS failure сохраняет тёплый PG/BGE process, пока сама Kaggle session жива. После завершения session необходим новый run, restore и warmup. Useful-idle 30 минут не равен task lease и не продлевается Pong.

Если BGE недоступна, вернуть явный pending/degraded lexical result в общем evidence deadline. Лексический результат не засчитывается как dense PASS. Canonical command outcome сохраняет exact domain IDs/body/evidence, но не зависит от заново вычисленных BGE floats/FTS/crops. При replay projection может стать dirty; её rebuild и actual dense readiness идут отдельно от canonical cursor, accepted BGE artifacts reuse. Уже delivered artifact bytes/hash/receipt не подменяются новыми bytes.

Read-only search не ждёт remote accepted-journal commit; real user turns используют durable request lifecycle отдельно.

## Проверка качества и времени

BGE-only Hit@5 ≥85%, Hit@10 ≥90%, RU→DE Hit@10 ≥85% на ≥30 held-out fact families. BGE-only, lexical-only и configured fusion проверяются отдельно; lexical не prefilter dense candidates. Minimum scale ≥100 book-equivalent sources/100000 active chunks. Warm backend p95≤1 s, MCP p95≤1.5 s, hard evidence deadline2 s.

[Исходный worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/scripts/production/bge_kaggle_worker.py) использует один FP32 CPU model, четыре Torch threads и один interop thread. Это code baseline, не измеренная достаточность совместного PG+BGE envelope. [Прежний документ](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/bge-kaggle.md) сохранён в immutable history.
