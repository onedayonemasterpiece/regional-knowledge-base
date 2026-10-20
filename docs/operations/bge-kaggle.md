# Эксплуатация Kaggle PG/BGE и прямого WSS

Обновлено 20.10.2026. Целевой runbook для [единого handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Это порядок проверки реализации, не отчёт о выполненном запуске.

## Startup и attachment

1. Existing controller выбирает один writer run и выдаёт scoped run/epoch authority. Existing BgeQueue сохраняет job IDs, claims и result hashes.
2. Notebook восстанавливает exact verified private Dataset snapshot в PostgreSQL 17 с pinned pgvector, проверяет manifest/extensions/internal cursor и replay remotely accepted tail.
3. Загружает pinned BGE-M3: 1024 FP32, input≤512 по exact tokenizer. Один process, без E5 и без model на DevCoveer.
4. Открывает outbound WSS к existing DevCoveer ASGI/HTTPS. HTTP pull и WSS не dispatch одну работу параллельно.
5. Проверяет actual SQL и dense query; только после readback принимает serving readiness. Snapshot upload, process liveness и vector counts недостаточны.

Database port остаётся внутри notebook. Credentials не входят в snapshot или public notebook output. Large files/accepted payloads передаются bounded byte transport по authorized refs.

## Reconnect и защита от повторов

При socket drop тот же живой notebook держит PG/model, сохраняет bounded inflight results и повторяет handshake с jitter/backoff. RESUME сопоставляет job/claim/generation/result hashes. Same operation + same payload возвращает прежний outcome; different payload под тем же ключом — conflict.

Новый attachment получает connection generation и fencing старого socket. Stale claim завершает соответствующее задание, а не убивает warmed worker. Revoked run больше не принимает новые authority-bound effects. Уже remote-accepted commands прошлого epoch остаются replayable.

Controller не запускает kernels_push только из-за потери socket. Когда Kaggle session закончилась, сохранение RAM невозможно: новый run выполняет restore/warmup. Unknown launch reconciles по существующим launch IDs; не создавать серию notebook по timeout одной попытки.

## Таймеры и ресурсные границы

Useful-idle — 30 минут от реального спроса. Writer lease, task claim, heartbeat/Ping, stage progress, source-cache idle и provider lifetime независимы. Keepalive не делает notebook вечным и не продлевает полезный спрос.

Frames, total buffers, inflight credits, handshake/send deadlines и retry windows ограничены. CPU encode, render, blocking DB/file/network operations вынесены из event loop. Control reads не используют BEGIN IMMEDIATE. После commit сигнал wakeup виден другим process; bounded reconciliation восстанавливает пропущенные notifications.

Query-first действует на общей queue boundary для всех producers. Import/story encode и PG maintenance учитывают общий CPU/RAM/IO envelope; защищён минимум один search reserve. Не ждать permit внутри SQL write transaction.

## Что сохраняется до ACK

Raw DevCoveer spool: implementation defaults 4 часа/512 MiB с измерением до cutover. Early receipt pending. Final accepted/Saved требует remotely confirmed exact intent/payload в existing Yandex recovery storage; Saved также требует domain commit и remotely verified exact outcome/receipt. Ready groups ждут flush ≤1 секунды: это окно набора, не гарантированный cloud RTT. Read-only search не проходит через remote journal.

Existing S3ObjectStore — put/get/delete transport, а seal/continuous manifest/CAS ещё реализуются и проверяются. Большие base bytes идут direct Kaggle→recovery object по scoped key/epoch/expiry без global/delete credentials; Dev 512 MiB spool их не пропускает. Presigned URL не отменяется сам при epoch change: old-writer upload остаётся orphan без promotion authority.

Canonical replay не хеширует заново derived BGE floats/FTS/crops. Exact domain outcome сохраняется, projection dirty/rebuild и dense readiness идут отдельно; accepted BGE artifacts reuse. Уже delivered artifact bytes/hash/receipt не подменяются.

Control checkpoint 15 min; changed PG base 4 h или раньше по replay/byte pressure/planned handover; daily independent-from-Kaggle Yandex base. Local raw GC после remote seal не ждёт old daily baseline; remote tail остаётся для designated recovery chains и unfinished consumers. UNKNOWN/request/receipt metadata не expires вместе с raw body.

## Отчёт readiness и recovery

Показывать отдельно transport state, model revision/readiness, database schema/cursor, query probe, active passage/story coverage, effective retrieval mode, replay backlog и last remotely sealed seq. Public status не раскрывает credentials/private text/чужие drafts.

Проверки включают socket drop живого run, kill завершённой session с восстановлением, late duplicate result, wrong generation, duplicate connection, lost ACK, failed remote seal, failed Dataset processing и restore без нужного WAL. Для release действуют [protected gates](../mass-ingestion-readiness.md); качественный ответ в 2 s нельзя заменить быстрым enqueue.

[Прежние pull/claim/complete deployment details](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/bge-kaggle.md) — исходный runbook. Новое развёртывание следует этому контракту и handoff, не восстанавливает прежний E5/Supabase runtime.
