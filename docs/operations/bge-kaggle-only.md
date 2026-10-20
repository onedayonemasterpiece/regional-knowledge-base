# Обязательная граница BGE-only / Kaggle-only

Обновлено 20.10.2026. Целевое правило [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md); документ не выполняет deploy или удаление runtime.

1. Весь BGE-M3 inference, query и document/story, работает только в Kaggle. На DevCoveer нет BGE weights, ONNX/model service, installer fallback или локального encoder.
2. В target нет E5, его indexes/tokenizer gates и fast fallback. Сохраняются самостоятельно нужные shared dependencies и SHA-pinned BGE tokenizer-only assets.
3. Pinned BGE revision **5617a9f61b028005a4858fdac845db406aefb181**, 1024 FP32, exact full input≤512 tokens. Иной model/precision/space вводится только через measured change gate.
4. Один PostgreSQL 17/pinned pgvector в Kaggle и один BgeQueue/controller. Fused QUERY выполняет local encode + dense/FTS/alias SQL + RRF + batch hydrate; Dev получает evidence/bibliography, не vector round trip. Direct outbound WSS использует общую auth/admission authority; public Kaggle listener, второй broker/model и parallel dispatcher не нужны. Dot → RKB и Mira → RKB независимы, Mira не проходит через Dot.
5. Temporary socket reconnect сохраняет уже загруженный model/PG только внутри живой Kaggle session. Завершение session требует restore/warmup; masking этого события как «быстрый reconnect» недопустим.
6. Serving ready означает actual SQL+dense query probe, совместимый cursor и current material hash. Vector coverage, health HTTP200, Pong и сохранённый job не подтверждают готовый semantic result.
7. Текущая недоступность BGE возвращает для ephemeral QUERY честный bounded degraded/unavailable result без durable resume; pending относится к явно выбранному durable request. Это не разрешение скачать модель на DevCoveer и не dense quality PASS.
8. Default — Python bootstrap/glue и существующая authority, один resident PyTorch FP32 process, native PG kernels и shared CPU budget. Нет per-request spawn/compile/model load. Precompiled Go/Rust допустимы как узкая замена transport/hot function либо компонента с воспроизводимой проблемой памяти/lifecycle/backpressure; для нового компонента эти требования могут обосновать native выбор до CPU hot loop, а проверенный reuse не требует полного Python дубля. Go на DevCoveer заменяет прежний компонент, не добавляет proxy/authority; native sidecar не является default, полный rewrite не нужен. Принять один вариант только при подтверждённом улучшении полезной задержки/ресурсов/отказов без регрессии quality/прав/recovery; local IPC допустим с учётом copies/serialization/Python BGE child/lifecycle и pinned bundle/ABI/protocol. ONNX/GPU/quantization — отдельный quality track, actual tokenizer backend проверяется.
9. QUERY/QUERY_RESULT отделён от durable DISPATCH/RESULT/ACK. Один read permit/search reserve, общий deadline/generation/min_data_seq; read audit асинхронен и не задерживает request/response. Remote accepted/Saved/replay остаётся обязательным для соответствующих mutations/jobs.

## Release readback

Проверить actual deployed SHA и pins, единственный writer/attachment, отсутствие target E5/local-BGE code/config/service/model artifacts, correct PG corpus adapter, current passage/story vectors и естественные held-out queries. Удаление shared каталога по одному слову в имени запрещено; перед retirement проверить владельцев files/services.

Не менять произвольные эксперименты других проектов или чужие systemd services. Scope cleanup относится к текущему RKB deployment и его связанным dependencies. [Историческое удаление local BGE и прежний E5 boundary](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/bge-kaggle-only.md) остаются evidence прежнего этапа, не instruction сохранить E5 сейчас.

Качество/latency/capacity: [mass readiness](../mass-ingestion-readiness.md). Recovery и raw spool: [storage](../storage.md). Local raw GC не удаляет unresolved request/UNKNOWN metadata и не заменяет проверку remotely sealed exact payload.
