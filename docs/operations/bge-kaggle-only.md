# Обязательная граница BGE-only / Kaggle-only

Обновлено 20.10.2026. Целевое правило [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md); документ не выполняет deploy или удаление runtime.

1. Весь BGE-M3 inference, query и document/story, работает только в Kaggle. На DevCoveer нет BGE weights, ONNX/model service, installer fallback или локального encoder.
2. В target нет E5, его indexes/tokenizer gates и fast fallback. Сохраняются самостоятельно нужные shared dependencies и SHA-pinned BGE tokenizer-only assets.
3. Pinned BGE revision **5617a9f61b028005a4858fdac845db406aefb181**, 1024 FP32, exact full input≤512 tokens. Иной model/precision/space вводится только через measured change gate.
4. Один PostgreSQL 17/pinned pgvector в Kaggle и один BgeQueue/controller. Direct outbound WSS использует существующую queue authority, не новый broker или отдельную story model.
5. Temporary socket reconnect сохраняет уже загруженный model/PG только внутри живой Kaggle session. Завершение session требует restore/warmup; masking этого события как «быстрый reconnect» недопустим.
6. Serving ready означает actual SQL+dense query probe, совместимый cursor и current material hash. Vector coverage, health HTTP200, Pong и сохранённый job не подтверждают готовый semantic result.
7. Текущая недоступность BGE возвращает честный bounded pending/lexical degradation. Она не разрешает скачать модель на DevCoveer и не считается dense quality PASS.

## Release readback

Проверить actual deployed SHA и pins, единственный writer/attachment, отсутствие target E5/local-BGE code/config/service/model artifacts, correct PG corpus adapter, current passage/story vectors и естественные held-out queries. Удаление shared каталога по одному слову в имени запрещено; перед retirement проверить владельцев files/services.

Не менять произвольные эксперименты других проектов или чужие systemd services. Scope cleanup относится к текущему RKB deployment и его связанным dependencies. [Историческое удаление local BGE и прежний E5 boundary](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/bge-kaggle-only.md) остаются evidence прежнего этапа, не instruction сохранить E5 сейчас.

Качество/latency/capacity: [mass readiness](../mass-ingestion-readiness.md). Recovery и raw spool: [storage](../storage.md). Local raw GC не удаляет unresolved request/UNKNOWN metadata и не заменяет проверку remotely sealed exact payload.
