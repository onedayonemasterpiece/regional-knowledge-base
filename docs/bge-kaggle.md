# BGE Kaggle CPU main tier

## Применение в переработке 19.10.2026

Нормативный целевой контракт: [единый файл агенту](prompts/rkb-scalable-kaggle-implementation-20261019.md) и [архитектура](designs/rkb-scalable-kaggle-architecture-20261019.md). Текущий runtime ещё не перенесён этой документационной задачей.

- Один PostgreSQL/pgvector corpus в Kaggle, BGE-only (1024 FP32 baseline, exact pinned BGE input≤512); небольшой durable control journal на DevCoveer.
- Прямой исходящий WSS notebook→existing DevCoveer ASGI с двусторонними заданиями/результатами, durable ACK/RESUME и одной BgeQueue authority. Старые polling/локальный E5/Supabase инструкции не целевой deployment.
- Fresh MCP import книг; перенос только historical/cartographic layers и согласованных зависимостей, со stable IDs/provenance и проверенным reanchor. Ручная работа не уничтожается из-за отказа от старого corpus.
- Source-bound facts/dates/observations сохраняются без обязательной Story; эпизод/досье/Variant/private draft/independent case различаются. Один persisted research workflow, bounded compose, без квот и выдуманной драматургии.
- Поиск accepted active book text и bibliography без per-book owner ACL; current withdrawal/revision, pixels/original/private draft grants и delivery audience остаются обязательными.
- One canonical person/request/dialog, fair admission и confirmed result/hash ACK; Мира в PH расширяет existing typed adapter, не создаёт отдельный voice/IAM/queue.

Численные protected gates сохранены: backend p95≤1000ms, MCP p95≤1500ms/hard2000ms, BGE Hit@5≥85%/@10≥90%, RU→DE@10≥85%, ≥30 held-out families, ≥100 sources/100000 chunks, storage≤80% steady/≤90% peak. Документ не является доказательством PASS.

## Граница старого runbook

Ниже — runbook исходного runtime до cutover, а не инструкция устанавливать прежнюю архитектуру заново. Не удалять shared runtime files/данные до проверенного переключения. Новое восстановление использует schema/codec compatibility, exact command/result hashes, snapshot+tail; TTL accepted jobs не может удалить данные незавершённого consumer. 10-секундная политика waiting notice не увеличивает 2000ms evidence deadline.

### Исходный runtime

E5 remains a separate always-ready service. BGE inference runs only in a private
Kaggle CPU notebook. The worker polls `/bge-worker/{heartbeat,claim,complete}` with
a short-lived run-bound credential; it receives neither database nor object-store
credentials. The broker accepts a fixed embedding protocol, not executable jobs.

The durable SQLite queue lives at the configured `RKB_BGE_QUEUE_PATH` in private
runtime state outside Kaggle. Immediate transactions fence concurrent starts,
claims and late results. Jobs are idempotent per authorized actor and request key;
query jobs precede one-passage document jobs. Source bytes must be authorized and
hash-verified before document submission, and revisions rechecked before storage.

The useful-work lease is 30 minutes; heartbeat/maintenance does not renew it.
Continuous demand starts one warming successor at 10h45m; the previous worker
drains only an already claimed job. Run credentials expire before 11 hours and
late results cannot overwrite reclaimed jobs. Unfinished jobs stay durable across
lost workers. Private generated notebook source contains the run credential and
is restricted to runtime state; do not copy it into reports or public Git.

Pinned dense contract: BAAI/bge-m3 revision
`5617a9f61b028005a4858fdac845db406aefb181`; torch2.11.0+cpu,
transformers5.16.1, tokenizers0.23.1, huggingface-hub1.29.0. Query and document use
no prefixes, batch1, max512 tokens, first-token CLS pooling, FP32 L2 normalization,
1024 dimensions. Space: `bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1`.
These versions are checked before readiness. Model files are downloaded only in
Kaggle at the pinned commit; no inference package/model is installed in MCP.

For ingestion validation only, the tokenizer JSON from that exact pinned BGE
revision is provisioned on the DevCoveer host outside Git and verified against
SHA-256 `21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08`.
The isolated E5 sidecar uses it only for untruncated token counting; BGE inference
and model weights remain in the private worker.

`011_bge_rankings.sql` adds a separate typed vector table and RLS-aware ranking
RPC. It returns branch/rank diagnostics; fusion never adds raw cross-space cosine
scores. Alias phrases remain a separate exact lexical signal supplied explicitly
by an authorized caller, never silently appended to an embedding query. They are
candidate identity signals, not proof of POI equivalence or automatic merges.

Disable consumers before applying `011_bge_rankings.rollback.sql`; it removes
only BGE storage/ranking RPC. E5 and legacy data remain. Actual deployment,
multilingual ablations, lifecycle acceptance and the chosen warm path must be
recorded in the dated acceptance report before claiming this tier complete.