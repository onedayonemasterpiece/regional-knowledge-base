# Историческая vector RLS v5: вход в evidence

Статус 20.10.2026: runbook прежнего SQLite-authority / Supabase-vector runtime. Его полный текст, rollout/rollback и измерения доступны в [immutable main a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/vector-rls-v5.md). [SQL migration023](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/sql/023_vector_rls_v5.sql) сохраняет проверяемый механизм.

Новое внедрение выполняется по [единому handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) и [query architecture](../architecture.md): PostgreSQL 17/pinned pgvector+BGE в Kaggle, без E5/Supabase.

Полезный сохраняемый вывод v5: authenticated application actor задаётся сервером transaction-locally; invoker role не bypass RLS; scope не является произвольным user claim. Independent current revision/material-hash и output permission checks остаются обязательными.

В target public accepted-book search нет per-book owner ACL и нет per-query перечисления всех документов в JSON revision map. Private draft/artifact/mutation scopes сохраняются. Индексируемый SQL должен выбирать bounded candidates до hydration, с plan/readback под настоящей application role. Owner-role timing не является acceptance. Наличие SQL function/HNSW не доказывает использование индекса.

Не применять старый v4/v5 переключатель как deploy новой архитектуры и не восстанавливать прежние E5 tables. Перед cutover — [quality/capacity/latency gates](../mass-ingestion-readiness.md); старый RLS report не является новым PASS.
