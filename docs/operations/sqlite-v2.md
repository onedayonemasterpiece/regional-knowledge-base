# Исторический SQLite v2 / Supabase vector plane

Статус 20.10.2026: historical recovery entrypoint для прежнего runtime. Полные команды, migration/retirement evidence, cache/proof details и прежние измерения сохранены в [immutable main a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/sqlite-v2.md). Они не являются действующим планом установки target.

Актуальное [единое задание](../prompts/rkb-scalable-kaggle-implementation-20261019.md): PostgreSQL 17/pinned pgvector+BGE в Kaggle, existing DevCoveer controller/MCP и короткий raw spool, existing Yandex exact recovery tail. E5/Supabase в target нет. [Storage](../storage.md) описывает новые ACK, checkpoints, local/remote GC и source caches.

Прежний corpus snapshot не загружается в новый target как импорт книг. Переносится только geo с необходимыми минимальными source references; всё негеографическое — fresh import/workflow. Старые IDs/hash/page refs для geo не доказывают reanchor к новому изданию.

Полезные прежние инварианты сохраняются: exact source text отдельно от search material, atomic active revision, bounded candidate hydration, idempotent source/revision/hash checks, scoped real-pixel proof и resumable original download. Новый PG corpus adapter должен покрывать эти функции.

Rollback после новых target accepted writes сохраняет remote tail и compatible replay. Нельзя восстановить старую целую SQLite поверх новых вкладов или повторно выполнить historic retirement script без отдельной проверенной необходимости.
