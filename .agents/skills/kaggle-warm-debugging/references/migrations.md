# Live PostgreSQL changes and replay

Use SQL/stored functions where set-based work, transaction atomicity or reduced round trips justify them. Do not move inference, orchestration or all business logic into stored procedures by default. Compare the query plan and actual end-to-end cost of the affected path, not language labels. Local SQL calls are not automatically a new RPC service.

## Minimal ordered migration

Each immutable migration needs an ID/hash, predecessor, expected starting schema/function revision, resulting revision, execution class and compatible executor/command semantics. Same ID and bytes return the recorded outcome; same ID with different bytes fails. IF NOT EXISTS is not drift detection.

Serialize application under the existing writer authority and a PostgreSQL migration lock. Re-read the ledger under the lock. Validate applied history is the expected prefix, not merely that discovered files are sequential. Bound lock/statement waits so a failed acquisition leaves the old serving path intact.

For transactional DDL, SQL, ledger and schema revision commit together. Preserve exact artifacts and outcomes in the existing remote recovery path where the project requires durable accepted/Saved semantics. A lost commit ACK is UNKNOWN until readback of the ledger in the original target DB generation; querying a newly restored/replaced DB does not settle the old transaction by itself.

Migrations are ordered boundaries in accepted-command history. Before an incompatible v2 change, place a barrier that drains all previously accepted/sealed v1 commands and their dependencies using v1 semantics, including commands still in the undispatched queue, not only in-flight work. Reconcile UNKNOWN outcomes against the original DB generation before crossing that barrier; unresolved old outcomes block the incompatible transition. Recovery must either replay commands with the matching schema/interpreter semantics or use an explicitly verified compatible adapter. Applying all latest migrations and replaying all old commands through latest functions is not generally correct. The same input hash may produce a different effect under function v2 than v1. Keep required migration/interpreter artifacts while retained snapshots/tails depend on them, and reuse an already committed domain outcome instead of recomputing it.

## ABI and nontransactional steps

- CREATE OR REPLACE FUNCTION cannot change the existing return type or argument identity. An argument change may create an overload. Use versioned functions and compatible callers when ABI changes; deliberately preserve privilege, search_path, volatility and parallel semantics. Avoid SECURITY DEFINER unless justified and safely scoped.
- CREATE INDEX CONCURRENTLY runs outside a transaction block and may leave an invalid index. Track a resumable maintenance step and read catalog definition/validity before calling it successful. Name existence is insufficient; an invalid unique index can still enforce uniqueness.
- ALTER TYPE ADD VALUE in a transaction cannot use the new value until commit. Separate dependent data work.
- Postmaster-context settings require PG restart; SQL functions ordinarily do not. Native extension library changes are not ordinary function-body edits and require handling loaded backend libraries.

## Replacement and rollback

For a compatible mixed change, expand schema/API, switch executor, then contract only after old consumers and retained replay dependencies no longer need it. In a disposable fixture, use a small maintenance window and one cluster unless a specific test requires more. Do not impose production recovery ceremony on throwaway data, or apply throwaway semantics to accepted user data.

Before commit, ordinary rollback applies. After commit, reverse only when compatibility and data preservation are established. After new accepted writes, default to a forward compensating migration. A previous bundle can return only if it remains compatible with current schema and accepted-command semantics. Restoring an old snapshot is recovery, not a safe general deployment rollback.

Sources: [CREATE FUNCTION](https://www.postgresql.org/docs/17/sql-createfunction.html), [CREATE INDEX](https://www.postgresql.org/docs/17/sql-createindex.html), [ALTER TYPE](https://www.postgresql.org/docs/17/sql-altertype.html), [pg_settings](https://www.postgresql.org/docs/17/view-pg-settings.html), [C functions](https://www.postgresql.org/docs/17/xfunc-c.html).
