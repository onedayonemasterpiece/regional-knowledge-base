# Read-only controller / notebook / PostgreSQL health

The owner requires progress toward full PDF/DjVu, actual pixels, facts,
Story.body, continuation and durable book acceptance. Health diagnoses that
path; it does not replace import or retrieval acceptance.

This owned adapter supplies two concrete callables, without changing shared
registration, runtime, WSS, authorization, pools or service configuration:

- `collect_health(queue=existing_queue,native_probe=bound_probe,controller_state=...)`
  reads only explicit safe control/run columns and the existing verified
  `current_binding`. It never calls `queue.status`, `maintenance` or
  `request_startup`, and cannot auto-start a cold notebook. Controller process
  liveness is separate and explicitly `unknown` unless a trusted existing
  controller observer supplies it. Notebook status is the control projection,
  not a fresh Kaggle provider status request.
- `probe_database_health(acquire=existing_pool.connection,binding=...,binding_current=...)`
  runs one fixed bounded SELECT of `recovery_state` / `read_frontier` in a
  REPEATABLE READ READ ONLY transaction, with statement/lock deadlines and a
  two-row ambiguity bound. It reuses the native probe's existing transaction
  idiom, not its large private catalog scan. No corpus, token, private catalog,
  original file or model access occurs. No new pool or database is constructed.

The response separates controller state, exact owned run/version/incarnation,
attachment generation, heartbeat age, actual SQL response, schema, applied and
projection sequences, pending state and elapsed time. A READY queue row is
insufficient to claim SQL response. A SQL response with wrong generation,
projection lag, pending write or insufficient sequence is insufficient to claim
`read_ready`. Attachment changes before/after awaits and transaction release
fail closed. Exceptions use fixed codes; cancellation propagates.

Root integration hooks: the existing `server.py:startup_status` registration
calls `KaggleSourceBackend.startup_status` under current OAuth authority. The
integration owner must call this adapter only AFTER that route's existing
authorization, validate the original request/reply identity over the existing
WSS read path, and recheck current authority before final issuance. Native
runtime supplies its existing reader-pool acquisition; Dev never connects to
Kaggle's PG socket. Root owns any typed protocol/result union, dispatch handler,
route response DTO and private grants. This leaf creates no second public
tool or transport and grants no authorization by possession of a binding.

Tests use the real queue/runtime-binding and OAuth fixtures plus synthetic
native I/O. A conditional native SQL case uses only the existing approved
GitHub CI PG17 fixture; absent that fixture it must report SKIP. No Dev PG,
model, real credentials, Kaggle push, service restart or deployment occurs.
This leaf is an integration input, not an installed MCP health endpoint.

Public receipt follow-up: notebook observations explicitly identify their source
as `controller_queue`; they expose only status, launch state, provider version,
connection generation and heartbeat age. Provider references, run IDs and runtime
incarnations remain internal. The SQL receipt similarly sanitizes its public
binding after full identity comparisons. Callback readiness also requires a
complete schema/sequence/pending observation, not a bare `sql_response=True`.
The native WSS request hook and runtime rollout remain integration-owner work;
queue observations alone never imply a successful SQL probe.

The integrated phase1 heartbeat hook is retained. Its observation read now
rechecks the exact current attachment and accepted generation before returning
metadata; a disconnect between the queue snapshot and runtime-row read suppresses
the PostgreSQL observation with `attachment_changed`. This is still explicitly
a heartbeat observation, not a fresh SQL response.

`kaggle_health_native.py` contains the same fixed read-only SQL probe without
controller/MCP imports. Its caller supplies the existing acquisition, expected
database generation/minimum sequence and a current authenticated lease callback.
It returns sanitized observation fields, not a new identity or capability.
The controller wrapper retains the exact existing TargetReadBinding and DTO.
The integration owner adds this single module to the existing native bundle and
dispatches through the existing authenticated WSS handler; no new pool or route
is introduced. A fresh-process import check blocks controller/MCP modules, and
server-side statement cancellation maps to the fixed `pg_timeout` reason.
