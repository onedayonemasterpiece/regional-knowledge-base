# Native SQL health on the existing admitted read channel

This source leaf follows the coordinator's sequencing after Linza's dispatch
handoff. Dependencies: PR234@fd53613169e5cddc2231854dc3ffe1435497921f,
PR239@910a3aa26999ff6945f679fdc64645f3962feab5, and
PR249@fd7d564830178d7c71e54a0cd74f35591146483b. The READY proof-epoch fix and
dispatch fastpath are preserved, not reimplemented.

The existing authenticated startup status continuation now requests SQL health
when the existing target is ready. Cold/pending status retains the explicitly
labelled heartbeat observation and never allocates a startup or reader pool.
The ready health request uses the existing OAuth authority, fair shared
admission, run/connection/turn/compute identity, sender validation and final
authority recheck. It has no caller-supplied SQL, corpus query or BGE inference.

`SQL_HEALTH`/`SQL_HEALTH_RESULT` are strict bounded internal protocol models.
The worker calls the already reviewed native-only `probe_sql_health` in
`kaggle_database_probe.py`, using the same pool acquire callback. Its fixed
read-only repeatable-read transaction observes schema/generation/cursors/pending
state. The WSS transport supplies an actual callback checking ownership of its
current read resource and connection; the probe checks that callback before
and after awaits, including transaction/pool release. Missing lease callbacks,
generation mismatch, incomplete metadata, timeout or stale lease cannot become
successful readiness. The controller independently rechecks its exact binding.

The worker never imports the controller-side health/public-backend/MCP graph
merely for this SQL probe. It uses no new module in the native source inventory,
new pool, listener, credentials, service principal or scheduler. The public
health DTO retains sanitized binding fields and explicit SQL observation;
run IDs, incarnation, tokens/claims and provider URLs are not returned.

Changed shared hunks are limited to health codec/admission/result routing,
worker SQL dispatch and the startup_status health hook. The Linza
dispatch/promote hunk, Rif illustration_fetch/read_issuance/native media code,
Quartz expert server.main composition and import/SQL migration paths are not
modified by this leaf. This is a source-only branch, not a canonical merge,
service restart, notebook submission or live acceptance.

Validation is GitHub-only: real protocol/worker/transport/admission/controller
classes and real ASGI WSS with synthetic OAuth and native I/O fixtures. Tests
cover complete response, no tokenizer/inference, missing/stale lease,
generation/pending/projection failures, resource/connection callback fencing,
actual WSS roundtrip and the existing public status output. Existing native
health PostgreSQL fixture remains the SQL-semantics check. No tests, builds,
installs, PG or models run on Dev. CI results must be read before claiming PASS.

Requirements remain the full v4.1, owner alignment and complete acceptance at
bd138b2449f68cabb2cbfa3df552df8471f40c78, tracked by PR238. A ready SQL probe is
not real-book import/Saved, media pixels or product-wide acceptance.
