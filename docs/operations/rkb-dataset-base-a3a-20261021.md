# RKB A3a — exact Kaggle Dataset base mount verifier (2026-10-21)

**Status: isolated code and synthetic fixtures only.** This is not a primary-storage implementation, not a live Dataset, not a PostgreSQL restore and not evidence of accepted/Saved durability.

The approved target uses a versioned **Kaggle Dataset as primary persisted PostgreSQL base** and a separate weekly Yandex backup. Existing `PhysicalSnapshotRecovery` and `SourceRestoreConfig` currently reference the restricted S3 recovery store; this new verifier does not silently relabel that S3 flow as the approved Dataset-primary architecture.

## Narrow reusable seam

`regional_knowledge.kaggle_dataset_base` defines `DatasetBasePin` and `verify_mounted_dataset_base`. An authenticated controller must bind the *same exact* owner/slug, Dataset version, notebook version, run ID, database generation and applied sequence before invocation. The notebook's `/kaggle/input/<slug>` path alone does **not** prove its mounted Dataset version.

The verifier uses no provider API, installs nothing, opens no PostgreSQL connection and performs no mutable operation. For a trusted Kaggle input root it checks only three declared direct-child files: canonical native base manifest, canonical source-control manifest, and physical archive. No path traversal, symlink or group/world-writable archive is permitted. Each file must independently match the declared exact size and SHA256; the manifest binds the archive and control hashes, database generation, applied cursor, journal head and catalog hash. On success it returns a readback with the archive path plus exact manifest/control bytes; it does **not** claim PostgreSQL readiness.

The new narrow `NativeSnapshotFactory.prepare_dataset_base` method checks the actual current WSS native binding, dataset pin against database generation/worker run, and fully pinned native PG/BGE versions *before* handing verified bytes to the **existing** `prepare` operation under the same idempotent `operation_key`. That existing native operation independently rehashes the archive, unpacks into an exclusive private cluster, runs `pg_verifybackup`, validates PG 17/pgvector and system identifier, and later participates in accepted-tail replay and full writer/search gates. The source bundle explicitly includes the verifier module. No live PGDATA replacement is allowed. **The controller/WSS has not yet been wired to call this method**, so this is an internal source seam rather than a functioning Kaggle Dataset restore flow.

## Verification

`PYTHONPATH=src:tests pytest -q tests/test_kaggle_dataset_base.py tests/test_kaggle_generated_restore.py tests/test_kaggle_target_runtime.py tests/test_kaggle_launch_envelope.py` — **60 synthetic PASS**, operation `op_546451c13c68f5f88620e268`. Includes **16** Dataset-specific fixtures covering wrong Dataset version, notebook version and run ID; archive/control hash mismatches; symlink/unsafe paths; noncanonical/control/database manifest mismatch; exact native factory `prepare` argument/bytes forwarding; missing or incompatible provider/model pins must refuse before the factory starts any native PG work; and exact source-bundle inclusion. The synthetic native factory is stubbed and the fake archive is explicitly **not** a PostgreSQL snapshot.

## Unmet prerequisites for integration

1. Real owner-approved private Dataset ref and immutable version + provider attestation of input version bound to the exact notebook execution (not present in this source branch).
2. A published exact PostgreSQL base+control+archive and independent byte/hash/version readback; provider API `COMPLETE` without files is insufficient.
3. A *single* accepted/Saved authority and a version-qualified durable tail covering the interval since that base. Existing always-online S3 cannot be silently used as primary; a weekly Yandex backup alone cannot acknowledge daily accepted mutations.
4. A bounded adapter from the existing controller/WSS `prepare` to this verifier, with native restore/activation proof and replay; preserve writer generation and slot ownership, no parallel scheduler.
5. Live acceptance: exact Dataset version → real PG restore → source/history/BGE readiness → successful authorized evidence search, plus restart/lost-ACK and retained-data verification.

Neither a published file nor this verifier constitutes a complete A3 solution. Current live Dataset identity and accepted tail coverage remain **NOT VERIFIED**; they have been explicitly requested from the coordinator via the shared Slack thread. This source slice does not start, stop or mutate Kaggle/PG.
