# RKB A3a — exact Kaggle Dataset base mount verifier (2026-10-21)

**Status: isolated code and synthetic fixtures only.** This is not a primary-storage implementation, not a live Dataset, not a PostgreSQL restore and not evidence of accepted/Saved durability.

The approved target uses a versioned **Kaggle Dataset as primary persisted PostgreSQL base** and a separate weekly Yandex backup. Existing `PhysicalSnapshotRecovery` and `SourceRestoreConfig` currently reference the restricted S3 recovery store; this new verifier does not silently relabel that S3 flow as the approved Dataset-primary architecture.

## Narrow reusable seam

`regional_knowledge.kaggle_dataset_base` defines `DatasetBasePin` and `verify_mounted_dataset_base`. An authenticated controller must bind the *same exact* owner/slug, Dataset version, notebook version, run ID, database generation and applied sequence before invocation. The notebook's `/kaggle/input/<slug>` path alone does **not** prove its mounted Dataset version.

The verifier uses no provider API, installs nothing, opens no PostgreSQL connection and performs no mutable operation. For a trusted Kaggle input root it checks only three declared direct-child files: canonical native base manifest, canonical source-control manifest, and physical archive. No path traversal, symlink or group/world-writable archive is permitted. Each file must independently match the declared exact size and SHA256; the manifest binds the archive and control hashes, database generation, applied cursor, journal head and catalog hash. On success it returns a readback with the archive path plus exact manifest/control bytes; it does **not** claim PostgreSQL readiness.

The existing `NativeSnapshotFactory.prepare` must subsequently verify the archive hash again, unpack into an exclusive private cluster, run `pg_verifybackup`, validate PG 17/pgvector and system identifier, replay accepted tail with original interpreter and confirm full current writer/search gates. No live PGDATA replacement is allowed.

## Verification

`PYTHONPATH=src:tests pytest -q tests/test_kaggle_dataset_base.py` — 14 synthetic PASS, operation `op_1681e350764f2ee1b207cdb8`. Covers wrong Dataset version, notebook version and run ID; archive/control hash mismatches; symlink/unsafe paths; noncanonical/control/database manifest mismatch. Fake archive is explicitly **not** a PostgreSQL snapshot.

## Unmet prerequisites for integration

1. Real owner-approved private Dataset ref and immutable version + provider attestation of input version bound to the exact notebook execution (not present in this source branch).
2. A published exact PostgreSQL base+control+archive and independent byte/hash/version readback; provider API `COMPLETE` without files is insufficient.
3. A *single* accepted/Saved authority and a version-qualified durable tail covering the interval since that base. Existing always-online S3 cannot be silently used as primary; a weekly Yandex backup alone cannot acknowledge daily accepted mutations.
4. A bounded adapter from the existing controller/WSS `prepare` to this verifier, with native restore/activation proof and replay; preserve writer generation and slot ownership, no parallel scheduler.
5. Live acceptance: exact Dataset version → real PG restore → source/history/BGE readiness → successful authorized evidence search, plus restart/lost-ACK and retained-data verification.

Neither a published file nor this verifier constitutes a complete A3 solution. Current live Dataset identity and accepted tail coverage remain **NOT VERIFIED**; they have been explicitly requested from the coordinator via the shared Slack thread. This source slice does not start, stop or mutate Kaggle/PG.
