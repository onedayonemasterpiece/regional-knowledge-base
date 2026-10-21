# RKB A — переход после первого snapshot: genesis → verified restore

Status 2026-10-21: **actual installed prelaunch failure confirmed by authorized
operator; source/fixture transition verified; real restore/production read NOT RUN.**

## Observed current installation (sanitized)
The running RKB controller uses yesterday's installed release
`6f2a59331ac4e7e799ad1f13edc023cdd2f2d16f`, first-drill wrapper, with
`database.mode=NEW_EMPTY_GENERATION`, `bootstrap_only=true`,
`recovery.drill_empty=true`. Its existing RecoveryJournal now has an accepted
nonempty head. The original `persist_empty_intent` correctly refuses to
initialize another empty generation and emits
`accepted history requires verified restore; initialization refused`.
This is the proven pre-provider `launch_preparation` failure. The installed
snapshot pointer was verified yesterday but the *current* accepted base/tail
and exact execution terminal require fresh independent proof.

## Existing source path; do not create another launcher
The existing `TargetLaunchSpec`, `SnapshotRecoveryConfig`,
`SourceRestoreConfig`, `SourceRestoreCoordinator`, `RecoveryJournal`,
`PhysicalSnapshotRecovery` and WSS `RecoveryPhaseDispatch` are the only
permitted authors of the transition. The typed source check
`preview_target_history` reads the exact journal head and does **not**
launch Kaggle, create a queue run, open PostgreSQL or authorize readiness.

**Required independent inputs:**
- Exact current immutable snapshot manifest `ObjectRef`: key, SHA256,
  length and last accepted sequence, with current pointer readback and
  native control receipt. A candidate pointer is not evidence of currency.
- Existing journal stream, exact current verified head, contiguous ancestry
  from the base to every accepted command/outcome, no unreconciled UNKNOWN
  publication; preserve logical `database_generation` and every accepted ID.
- Actual PostgreSQL 17/pgvector, model/tokenizer, schema/function/executor,
  catalog/ACL and Stage0/native bundle pins, plus an authorized writer rollover
  generation. Reuse the existing immutable manifest and retained original
  replay interpreters; do not re-infer accepted model choices.
- Exact old Kaggle execution identity and an **execution-qualified terminal
  proof**, not merely TTL, stale WSS, a different notebook source or
  version-only `COMPLETE`. The old `dispatching/dispatched UNKNOWN` rows
  stay fenced until proven.

**Target contract after independent verification:**
- `OwnedDatabase.mode='VERIFIED_INITIALIZED'` for the **same** logical DB,
  with target-specific writer generation and existing PG socket. This
  descriptor has no `NEW_EMPTY_GENERATION` cluster/uid/gid/native_paths.
- `TargetLaunchSpec.bootstrap_only=false`,
  `SnapshotRecoveryConfig.drill_empty=false`, exact native bundle + Stage0 +
  dependency/schema SHA + command handler closure. Its
  `initialized_receipt` binds the *same* DB wire/stage0/schema, but is only
  launch metadata, **not** a live PG capability.
- DevCoveer source service: `source_snapshot=None`,
  `source_restore=SourceRestoreConfig(manifest=<exact verified ObjectRef>,
  operation_key=<stable original restore key>,
  versions=<native/catalog/model/tokenizer/schema pins>,
  launch_config_sha256=<exact generated config>,
  allowed_origins=<retained approved source storage origins>)`.
  Continue to use its one canonical `QueueWriterAuthority`, stream, spool,
  participant ACL and recovery adapter.
- After ATTACH, current native PG restore and exact accepted-tail replay,
  `SourceRestoreCoordinator.refresh` performs
  `writer.begin_restore → reconcile → physical.recover →
  writer.activate_recovered`. Only fresh native/source-history checks,
  `VerifiedInitializedDatabase` and current WSS/PG/BGE proofs can return
  search readiness.

**No-go conditions:** missing/mismatched snapshot manifest, unknown current
head/accepted tail, missing old provider execution terminal proof, wrong
writer/corpus generation, copied receipt without native verification,
`drill_empty=true`, any attempt to clear journal, or inability to prove a
retained original executor. An S3 snapshot may be a verified historical
restore artifact, but does not by itself satisfy the approved long-term
Kaggle Dataset primary + independent weekly Yandex backup design.

## Reproducible no-provider evidence
`PYTHONPATH=src:tests pytest -q tests/test_rkb_controller_target_preflight.py
tests/test_kaggle_target_runtime.py tests/test_kaggle_generated_restore.py
tests/test_kaggle_service_startup.py tests/test_queue_restore_writer.py`

Synthetic nonempty accepted journal → new genesis refused unchanged; empty
journal → genesis eligibility only; valid typed verified restore metadata →
generated notebook source with no empty-genesis intent, same accepted head,
and no readiness claim until native verified capability. The reference
tests never call Kaggle, mutate the installed queue or make a live PG backup.

**Product acceptance still outstanding:** authorized normal MCP read should
create/reuse one cold demand; actual controlled Kaggle launch using current
verified restore mode → snapshot + accepted tail → native PG/BGE/WSS ready →
useful source-backed search result with lineage, no double push or lost Saved.
