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
  dependency/schema SHA + command handler closure. In a COLD restore the
  target sets `initialized_receipt=None` **only with the explicit typed
  recovery owner**; the new process cannot possess a PG initialization receipt
  before performing native restore. Existing genuinely initialized releases
  may retain an independently sourced matching receipt. The required actual
  `VerifiedInitializedDatabase` capability is created only by successful
  native prepare/finish/verify and rechecked before PG pool/readiness; no
  caller-supplied metadata can substitute for that live capability.
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

## Stage0 signed-link lifetime before a new intent

An immutable artifact hash does not keep its signed download URL alive. The
installed native package and manifest links expired on 2026-10-20, so reusing
the old target unchanged would fail before native restore. The controller-side
`kaggle_stage0_urls` helper renews explicitly listed path-style S3 objects using
an existing service client. Its `PinnedStage0Object` inventory fixes origin,
bucket, key, artifact name, size and SHA256; it validates the entire old plan
before signing, requests GET only and rejects changed destinations, malformed
signatures or insufficient validity. Fresh links and unrelated URLs are reused.

Use `renew_target_stage0_urls` **before** generating source or persisting runtime
expectations for a new attempt. It preserves every other target field and all
artifact/package pins. Already prepared expectations and UNKNOWN launches are
immutable: never re-sign and overwrite their intent. Signed URLs stay in private
runtime configuration, outside source, logs and receipts. The helper performs no
download, credential discovery, queue mutation or Kaggle launch. Its integration
into the controller lifecycle is a separate source change; helper tests alone
do not establish automatic production renewal.

## Dataset availability and mounted permissions

Keep the Dataset slug stable and use its built-in versions. After creating or
updating a version, wait for provider readiness and verify that exact version's
file inventory and metadata before launching a consumer; publication and launch
are separate steps. An already running notebook keeps its mounted version.
Kaggle input directories can retain permissive Unix mode bits on a read-only
filesystem. The verifier checks `fstatvfs(ST_RDONLY)` on opened descriptors before
rejecting those bits; size, SHA-256, version binding and no-follow checks still
apply. A failed native prepare wakes its initialization waiter with the original
error. A finish interrupted by detach retains its existing reconciliation path.
