# Native intake: ordinal vs verified Dataset checkpoint boundary (2026-10-22)

**Status: isolated compatible candidate CI39759312648 passed9 tests, including all5 new boundary selectors. No production fix, live Dataset or Kaggle acceptance.**

## Scope and mandatory product contract

Sources: [implementation §6/§7](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md), [owner-alignment](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/designs/rkb-owner-alignment-20261021.md), [acceptance](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/testing/rkb-scalable-kaggle-acceptance-20261019.md).

Exact remote intent/sequence/actor/payload must precede any consequential external effect. A locally `APPLIED` PG/domain outcome with `persistence_pending` is **not** remotely verified `Saved`. Final accepted source and persisted research continuation require exact replayable, remotely covered outcomes. Telegram sends require **separate remotely covered send intent**, stable native ID/reconciliation, no blind resend after UNKNOWN. Full PDF/DjVu pixels, figures, facts, Story.body and reachable continuation cannot be removed to make this test pass.

Immutable tested target `handoff:src_e58a4fd12145226948c3a380`: base `bd138b2449f68cabb2cbfa3df552df8471f40c78`, 85 staged integration files, **zero unstaged/untracked**, workspace digest `a3ef599be26f85c709a79e9c43acc9d94c93f2a650817c7c8e21493435ba2323`. Frozen `kaggle_native_source_intake.py` SHA256 `122c12bf5a9b7c1daeec2fa98b0f0ac30145e268ec6c8e110e73dea381d974da`; frozen `test_kaggle_native_source_importer.py` SHA256 `ec488f992bf911bdd09324f06ae9eaeeceecfbfa9e9797805b9ca373c3470be4`. Both belong to integration owner, never edited.

## Real source callsites / meaning of a "checkpoint"

- `NativeSourceIntake._drive`: if `entry.outcome_state == 'DURABLE'` then `_settle`; otherwise remains pending, enqueues exact existing command. A native apply receipt (`apply_state='APPLIED'`) **does not** advance `next_ordinal`. `_settle(materialize)` validates exact ordinal, total_inputs and knowledge SHA, then increments `next_ordinal`. `finalize` stages exactly **one** new `source.materialize.v1` per poll (or separate `source.activate.v1` at the end). `finalized` requires the activation result itself to be DURABLE.
- `ControlSpool.record_apply` records the native PG apply receipt; `ControlSpool.publish_outcome` + `RecoveryJournal.record_outcome` require verifiable remote receipt to change `outcome_state` to `DURABLE`. With `DatasetRecoveryStore` and uncovered bytes the journal raises `JournalUnknown('outcome_pending_dataset_checkpoint')`. Do not confuse local `accepted` state with remotely covered provider-effect intent.
- `DatasetRecoveryStore.promote` marks *only exact bytes contained in the native-readback-verified Dataset version* remotely covered; writes after capture remain pending. `SourceDatasetCheckpoints.refresh` can capture **multiple already accepted/APPLIED outcomes** in a drained work-cycle version. Thus the Dataset coordinator **does not universally require a separate version for each command**.
- But the current **serial native intake** cannot create ordinal N+1 while ordinal N lacks remotely DURABLE outcome. With one accepted stage batch and no separate previously covered writes, there are **N+3 successive logical durability barriers**: stage, seal, N materialize ordinals, activate. Two inputs = **5**, three inputs = **6**. In Dataset-primary routing those sequential transitions may force 5/6 separate verified checkpoint promotions, though **no actual 5/6 provider version calls were observed**. This is a **source-derived serial coverage bound**, not a universal physical Dataset-version count nor a latency benchmark. Original-intake bytes/rights and other checkpoints are additional.

## Actual authorized executable regression receipt — unchanged frozen source

Read-only pytest command (real existing `NativeSourceIntake`, `KaggleSourceImporter`, `ControlSpool`, `RecoveryJournal`, `DatasetRecoveryStore`, `SourceDatasetCheckpoints`; only synthetic native/provider I/O):

```bash
PYTHONPATH=src:tests PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -ra -p no:cacheprovider \
 tests/test_kaggle_native_source_importer.py::test_stage_status_validate_materialize_activation_require_exact_durable_outcomes \
 tests/test_kaggle_native_source_intake.py::NativeIntakeTests::test_bounded_materialize_per_poll_and_finalized_only_after_durable_activation \
 tests/test_kaggle_dataset_checkpoint.py::test_capture_materializes_crash_gap_outcome_before_export_and_keeps_it_pending \
 tests/test_dataset_recovery_store.py::test_native_verified_checkpoint_promotes_only_exact_captured_bytes_and_lost_ack_is_idempotent \
 tests/test_kaggle_dataset_checkpoint.py::test_work_cycle_waits_for_quiet_then_resumes_published_version_after_lost_native_readback
```

Receipt `op_331ecbfe359ac14f81506cd3`: **exit 0, 6 passed, 0 failed, 0 skipped; 6,634 ms**, one Starlette deprecation warning. Two version-resume variants are included. In native intake tests `finish(...,publish=False)` records APPLIED but keeps status `processing` and next ordinal unchanged; actual remote outcome publication is required before next ordinal or finalized source. Dataset tests independently prove an APPLIED local outcome remains uncovered, verified promotion exact captured bytes, lost-ACK idempotency, and work-cycle checkpoint batching semantics. **This is synthetic boundary proof, not live Dataset/PG/provider.**

## NEW test-only addition, independently staged (unexecuted)

`tests/test_kaggle_native_intake_checkpoint_boundary.py` parameterizes 2 and 3 materialize inputs using the existing *actual* `native_intake` fixture. It asserts each APPLIED-but-not-DURABLE result retains the original pending key/bytes, creates **no next ordinal/activation**, and only remote synthetic outcome publication advances the ordinal. Activation itself must become DURABLE before `finalized`. Expected logical barriers: 5 and 6.

**Execution status of this NEW selector: NOT RUN.** Cloning the immutable frozen handoff into a test-only copy through `capture_handoff` was denied exactly with `handoff_denied: capture_handoff requires a canonical project` (operation `op_f958fe6fe4c9b812a5e4ba02`). This particular copy operation was not retried via an alternate executor. The frozen handoff was left untouched. The NEW test/doc live on an independent GitHub source branch for explicit integrator-approved application to its next candidate; **do not run this new test against main without the frozen schema14 source/fixtures** or claim its success from the six earlier tests.

## Shkiper test-only continuation

Own writable branch `chatgpt/rkb-intake-checkpoint-boundary-20261022` starts at
the unchanged Linza leaf9699e260205e40f86c49ed426482269baa9a7bd2. It changes only
this report and the adjacent test file. The two terminal assertions now await
entries into an ordinary list: `all` cannot consume the original asynchronous
generator expression. Three additional cases directly exercise actual
`NativeSourceIntake._stage_exact` under real owner access after an actual
ControlSpool apply: unpublished outcome sets both real `scope.pending` and
`read_facts.pending` and prevents allocation; after publication, loss of the
existing synthetic GEO proof makes `_intake_coverage_current` false and also
prevents allocation. A separate native-history-pending case uses the real
`ControlSpool._replace_source_heads` recovery projection: local scope is clear,
coverage is current and facts are contiguous, but real `read_facts.pending`
still refuses allocation. No `_drive` replacement, source guard patch or private
storage-identity reset is used.

These five cases initially remained NOT RUN pending a compatible candidate.
After the owner's explicit instruction to proceed independently, an isolated
writable copy was prepared and executed as recorded below. The leaf's main base
does not contain the native14 fixture, so execution against it would be an
unrelated import failure, not a meaningful RED result. Frozen5f63/e58 and the
deniedb833 path remain untouched. The native fixture's conditional object store
does not measure real Dataset versions; existing checkpoint-class tests and
actual provider acceptance remain separately identified above.

## Actual compatible candidate execution

GitHub-hosted CI [39759312648](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39759312648)
at `317a539cdb9fb20fa33db75557025b52996d324e`: **9 PASS,0 FAIL,0 SKIP,4.292s**.
The existing physical worktree was reused. Candidate baseline commit01a1c71
copies exactly77 source paths from immutable `src_5f63d09f8c1a0d3df77220f6`,
basebd138b2449f68cabb2cbfa3df552df8471f40c78, captured input patch
`ae08868278c5227557ab105ca45673163eadceef675bb99dd5680581a3915f09`.
Every copied source file was rechecked byte-identical after execution; source
and frozen handoff were not patched. Only owned test/doc and branch-only CI
were added. Candidate is a synthetic test slot, not a second release.

Five new selectors cover2/3 materialize inputs and the three independent
allocation barriers. Four selected real checkpoint-class cases cover exact
captured-byte promotion, crash-gap outcome capture and lost-readback recovery.
Initial run39759090976 found a genuine standalone test fixture-registration
error (4checkpointPASS/5setupERROR); `target`, `auth` and `people` fixtures are
now explicitly imported. Earlier async-generator assertions were also fixed.
No source guard was weakened to obtain PASS.

Retained evidence:
`/home/dev/artifacts/regional-knowledge-base/20261021T124437Z-rkb-shkiper-d-preflight-20261021/intake-boundary-ci39759312648/`
contains JUnit and per-file provenance. No Dev install, PG/model, Telegram,
Kaggle or runtime mutation occurred. Logical5/6 barriers remain distinct from
physical Dataset-version counts; full book/pixel/Story.body/Saved/live reads
are not certified by this receipt.

## Owner's minimal future correction contract — not implemented

A reviewed optimization could separate **bounded internal native progress at APPLIED/persistence_pending** from final external Saved/Telegram effects, *only if* the exact accepted immutable command/actor/sequence/handler/payload has required verified remote intent coverage, stable idempotent keys, fencing and recoverable contiguous cursor. Dataset snapshot may coalesce multiple already-applied outcomes, but no uncovered state may be emitted as Saved, final source activation, public evidence or a provider send. Telegram requires an independent covered send-intent and exact provider receipt/UNKNOWN reconciliation. End-of-book activation plus research continuation must be atomic and replayable; native capture/readback must verify the complete prefix before pointer promotion.

This source slice establishes the bottleneck **location**, not a performance number and not authorization to change the integrator-owned `_drive/_finalize`. Nothing in this report relaxes PDF/DjVu, images, facts, Story.body, continuation or corpus/readiness gates.
