# M2 native accepted-source revision2/rechunk — independent changed-boundary contract
2026-10-22 · RKB-Линза · **TEST CONTRACT ONLY / NO REVISION2 EXECUTION**

## Required product/source authority

Baseline is the **approved immutable diagnostic source**
`2b67970043fb25d4e0ecca11de5ff1cbd97a3136`, not canonical deployment.
The exact pinned contracts remain [implementation §6/§7](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md), [owner alignment](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/designs/rkb-owner-alignment-20261021.md), [acceptance](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/testing/rkb-scalable-kaggle-acceptance-20261019.md), and [delivery plan PR238@550a3c6](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/550a3c6d2b1c31b283a3bd4bce0aea0bb78a5386/docs/plans/rkb-product-delivery-checklist-20261022.md).
Accepted source includes **actual PDF/DjVu original, verified pixels/figures, facts, real Story.body where justified, and recoverable continuation**; a text-only/revision counter shortcut cannot certify it. Dataset-primary remote outcome coverage is separate from local PG APPLIED. No Telegram or external provider effect before independently covered send intent; UNKNOWN means reconcile original, not resend.

**Integrator API handoff is provisional, not a committed implementation** (RKB-Импорт Slack ACK 2026-10-22 14:xx):
`REVISION2_REVISIONS`; versioned routes `source.stage.v2`,
`source.stage.seal.v2`, `source.materialize.v2`,
`source.activate.v2`; expected predecessor revision1, unchanged
canonical source_id, **new ingestion_id**, exact original edition/hash and
owner-checked inherited original receipt. Proposed ordered migration
`sql/kaggle/017_source_revision2.sql` is reserved **after actual media
SQL015/016**; SQL013/014 immutable historical pins must not be overwritten.

At this base **revision2 does not exist**: installed native source
schema14 has v1 stage/materialize/activate, and legacy import SQL
requires absent_source/revision1. A green revision1 test is **not**
a revision2 acceptance receipt.

## Explicit prerequisite before NEW executable test

RKB-Импорт must supply a **committed exact branch/base/HEAD and files**:
(1) verified SQL017 after actual SQL015/016 with a versioned upgrade callable
and a frozen ordered migration/pin manifest;
(2) real `NativeSourceIntake`/Stage/Activation/Compiler, original lease
and rebind API and revision2 accepted/rejected outcome DTOs;
(3) `kaggle_native_source_bindings` plus **actual**
`kaggle_source_commands.py::fixed_registry` schema17 and
`WorkerSourceExecutor` dispatch/materializer v2 branches;
(4) exact `DatasetRecoveryStore` covered final outcome/readback seam;
(5) existing real class fixture names and approved GitHub PG fixture.

**Known wiring gap:** the base `kaggle_source_commands.py` registry handles
schema14, and its worker routes materialize/stage seal v1 only. SQL017
and compiler alone cannot be counted as an installed usable revision2.
RKB-Импорт requested exact ownership of two sub-hunks; no tests are
allowed to substitute fake route handlers when those branches are missing.

## One NEW file and actual assertions, when implementation exists

Owned future test:
`tests/test_kaggle_native_revision2_acceptance_20261022.py`.
Test must call **real existing production classes/ordered SQL**, not a
test-defined revision2 implementation. Synthetic native/provider I/O may
substitute *only* outside the genuine source/SQL controller and must carry
original immutable operation bytes/keys and signed receipt identity.

| Assertion ID | Required actual state transition or rejection |
| --- | --- |
| `R2-01 ordered_install` | SQL015 media, SQL016 story, then SQL017 revision2 are applied in predecessor order to an **isolated** PG17 fixture; revision1 pinned migrations/hashes unchanged; actual handler registry and WorkerSourceExecutor can dispatch versioned v2. Missing route or wrong schema pin is **RED**, not SKIP/PASS. |
| `R2-02 inherited_identity` | Seed one **actual accepted/retrieval-readable revision1**, remote-covered Saved; start owner-only reprocess/rechunk referencing its immutable original proof. Stage v2 has **same canonical source_id** and new ingestion_id, exact prior revision=1, exact work/edition/original SHA, original archived bytes/receipt reused without a second provider upload. |
| `R2-03 v1_history` | Before and after revising, original revision1 source provenance, accepted byte hashes, selected page/figure receipts, facts/story/frontier and historical read path remain addressable and byte-consistent. No UPDATE/DELETE of immutable accepted revision1 or remapping to a synthetic source_id. |
| `R2-04 prior_fence` | Reject expected predecessor 0, 2, stale source/edition/revision/hash, wrong stage_head, or a second concurrent successor. No allocation of new native op/ingestion, no source revision switch, no operator side effect on these attempts. |
| `R2-05 actor_lease` | Reject different actor, revoked grant, stale owner import lease or copied queue/WSS generation. A previously authorized token/identity that has since changed cannot invoke versioned stage/finalize. Exact owned actor/context retained on stage, seal, materialize and activate. |
| `R2-06 receipt_mismatch` | Reject altered original SHA, page_count/edition/ingestion, wrong media-issued receipt/region/page locator, changed handler/model/schema hash, different payload under same operation key, wrong command_hash/outcome bytes/sequence. The native candidate cannot silently replace original source pixels or fabricate Story.body. |
| `R2-07 pre_saved_no_activation` | Native v2 stage+seal+materialize locally **APPLIED** or persistence_pending retains revision1 as active; source index/graph and public ordinary reads cannot expose revision2 before atomic activation. Partial materialize/UNKNOWN does not mean Saved. |
| `R2-08 idempotent_replay` | Same exact command identity/hash after lost native ACK or reconnect yields identical outcome/one revision2, no duplicate model inference, archive transfer, passage/graph rows or source-history mutation. Same key with changed bytes strictly rejects; UNKNOWN provider/native state is reconciled, never blind resend. |
| `R2-09 exact_dataset_saved` | Activation of revision2 becomes accepted/Saved **only after exact Dataset version/native readback covers command, PG applied outcome and original/root frontier**. Uncovered version or mismatched cursor/hash stays pending, no external effect; after verified promotion it atomically switches active_revision to 2. |
| `R2-10 ordinary_reads` | Current authorized source search/fetch/graph returns active rev2 with pinned correct evidence; requested historical rev1 continues to resolve under scope. Stale/wrong authority or revoked source excluded, no cross-edition leakage; source/Story.body/continuation survives an approved synthetic restore. |

Tests should use one physically small **synthetic original** carrying genuine
file/page identities and controlled issued-image receipts; no OCR-generated
"visual gold" and no fabricated source narrative counts as external acceptance.
Specific data values will be taken from RKB-Import's real fixture, not
invented SQL-v2 success mocks.

## Existing PG fixture and exact read/write boundary

A hosted **GitHub Actions** mandatory CI service already provides PG17 with
pgvector 0.8.2 at strict `RKB_KAGGLE_TEST_DSN`. Existing
`tests/test_kaggle_pg_driver.py::database` checks the exact repo,
DSN/host/port/role and creates isolated `rkb_v4` under SQL001;
**it does not install native SQL013–017 by itself**.

**Existing real PG tests to reuse on Shkiper-approved source baseline
`4319a87df62419efa3bf32f455b803ef57fdc936`:**
`tests/test_source_stage_pg.py::native_stage` runs real SQL013;
`tests/test_source_activation_pg.py::image_pg` runs real SQL014,
`apply_accepted`, and the compiler;
`tests/test_native_source_upgrade_pg.py::schema12` drives genuine
SQL012→014 native upgrade and rollback; and
`tests/test_shkiper_ordered_013_016_pg.py::test_pg17_exact_ordered_sql13_media15_case16_preserves_history_and_rolls_back`
already applies exact ordered **SQL001–016** to the existing
GitHub PostgreSQL service, checking old-function catalog and rollback.
All these are **revision1/predecessor proofs only**, not v2 PASS.

The independent test must extend those existing ordered migration
classes/fixtures with the committed actual SQL017/handler registry/worker
callable, assert the same old accepted function/catalog identity before
and after rollback, and exercise real v2 public activation.
If SQL017, the v2 callable, or its pg fixture is missing, fail/hold at
the exact source seam: **no importorskip, no fabricated compatible v2
handler, no second PG runner/socket or local Dev PostgreSQL**, and
no modification to mandatory `.github/workflows/ci.yml`.

Do not emit private original/page text, credentials, signed file tickets or
real provider responses in CI logs. Record exact installed schema/handler
hash, executed test names, cases passing/failing, and unknown/skip totals.

## Current evidence and ownership

- **Provisional source HEAD:** Import branch `chatgpt/rkb-native-revision2-20261022` @ `b4359bf82f20259e32a773d165420738fa219aa2` contains **actual proposed SQL015/016/017**, but `kaggle_native_revision2_upgrade.py` does not yet exist and `source_stage.py`, `source_activation.py`, `kaggle_native_source_intake.py`, `kaggle_source_commands.py` remain original schema14 Git blobs. Thus the exact **runnable Python v2 API/registry/worker HEAD is NOT YET AVAILABLE**. The SQL017 bytes are an independently reviewable partial source artifact, **not** a working applied migration or a positive accepted-source result.
- **NEW revision2 test:** `NOT CREATED / NOT RUN` pending exact committed API.
- **Old SQL013/014 / native revision1 tests:** not a substitute.
- **Actual PG17 revision2 test:** `NOT RUN`.
- **Live/Kaggle/provider and real PDF/DjVu acceptance:** `NOT RUN`.
- **Ownership:** RKB-Импорт writes revision2 SQL/source, Шкипер reviews
  compatible source assembly, Opus alone merges/operates live.
  RKB-Линза writes **only NEW revision2 acceptance test and this report**
  after source handoff. No shared code, WSS, health, media, SQL, legacy
  pins or grants are changed by this independent test slice.

No additional product release gate is created: this matrix instantiates
the already-required same-source accepted reprocess/rechunk, durability,
source-history and ownership boundaries. External first-book visual/story
and broader quality gates stay separately mandatory.
