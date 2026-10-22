# First native context: schema11 Dataset to ordered schema14

Status: executable **RED compatibility contract**, not an integrated release or
snapshot/PG/model acceptance. The test patch changes only its new test and this
document; shared restore, history, runtime and inventory code remains untouched.

Diagnostic source: PR212 `cda234538dcd61880955a12c13bb6007885e97a8` public
`src/` and `sql/`, plus the exact new native-host module from PR216
`9379db88abca68edb97a1c621430546b64b52f86`. This read-only source export is a
managed diagnostic artifact, not a Git worktree or the integration owner's
candidate.

The unchanged test was also run against the coordinator's frozen integrated
handoff `src_f3548be35d1c99fd13ff02ac`, base
`bd138b2449f68cabb2cbfa3df552df8471f40c78`, captured patch SHA-256
`e336a669acfd8f5f7b1f410f16b25f4ce3456ac468f5de81dcbba6a8aaf04597`.
Result: **2 failed, 5 passed in 3.62 seconds**, with the same inventory and
`pg_snapshot_recovery.py:152` origin-control failures described below. This
handoff contains only the first integration checkpoint; its shared wire/ACK,
retained factory and startup fence are not yet connected. The retained JUnit
receipt is `compat-integration-f3548.xml` in the managed task evidence directory.
No source in that handoff was changed by the test run.

The follow-up adds three rejection cases with internally hash-consistent Dataset
metadata but unreviewed origin ACL, registry or routes. Result on the same frozen
handoff: **2 failed, 8 passed in 5.50 seconds**; receipt
`compat-integration-origin-negative.xml`. The original positive restore must
pass alongside these negative cases after the fix. Their current rejection
alone does not prove the fix: the existing guard also rejects valid old control.

## Versions and authority at each boundary

| Boundary | Required state |
| --- | --- |
| Dataset2 manifest/control and `SourceRestoreConfig.versions` | Preserve original schema11, catalog/ACL/registry, base sequence20 and original accepted head |
| New context `OwnedDatabase` and launch expectation | Final **expected** schema14 is permitted; it is not observed readiness or an initialized receipt |
| `SourceRestoreCoordinator._observe` | Initial launch expectation must match its final target database; expectation11 with target14 is refused |
| Native physical prepare | Verify the actual base using the original schema11 versions and historical applied-prefix digest |
| Accepted tail | Preserve original20 outcomes (16 applied,4 rejected); use exact ordered11→12 transition, repaired GEO commands, then12→14 transition |
| Native finish | Require real target catalog/frontier/schema14 before creating a retained initialized capability; schema11 cannot gain this capability from target14 metadata |
| READY/current adoption | Only after successful finish/readback; preserve original launch identity and separate later warm native-generation adoption |

The declared source target is not a reason to rewrite the Dataset manifest,
old outcomes, genesis, command versions or original function pins. For this
first cold context the desired final launch expectation can already be14;
later warm adoption is a different barrier, not a relabel of the old base.

## Executable diagnostic and observed failures

Run with installed project dependencies and the selected source tree:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/path/to/pinned-source/src:tests \
  python -m pytest -q tests/test_native_update_restore_compatibility.py \
  --basetemp=/home/dev/artifacts/<managed-task>/compat-fixture \
  -o cache_dir=/home/dev/artifacts/<managed-task>/pytest-cache
```

On the diagnostic composition above: two failing contracts and five passing
negative/metadata contracts. The failures are deliberate assertions of desired
compatibility, not xfail/skip or weakened validation:

1. `source_bundle` does not include `regional_knowledge/kaggle_native_update.py`.
   Having a file in a development tree does not ship the hook in the verified
   native inventory. The integration owner must include it and recompute full
   packaging sizes/counts and code pins.
2. `PhysicalSnapshotRecovery.recover` rejects the original base with
   `snapshot_source_control_binding` **before native prepare**. It compares
   original control ACL/registry/routes against the final verifier's closure.
   PR212 already supplies reviewed `history.origin_closure`, but this restore
   comparison does not use it. Original closure pins must remain intact and
   may advance only through the accepted ordered transitions.

The same scenario also asserts target closure at finish. Existing code updates
final control catalog and transitions while leaving ACL/registry/routes at the
original values. After fixing the first comparison, the final-control
assertions must pass through a supported ordered closure update; do not make
the test pass by rewriting the original snapshot or using final pins as origin.

The fixture uses real `DatasetRecoveryStore`, `DatasetSnapshotTransport`,
`RecoveryJournal`, historical and ordered `HandlerRegistry` entries,
`WssSourceHistoryVerifier`, `PhysicalSnapshotRecovery`, `SourceRestoreConfig`,
`SourceRestoreCoordinator`, queue/writer and readiness classes. Original20
synthetic commands are genuinely sealed and retain byte-identical outcome
envelopes under covered Dataset2 metadata. The actual native
`applied_prefix` function hashes their synthetic native rows. The six later
commands are local pending accepted tail; the test never labels them Dataset
Saved. No fallback to current command revisions is allowed.

Native/provider I/O, the archive body and SQL execution are substituted. The
native mock requires ordered21…26 and target schema before finish. A separate
test invokes the **real** `NativeSnapshotFactory.finish` with a schema11 probe
and target14 and proves it emits no initialized capability. These are source
contracts, not proof that a real tar archive restored or SQL/model ran.

The existing GitHub PG test in PR212 separately checks ordered11→12→14 SQL and
duplicate/rollback behavior. No Dev PostgreSQL is needed to reproduce the
controller failure, which occurs before native prepare. Real snapshot restore,
host-hook installation, current adoption, native book intake and end-to-end
search still require the single runtime operator's integrated acceptance.
