# Native updates with resident PG and BGE

Status: isolated source leaf, **not wired or deployed**. Source baseline:
`bd138b2449f68cabb2cbfa3df552df8471f40c78`; ordered book migration reviewed at
PR212 `cda234538dcd61880955a12c13bb6007885e97a8`.

## Current process and future hook

The existing native process cannot acquire this hook through its current
protocol. Its read-handler maintenance changes only a stateless read slot.
Its source executor, recovery factory and WSS receiver are retained objects;
API restart does not replace them. Installing the first host hook requires a
reviewed new native context. This document does not launch or prescribe an
automatic cold successor.

For subsequent compatible updates, `BgeWssTransport.encoder`, the owned PG
process and reader/writer pools survive. `NativeUpdateOwner` holds one aggregate
of callbacks and metadata. Every ordinary dispatcher must capture that
aggregate through `bindings()`; the existing read-only maintenance remains
independent and must not become a writer or execution shortcut.

## Typed native contract

`NativeUpdatePlan` binds the operation key, reviewed commit, existing
`WarmHandlerPatchApproval` object identity/size/SHA, original code SHA, original
and target revision tuples, registry fingerprint, exact ordered SQL pins,
original settled head and barrier accepted-head SHA. There is at most one
accepted schema transition per update: SQL11→12, then a separate SQL12→14.
The latter installs SQL013 and SQL014 atomically through PR212's existing
`ordered_migration_prepare` and `NativeSourceMigration`, not a new SQL runner.

The artifact uses the existing `source_bundle` canonical base64 file map.
Staging verifies bytes, file paths, code/SQL/registry digests, historical
handlers, model/dependency/launch pins, same database generation and **the same
resident object aggregate**. It never executes source, loads a model, starts
PG or accepts a module path/constructor/SQL string from a wire caller. A
reviewed native composition must construct the candidate callbacks through a
fixed host loader; that adapter is not implemented by this leaf.

The source-bundle identity is **not** the loaded code hash. The latter retains
the existing `VerifiedArtifacts.role_hash` format over the complete code
inventory, including unchanged native tar/manifest rows. Only the
content-addressed Python/SQL source rows change. The native update host and
stable ABI modules remain byte-identical; a fixed loader must alias those
retained contract modules rather than create duplicate Python contract types.
A host ABI or model/dependency change requires a separate context decision.

1. `stage`: verify artifact and typed candidate, then run the retained pure
   validator. Old bindings remain usable. Recheck the attachment after awaits.
2. `activate`: enter the existing admission/drain and writer barrier. No
   ordinary read, write, publication or unknown effect may cross cutover.
   Only the exact already accepted transition hashes are permitted as pending
   barrier writes; the accepted head, predecessor cursor and hash must match.
3. Apply through the existing accepted-command proof, ordered prepare and
   `apply_accepted` path. **Dev accepts/seals the commands before this phase**;
   Kaggle receives captured existing proofs and never seals a new command.
4. Verify full correlated `ApplyReceipt` outcomes and probe actual target
   catalog/schema/frontier. Switch one aggregate synchronously; retain the
   previous aggregate and exact same encoder/PG/pools.
5. Keep ordinary dispatch fenced in `awaiting_ack`. The existing authenticated
   result path must durably adopt the exact native generation/readiness on
   Dev, then match its ordinary RESULT_ACK to job/claim/request/operation.
   Only that retained host callback may call `acknowledge`; an arbitrary ACK
   or caller-supplied success flag is not sufficient.

## Rollback and lost results

A stage can be discarded without effects. A compatible code-only activation
can roll back to its retained bindings under the same quiescence and unchanged
head, followed by another matched adoption ACK. A committed schema transition
cannot be downgraded or removed from accepted history.

If a transition may have committed, or cutover exits with an uncertain result,
dispatch remains fenced. `reconcile` reads the exact existing native ledger
through a retained observer; it does not resubmit. Matching applied receipts
and a successful target probe complete the same update without a second
generation increment. No receipt alone proves nonexecution. Only an explicit
`NativeTransitionReadback` proving the immutable predecessor cursor, origin
semantics, exact command absence and reviewed preparation pins permits a
later explicit activation of the **same accepted bytes**. Unrelated pending
or unknown operations still block cutover. No model decision is rerun.

Native process loss continues to use the existing Dataset/accepted-tail
recovery; an in-memory host receipt is not Dataset Saved.

## Shared integration paths requiring one coordinated owner

| Existing file | Required hook, not implemented by this leaf |
| --- | --- |
| `scripts/production/bge_wss_transport.py` | Retained aggregate dispatch wrappers, current prepared readiness, drain, matched adoption ACK and reconnect reconciliation before ordinary READY/dispatch |
| `kaggle_target_runtime.py` | Build candidate from a fixed reviewed package using existing encoder/PG/pools and ordered migration preparation |
| `kaggle_recovery_phases.py` | Explicit typed native-update phases; host-only status/stage/activate/reconcile/rollback, not generic exec |
| `kaggle_recovery_dispatch.py` | Existing scoped artifact ticket, durable operation identity and accepted transition proofs; correlated native result validation before adoption |
| `bge_queue.py` | Preserve immutable original launch expectation; durably record current native-generation adoption with exact predecessor and transition evidence |
| `bge_channel.py`, `kaggle_readiness.py` | Authenticate update result/ACK and validate current generation/readiness; preserve attachment and actor/writer fences |
| `kaggle_target_launch.py` | Ship the host hook in the independently verified bundle and recompute actual complete packaging inventory |

The Dev barrier must prevent creation of new **accepted** writes under the old
revision until adoption. Merely stopping native dispatch is insufficient.
Fresh source-history and catalog closure must be established before product
reads/imports reopen. After a lost adoption ACK, reconcile the same durable
job/result and actual native generation on the existing authenticated channel;
do not relabel the original launch or create a second update operation.

No shared hook above has been edited by this patch. No Stage0 limit, transport,
scheduler, credential or authority was added. Wiring and real native SQL/PG,
model identity, reconnect and product acceptance remain required. Synthetic
host tests prove only the leaf's state transitions and retained identities.
