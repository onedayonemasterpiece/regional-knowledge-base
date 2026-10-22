# Cold successor: retained writer-generation refusal

Owner: Opus (lifecycle/restore/writer binding). dot requested this bounded
reproduction, not a parallel lifecycle implementation. No runtime code changes
or provider actions accompany this receipt.

Current proof: source `9d00cb9e7a69937d485b144a0341aeaf7515670f`, GitHub
[CI39782254279](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39782254279),
both Python passed3078/35 skipped and actual launch metadata2/0 skipped.
The following existing cases passed on both matrices:

- `tests/test_bge_controller.py::test_restore_launch_refuses_writer_generation_used_by_another_run`
- `tests/test_stage0_dataset_controller.py::test_main_observes_dataset_before_claim_and_preserves_existing_launch_boundaries[writer_conflict]`

The first uses the actual controller guard and queue SQLite connection. A
retained row associates `writer-old` with a previous run. The old same-run
identity and an unused writer generation are allowed; a different successor
run cannot reuse `writer-old`.

The second exercises actual `controller.main` and `BgeQueue` with a typed
VERIFIED_INITIALIZED target and synthetic existing Dataset/Kaggle SDK replies.
Dataset readiness succeeds. The launch is claimed, then the retained previous
writer row causes `require_writer_binding` to refuse. The queue records a known
failed, not-sent preparation; `kernels_push` is never invoked. The input is a
synthetic previous-generation row, not proof that any real old provider
execution terminated. No PG/model/credentials/provider effects run in this test.

Exact callsites in `scripts/production/bge_kaggle_controller.py` at this source:
`main` loads `target_spec` once at282–283; launch claim342; writer binding
preflight360–361; source/metadata composition follows; provider push379.
`require_writer_binding` at211 checks for a matching generation used by any
other run. The loop has no per-run target/recovery-generation preparation.

Required owner decision: prepare and persist one exact successor target/restore
binding before immutable runtime expectations/provider submission, using the
existing writer authority/journal/recovery ownership. Reconcile any uncertain
effect under its original identity. Do not remove the guard, copy old live
writer authority, merely rename a generation, rewrite accepted history through
new handlers, or blind-push a replacement. The receipt diagnoses a source
boundary; deployment and a real cold/restore successor remain unverified.

Retained parsed proof is `cold-successor-existing-harness-repro.json` under the
managed RKB Shkiper task artifact directory, with testcase names/timings and
their exact XML paths. No new duplicate regression test was invented.
