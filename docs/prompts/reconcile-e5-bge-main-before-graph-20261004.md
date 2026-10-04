# Regional Knowledge — reconcile E5/BGE production stack into main before graph

Date: 2026-10-04

This is a **repository/runtime reconciliation task**. Do not start graph work in
this task.

## Why this task exists

Production runtime has already accepted:

- E5 fast tier from PR #27;
- BGE/Kaggle main tier from stacked PR #28.

However, at the time of this handoff both PRs are still open and PR #28 is based
on the E5 feature branch rather than current `main`. PR #26 is also still open
and currently non-mergeable; it contains the source-coverage/preview-continuation
fixes needed for a correct future book reimport.

Do not allow production to remain indefinitely ahead of canonical `main`.

## Goal

End with one canonical `main` that contains the accepted E5 and BGE runtime,
plus the smallest safe source-completeness transport fix needed for future
ChatGPT-led reimport.

Then deploy/read back exact `main`.

## Required sequence

1. Inspect current `main`, PR #27, PR #28 and PR #26.
2. Preserve all accepted production behavior and reports.
3. Merge/integrate E5 first.
4. Rebase/retarget/integrate BGE onto the resulting `main`.
5. Re-run CI and verify the BGE diff no longer depends on an unmerged feature
   branch.
6. Do **not** merge PR #26 wholesale if conflicts make that unsafe.
7. Port only the still-needed source-completeness changes from PR #26 onto the
   new `main` in a small focused PR/commit:
   - bounded native-block continuation;
   - explicit preview/full/visual-review provenance;
   - validation that page IDs/native previews alone do not prove source
     completeness;
   - the associated focused tests/docs.
8. Preserve PR #26 benchmark/report evidence even if its code branch is
   superseded; close or mark it superseded only after the retained evidence is
   reachable from canonical docs.
9. Deploy the exact resulting `main` and read back:
   - E5 service ready;
   - BGE queue/worker integration ready;
   - no model inference loaded in MCP;
   - no paid/external embedding fallback;
   - migrations 010/011 present;
   - source-completeness transport behavior present.
10. Run full tests and CI on the final integrated `main`.

## Do not do

- Do not implement the accumulative graph yet.
- Do not reimport the Gause book yet.
- Do not change the accepted warm retrieval choice: BGE + lexical.
- Do not add E5 into every warm BGE query; the measured all-three path was worse.
- Do not replace Kaggle CPU with GPU.
- Do not add external paid inference.
- Do not refactor into a new embedding platform.
- Do not rewrite the accepted queue/lifecycle architecture without a measured
  defect.

## Acceptance facts to preserve

E5 acceptance:

- 747/747 active chunks have E5 vectors;
- 907 legacy vectors preserved;
- public search/fetch p95 at 1/5/10 users: 0.879 / 1.412 / 2.121 s;
- one CPU / <=1 GiB encoder;
- lexical fallback works.

BGE acceptance:

- BGE backfill 747/747 and idempotent replay;
- selected warm path: BGE + lexical;
- all-three E5+BGE+lexical was worse on the fixed multilingual fixture;
- first measured cold BGE readiness about 73.7 s;
- 30-minute useful-work lease;
- planned pre-11-hour succession;
- single-start/fencing/recovery verified;
- source completeness and unsupported-answer limitations remain explicit.

Do not turn these historical measurements into new SLA guarantees.

## Definition of Done

Done only when:

1. E5 and BGE accepted code are both reachable from canonical `main`.
2. PR #28 no longer depends on an unmerged PR #27 branch.
3. final integrated CI is green;
4. deployed runtime is exact final `main` source;
5. E5/BGE status/readback is healthy;
6. no paid inference fallback exists;
7. source-completeness transport fix from PR #26 is integrated or a concrete
   blocker is documented;
8. PR #26 is either safely merged in narrowed form or explicitly superseded with
   its evidence retained;
9. graph prompt is left untouched except for prerequisite status;
10. final report is committed:
    `docs/reports/e5-bge-main-reconciliation-20261004.md`.

At the end return:

- exact final main SHA;
- merged/superseded PR status;
- runtime SHA/readback;
- CI link;
- whether it is now safe to launch
  `docs/prompts/accumulative-knowledge-graph-mvp-after-bge-20261004.md`.
