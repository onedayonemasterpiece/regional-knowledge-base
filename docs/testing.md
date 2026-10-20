# Testing method for the redesigned RKB

Updated2026-10-20. [Target acceptance matrix](testing/rkb-scalable-kaggle-acceptance-20261019.md) and the [single handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) define the gates; `.devcoveer/requirements.json` protects their quantitative thresholds. Historical [testing journal](testing-journal.md) is retained, not reclassified as new deployment acceptance.

Start with isolated source-grounded fixtures and focused tests of changed failure boundaries. Then one complete fresh-import/story/case/channel scenario; expand only to resolve actual risks and required capacity/quality/latency gates. No production book mutation just to get a screenshot, no real-world fake expert opinion or test publication without an authorized test route.

Report each applicable criterion PASS/FAIL/BLOCKED/NOT RUN with exact source/deployed SHA, schema/model/tokenizer/search config, corpus/revisions, tested audience and reproducible evidence. Distinguish backend unit/integration, real provider readback and user-client E2E. Missing provider credentials is BLOCKED, not PASS from a mocked adapter.

Source fidelity: exact original/pages/regions/spans, visual coverage and semantic units, full BGE input bound; facts survive zero stories. Story quality:15–20 newly read originals, held-out cases, blind baseline/new comparison of actual saved body after restart through normal get/search/voice. Test unsupported causality, bibliography-as-body, empty variant success, false How/Why title, private draft and unknown-outcome coherent story. No story quota/mandatory second model judge.

Search: held-out fact families, BGE-only/lexical/fusion ablations, RU→DE slice, broad/scoped ANN vs exact, prepared generic/custom plan, inactive revision tail, concurrent import/search and maintenance. Measure p50/p95/deadline misses, cache and cold/warm separately; include errors/pending in denominator. Do not call Hit@K answer accuracy.

Recovery: failures before/after remote intent, PG commit, outcome receipt, WSS ACK, snapshot upload/manifest promotion and provider effect; control-disk loss, restore runtime/collation drift and derived projection rebuild. UNKNOWN resolution is explicit and scoped. No duplicate model inference/publish and no TTL deletion of accepted questions.

GEO-only migration verifies actual IDs/CRS/geometry/topology/provenance and unresolved anchors, not old story transfer. Rollback after new target data cannot overwrite that data. Auth checks include real non-owner/demo/expert/local_historian, illustrations/proof/original separation, revocation, shared route, selective private draft publication and multi-client fairness.

Use one test_run_id and isolated case/corpus. Any approved canonical test mutation needs baseline/allowed effects and narrow idempotent CAS compensation preserving later real work; rollback_conflict is not permission for force overwrite. Repeat testing only for changed code or concrete remaining risk. Documentation-only validation checks links/contracts/schema and CI, not runtime metrics.

The qualitative story pilot uses the pinned previous prompt/contract on the same freshly reviewed originals and actual stored outputs; missing baseline body is recorded honestly. Owner/designated human provides blind preference and factual-loss notes. Without that verdict, qualitative acceptance is NOT RUN; mechanical invariants still run. This is not per-story publication approval. Core cutover, individual channel release and whole-project completion follow handoff§14.2; an unverified new channel is not enabled or claimed complete.

## Voice fast read and native runtime decision

Follow handoff §14.4: real PH provider wiring precedes Mira/Dot comparison. Record independent smart routes, equal source-grounded tasks/corpus/rights and common tool budgets, then actual voice/Slack experience separately. Include tool-lock/capability/admission wait, end of speech to first useful grounded audio/full answer, errors, cancelled/late work, current revocation and missing evidence checkpoint after restart. Do not count a progress announcement as a useful answer.

Verify QUERY without fsync/cloud seal and with a failed telemetry sink, bounded response and generation-safe cancellation; keep PH input utterance recovery and durable mutation receipts. Same-host monotonic stages do not require synchronized host clocks. Native optimization compares one justified candidate, including IPC/ABI/lifecycle/CPU contention; no three-language rewrite or automatic change of BGE numerical runtime. These are NOT RUN design gates, not achieved latency.

## Warm debug and lifecycle

Follow handoff §14.5 DBG-1–10. Before a live run, use small local/CI fixtures for read-handler import/probe rollback, migration C-v1/lost-ACK/v2 semantics and immutable test-fixture reset guards. Synthetic test applied_ops must remain distinct from real product Saved. No BGE/E5 is installed on DevCoveer.

Use one bounded authorized owned run with a stable notebook slug to verify actual SDK capabilities, early HELLO/stage diagnostics, exact loaded versions, SQL patch without model reload, artifact completeness and scoped stop/provider readback. UNKNOWN launch/stop and multi-page failure cases begin with deterministic adapter fixtures; repeat a real run only for an unresolved provider boundary. Version-specific status must identify the execution, not merely a saved notebook version. Cancel ACK, resource deletion or lease expiry is not terminal proof.

Measure edit→apply→first useful result, starts/restarts/model loads, install/restore time and repeated bytes; distinguish cold/warm/incompatible changes. Full protected scale/quality/channel gates are not rerun on each small patch. Exact schema/function/executor transitions and replay are ready before the first real accepted/Saved. All newly designed runtime checks remain NOT RUN until actual execution evidence exists.
