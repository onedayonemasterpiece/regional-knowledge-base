# RKB project profile

This profile records the owner's RKB decisions available on 2026-10-20. Read the latest canonical handoff before execution; newer owner decisions supersede this dated profile. These are not universal Kaggle limits.

- Notebook slug stays stable; new launches use versions under that slug, not a new slug for each edit.
- Existing DevCoveer controller/broker and outbound WSS are the intended control path. Do not add an unconstrained remote shell or second scheduler.
- Approved runtime concurrency is bounded by actual independent Telegram authorizations. TELEGRAM_RKB1 and TELEGRAM_RKB2 are names, not credential values; RKB2 was empty/unverified. A variable's existence does not establish usable authorization/archive access. Do not infer a third development session or exempt a text-only reader from the owner's cap without a changed decision.
- Hold exclusive session-use ownership while the old run's outcome is UNKNOWN. Revocation may resolve session-use safety but not compute death. Never copy credentials into diagnostics or this skill.
- The target keeps PG and BGE resident where process ownership permits. Only small resource-bounded PG fixtures without full corpus/BGE belong on DevCoveer for local debugging.
- Normal search cancellation is separate from accepted mutations. Preserve exact remote accepted/Saved semantics, domain receipts, migration/replay dependencies and resource-scoped UNKNOWN reconciliation. Do not use disposable-fixture shortcuts on imported/user data.
- No runtime runs, stops, migrations, installs or imports were authorized by the request to author this skill. The v4 documents describe a target and verification gates, not already implemented behavior.

Canonical document: [RKB implementation handoff](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md). At each execution pin the version actually read in the task receipt.
