# Native book first-context source checkpoint — 2026-10-22

This is an intermediate source checkpoint, **not full import or release acceptance**.
Base: bd138b2449f68cabb2cbfa3df552df8471f40c78.
Import/Telegram source: PR212 cda234538dcd61880955a12c13bb6007885e97a8,
rebased with the existing main service-startup fixes and its three fixture corrections.

## Unchanged product contract
- [Canonical implementation](../prompts/rkb-scalable-kaggle-implementation-20261019.md)
- [Owner alignment](../designs/rkb-owner-alignment-20261021.md)
- [Acceptance](../testing/rkb-scalable-kaggle-acceptance-20261019.md)
- [Full parity, including media and Case](../testing/rkb-migration-parity-20261021.md)

A first real book is an intermediate check, not a replacement for full media,
Story body, facts/dates/people/GEO/disagreement, recovery, access and scale gates.
The original owner-only import, Kaggle-only tokenizer/BGE/PG, Dataset primary,
scoped proof access and current exclusive Telegram session remain required.

## Initial context versus future updates
The canonical handoff §5.4 lines286–298 specifies narrow stateless handler
updates, ordered mutation barriers and controlled incompatible context changes.
A new first context can prepare an exact Dataset2 schema11 base, replay the exact
accepted SQL12 upgrade/repair then SQL13+14 transition, probe final14 and only then
issue READY14. Initial launch expectation14 is not observed schema14 and must not
relabel the base or the old run.

This checkpoint does not depend on the experimental NativeUpdateOwner/host
loader/adoption table. That separate source work remains preserved in its own
worktree and frozen snapshots; it was not reverted or erased. Existing compatible
SQL/read-handler maintenance requirements still apply. No live restart or new
notebook is requested by this document.

PhysicalSnapshotRecovery now checks original ACL/registry/routes against the
reviewed origin closure for the schema11 base; advances revision tuples only on
an APPLIED ordered transition; and writes final closure only after the target
tuple is reached. Original Dataset bytes and all original outcomes are retained.

## Verification
94 focused source/synthetic tests passed, exit0, Direct Ops
job_90c2a296dfb2b65f5cffa728. They cover real DatasetStore/Journal/restore classes,
20 original GEO outcomes (16 applied,4 rejected), ordered tail21–26, wrong origin,
rejected final transition, real accepted-command contracts, fresh notebook source
imports, retained encoder identity and fake WSS/Telegram challenge/late-binding.
External PG/model/provider I/O is substituted. No PostgreSQL ran on Dev.

tests/test_first_book_restore.py derives its restore fixture from public
8da6375a926d7fab8be1ae64c0d96ca0ec62b2cc. Its inventory assertion is explicitly about
book and Telegram modules. The original separate hot-host inventory test is
unchanged in its preserved suite, not silently skipped or reported as passed here.

## Remaining inputs and acceptance
- Source inventory is 88 Python modules +14 SQL artifacts =102 entries.
  Existing59 native/base entries plus proposed4 Telethon wheels project165,
  beyond the unchanged160 ceiling. This is not a launchable approved manifest.
- Stage0-approved dependency lock, pyaes permanent/scoped artifact placement and
  verified ddjvu presence in the native archive are still required from packaging.
- Full media15/16 leaf/ordered transition and shared runtime integration remain
  mandatory outstanding work; no empty migration or placeholder installed claim.
- Dataset pending progress per materialization ordinal is under independent
  regression review; neither fake Saved nor unpersisted external effects is allowed.
- Actual final catalog/ACL/function pins, exact accepted tail, Telegram authorization,
  Gause original import, model-authored Story bodies/page proof/retrieval and final
  Dataset coverage need the single runtime operator's real acceptance.
- Full media/Case/rights/recovery and protected mass-ingestion/performance gates
  remain open according to the acceptance matrix.

No credentials, corpus bytes, private configuration, provider calls or deployment
are included in this checkpoint.
