# Combined source candidate inputs — 22 October 2026

This is the input receipt requested by dot for the existing diagnostic branch
`chatgpt/rkb-intake-boundary-candidate-20261022`. It is not a second product plan,
a canonical integration branch or a deployment. Canonical integration remains
`b932c15ef133d668a0eaa3570cc895e750876c77`; Opus owns merge and live changes.

The starting candidate is `6d51bfd82132048bd4642ea4fabd613583505ce3`.
Its existing native intake source baseline, packaging, health, prepared-file
bound and compact wheel codec remain intact. The previous combined CI was
39769305258: 2736 passed / 34 skipped on each Python, with the separate actual
launch metadata job passing (64139 config bytes, 851559 notebook bytes).

| Input | Exact head | Boundary | Merge outcome |
| --- | --- | --- | --- |
| PR239 | `910a3aa26999ff6945f679fdc64645f3962feab5` | READY_ACK race | Clean; bge_channel auto-merged |
| PR244 | `eaf96f8a5f8f7c9667dd1fdeb35e73d2f1f7fba5` | Reachable import continuation | Clean |
| PR236 | `655a2a02371a2f3595400c91f9ba572ec9a57b06` | Dataset-covered expert control/receipts and ingress | Clean; server and source-owner auto-merges |
| PR245 | `ad49c375f76ae6e50086ddca0340c4055603dda3` | Expert evidence composition and ASGI fixture | Clean; two new files |
| PR241 | `5d9c07b17568bf7aa44be13022e5e98980222766` | Public illustration gate/native reader/server output | Clean; server auto-merged |
| PR223 latest | `7212ea0aa5e43ad06002bc774fd4e708b1d60ca9` | Placement metadata supplement | Clean; one new metadata file |

All inputs are real Git merges, preserving their ancestry. No whole-file
replacement or conflict resolution was required. Clean textual merges are not
proof of semantic compatibility: the combined GitHub workflow runs the full
suite on Python 3.12/3.13 with the existing PostgreSQL service and separately
rechecks the real 103-package native manifest in the actual launch envelope.
Candidate `768bca5c950507b053b157626e34c4ffcf6539af` passed CI39773458811:
2901 passed /44 skipped on each Python; separate actual metadata1 passed/0 skipped.

## Subsequent source inputs

| Input | Exact head | Boundary | Merge outcome |
| --- | --- | --- | --- |
| PR246 | `1836c3243b86125f03718881918eaaa5c8f3bc00` | Dataset Stage0 copy and controller pre-claim readiness/metadata | Clean |
| PR247 | `a8033238904ee4c36b99df459a9fadb9caf5d76d` | Expert-only server.main receiver composition | One conflict in main's build_server call; retained exact incoming e928fc4 composition/call hunk; earlier health/media hunks preserved |
| PR223 Dataset receipt follow-up | `5fad3eb6c3e7b8a12ce2910e27adcaf6f830d4f7` | Metadata wheel-lock accepts exact Dataset placement | Clean; no package bytes/pins changed |
| PR248 compatible importer input | `3e1f6c948d7b1bd937eb5b443b87c292a0faab5f` | Exact importer/test delta on native baseline6d51bfd; main PR248 head d8fc88a9b2933834fe7fd4963f5fd9e008f225ee | Clean; only2 paths, not the old whole importer |
| PR249 | `5d5a50f19f181f7b9769f19156d9e3f78b1b7eff` | Dispatch status fastpath after included PR239 | Clean; explicit legacy dispatch Event sync over0d7fee, production unchanged; PR239 epoch fix retained |
| PR250 | `ba57b276293aaf88548c37d56b9dd99f9882cbe0` | Inert authenticated executor claim receiver | Clean; includes author's fixture import fix after initial1283b13; new files, not mounted |
| PR251 | `cfe026b20ebe31e8251e5061062b12c08df27b5a` | Same-client leased native Telegram crop bytes | Clean; final native binding guards and changed-boundary media tests/doc over ddb69be |
| PR252 | `f99e4f4fe3cde549572964db8c0bcf677c5d127b` | Persisted pending research continuation in native status | Clean; importer helper/callsite added on top of PR248 delta, not a whole-file replacement |
| PR253 | `592a2a2a3e87ebe64d9d5b166898aeb5c31b1ce4` | Current admitted native SQL health and status hook | Clean; health-only changes preserve dispatch/media/expert |
| PR254 | `ec1a82dee9fac4cdb0a4f63a188de1b6b2d8dddb` | Bound Expert claim/result workflow and expert-only server routes | Clean; only3 own paths added over already included PR247/250 |
| PR217 selected SQL prerequisites | `2991af968ad40bef9f6e43d181548fa25b7650c5` | Exact SQL015/016 and adapted ordered PG fixture | Only two new SQL files; no old whole-file source merge |

PR246 source depends on already included PR242/243. Its earlier fbdafb5
implementation passed CI39774529351 on both Python; the later1836c32 controller
wiring has a new run pending. PR247 brings its own main composition test;
no source outside the declared main hunk was replaced. This later combined
update requires its own full CI before any compatibility claim. PR246 controller
HEAD1836c32 passed full CI39774994627 on both Python. The next actual metadata
job checks both original HTTPS descriptors and the owner's selected Dataset
transport using the same actual native manifest and all55 wheels/165 entries.
Candidate `d2d0d91d00735d9117528607ca9ed442cfdb3dd7` passed CI39775995609:
3023 passed /45 skipped on each Python and actual metadata2 passed/0 skipped.
The later PR249 final-test update and PR252 continuation input require a fresh
combined run; source-only work continues before the next batched CI submission.
PR253 passed CI39778885562 on both Python; Python3.13 log2455 passed/32 skipped.
Its initial cancellation bug and own pending-startup fixture were corrected;
the separate inherited PR249 legacy observer race is reported to Linza for a
test-only synchronization follow-up (no production bypass or waived assertion).

## Ordered media/Story SQL prerequisites for revision2

The existing SQL013/014 Git blobs match PR217 exactly. Selected SQL015 and016
are copied without modification from PR217, with SHA256 respectively
`7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773` and
`f6bf7f45f8a716ebe59b73413d724725577319f6d78c2a57d4d751dc007eafb8`.
This enables the existing previously skipped PR241 native media PG tests.

`test_shkiper_ordered_013_016_pg.py` is the PR217 ordered rollback/catalog
fixture, adapted only to import the identical validated existing CI DSN helper
from the already included public media test instead of importing the separate
Story runtime stack. It applies real001–016 in the existing GitHub PG17 fixture,
checks preservation of older functions/Case/media relationships and rolls back
all DDL and fixture NOLOGIN roles. No Dev or live SQL is executed.

This does not register a new native warm-upgrade route, modify old SQL pins,
change the selected launch schema14 or claim installed SQL015/016. SQL017 is
reserved to RKB-Import's separately assigned revision2 implementation; this
source assembly does not edit that migration or its handlers.

CI39780127597 on4319a87 passed3076/35 skipped on both Python; the53 existing
native-media/ordered-SQL cases had0 skips/0 failures, including the formerly
missing real SQL015/016 prerequisites. The paired actual launch metadata passed
twice. CI39780501898 on6a91209 (adds Expert254) also completed all jobs SUCCESS.

PR249's event synchronization itself passed both variants on both matrices;
its3.12 full run39779548884 failed an unrelated existing editorial fixture:
`context` assumed async journal/post-preflight finished after300ms polling.
The diagnostic candidate replaces only that test helper with bounded actual
admission/task event observation; production authority, native deadlines,
manager role, command and receipt assertions are unchanged. A new combined run
must verify this helper repair; the preceding failure is retained as evidence.

## Explicit exclusions and next boundaries

- No canonical integration, service, notebook, Dataset or provider mutation.
- Health/WSS dispatch is deferred to a separate handoff; no latency patch was
  applied. PR240 contains an inert proposal, not an installed source fix.
- PR245 composition and Quartz's PR247 expert-only `server.main` mounting hunk
  are present in the diagnostic source. Installed read/send remains unverified.
- PR241's Dev archive bridge remains gated: Rif's PR251 native same-client bytes
  leaf is present, but protocol/transport/native-runtime composition is still a
  separate handoff. No real media read or Telegram send is claimed here.
- RKB-Import's reprocess/rechunk preflight is present via its compatible input;
  actual revision2 activation still requires ordered native SQL capability.
- PR223's S3 placement proposal remains unexecuted. Owner clarification selects
  reusable versioned Kaggle Dataset delivery for bootstrap dependencies. Dataset
  readiness precedes notebook launch via PR246's assigned source-only controller
  hunk. Actual Dataset placement/mount readback and deployment remain unverified.

The native assembly and synthetic decoder CI receipts do not establish Kaggle
ABI, real-book Saved, crop/Telegram/MCP pixels or installed product acceptance.
