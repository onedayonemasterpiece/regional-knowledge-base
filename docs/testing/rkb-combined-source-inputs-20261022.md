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

PR246 source depends on already included PR242/243. Its earlier fbdafb5
implementation passed CI39774529351 on both Python; the later1836c32 controller
wiring has a new run pending. PR247 brings its own main composition test;
no source outside the declared main hunk was replaced. This later combined
update requires its own full CI before any compatibility claim. PR246 controller
HEAD1836c32 passed full CI39774994627 on both Python. The next actual metadata
job checks both original HTTPS descriptors and the owner's selected Dataset
transport using the same actual native manifest and all55 wheels/165 entries.

## Explicit exclusions and next boundaries

- No canonical integration, service, notebook, Dataset or provider mutation.
- Health/WSS dispatch is deferred to a separate handoff; no latency patch was
  applied. PR240 contains an inert proposal, not an installed source fix.
- PR245 composition and Quartz's PR247 expert-only `server.main` mounting hunk
  are present in the diagnostic source. Installed read/send remains unverified.
- PR241's archive byte issuer remains gated and assigned to Rif. No real media
  read or Telegram send is claimed by this receipt.
- The new reprocess/rechunk work belongs to RKB-Import and is not yet an input.
- PR223's S3 placement proposal remains unexecuted. Owner clarification selects
  reusable versioned Kaggle Dataset delivery for bootstrap dependencies. Dataset
  readiness precedes notebook launch via PR246's assigned source-only controller
  hunk. Actual Dataset placement/mount readback and deployment remain unverified.

The native assembly and synthetic decoder CI receipts do not establish Kaggle
ABI, real-book Saved, crop/Telegram/MCP pixels or installed product acceptance.
