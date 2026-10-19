# Disagreement journal implementation

Status: **in progress**, against the complete owner scope in
[design v3.3](../design/rkb-disagreement-journal-design-20261018.md),
[PMI](../testing/rkb-disagreement-pmi-20261018.md) and
[60-item audit](../reports/illustration-critical-audit-claude-20261019.md).
The [acceptance matrix](../testing/rkb-full-implementation-status-20261019.json)
retains all 58 KB checks, nine E2E routes and 60 audit entries. Partial component
evidence is not a PMI PASS or live acceptance.

## Current implementation

`disagreement_journal.DisagreementJournal` is the domain storage layer, using
the **existing corpus SQLite file**, not a parallel registry or a story card.
Its additive tables retain cases and immutable versions, exact source evidence,
independent versioned contributions, conclusions, per-case events and atomic
operation receipts. Typed input models reject extra actor/expert/confirmation
fields. Actors come from authenticated `Principal` and existing active RKB UUIDs.

- Shared case revisions use compare-and-swap. Independent contributions append
  without overwriting each other or requiring a shared content revision.
- One atomic write transaction commits the event, domain change and receipt.
  Exact-key replay survives a process restart; operation status resolves a lost
  create response without requiring a previously returned case ID.
- Case grants never grant source access. Current source ACL applies to every
  evidence version and derived history; receipt replay/status repeats access
  checks after revocation. Access checks and reads share one SQLite snapshot.
- Evidence validates exact document/source SHA/revision/page/region and literal
  character span. Accepted transcription match is separate from visual proof;
  the domain layer never invents a visual or human confirmation.
- A contribution's author may correct or withdraw it through a new immutable
  version. Even the case owner cannot overwrite another person's words.
- A conclusion records exact source/contribution versions and a material-event
  boundary. A concurrent later argument remains visibly unreviewed. Individual
  opinions cannot replace the shared model synthesis automatically, and model
  text acquires no human support from the fact an MCP account saved it.
- Per-case event pagination includes every retained version. Consistent SQLite
  Online Backup preserves source references, grants, contributions and receipts.

This layer has **not yet been exposed or deployed**. It does not claim completed
participant authority, source-proof grants, scan/lease fences, BGE case retrieval,
public export, Slack/VK delivery, test-run compensation or Astro pages. Those
remain required parts of the same objective, not waived follow-ups. Import writes
must not be routed through this layer until scan context and generation fencing
are connected atomically. No production database or live bot has been modified
by this component work.

## Verification

Synthetic domain tests exercise independent concurrent writers, exact replay,
revocation, source ACL separation, quote/revision rejection, preserved correction
history, conclusion snapshots, pagination, rollback on a crash before commit,
forged input rejection and actual SQLite backup/restore. They do not impersonate
real Slack users or replace the nine required E2E routes.
