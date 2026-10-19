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

The full MCP profile now registers typed `disagreement_participants`,
`disagreement_get`, `disagreement_edit`, `disagreement_contribute` and
`disagreement_conclusion` when the existing SQLite authority and trusted
`RKB_OWNER_SUBJECT` are configured. Live and story profiles do not acquire these
tools. Authentication is resolved independently for each call. Participant
policy and source/case ACL are rechecked inside the same mutation transaction;
no caller-supplied actor or channel identity is accepted.

`participants.Participants` extends existing `rkb_users.id`, not bibliographic
authors or a second IAM. Profiles, explicit expiring grants, scoped domain
weights, external bindings, immutable audit snapshots and operation receipts
share the corpus file. Categories and credentials do not grant capabilities or
source ACL. Administration requires the canonical owner on the owner route;
even the same person on the expert route cannot import or administer. Disabling
an RKB user, disabling/expiring its participant profile, revoking its capability
or external binding is checked on later operations and delivery authorization.
Existing corpus search/ingestion authentication has not been converted into a
multi-user OAuth service by this component.

External accounts begin pending. A live provider/tenant/subject tuple is unique;
Slack permits explicit owner-confirmed exact binding with audit. VK requires a
positive person ID and a receipt from a trusted provider verifier; a community
ID or owner attestation is insufficient. Projects Hub actor IDs remain external
identities. Observation receipts are bound to the pending link revision,
audience and expiry and consumed atomically. No client-facing operation creates
an observation. Real provider verifier adapters remain to be connected.
Contributions retain the binding revision and assigned weights at write time.
Incorrect bindings are revoked and flag retained contributions for attribution
review without rewriting their original payload.

This RKB backend candidate has **not been deployed**. It does not claim completed
source-proof grants, scan/lease fences, BGE case retrieval,
public export, Slack/VK delivery, test-run compensation or Astro pages. Those
remain required parts of the same objective, not waived follow-ups. Import writes
must not be routed through this layer until scan context and generation fencing
are connected atomically. No production corpus database has been modified
by this component work. The manual case tools are not an import scanner; adding
the scanner's writes still requires the generation/fragment fences.

## Slack installation

[Kaliningrad Base manifest](../../deploy/slack-kaliningrad-base/manifest.json)
defines a separate Socket Mode bot for mentions and personal messages. It has
no user scopes, channel-history access or permission to write to unjoined public
channels. Socket Mode additionally needs an app-level `connections:write`
token, and the app must be installed in the intended workspace. Store bot/app
tokens in protected service configuration, never the identity table, repository
or chat. The manifest alone is not proof of app creation or installation.
See Slack's [manifest reference](https://docs.slack.dev/reference/app-manifest/)
and [Socket Mode guide](https://docs.slack.dev/apis/events-api/using-socket-mode/).

The owner-authorized Slack connector test on 2026-10-19 sent a message into the
supplied test thread, then independently read it back as the authenticated owner
with the ChatGPT attribution. This resolves the sender-transport uncertainty,
not full research intake/delivery or non-owner E2E. Private channel/user IDs and readback
are retained in the managed implementation evidence, not this public document.
The available Slack CLI was authorized as the same owner in the intended workspace.
Its manifest validation succeeded; it created and installed the separate app,
and `auth.test` confirmed its distinct bot identity. The bot was invited into
the owner-specified channel. A finite transport test then received a real
owner-authored mention via Socket Mode, committed the event before ACK, sent an
automatic connection-check reply from the bot and independently read back the
same thread. Provider event/request/message receipts are retained privately.
Bot/app credentials were saved only to protected service configuration (0600).
The temporary CLI provisioning hook was restored and no secrets were committed.

This finite check is **transport acceptance**, not a continuously running bot,
dot wake-up, verified participant binding in production or full research E2E.
The persistent authenticated request/executor/result/outbox adapter remains
required; app creation and the test response do not waive it.

## Verification

Synthetic domain tests exercise independent concurrent writers, exact replay,
revocation, source ACL separation, quote/revision rejection, preserved correction
history, conclusion snapshots, pagination, rollback on a crash before commit,
forged input rejection and actual SQLite backup/restore. They do not impersonate
real Slack users or replace the nine required E2E routes.

Additional participant and actual MCP calls check temporal grants, owner versus
expert routes, pending/verified/revoked links, wrong attribution, consumed/expired
proof, unique account bindings, current rights before replay, concurrent CAS,
transaction rollback and per-call actor isolation. All full-PMI entries remain
NOT_RUN until their prescribed actors, transport and outcomes are actually used.
