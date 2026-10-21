# Native disagreement Cases (ordered schema 010)

The reusable public payload shapes come from the existing PR151
`adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4` contracts. Its SQLite journal/draft
implementation is not a target authority and is not imported. No old cases,
contributions or drafts are migrated. This module alone does not enable tools or
claim native/live acceptance.

Resolved commands have stable case/object/operation IDs before acceptance, actor
ID equal to the canonical accepted command person, explicit case/object CAS
preconditions, immutable model-authored input, exhaustive current source
content/eligibility dependencies and trusted attribution. Human reading/support
is never inferred. Payloads are bounded to 64 KiB; larger commands are explicitly
unsupported, never clipped. Historical schema/version validators remain exact.
A later combined schema must explicitly bind compatible handler revisions; merely
registering schema9 and schema10 routes together does not make both executable
under the current single semantic revision barrier.

The four fixed internal routes reuse the existing spool/accepted dispatch:

| Route | Scope | Additional current Participants capability |
| --- | --- | --- |
| disagreement.edit.v1 | shared case | case_contribute for create/evidence/own contribution; maintain_shared_summary for revise/state/model synthesis; personal_conclusion for individual opinion |
| disagreement.access.v1 | shared restriction | maintain_shared_summary plus native manager role |
| disagreement.draft.v1 | actor-private case/object | personal_conclusion |
| disagreement.publish.v1 | shared case | personal_conclusion and case_contribute |

Every route also requires current `case_read`. The trusted intake derives this
from the exact validated action, never a caller-provided capability. Native case
roles and per-object authorship are independent additional checks. Case managers
and the global owner do not gain another author's private drafts. Draft versions
stay outside shared history; explicit selected-span publication must atomically
create only the exact selected contribution and retain the private original.

Evidence must match the retained source original SHA, active content revision,
actual accepted source region/page IDs and exact Unicode quote offsets. Current
source eligibility applies to case text, evidence, derived conclusions, history,
receipts and search representations. All retained historical dependencies apply;
removing a link cannot launder derived content. Exact-source text matching is
`accepted_text_match`, not visual or human verification.

Fresh9 source manifests retain physical page indices and exact native region
geometry. Accepted evidence copies the matching page and region provenance from
that immutable manifest, including printed page labels and native block offsets.
These fields do not assert visual or human verification. Structured author/title
metadata likewise comes from retained typed fields, never parsed bibliography.

The target startup binds the existing `disagreement_get`, `disagreement_edit`,
`disagreement_contribute` and `disagreement_conclusion` tools only when an exact
schema10 or explicit compatible schema11 Case handler is installed. `CASE_READ`
shares the authenticated ephemeral resource and performs no token counting or
embedding. Its whole native record is bounded to96KiB; pages contain at most20
records and return continuation positions. No source dependency is invented for
an empty private Case.

The current output gate compares native Case role/ACL/event/source scope against
a compact projection of exact verified successful journal commands. Draft access
also requires the exact author and current personal-conclusion capability. The
native reader reconstructs complete Case projections from the applied ledger
before reading; forged heads, grants or bodies fail verification. A cold physical
restore without the compact Case projection remains closed until full history is
verified. Pending own draft operations do not change book counters or another
person's shared Case view.

Writes retain model-authored inputs in the same short control spool, seal them
through the existing remote accepted journal, and enqueue the existing fixed
source-command executor. `operation_status` resumes the same stored command and
predecessor after an unknown result. Pending/accepted is not Saved; only a native
applied outcome with a verified durable remote receipt produces `saved=true`.
Current participant, Case and source checks remain active at final MCP issuance.
After raw control-spool retention, `operation_status` accepts the exact
`operation_key` alone **for an original CREATE whose response was lost**. It
derives only that CREATE's `case_id` from the canonical actor-bound key, then
verifies the retrieved native PG receipt, exact immutable CREATE command and
remote sealed outcome; a derived candidate is never evidence by itself.
For non-CREATE operations supply the existing `case_id` after raw retention;
no unscoped all-case enumeration or new SQLite receipt registry is introduced.
The same bounded `CASE_READ` checks current actor/case rights. Recovery also
requires the retained compact remote receipt and exact readback of the original
command and remote outcome, matching the native result, current database
generation and canonical actor. Native `native_only`, missing remote bytes or mismatches cannot produce
Saved. A retry of the same CREATE key **after** raw spool collection consults
the retained compact receipt before reserving a new sequence or applying a
new command; it returns the same verified outcome only when the **full original
caller envelope** matches: action, typed model payload, Case and object IDs,
case_revision and object_version. Changed payload, expected revision or draft ID
with the same key is an explicit conflict. The check is repeated under the existing single-writer
control lock to close the racing GC/retry window. This recovery path performs
no enqueue, publication or replay effect.
Current Case/source and participant revocations apply on readback and again at
final output. The current native operation function exposes successful applied
receipts only; rejected preconditions after raw retention remain unavailable
rather than being inferred from missing native rows.
This implementation and synthetic tests do not establish deployed, physical-phone
or non-owner live acceptance.

Case search/BGE projections, public exports/discussions, exact human support and
research source requests are separate required product paths. No lexical-only
case result may be called hybrid acceptance. These remaining domains are not
silently routed to a SQLite journal or a lexical-only success result.
