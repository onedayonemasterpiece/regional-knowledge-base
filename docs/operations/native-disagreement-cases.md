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

Case search/BGE projections, public exports/discussions, exact human support and
research source requests are separate required product paths. No lexical-only
case result may be called hybrid acceptance. Public tool activation must wait
for actual native ACL/source provenance, current grant/restriction history and
final output issuance integration on the existing typed ephemeral channel.
