# Rights, source policy and artifact access

Updated 2026-10-20. This is the normative target policy, not a claim that the current runtime already enforces it. The [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) defines the rollout. The owner has authorized the public illustration policy below; it requires no further per-book or per-figure editorial approval.

## Separate the resource classes

| Resource | Target access | What that access does not grant |
| --- | --- | --- |
| Accepted, active book bibliography and searchable source text | Public research projection, without a per-book owner read filter | Original files, proof pixels, private drafts or writes |
| Accepted illustration rendition | Public for everyone, including a shared Slack audience | Adjacent source prose, quote/page scans, source archive locators or full files |
| Quote stripe, source-page scan, source crop and localization preview | Canonical owner and authorized `demo`, `expert`, `local_historian` participants through the existing `scan_proof_read` capability | Full PDF/DjVu or unrestricted source download |
| Original PDF/DjVu and recovery objects | Independent existing private source/resource authorization | No grant follows from text search, a public figure, POI membership or `scan_proof_read` |
| Personal drafts, draft history and restricted contributions | Their current actor/resource scope | Neither a shared Story variant nor a case-manager role opens another person's draft |

The participant categories already exist. When `demo`, `expert` or `local_historian` is recorded as `owner_attested` or `independently_verified`, atomically materialize its existing `scan_proof_read` grant with the category/profile change. Record the audit event and originating category revision; `claimed` creates no grant. A demo-derived grant expires with the demo category. Removing/expiring a category disables its derived grant. Respect manual revocation: neither reading nor replaying the same category assignment recreates a revoked grant. Any later restoration requires an explicit authorized grant action with audit.

The renderer checks the current capability/grant, artifact scope, expiry, revocation and delivery audience, not an alternative category-name bypass. Canonical owner access retains its existing authorization; never impersonate that owner. Other categories do not gain the capability merely by searching public material. No new IAM, role hierarchy or per-image approval is introduced. Transport authentication, where required by MCP, is separate from per-book content access.

## Artifact class and actual pixels

An artifact's authoritative class is set at creation from the accepted source graph, operation purpose and validated geometry. A client cannot relabel a page, arbitrary crop or quote proof as an illustration. A public rendition contains only the accepted figure and its approved intrinsic content/caption area. It excludes surrounding unrelated source text, marginalia and private source material. A genuine full-page plate or map may be an illustration when that is the accepted figure; page size alone neither grants nor denies public status.

Keep the original figure/caption evidence, source SHA/revision, physical page, geometry, transform and rendition hash. Source text, printed caption and model observation remain distinct. Creating the rendition under the authorized policy is not a new manual approval gate.

Public illustration, restricted proof and full-original artifacts have separate cache/delivery identities. Include class, source revision, geometry/rendition, policy version and applicable audience/scope in their keys. Never satisfy a public image request from a cached full-page proof with a narrower label. Validate actual output geometry as well as the model input; a highlighted region over an unrestricted page still exposes that page.

## Authorization and current policy

Authorize the requested class and scope before source retrieval. For an authorized nonowner proof request, the renderer may read the exact original through a narrow internal purpose-scoped service context. Do not impersonate the owner in `quote_proof`/`source_archive`, make arbitrary source downloads public or replace the existing source-owner check with unconditional service access. The service returns only the requested authorized artifact.

Recheck active source/revision, withdrawal, current policy, grants and delivery audience on cache hit, retry, locator callback and final delivery. An archive entry, old receipt, signed URL or stale snapshot is not permanent authorization. Source withdrawal stops new derivative delivery as well as text retrieval; revoke accessible cache/URLs according to their serving contract. Already downloaded public bytes cannot be recalled, and the service must not claim otherwise.

Shared Slack requests use the existing `public_only` audience. They may return public text and public illustration renditions. Do not union personal grants from channel members or use a private participant capability to attach proof scans to the group. A permitted private response remains bound to its recipient and purpose.

## Rights metadata is evidence, not the access switch

Store physical/source ownership, provenance, publication rights and the product's versioned access policy separately. Keep the existing rights vocabulary:

`unknown`, `restricted`, `licensed`, `permission_granted`, `public_domain_candidate`, `public_domain_verified`, `statutory_access_verified`.

The owner-approved public illustration projection is an explicit product policy. It does not turn `unknown` into `public_domain_verified`, assert a license or invent collection/legal evidence. Record the policy ID/version, resource class, source/collection scope, owner's decision and effective time; retain authentic rights evidence unchanged. If a current withdrawal or policy restriction applies, enforce it and report the reason. Do not require a second per-figure legal/editorial approval to implement the authorized default.

Verified legal/institutional statuses still require their actual supporting evidence. Publication year, language, nationality, ISBN and a model hypothesis do not establish those statuses. A work, edition, uploaded copy and illustration can have different provenance. A source file may contain private annotations even when an illustration rendition is public. A policy change reevaluates visibility and cached derivatives; it does not require retranscribing or re-embedding unchanged source text.

The former rights-policy discussion, including collection-specific examples, is preserved in the [immutable 19 October document](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/rights.md). Its generic verified-status gate is not the target authorization rule for the owner-approved public illustration projection.

## Data cutover and acceptance

Only GEO content migrates under the [GEO cutover contract](migrations/rkb-layers-only-cutover-20261019.md). Old books, stories, personal drafts, contributions and cases are not copied into the target. Their functions and access distinctions remain available for newly imported material. Infrastructure identities, grants/configuration and service registrations are control-plane state, not old content to discard.

Acceptance must exercise public image delivery to a caller without a book grant; denial of surrounding proof pixels/full originals to that caller; permitted owner/demo/expert/local_historian proof; atomic verified-category grant creation; claimed-category denial; expired demo, category removal and manual revocation surviving subsequent reads/replays; shared Slack `public_only`; a mislabeled page crop; warm-cache withdrawal; and a late result generated under an obsolete policy. Validate actual bytes/MIME and rendition geometry, not only metadata flags.
