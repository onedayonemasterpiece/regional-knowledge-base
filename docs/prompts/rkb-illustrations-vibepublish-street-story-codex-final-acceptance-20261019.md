# Retired handoff: illustration / VibePublish / Street Story acceptance

**Retired entry point, 2026-10-20.** The coding agent receives one current file: [RKB scalable Kaggle implementation](rkb-scalable-kaggle-implementation-20261019.md). Do not start a second rollout from this former prompt.

The complete historical task, its then-current release/PR states and measurements are preserved at the [immutable a8cf version](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/prompts/rkb-illustrations-vibepublish-street-story-codex-final-acceptance-20261019.md). That historical authorization/status snapshot is not a command to redeploy an old release, reopen closed findings or reuse the old corpus.

## Product behavior that remains protected

- Natural-language source search → exact accepted figure/caption evidence → `illustration_fetch` → real MCP ImageContent, with actual MIME, registered media origin and source/page/figure identity; Telegram availability is not gated by custom rendition hash comparison. No generated facsimile or inferred same-page association substitutes for the image.
- The current [owner alignment](../designs/rkb-owner-alignment-20261021.md) requires direct Kaggle Telethon uploads and downloads without VibePublish/Dev bulk proxy. Originals use topic2; illustrations use topic4, photo by default and high-resolution document only when needed. Success is accessible media at its registered locator, not a full download/hash readback.
- Stable publication/read keys, bounded pending/recovery, verified fallback after natural cache eviction, late-callback fencing and no duplicate sends. Preserve lessons about bounded recovery, unknown sends and fair progress, not the former mandatory provider-digest/Vibe protocol. The old exact implementation remains historical evidence only.
- Keep Vibe full-frame/Original, existing compositor behavior and Street Story owner integration intact. Record actual process/release identity and native readback, not just merge/CI success.
- Public accepted illustration renditions are available to everyone. Restricted proof/page/crop scans use current owner/demo/expert/local_historian `scan_proof_read`; full files stay independently private. Verified categories materialize audited grants under [rights.md](../rights.md), not a render-time category bypass; claimed status, expiry/removal and manual revocation are respected. Validate artifact class and actual geometry, current withdrawal/policy, shared Slack `public_only` and separate cache scopes. No owner impersonation or additional per-figure editorial approval is introduced.
- Multi-item output retains successful allowed items while others are pending/denied. Preview/render dependencies live until localization callbacks terminate; delivery ACK alone does not permit their GC. Slack transformations can change a delivery hash without changing source pixels, so keep hashes and verification claims separate.

## Evidence and current scope

The [live archive acceptance](../reports/illustration-archive-live-acceptance-20261019.md) and [audit remediation](../reports/illustration-audit-remediation-20261019.md) retain real prior readbacks and closed findings. These do not prove fresh book import, PG proof access, direct Telegram/Kaggle reset recovery or all-book semantic quality. Current requirements are in [illustration operations](../operations/illustration-pipeline.md), [archive operations](../operations/telegram-archive.md) and the single handoff.

The target uses PostgreSQL/pgvector+BGE on Kaggle, one DevCoveer controller/WSS and bounded durable recovery. **Only GEO migrates.** Old books/chunks/vectors/stories/drafts/contributions/cases do not transfer; fresh MCP import reconstructs the non-GEO corpus. Preserve infrastructure identities/configuration and GEO provenance/IDs without treating historical manual work as a required active transfer or permanent backup.
