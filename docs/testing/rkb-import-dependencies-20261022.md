# Full import dependency packaging

This leaf preserves the v4.1 PDF/DjVu, actual illustration pixels, facts,
Story.body and continuation requirements. Packaging is not product acceptance.
It does not change runtime, registry, launch composition or shared bounds.

`scripts/production/prepare_import_dependencies.py` calls the existing
`resolve_native_closure` and `assemble_native_collection` implementations.
The input Noble PostgreSQL17.11/pgvector0.8.2 lock is the existing captured
85-package candidate, not a newly installed system environment. Initial CI captured
Ubuntu Packages bytes and hashes and resolved exact djvulibre-bin
3.5.28-2ubuntu0.24.04.2 and all Depends/Pre-Depends, and rejects any collision
that would silently change an existing package pin. The resulting lock pins
every downloaded .deb by version, size and SHA-256. The captured DjVu lock from
CI39745705370 is committed byte-identically (SHA256
`2c4dd9be5a41e0f8cc64152c26d485aa7784924d4f01cd63b5451276f36edddb`).
Default CI uses this fixed input; `--refresh-djvu-metadata` explicitly captures
new moving indexes. `--native-lock` rebuilds an approved exact merged closure
without resolving moving metadata again.

The initial captured integration was UNRESOLVED: union103 exceeds current96 and
libhogweed6t64/libnettle8t64 select3.9.1-2.2build1 rather than baseline
3.9.1-2.2build1.1; liblzma5 selects5.6.1+really5.4.5-1ubuntu0.4 rather than
baseline0.3. `native-merge-review.json` preserves every complete conflicting
package pin alongside both full input locks. It declares `deployable=false`,
`manifest=null`, `elf_receipt=null`. Green receipt generation must not be
reported as a built or ELF-verified native artifact. The integration owner must
approve a coherent dependency lock and package bound, or a separately reviewed
executable ELF packaging plan; this leaf does neither implicitly.

The follow-up supplies retained dependency metadata for all103 desired packages
in `native-preserved-{ubuntu,pgdg}-Packages`, with full Depends/Pre-Depends/Provides
and original download pins. The accompanying metadata JSON binds both pruned
snapshots by SHA-256 and lists the exact earlier captured source indexes and
their hashes. Only public package metadata is committed, no executable bytes.
The existing resolver verifies constraints on CI; the adapter then requires all
85 original package version/architecture/size/SHA/URL pins unchanged. It does
not hand-edit the three collision versions or pretend dependency edges suffice
to prove their constraints. Default receipt production emits the coherent
`native-package-lock.json` if resolution succeeds, alongside both earlier input
locks and the remaining unresolved96-package bound. No cap change or native
manifest/ELF readiness is implied by a resolved lock.

The existing assembler retains the PG native data and libraries and adds
ddjvu plus loader/library paths. The existing safe extractor materializes and
checks the manifest on the disposable GitHub runner. The existing `kaggle_djvu_elf.pinned_djvu_elf_closure` verifies ddjvu's
PT_INTERP, explicit pinned loader and recursive DT_NEEDED. CI overlays only
that unchanged file from the reviewed leaf622e5b8e591f6ecf36404078eb41fb659b9e2cae,
SHA256904b782aabd7af8a798c608655ed4671d60249ffc370ba0501d80081b2db96b9,
identical to frozen e58. No duplicate ELF parser, host `ldd`, ambient library
resolution or maintainer scripts are used.
This proves a byte-bound ELF closure, not Kaggle execution or decoding quality.
If the complete closure exceeds the existing 96-package bound, CI saves the
exact lock and explicit unresolved bound receipt; it never removes packages or
changes the shared runtime cap.

The Telegram input preserves Linza's reviewed candidate codec and evidence
(CI39698152544, source9191f6a). The adapter explicitly produces the existing
Stage0 codec `rkb.candidate_wheel_lock.v1` with exact package key `Telethon`,
only when supplied an approved permanent content-addressed pyaes URL receipt
binding its size/SHA-256. Until then `stage0-wheel-lock.json` has a different
unresolved-input codec and `runtime_lock=null`. No hosting URL is invented.
All51 PDF wheel entries remain byte-for-byte unchanged; four Telegram wheels
are appended. This does not solve the integrator-owned prepared-file budget.

Only `.github/workflows/rkb-import-dependencies.yml` runs focused tests and
native packaging. No Dev installs/builds/PG/models, Kaggle push, runtime
artifact upload, credentials, or deployment occur. The artifact is retained
in GitHub CI for14days, so it is not permanent Stage0 hosting. Test fixtures
use fictional placement receipts and cannot authorize real publication.
