# Full import dependency packaging

This leaf preserves the v4.1 PDF/DjVu, actual illustration pixels, facts,
Story.body and continuation requirements. Packaging is not product acceptance.
It does not change runtime, registry, launch composition or shared bounds.

`scripts/production/prepare_import_dependencies.py` calls the existing
`resolve_native_closure` and `assemble_native_collection` implementations.
The input Noble PostgreSQL17.11/pgvector0.8.2 lock is the existing captured
85-package candidate, not a newly installed system environment. CI captures
Ubuntu Packages bytes and hashes, resolves exact djvulibre-bin
3.5.28-2ubuntu0.24.04.2 and all Depends/Pre-Depends, and rejects any collision
that would silently change an existing package pin. The resulting lock pins
every downloaded .deb by version, size and SHA-256. `--native-lock` rebuilds
that exact closure without resolving moving metadata again.

The existing assembler retains the PG native data and libraries and adds
ddjvu plus loader/library paths. The existing safe extractor materializes and
checks the manifest on the disposable GitHub runner. A bounded `readelf`
walk verifies ddjvu's PT_INTERP and recursive DT_NEEDED against manifest-pinned
files; host `ldd`, ambient libraries and maintainer scripts are never used.
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
