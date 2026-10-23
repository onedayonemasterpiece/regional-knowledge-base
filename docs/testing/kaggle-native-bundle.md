# Pinned native PostgreSQL package collections

The controller's native preparation path uses two top-level code artifacts:
a deterministic outer tar of pinned `.deb` packages and a pinned JSON native
manifest. `NativeBundleSpec(manifest_name, manifest_sha256)` is the small launch
reference. The existing 128 top-level artifact and 64 KiB configuration limits
remain intact; thousands of native files belong to the nested manifest.

`kaggle_native_recipe.resolve_native_closure` consumes captured, hash-checked
same-release PGDG/Ubuntu Packages snapshots. Exact root versions remain fixed.
It resolves all Depends and Pre-Depends, alternatives, version constraints and
virtual providers, treating no image package as already present. Its output
locks package versions, URLs, sizes, SHA-256 and metadata source hashes. This is
candidate metadata, not signature verification, observed image ABI, ELF loading
or a PostgreSQL readiness result. Unsupported or missing dependencies fail.

`build_native_manifest` streams already pinned local `.deb` archives without
installation or execution. Explicit include prefixes select native binaries,
shared libraries and share data. It derives every selected file's hash/size and
the exact uncompressed archive size. `assemble_native_collection` writes the
deterministic outer tar and version-2 manifest and returns both top-level pins.
Local public artifact preparation belongs in a managed central artifact task.
Before larger copies, reserve the source/output size and leave runtime headroom.

`prepare_native_bundle(root, manifest_name, manifest_sha256,
inventory=verified_top_code_inventory)` runs inside the existing target
preparation lifecycle after Stage0. It requires the exact `/kaggle/working` root
and independently observed OS ID/version, machine and glibc name/version to match
the manifest. The manifest and outer archive must match the verified top code
inventory. Each contained `.deb` must match its nested pin, and each final native
file must match its exact source package/member, size and SHA-256.

The helper never runs apt/dpkg, maintainer scripts, a compiler, a PG binary or a
model. It ignores control archives. Archive paths, headers, decompression memory,
bytes, deadlines and file counts are bounded. Sparse/special files and path
escapes are rejected. File aliases resolve only to pinned regular targets;
directory aliases become explicit copies of pinned regular leaves. No real
symlink/hardlink is exposed inside the native prefix. Root-relative package
links are interpreted only within that pinned archive namespace, never read
from the host filesystem.

Private temporary directories are checked completely before atomic Linux
`renameat2(RENAME_NOREPLACE)` publication. Existing mismatched or unknown paths
are preserved and rejected. Reuse rechecks the full manifest/archive/file chain;
only this invocation's incomplete directories are cleaned. Package and extracted
native caches retain their exact pins. Disk checks include nested archive and
extracted-byte reservations; the helper does not remove other caches for space.
Limits are 96 nested packages, 8,192 native files, a 4 MiB manifest, bounded 256 MiB
native files and 4 GiB declared total bytes, with at most 900 seconds per plan.

The retained `VerifiedNativeBundle` exposes `root`, `prefix`, `manifest_sha256`,
exact `(absolute_path, size, sha256, executable)` pins and `current()`. It is not
a JSON success flag or a user permission. The existing OwnedPostgres boundary
rechecks this capability before execution, grants only explicit native files
and their parents the service group's read/execute access, sets the owned
library path, and verifies actual pg_config path relocation. Model files and
launch secrets receive no such grant. A verified byte bundle alone does not
prove the complete service can start or query.

PGDG package data uses xz; current Ubuntu dependencies also contain zstd. Python
3.14 offline preparation uses its standard bounded zstd decoder. The CPython
3.12 target requires the exact Stage0 `zstandard==0.25.0` wheel before native
preparation. Its C API takes the maximum window in bytes despite the Python
documentation's KiB wording; the explicit bound is 128 MiB. There is no ambient
zstd command/library probing or installation fallback.

On 2026-10-20, public metadata and source archive bytes produced two candidates:

| Candidate | Packages | Native files | Extracted bytes | Outer tar bytes |
| --- | ---: | ---: | ---: | ---: |
| Ubuntu 22.04 / glibc 2.35 | 90 | 3,466 | 477,255,759 | 92,948,480 |
| Ubuntu 24.04 / glibc 2.39 | 85 | 3,487 | 505,769,039 | 93,255,680 |

Both pin PostgreSQL 17.11 and pgvector 0.8.2. Actual package inspection located
initdb/postgres/pg_config under `usr/lib/postgresql/17/bin`, vector.so under
`usr/lib/postgresql/17/lib`, and vector.control/vector--0.8.2.sql under
`usr/share/postgresql/17/extension`. No package was installed or executed during
assembly. Provider image labels identify 24.04, so Noble is the launch candidate;
actual target ABI and native execution still require the live preparation
readback. Tests cover synthetic archive variants, inventory binding, ABI mismatch,
cache tampering, links, publication races, builder hashes and bounded decoding.
