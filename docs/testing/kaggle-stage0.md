# Pinned Stage0 preparation

`kaggle_stage0.py` is a standard-library-only preparation boundary for the
existing worker. `load_encoder` calls `prepare_stage0(prepared_model, stage0)`
after the WSS HELLO/control attachment and before bootstrap/torch imports.
Target launch composition supplies the manifest; this helper does not discover
or select model revisions, native builds, package versions or download sources.

The frozen `Stage0Spec` contains download records (relative name, HTTPS URL,
exact bytes, SHA-256, executable flag), local wheel names, exact package versions,
installation disk reservation and one total deadline (default 900 seconds).
Wire decoding rejects unknown fields and coerced bounds. Every download must
match the final prepared-model artifact inventory. Wheels belong to its code
inventory, outside the exact model directory. Existing prepared artifact and
loaded dependency/model checks still run after Stage0; its receipt cannot
substitute for readiness or runtime attestation.

Preparation writes only below the exact `/kaggle/working` root. It checks all
existing files and the missing-byte plus install reservation before network
work. Directory descriptors and no-follow opens reject symlink redirection;
private exclusive temporary files are streamed in at most 1 MiB chunks, checked
for exact size/hash, and published atomically without overwriting. A mismatched
existing file is preserved and fails preparation. Exact cached files need no
network. No unknown files or previous run outputs are removed.

HTTPS redirects remain HTTPS, reject userinfo, have a five-hop bound and reuse
the original deadline. Intermediate response bodies are closed without an
unbounded drain. Downloads have no ambient proxy routing or credential
configuration. Errors use fixed codes and receipts contain no URLs. The helper
permits at most 128 files, 16 GiB per file and 64 GiB total declared bytes; these
are safety caps, not claims about actual account capacity or model performance.

When actual `importlib.metadata` versions already equal the declared map,
installation is skipped. Otherwise, one isolated pip invocation receives only
local verified wheels with SHA-256 hashes through a private requirements file:
`--no-index --no-deps --only-binary=:all: --require-hashes --no-cache-dir
--no-compile`. No index lookup, source build, dependency resolver fallback or
second installation attempt occurs inside this call. Actual versions are read
again before success. The helper keeps a private successful plan-hash/version
manifest under Kaggle working; temporary installer inputs belong only to that
invocation and are removed afterward.

`tests/test_kaggle_stage0.py` uses fake HTTPS streams and a fake installer under
a managed central artifact test root. It covers exact streams, hashes/sizes,
cache reuse, existing-file preservation, symlinks, final inventory binding,
disk reservation, redirects, deadlines, offline flags and post-install versions.
It performs no real downloads, pip installation, model inference, PostgreSQL or
provider operation. Live Stage0 downloads/install/runtime validation remain
separate acceptance evidence.
