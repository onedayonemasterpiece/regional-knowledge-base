# Kaggle Stage 0 evidence and validation

The existing controller can use `regional_knowledge.kaggle_readiness` as pure
validation helpers. This module does not launch, stop, poll, install, allocate a
slot, or establish adapter capability by itself. A successful fixture is not a
serving, Telegram, deployment, or live provider acceptance receipt.

Before attaching a run, preserve the controller's launch intent and exact owned
slug, provider version, logical run ID, nonce and runtime incarnation. Preserve
a native execution ID when exposed; keep it `None` when unknown. Never derive it
from a timestamp or config. Runtime validation compares the authenticated READY
identity and all six expected SHA256 values: `code`, `launch_config`,
`executed_source`, `dependency`, `model`, and `schema`. HELLO alone is not READY.
A competing incarnation with the same nonce blocks attachment and slot release.
The controller must retain the original incarnation and provide the competing
instance IDs; the helper is deliberately stateless.

Target ATTACH now reports bounded Python version/cache tag, machine, glibc and
OS-release metadata before Stage0 or model loading. The authenticated channel
logs it as `kaggle_bootstrap_environment` with run/incarnation/connection identity.
This is an observation for choosing independently pinned artifacts; it grants no
READY state, permissions or lifecycle terminal proof. Legacy workers can omit it.

[`kaggle-cp312-x86_64-wheels.json`](../../scripts/production/manifests/kaggle-cp312-x86_64-wheels.json)
is a **metadata-only candidate**, not an observed Kaggle ABI or deployment.
It fixes 50 complete transitive package versions and one compatible wheel per
package (259,629,367 compressed bytes), including the protected CPU/BGE versions.
The resolution used uv 0.12.15, CPython 3.12 and
`--python-platform x86_64-manylinux_2_28 --only-binary :all: --torch-backend cpu`.
PyPI/official PyTorch metadata supplies SHA256, URL and size; the Torch size was
read by HTTPS HEAD. No wheel or model was installed or executed on DevCoveer.
Select it only after observing the actual target ABI. Native server libraries,
expanded install space, exact installed dependency fingerprint, imports and
resident BGE inference remain separate mandatory checks. The candidate does not
silently authorize latest-version resolution or a different model space.

`CapabilityReceipt` records selected installed package/version, adapter path and
evidence reference. Flags default to unverified. Call `validate_capabilities`
with capabilities required by the operation, rather than treating unsupported
features as a universal startup error. Stage 0 must separately prove exact
status execution semantics, version-qualified output recovery and SDK automatic
pagination, scoped stop behavior, and writable paths outside published output.
Metadata inspection alone cannot set these flags. No extra paginator or cancel
API is introduced here.

Declare required small probe artifact names, sizes and hashes in the controller
before work. `verify_outputs` compares the manifest against that inventory and
independently verifies each downloaded member's identity, actual bytes/hash and
native readback attestation. Missing artifacts remain incomplete despite provider
COMPLETE. Validation errors contain fixed codes, without private names/payloads.
The default budget is 128 artifacts and 16 MiB, with safe relative names. Bound
transport before allocating bytes; this validator only bounds inventory/hash
work after download. General large or dynamically enumerated product inventories
must use the existing controller's own declared completeness rule, not weaken
this small Stage 0 probe's inventory into an empty success manifest.

`validate_terminal` accepts only a fresh native provider terminal observation
bound to this exact execution, after its last heartbeat and without contradictory
runtime evidence. If the provider exposes only a version label, Stage 0 must
prove that status identifies this execution rather than a repeated session of
that version. Without that proof, including when the SDK is absent, termination
stays UNKNOWN. STOP ACK, deletion, inventory absence, child exit, lease expiry,
socket loss and Telegram revocation cannot prove compute termination. The
controller retains compute capacity ownership after any failed validation.
Telegram authorization revocation/use handoff remains a separate verified gate.
Late heartbeat handling, durable terminal monotonicity and bounded reconciliation
remain responsibilities of the existing controller.

## Implementation evidence (2026-10-20)

Read repository `AGENTS.md`, handoff sections 5.4 and 14.5, and the installed
skill at `/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging/`
(`SKILL.md`, `references/rkb-profile.md`, `references/lifecycle.md`). Source
worktree started at `85676094cf1a019fd34a29daa930d4ebb16eedb7`.

Read-only metadata inspection of the existing project Python 3.14 venv found
psycopg 3.3.6, psycopg_pool 3.3.3 and pytest 8.4.2; no Kaggle SDK/CLI or Torch
distribution metadata was present. This is local dependency evidence, not the
notebook's installed environment. No environment secrets were read, provider
calls made, packages installed or model runtime started.

Real exact-status semantics, pagination, output transfer, scoped Stop, writable
runtime/output separation, HELLO/READY, Telegram authority, cold bootstrap and
warm edit-to-useful timings: **NOT RUN**. Local regression fixtures cover wrong
latest versions, missing/corrupt output, unsafe paths, unknown stop, stale
terminal evidence and competing runtime incarnations.

## Tokenizer materialization after the first native launch

The first target launch on 2026-10-20 attached successfully with Ubuntu 24.04,
CPython 3.13.15 and glibc 2.39, then failed before PostgreSQL startup with
`loaded_tokenizer_settings_mismatch`. The protected Transformers/tokenizers
versions materialize legacy tokenizer JSON into a different backend structure;
raw JSON equality is not a valid loaded-state check.

`ResidentEncoder` now independently reloads the tokenizer from the same
hash-verified local tokenizer/config files, with the protected package versions,
local-only loading and remote code disabled. It compares complete materialized
backend state, special-token/configuration metadata, batch-one inputs and exact
input IDs. Identical explicit counting calls clear per-call padding on both
handles; no backend fields are excluded. The raw tokenizer SHA, model revision,
embedding space and 512-token bound remain unchanged.

An independent counter for ingestion must reproduce this protected HF
materialization. Loading only the raw JSON can produce different counts for
whitespace and special tokens. A tokenizer-only reproduction uses no Torch,
model weights or embedding inference. These checks do not establish native PG
readiness, public hybrid search, accepted imports or product acceptance.

## Cold private runtime parent

Actual target v89 failed with `FileNotFoundError('/kaggle/temp')` on 2026-10-20,
after resident encoder preparation and before any native PG readiness receipt.
`OwnedPostgres.start_empty` resolved the configured cluster parent strictly;
the provider image had not created that parent. This failure does not establish
a running PostgreSQL server or target readiness.

`ensure_private_runtime_root` now creates only the fixed session-only
`/kaggle/temp` beneath the already existing trusted `/kaggle`. It rejects
symlinks, unexpected ownership and group/world-write permissions, probes actual
supervisor writability, and checks traversal for the configured PG identity.
When a root supervisor owns a nonroot PG child, a newly created parent is
`0711`; the child can traverse it and cannot create entries there. Cluster and
socket directories remain exclusive `0700` leaves with the exact native UID/GID.
Snapshot workspaces use the same check. Existing clusters are never adopted or
overwritten, and missing caller-supplied nested parents remain an error.

The cold-root regression reaches the exact initdb invocation boundary using
synthetic files and a stopped fake command, with no PG, model or provider call.
It also checks restrictive umask, native traversal, unsafe parents and exclusive
retry refusal. Stage0 installation and native bundle temporary directories are
under the existing verified `/kaggle/working` root; they do not depend on
`/kaggle/temp`. Native PG startup and useful reads after this patch still require
an actual independently reviewed bootstrap receipt.

Bootstrap profiling emits fixed `bootstrap_stage` start/complete/failed events
for artifact download, offline installation, model/tokenizer loading and its
provenance guard. Durations use the worker's monotonic clock and carry only
the run ID, fixed stage/state and seconds; source content, signed URLs and
exception messages are excluded. A complete model stage is distinct from PG
readiness or a successful public query. These events instrument future runs;
they do not retroactively measure v88/v89 or establish a latency improvement.

Actual v90 completed download (60.085 s), installation (21.330 s) and protected
model/tokenizer loading (44.948 s, including its 4.758 s tokenizer guard), then
reported `native_pg_start_failed`. PostgreSQL exited with code 1 and retained
428 bytes of private startup diagnostics; no bootstrap observation arrived.

A bounded Ubuntu 24.04 fixture used the exact 85 pinned Noble packages and
PostgreSQL 17.11 binaries without a model or corpus. With production-style
search-only code directory permissions, initdb succeeded and postgres failed
while opening its native library directory. Giving the configured PG group read
and search permission on verified code directories made startup and a native
version query pass. All three owned fixture containers were removed; aggregate
runtime was 25.439 s. This reproduces a concrete permission defect and validates
its correction; actual Kaggle PG readiness after the correction remains unverified.

The grant remains limited to explicit verified public-code parents and files.
It adds no group write or world access, changes no file owner, and leaves the
working root, model and private configuration permissions unchanged. Startup
logging classifies directory-read denial with a fixed code and no path content.

## Notebook source envelope

The complete shipped Python/SQL package is transported as bounded compressed
JSON inside the existing private notebook source. The generated decoder rejects
corruption, extra streams and expansion beyond 3 MiB before extracting files.
Every extracted file still passes the original byte/hash inventory checks.
The generator rejects notebook source above 1,000,000 bytes before provider
submission; the complete launch configuration retains its separate 64 KiB limit.

The 2026-10-20 fresh11 candidate measured 1,562,414 source bytes before this
change and 622,363 after it, with the same 132 prepared entries and nine command
handlers. These are local generation measurements, not provider acceptance or
PG/BGE readiness. The original HTTP error did not retain a provider response,
so a provider size rejection remains a hypothesis.
