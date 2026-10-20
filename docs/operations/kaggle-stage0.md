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
