# Reusable Dataset bootstrap dependencies

Owner clarification: bootstrap dependency artifacts can be uploaded once to a
versioned Kaggle Dataset and reused by cold sessions. Keep its slug stable,
create a new version only when dependency bytes change, and wait for that version
to be ready before submitting the notebook. This does not change Dataset-primary
corpus persistence or the independent weekly backup.

## Changed boundary

`Stage0Download` now also accepts an internal locator:
`kaggle-dataset://OWNER/SLUG/VERSION/RELATIVE_FILE`. It is never sent to urllib.
The exact size, SHA-256, destination name and prepared inventory remain required.
The existing worker already calls `prepare_stage0` before imports/model load;
that path now copies a Dataset file into the existing working cache using the
same bounded streaming hash, disk reservation and atomic no-overwrite publish
as HTTPS inputs. Missing, unsafe or mismatched Dataset bytes fail without a
network fallback. Warm verified cache reuse needs no further mount read.

`Stage0Spec.dataset_sources` returns the sorted distinct version-qualified
`OWNER/SLUG/VERSION` dependencies. Two versions or owners sharing a mount slug
are rejected because `/kaggle/input/SLUG` could not disambiguate them. The mount
slug alone does not attest the provider version: attachment comes from the
controller; consumed bytes must independently match their pinned size/hash.
Symlinked mount roots/parents/files, nonregular files, hardlinks and effectively
world/group-writable source files are refused. Read-only Kaggle filesystems may
preserve permissive source mode bits, as in the existing Dataset-base reader.

The preparation receipt distinguishes `downloaded_bytes` from
`dataset_copied_bytes`/`dataset_copied_files`; neither implies provider readiness.
HTTPS inputs and the exact offline wheel installer retain their existing path.

## Integration handoff and limits

This branch depends on PR242 and PR243. dot explicitly handed off the source-only
controller pre-claim and metadata hunk after the initial Stage0 implementation.
The existing controller now attaches `list(target.stage0.dataset_sources)` and
checks the exact Dataset version's readiness BEFORE launch claim/push.
`kaggle_stage0_dataset.probe_stage0_datasets(api, plan)`
supplies the read-only preflight using the existing authenticated client and
`KaggleDatasetPublisher.observe`: private metadata, ready status, bounded
version-qualified file listing with pinned sizes, and a final stable observation.
A newer ready version does not replace or invalidate an explicitly selected
older immutable version. Only exact selected file metadata is checked; the
worker verifies the actual SHA bytes. Fixed pending reasons do not echo SDK
exceptions. The helper makes one bounded probe, never sleeps or submits effects;
the existing controller retries on a later tick, no more than once per30 seconds.
Until ready, the real queue launch stays pending; the original5-second loop
continues other reconciliation without a new blocking wait. Existing writer
guards and not-sent/UNKNOWN paths are preserved. The actual-main/real-queue
fixture checks warming→readiness→claim→push, cadence, unavailable observation,
writer refusal and lost push ACK without a second submission. This source wiring
has not been deployed or used to perform a provider action here.
Existing private launch configuration selects the Dataset locators. Do not
replace the chosen Dataset slug on each upload, use signed S3 URLs for these
files or make private artifacts public merely to satisfy the old HTTP path.

The pending artifacts are the existing 96378880-byte native archive,
955336-byte native manifest and 26422-byte pyaes wheel. Their previously recorded
SHA pins are retained; no dependency is rebuilt and no source book is included.
The previous S3 placement request was never executed and is not the selected
delivery scheme.

Tests use real temporary filesystem reads/copies with small pinned fixtures,
disable all network requests and installation, and cover cold copy, warm reuse,
content mismatch, missing/unsafe input, disk budget and ambiguous version binds.
Execution belongs on GitHub CI, not Dev. These are source/fixture checks, not an
actual Dataset upload, Kaggle mount verification or live product acceptance.
