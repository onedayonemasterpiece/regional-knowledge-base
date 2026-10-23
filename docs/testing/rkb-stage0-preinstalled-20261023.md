# RKB A3-c — preinstalled CPU dependency archive in an existing Dataset

**Status: source-only candidate. No archive was built on Kaggle or published to Dataset.**

Actual v127 environment observed: Ubuntu 24.04, CPython 3.13.15,
x86_64, glibc 2.39. Controller sets GPU/TPU disabled, and pins an
immutable Kaggle base image digest. Selected protected packages include
torch 2.11.0+cpu, transformers 5.16.1 and tokenizers 0.23.1.

## Existing facility and remaining cost

Stage0 already supports an exact-version kaggle-dataset://OWNER/SLUG/VERSION/file
locator, pinned size and SHA-256, controller preflight of the selected ready
version, safe copy from the Kaggle input mount and no network fallback.
However mounted wheels still trigger the existing offline pip installation
in fresh sessions. A wheelhouse alone is not a preinstalled environment.

## Opt-in artifact and one-time preparation

The new optional Stage0Spec.preinstalled selects exactly one immutable tar.gz
artifact, listed as an ordinary size/SHA-pinned Stage0Download whose URL is
the version-qualified internal Dataset locator. It replaces the wheel
downloads and sets wheels to an empty tuple; every other native/model pin
and owner binding remains unchanged. The existing Stage0 path applies
unchanged whenever preinstalled is absent.

Run the one-time packer only under the exactly selected CPU Kaggle image,
after one authorized installation of the pinned complete package closure.
The builder reads the installed distributions' RECORD-listed files, rejects
unsafe symlinks/import hooks, and creates a reproducible archive. It does
not launch the provider or call any upload/publish API.

Command (operator-only, illustrative paths; not executed by this PR):

    python scripts/production/prepare_kaggle_stage0_env.py \
      --site-packages /path/to/exact/site-packages \
      --packages-json scripts/production/manifests/kaggle-cp313-x86_64-wheels.json \
      --output /private/one-time/python-preinstalled.tar.gz \
      --artifact-name deps/python-preinstalled.tar.gz

The packer emits the archive name, size, SHA and Stage0 preinstalled
manifest/ABI pins, with no URL or credentials. Shkiper alone publishes
the bytes to the already owned Dataset, chooses its exact ready version,
and updates the trusted launch config to use that selected version.
The controller's existing Dataset preflight and WSS Stage0 handle it.

## Activation guarantees

The preinstalled plan binds exact CPython patch/cache tag, Linux machine,
libc and OS release, CPU wheel versions, package closure hash, unpacked
bytes/count, member manifest SHA and archive size/SHA. Once the existing
Stage0 copies pinned bytes, activation verifies every regular file and
complete installed distribution set before adding the private
site-packages overlay to Python's import path. Invalid ABI, archive,
symlinks, extra files, wrong hashes, partial closure or a non-Dataset URL
fail closed; **no pip or network fallback** is attempted in this opt-in mode.

## Acceptance boundary

Synthetic filesystem tests confirm one immutable versioned mount and
no network/pip activation, reproducible archive, mismatched ABI, missing
or corrupt input, symlink and GPU pin refusals. These are NOT live Kaggle
import results. Environment field agreement by itself does not prove the
native .so dependency closure; the same provider image digest must be
maintained and actual Torch/tokenizer/native PG/search smoke performed
by the sole live operator after publication. CPU-only means no CUDA
dependency is selected; GPU/CUDA changes require separate verification.

Source changes do **not** change restore/init/verify (A3-a), API services,
Dev journal, live runs, Dataset versions, deployment or credentials.
The earlier v127 cold profile reported Stage0 download 44.632s and
offline install 22.647s. No time saving has been measured with this
candidate; actual before/after cold stage timings belong to Shkiper.
