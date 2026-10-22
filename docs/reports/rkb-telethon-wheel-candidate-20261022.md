# Telethon v1 wheel closure — GitHub-hosted candidate lock, 22 October 2026

**Decision:** 4-wheel closure **reproducibly built and verified for CPython 3.12 and 3.13**, but **NOT an approved Stage 0 runtime lock**: the self-built `pyaes` wheel has no approved immutable permanent HTTPS placement. No runtime installation, provider/Telegram connection, PG/model, main merge or release has occurred.

## Inputs, method, and exact evidence

The existing repository candidate manifests use per-package name/version, canonical wheel filename, HTTPS URL, *integer* byte size and SHA-256, with separately selected CPython ABI and complete transitive closure. There is no existing wheel-building workflow; the only preexisting mandatory `.github/workflows/ci.yml` runs application tests. This isolated work reuses the **wheel-lock metadata pattern** without modifying it. New branch-only `.github/workflows/rkb-telethon-wheel-candidate.yml` runs `scripts/production/verify_telethon_wheel_candidate.py` on separate GitHub-hosted Python **3.12.15** and **3.13.16** jobs.

Workflow receipt: [run 39698152544](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39698152544), **both jobs SUCCESS**, tested implementation commit `9191f6a43d8d37e15319aeac519c3d47203ce88d`.
CI evidence: [CPython 3.12 artifact 12391174848](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39698152544/artifacts/12391174848) (archive SHA-256 `914d843e73dd67d53de0b71a68844e20ef7fbd8b80a06f024d1d818ea6f6511d`); [CPython 3.13 artifact 12390783707](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39698152544/artifacts/12390783707) (`d4b0abd59566d8d4e78a42174bea750726305c1cd2db1321958201f52e803714`). Both temporary CI artifact links expire; **neither is an approved Stage 0 download location**.

CI reads official PyPI JSON release metadata, requires exact release and transitive dependency closure, checks `Requires-Python`, official URL and SHA-256, then downloads the three upstream universal wheels and the exact `pyaes` 1.6.1 sdist with verified declared byte sizes and SHA. **pyaes sdist SHA checked before extraction or wheel build**. Sdist: [PyPI pyaes 1.6.1](https://pypi.org/project/pyaes/1.6.1/), **28,536 B**, SHA-256 `02c1b1405c38d3c370b085fb952dd8bea3fadcee6411ad99f312cc129c536d8f`; the official release offers **no wheel**.

Build inputs are pinned `pip==25.2`, `setuptools==80.9.0`, `wheel==0.45.1`, `packaging==25.0`, `SOURCE_DATE_EPOCH=1700000000`, `PYTHONHASHSEED=0`, `TZ=UTC`, `LC_ALL=C.UTF-8`; two independent clean extractions and offline `pip wheel --no-deps --no-build-isolation --no-index` operations **per Python version**. All four wheel contents are hash verified, then installed only to CI-local target with `--no-deps --no-index`. Python tests import actual Telethon 1.45.0, RSA, pyasn1 and pyaes, inspect `get_messages`, `iter_download(request_size,chunk_size)`, `is_user_authorized`, `StringSession`, exercise a synthetic factory with all five device metadata fields and zero retry flags, and instantiate an actual client without connecting, with socket creation prohibited. **This exercises the documented factory shape, not the frozen private runtime's full authority/lease logic or a live Telegram connection.**

## Canonical four-wheel candidate

Structured machine-readable record: [`scripts/production/manifests/telethon-cp312-cp313-wheel-candidate-20261022.json`](../../scripts/production/manifests/telethon-cp312-cp313-wheel-candidate-20261022.json). The three official wheel URLs are exact `files.pythonhosted.org` release URLs within it; the fourth URL is deliberately `null`.

| Package | Wheel file | Exact size (B) | SHA-256 |
|---|---|---:|---|
| Telethon 1.45.0 | `telethon-1.45.0-py3-none-any.whl` | 804,098 | `339e24c83aedc9f12cbe389264627edc49779e4289c535aa90aafc4ce62f7972` |
| rsa 4.9.1 | `rsa-4.9.1-py3-none-any.whl` | 34,696 | `68635866661c6836b8d39430f97a996acbd61bfa49406748ea243539fe239762` |
| pyasn1 0.6.4 | `pyasn1-0.6.4-py3-none-any.whl` | 84,410 | `deda9277cfd454080ec40b207fb6df82206a3a2688735233cdcd8d3d565f088b` |
| **pyaes 1.6.1 (CI built)** | `pyaes-1.6.1-py3-none-any.whl` | **26,422** | `18115ebd82125cdf6100accc0bf88620b2e662a48710b1bb8cd7d9d4023964f3` |
| **Total** | 4 wheels | **949,626** | Each component pinned individually |

**Reproducibility:** both independent outputs in CPython 3.12 were byte-identical; both in 3.13 were byte-identical; the final pyaes wheel also has the **same exact byte count and SHA-256 across Python 3.12/3.13**. The three official wheel hashes and bytes also match independently in both CI artifacts. `cryptg` is an *optional Telethon extra* and deliberately not in the minimal closure. Telethon requires `pyaes` and `rsa`; `rsa` requires `pyasn1>=0.1.3`; other mandatory dependencies are absent in this pinned metadata. Note that `rsa` is archived upstream; future dependency security policy requires owner review.

## Integration boundary / required remaining acceptance

This JSON uses a **distinct candidate codec** and status `ci_verified_candidate_not_approved_stage0`, **not** the project's deployable `rkb.candidate_wheel_lock.v1`. To make it Stage 0 input later, the authorized owner must place the *exact verified* pyaes wheel in an approved permanent, immutable HTTPS artifact store with a recorded URL/size/hash/readback receipt; independently verify target ABI, complete existing external artifact inventory, dependency attestation and count cap, and issue a separately reviewed production lock. **Do not use GitHub's 14-day Actions URL as the permanent wheel URL or silently patch the runtime.**

Packaging count (from the previously supplied **sanitized** operator inventory, not from this CI): projected 102 source files including pending ordered helper + 59 external files + **4** new wheels = **165 top-level entries** if no shared-identity deduplication, i.e. five above current `MAX_PREPARED_FILES=160`. A bounded single-constant 160→165 proposal remains **review-only**, with 165 accept / 166 reject and full source/role/hash/provenance parity checks; **no cap changed here**. Finished notebook source length vs 1,000,000 B, final native installation and target dependency fingerprint remain **UNKNOWN**.

Only these additions were written in the isolated source branch: CI verification script, isolated workflow, small manifest, this report. **No wheel binaries committed to Git, no release, no merge, no Dev4GB or Kaggle install, no credentials, no Telegram provider calls, no PG/model and no edits to PR209/PR212/mandatory CI/runtime constants.**
