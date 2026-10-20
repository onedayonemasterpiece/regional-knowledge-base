# Version-qualified provider observation

The installed Kaggle 2.2.4 `kernels_status()` parses a requested version but does
not populate the native SDK request's `version_label`. Calling it with a version
therefore still observes the latest slug. The compatibility controller now uses
the existing SDK's `ApiGetKernelSessionStatusRequest` with an explicit `v<N>`
label and rejects unbound versions or an unsupported request shape. It never
falls back silently to latest-by-slug status.

During Stage 0, kagglesdk 0.1.37 accepted a version-qualified read with `v<N>`;
the corresponding bare numeric label returned 404. No launch, stop or deletion
was performed. This agrees with the upstream CLI's
[`_kernel_version_label`](https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/api/kaggle_api_extended.py)
normalization. Provider errors are observations, not proof of absent compute.

The response exposes a phase, not an independently verified execution identity.
A version-qualified COMPLETE is not alone permission to release a Telegram
lease or declare a scoped Stop complete. Apply the separate execution-binding,
freshness and contradictory-evidence gates in `kaggle_readiness` before target
runtime ownership/termination acceptance. These helpers do not repair all
legacy launch/expiry semantics or certify target serving readiness.
