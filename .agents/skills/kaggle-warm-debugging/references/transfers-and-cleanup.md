# Direct bytes and temporary Dataset cleanup

Keep the existing control plane small. Prefer the existing WSS channel for commands, receipts and bounded diagnostics; avoid routing entire books, images or snapshots through DevCoveer merely because it controls the task. Do not turn bulk transfer into unbounded WSS buffers either.

Choose an already available authorized data path: Kaggle pulls a known source with a scoped short-lived grant, a client sends to an existing supported upload endpoint, or notebook and client exchange an authorized object-store reference. Reverse-direction artifacts can use the same separation. Do not assume public inbound networking on Kaggle or a generic ChatGPT-to-notebook upload capability exists. Reuse actual interfaces and report unsupported paths rather than building a new server or exposing a port by default.

For each transfer bind the caller's ACL/audience, operation identity, exact object, size/hash and expiry; validate downloaded bytes and acknowledge the verified result. A link alone is not a completed transfer. Keep grants/credentials out of logs and documents. Creating or expanding persistent access is a separate authorized action, not implied by this skill. Do not transform a private source into a public URL for convenience.

Temporary Datasets are a fallback, not the inner-loop patch transport. Identify each as temporary or retained recovery/DB snapshot before creation, with owner operation, intended consumers and completion condition. Track dedup/retry/recovery references so an apparently idle dataset is not deleted while a queued consumer or unresolved run still needs it. Notebook deletion or timeout alone is not proof that references ended.

When no authorized consumer/retry/recovery dependency needs a temporary Dataset, clean it up using the project's permitted provider operation and record its exact identity and observed outcome. Do not let disposable Datasets accumulate until quota exhaustion. Reconcile uncertain cleanup before repeating; do not substitute an unrelated dataset sharing a name. If permanent deletion needs additional approval, surface it instead of silently deleting. No deletion is authorized just by reading this reference.

Retained database snapshots follow the recovery retention contract, not temporary-input cleanup. Retiring an old baseline requires its replacement and retained tail/dependencies to be verified. Do not delete the sole recovery copy or remove input to claim a queued run was stopped.

Reuse boundary: [my-data-hub brokered_upload.py](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/checkpoints/brokered_upload.py#L292-L403) implements notebook-to-provider checkpoint upload through a scoped grant. It demonstrates that bulk bytes can bypass DevCoveer, not that generic client-to-Kaggle book upload is already implemented.
