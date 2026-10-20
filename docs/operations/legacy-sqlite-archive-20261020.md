# Private legacy SQLite archive — 2026-10-20

The retained **pre-cutover** snapshot is stored in the private Kaggle Dataset
[RKB Legacy SQLite Archive, version 2](https://www.kaggle.com/datasets/zigomaro/rkb-legacy-sqlite-archive/versions/2)
(dataset ID `12740886`). Native provider metadata confirmed `is_private=true`
before and after exact-version downloads. Mandatory license metadata is `other`;
this is restricted backup storage and grants no redistribution rights over books,
private drafts or any other legacy material.

This archive preserves the original SQLite corpus and its original pending
operations/receipts as recovery data. It is **not** a target PostgreSQL restore
base, a source import, an authority activation, or evidence that the new store
works. It is not attached to the runtime notebook. No OAuth state/key or
credential files were uploaded. No source, runtime or local backup was deleted.

## Verified bytes

Snapshot capture: 2026-10-20 20:28:19 UTC, before cutover. The source is the
operator-validated consistent SQLite backup, not a copy of an open database file.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Uncompressed legacy SQLite database | 114008064 | `e909c0cd4eeda4e198e7a09a4ca908d979c1bab456017cb9061a2be4a1d655f3` |
| `legacy-corpus-precutover-20261020.sqlite3.gz.bin` | 30076859 | `78accf2c4e4322eb3faf2d930a06adec50aa7f368b0a139e9a8a9fc318722155` |
| `archive-manifest.json` | 1425 | `5b75c16e351b292edf1fd741824f3a5f9abdf8a99af193b654bb534e6e71a0bb` |
| `schema-metadata.json` | 53207 | `505874a7e1c19d5937ce2a6c91fb142a13b2d1b4e8509537ceca0d29e052e563` |
| `README.md` | 1163 | `feb612c855787d06788f88eb86efd5ed4ea9438d382cc7a2775675cea43784b7` |

The exact version-2 native file listing matched all four names/sizes. Every file
was downloaded through the version-qualified provider API and hashed. Streaming
gzip decompression of the downloaded bytes produced exactly the uncompressed
size/hash above. The validated source database passed SQLite `integrity_check`
and had zero `foreign_key_check` violations; exact uncompressed byte equality
binds those checks to the downloaded archive. This does not measure application
restore RTO or establish target service readiness.

The `.gz.bin` file is gzip with an opaque suffix. Kaggle automatically expanded
`.gz` during version-1 processing; version 2 preserves the compressed bytes.
Version 1 remains private and retained, but its automatically expanded file set
is not the verified compressed-file manifest above. Do not use latest-slug
resolution as a substitute for the explicit version and hashes.

## Publication and reconciliation evidence

One stable private Dataset create intent was persisted before the first create,
with retries disabled at the create boundary. The first create's local result
assertion left publication uncertain. Read-only native metadata reconciled that
same Dataset identity as private version 1, READY; no second Dataset was created.
After observing the automatic expansion, a separate exact version-2 intent was
persisted and one version-create request was sent, with old-version deletion
disabled. Version-2 verification completed through native metadata/file listing,
all-file download and streaming uncompressed hash verification. No unknown
publication was retried and no alternate slug was used.

Retained private operator evidence is under
`/home/dev/artifacts/regional-knowledge-base/20261020T203021Z-private-legacy-sqlite-archive-20261020/`:
`preparation-receipt.json`, `upload-intent.json`,
`readonly-create-reconciliation.json`, `version2-intent.json`,
`version2-state.json` and `archive-receipt.json`. Raw private database/archive
bytes stay outside this repository. No signed provider URLs belong in docs.

## Final cutover and later disk release

Keep the current legacy runtime/database/backups until the new store is proven
and the operator coordinates the final boundary. Pending operations may be
retained with their original recovery state and exact release; cancelling or
copying their bodies into the new store is not required to preserve them.
Automatic legacy workers and model-authored continuations have different
recovery routes; keeping rows alone does not prove that the target MCP exposes
an old continuation tool. Preserve their original identities/receipts and
review that route before retiring its runtime.

At actual cutover, quiesce all writers of the old store, take a final consistent
snapshot, publish a separately identified private version with new manifest
pins, and repeat exact-version download/decompression/SQLite checks. Document
that final version and verify the current target store, current authority and
pending-work recovery disposition. Only then may the operator apply the owner's
old-store removal decision and release Dev disk. This version-2 receipt does not
itself authorize deleting an active database, backups or unresolved operations.

The tiny current authority/OAuth files remain independent of this content
archive. Never replace current rights/revocations with archived stale identity
rows during recovery, and never use an archived lease/token as a new execution
authorization. Restore old recovery data only into an explicitly owned isolated
location, check current canonical actor and original content ACLs, and preserve
accepted semantic choices rather than rerunning them.
