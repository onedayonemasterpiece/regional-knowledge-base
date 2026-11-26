# Restored Kaggle account: bounded query operation

The owner's moderation appeal identifies the earlier restriction as an
automated false positive. Resource usage is a risk to control, not a proven
explanation for that restriction. No implementation can promise that a provider
will never flag an account again.

## Current scope

The owner requested existing-corpus questions and answers, without imports.
Use the existing `live` MCP profile and leave both import receivers disabled.
The source-backed live profile keeps the existing read-only `search` and
`fetch` names as well as `knowledge_search`, so connected clients with an
earlier tool catalog continue to work. It does not expose import tools.
There is one normal CPU runtime. Reuse its connection and queue; do not start
additional diagnostic notebooks, model processes or corpus replays.

Use only the dedicated `KAGGLE_RKB_USERNAME` / `KAGGLE_RKB_KEY` pair for this
account. A 401/403 must not become empty inventory or permission to create a
replacement Dataset. The controller exits with code 77 on these denials;
systemd's existing RestartPreventExitStatus prevents an authentication loop.
Never print credentials, signed URLs or private provider error bodies.

## Dataset history

The owner's ceiling is the current version plus at most three old versions.
Kaggle's supported create-version request exposes `delete_old_versions`, a
boolean, rather than a keep-last-three setting. RKB uses that native option:
each successful new version replaces the previous history, leaving the current
version and zero old versions. This deliberately gives up Dataset-history
rollback; it does not claim to preserve three previous versions.

Do not call Dataset deletion with a version number: it deletes the Dataset.
Do not create rotating Dataset names or accounts to circumvent quotas. No
separate pruning service, version counter, timer or repeated cleanup request
is needed. A failed or unknown create-version request is reconciled by its
existing operation identity and is never blindly resubmitted.

Before installing this retention change for the recovered corpus, finish the
ordinary physical restore and native verification, retain the independently
verified recovery notebook output, and verify a new Saved checkpoint. Preserve
accepted operation receipts and original unknown-publication packages. A green
PR does not prove provider history was pruned: read the resulting provider
version history after a successful ordinary publication.

## Rest and request volume

The existing queue requests idle drain after 1,800 seconds without useful work.
Idle shutdown must not itself generate demand for another notebook. Keep this
normal idle behavior; do not run synthetic queries to keep compute alive.
Long sessions retain the existing finite lifetime and checkpoint-before-stop
path. Provider Retry-After and quota responses take precedence over demand.

Reuse existing bounded status observation and the same in-flight startup ref.
Read local progress while restore runs rather than repeatedly asking Kaggle
quota or launching replacements. The checkpoint owner serializes captures;
pages do not each create a Dataset version. A lost acknowledgment is resolved
by exact provider readback, not a second finalize.

## November 26 comparison

Read-only metadata and hashed small manifests show restored-account Dataset
version 472 at accepted prefix 23110. The earlier recovery notebook output 40
contains prefix 23149 in the same database generation. Its 2,248,578,805-byte
compressed archive was independently streamed and SHA-256 checked on November
26; no second full replay was run. The recorded PostgreSQL restore and SQL
probe for that recovery output succeeded during the recovery task.

The ordinary service starts from version 472 and reconciles the short accepted
tail. It must report actual native coverage and query results before claiming
it has reached or exceeded recovery output 40. The four trailing pending or
unknown operations retain their original identities and guards.
