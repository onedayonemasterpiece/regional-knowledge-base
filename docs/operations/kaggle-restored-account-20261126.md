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
new version creation requests deletion of prior version files. This deliberately
gives up Dataset-history rollback; it does not preserve exactly three previous
versions. Historical metadata rows can remain visible after their old payloads
are removed: after the real cleanup, downloading version472's base manifest
returned HTTP404 even though its number was still listed. Version473 reused the
same files as474, with no second archive upload. A history-row count alone is
therefore not a count of retained independent multi-GB snapshots.

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

The installation and cleanup are now complete. A new, separately recorded
retention intent reused the already verified package once. Both the normal
checkpoint473 and retention version474 passed native readback and became
verified Saved checkpoints at prefix23153. No old unknown finalize was retried.

## Rest and request volume

The existing queue requests idle drain after 1,800 seconds without useful work.
Idle shutdown must not itself generate demand for another notebook. Keep this
normal idle behavior; do not run synthetic queries to keep compute alive.
Long sessions retain the existing finite lifetime and checkpoint-before-stop
path. Provider Retry-After and quota responses take precedence over demand.
There is no separate mandatory hour-long cooldown under continuous genuine
demand in this query-only release. Define and verify that workload policy before
resuming mass import; idle shutdown is not a claim to have implemented it.

Both existing units are enabled for reboot. Existing systemd restart bounds
are three starts within300 seconds and a30-second restart delay; code77 does
not restart. No new service or agent timer was added. The account appeal's
false-positive finding remains the attribution, not any inferred resource cause.

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

The ordinary service restored version472, applied43 original accepted commands
and verified both PG and the search projection at23153 with no pending write.
The four trailing commands used their original identities. This is a longer
verified accepted prefix than the recovery output's23149. A fresh COUNT of every
domain table was not performed; the earlier66-source/10666-page totals belong
to the recovery report, not a new census of the current PG.

Three real OAuth/MCP evidence searches returned sources in1.0–1.6 seconds.
The connected app itself was then tested: its existing `search` returned a
sourced answer about the university's400th anniversary, and `fetch` read the
same passage with bibliography and page. That separate query took about8.5s;
these are measured examples, not a general latency guarantee. The earlier
`Unknown tool: search` failure was fixed by reusing the same existing authorized
source read declarations in the live profile, without enabling import methods.

The compressed recovery archive's full byte hash was checked independently.
The existing full PostgreSQL restore and SQL-probe evidence for that archive
comes from the recovery task. The current runtime's physical restore and native
verification were performed again on the selected Dataset plus its short tail.
Book-by-book semantic completeness was not re-reviewed. Imports remain off.

Source fixes and their exact full CI: PRs
[879](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/879),
[880](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/880),
[881](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/881),
[882](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/882), and
[883](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/883).
All were merged after both Python CI jobs passed. The installed compatible
release preserves the accepted-command compiler rather than blindly replacing
it with the integration compiler. The existing notebook was retained across
the final API-only release; no additional compute notebook was started for it.

Official references: [Dataset version publication and pruning](https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md),
[Kaggle notebook lifecycle](https://www.kaggle.com/docs/notebooks).


## Safe mass-import preparation requested November 26

Preparation does not itself enable readers. Keep the read-only MCP profile and
import-activity receiver disabled while staging the launch configuration.
Never automatically reactivate all historical `active` Launcher tasks: their
last reports include explicit refusals, finished books and unknown deliveries.

Reuse the existing run lifecycle: after three hours the current notebook drains
accepted work, performs its existing checkpoint-before-stop path, and ends.
A subsequent productive notebook cannot launch until one hour after the exact
native terminal receipt. A local lease expiry or Stop request does not start
that hour. Pending demand survives the rest and resumes through ordinary
maintenance. Failed cold starts that never reached READY retain the existing
finite startup-failure budget rather than gaining an artificial rest.
This is an owner workload policy, not a documented Kaggle quota or a guarantee
against moderation false positives. Queries share the notebook's off window.
The existing idle drain after thirty minutes without useful work remains.

Restore Launcher book cycles independently: no new turn after three hours,
finish the issued turn, then the existing full 35–90-minute pause. Preserve
book IDs, cycle clocks, accepted pages, unknown sends and explicit-refusal
fences. Development and other non-book jobs do not inherit these book rests.
Reader pauses alone do not prove the shared notebook is off; the native
terminal receipt and its one-hour admission delay enforce that separately.

Start with the owner-authorized ordinary six-book concurrency, not all
ten stale active tasks or temporary November 8 surge. Select six existing
eligible unfinished books; do not reissue an explicit refusal or accepted
page. A reader uses ordinary status and the accepted cursor, not a journal
replay or a second source reader. Increase concurrency only after measured
full-cycle throughput and storage headroom, under the owner's existing limits.

Use one private CPU notebook, one stable slug, the dedicated RKB credential
pair and existing shared admission. Preserve at least one query slot. On an
actual 401/403, stop that provider path; do not switch identities or turn a
denial into an empty Dataset inventory. Keep systemd's exit77 denial fence
and bounded restarts. Honor provider quota and Retry-After; do not quota-poll
or add keepalive queries. The controller's existing single launch intent and
exact receipts prevent parallel retry starts.

Checkpoint publication remains serialized by its existing owner. No per-page
save or duplicate finalize. Native readback and promotion, not upload alone,
advance Saved. `delete_old_versions=true` requests native removal of old
payloads; historical version metadata can remain visible. Keep the independent
recovery output and the original unknown intents. Do not claim a literal
three-row history or create rings of new Dataset names. All restore inputs
for future runs must resolve the current verified Saved, never deleted472.

Before actual activation, inspect the current PG/Saved prefix, current run,
source/compiler compatibility, private Telegram archive locator, and Dev disk
headroom. Restore intake only for the next ordinary matching worker; enabling
an API receiver does not give an already running query-only worker Telegram
capabilities. Do not rotate a notebook merely to test readiness. Stage full
import settings privately and leave them disconnected until launch.

Measure the first resumed work across a complete work/rest cycle: unique new
TEXT physical pages and SEM separately, accepted versus Saved, provider
versions and request errors, actual assigned slot-hours including rest and
outage, plus runtime-off duration. Packet speed is not sustained throughput.
A fresh capture/restore and exact terminal after productive import still need
live observation once import is actually authorized; local tests cannot stand
in for that outcome.
