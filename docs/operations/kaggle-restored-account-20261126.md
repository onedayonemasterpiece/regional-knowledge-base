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

The owner's ceiling is the current version plus at most three old payload
generations. Kaggle exposes `delete_old_versions`, a boolean, rather than a
keep-last-three setting. Historical version metadata can remain visible even
when its unique old file bytes have been removed.

Ordinary capture uses `delete_old_versions=false`: the previous verified copy
survives while the new archive is read back and checked. After two ordinary
verified checkpoints, the existing checkpoint owner publishes a metadata-only
copy of the newest verified package with native pruning enabled. It reuses
all four exact uploaded files and performs native verification/promotion; it
does not capture or upload another PG archive. Successful normal cycles leave
at most two old payload generations before cleanup, reserving one slot for
provider visibility. This is retention policy, not a Kaggle quota claim or a
guarantee about delayed external operations.

The pruning request has its own existing seed-table operation key. Its unknown
submission is read back by that same key; no newer checkpoint or schema
publication can pass it, because a delayed destructive request could delete
a newer base. Ordinary imports and reads are not globally paused by this
publisher fence. A predecessor mismatch detected before create is known not
sent and may rebind through the existing pending state. A lost response after
submission never does. The original older, non-pruning unknown intents remain
intact and do not gain a new global block.

Do not call Dataset deletion with a version number: it deletes the Dataset.
Do not create rotating Dataset names or accounts to circumvent quotas. No
separate pruning service, scheduler, persisted counter or new phase is used.
The owner lock, writer guard, pending/publishing/published/verified receipts,
native readback and Saved pointer already own this work. A current Dataset
that disappears is not silently recreated by ordinary checkpoint maintenance.
Native401/403 metadata/file denials exit77 and use the existing systemd fence.

The separately verified recovery notebook output remains the independent
fallback. The current local accepted journal must be preserved. Green CI and
this policy do not replace actual verification of the first resumed work/rest
cycle or a periodic independent backup of later imported data.

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
advance Saved. A verified metadata-only copy requests native removal of old
payloads; historical version metadata can remain visible. Keep the independent
recovery output and the original unknown intents. Do not claim a literal
three-row history or create rings of new Dataset names. All restore inputs
for future runs must resolve the current verified Saved, never deleted472.

The corpus, scans, models and PostgreSQL working data remain in Kaggle. Dev
keeps its bounded raw spool and durable control journal. The root operator
incorrectly turned a shared-volume20% free-space observation into a demand for
20GB of additional import space. That new activation condition was withdrawn
after the owner challenged it. It was not a measured import requirement.
The overall protected capacity criteria remain separate and are not declared
passed by this correction. No paid resize was performed.

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


## November27 preparation delivery (Kaliningrad time)

The owner has replaced preparation-only and six slots with an actual staged
rollout: one ChatGPT window, then two, gradually up to four. Original
35–90-minute book rests remain, together with three-hour notebook work
periods and one full hour off measured from the sealed native terminal.
Exact refused duplicate tasks remain excluded without resending prompts.

PR884 passed both Python jobs and the notebook-envelope check, merged as
5775716397582def68c6b1f8b58775a7842a6440. Compatiblef51b5552 was installed
in the existing API/controller units. Native281 had already ended normally;
no notebook was started for the host release. Existing Saved474/base23153
and all original unknown publication intents were retained.

Independent backup of Saved474 completed at01:07 on November27: four files,
4,671,112,994 bytes in the existing private Yandex Object Storage bucket. The
PG archive is3,920,465,920bytes, SHA-256
5814fba864fec5bdba259fd35fc375ef566f81cb3501ba88a11544e11bc71f2f.
Source size/hash were checked while streaming; S3 object metadata was read
back, and the final index was written last and then read successfully. No
archive was stored on Dev, no new PG capture or Kaggle publication occurred.
The first operator readback mistook metadata key `Sha256` for missing
`sha256`; the second invocation reused the already uploaded archive and only
completed the remaining files/index. A PG restore from this independent S3
copy has not been run; the identical source Saved474 had native verification.

The existing native Codex wake mechanism can perform the weekly copy. It
reuses the same immutable Saved manifest key: unchanged Saved means no new
archive transfer. Do not add a service, keepalive notebook, second reader,
new Dataset ring, credential identity or repeated finalize. The next weekly
backup deadline is December4 at01:07 Kaliningrad; a pending native receipt
will be required before claiming that future work is scheduled.

Disk cleanup removed11 own clean merged worktrees and34 inactive immutable
source releases after exact ancestry and live-reference checks. Git history
and active/rollback releases remain. This recovered2,890,000,000bytes;
about3.5GB was available afterwards. Production data, retained recovery
evidence, accepted journals, private configurations and other projects were
not deleted. Earlier identical synthetic test bytes were deduplicated while
preserving their paths and content.

A restart exposed another precise issue: the same manifest reused by verified
versions473 and474 retained first coverage473 in the local metadata cache.
The restart selector required equality with474 and exited. Reading the exact
verified474 manifest and reconciling its local alias restored the API.
PR885 removes that unnecessary version equality while preserving verified
coverage, current Saved identity, generation, cursor, SHA, size and ABI checks.
Two distinguishing cases fail before the fix;56 affected compatible tests
and32 integration restart tests pass. Invalid/future/unverified coverage
remains rejected. PR885 subsequently passed both Python CI jobs and the
notebook-envelope check, merged as b464d898, and compatible4509e24d was
installed. A real restart succeeded with the original manifest coverage473
and current Saved474.

At01:25 Kaliningrad the existing full-import configuration was activated.
Launcher114 added a single process concurrency setting, limited to1–4;
stage one is set to1. Launcher115 fixed the existing replacement path for
a specifically proven pre-Send failure in a book continuation. Unknown
post-Send delivery and refusal fences remain unchanged. Both changes passed
exact CI and merged; compatible relay bbcdad33 is installed.

The driver itself created and sent the first Hinz continuation at01:33,
and the model reported progress. Its ordinary demand launched native282.
At01:39 it was starting with generation1/channel1; accepted prefix remained
23153, with no new TEXT or SEM receipts. Root did not dispatch a reader,
press Send, read original pages or start a diagnostic notebook.

The next acceptance is new ordinary TEXT and SEM plus a native-verified
Saved covering those operations. Only then advance the existing concurrency
setting to2 and subsequently up to4. Actual notebook terminal, the full
hour off and ordinary resume still have to be observed. Existing Saved474
and the independent backup remain intact; historical pages are not new
import throughput.


At02:17 Kaliningrad on November27, native282 is Ready and Hinz has22 newly
accepted physical pages with both TEXT and SEM, above the old five-page
baseline. One semantic rejection was corrected by the ordinary reader.
Concurrency remains1.

Kaggle has Ready475–477, but local verified Saved remains474: advancing
imports caused the deployed checkpoint selector to skip readback of a
committed snapshot with an earlier prefix. Main already contained fix
d198c5ce; it was omitted from the compatible runtime release. PR886 contains
compatible b7312a9e through an ancestry-only merge whose integration tree
equals main b464d898. All77 focused compatible tests passed; full CI is
running. Deployment has not started. Acceptance will be ordinary native
verification and promotion of the existing Ready package, without another
upload or notebook rotation. No request to resubmit an unknown publication
was made.


At02:50 Kaliningrad PR886 had passed both full CI Python jobs and merged
as9307734c. Compatible b7312a9e was installed in the API, with the notebook
and controller unchanged. The ordinary native readback of existing483
started, then returned snapshot_io_rejected with failure_code unclassified.
Local Saved remains474. From02:52 the same notebook began closing its
channel and reconnecting; the cause of that failure has not been established
from the available receipts. At03:01 the first book had47 new physical
pages with TEXT and SEM. Its next stage operation is SENT_UNKNOWN and must
not be replayed manually. Concurrency remains1; rollout to2–4 is incomplete.


## November 27, 11:48 Kaliningrad: import and durable-save acceptance

The earlier Saved474 observation above is historical. At09:09 UTC the
ordinary system had promoted Saved501/base23623, and native285 was READY.
Passive APPLIED receipts counted182 new unique physical pages since the
restart baseline23153, all with TEXT and SEM. Two additional semantic
completions concerned older TEXT pages and were not counted as new pages.
The native verification of later snapshots succeeded; the earlier282
transfer failure is not established as a deterministic recurring defect.

Two completed notebook cycles had actual off periods of3673.549 and
3607.900 seconds between observed sealed terminal and next ordinary launch.
No operator Stop or forced rotation was used. Hinz and Kiseliūnaitė later
received explicit client safety rejections; their rejected calls were not
resubmitted. The ordinary driver proceeded with other authorized books.

The existing Launcher setting was advanced from1 to2 at09:14 UTC. The
second ordinary reader83584524 was delivered and reported model progress.
By09:46 UTC Städtehandbuch had322 TEXT/SEM pages against its308-page
baseline, adding14 new physical pages. Native-verified Saved503/base23644
contains eight of those new pages, physical314–321, with both components.
The first selected book is on its existing rest until10:18:04 UTC; its
clock was not reset.

The count3 transition was attempted but stopped before changing settings:
a different browser delivery was still claimed. The actual concurrency
limit remains2. Next acceptance is ordinary third-reader delivery, new
TEXT/SEM and native-verified Saved, followed by the fourth window.

PR887 passed both Python CI jobs and its envelope check, then merged.
Compatible ce51b8d4 was installed only in the existing controller for the
next ordinary worker. Current worker285 and APIb731 were unchanged. The
four fixed transfer failure codes improve error classification; they do
not repair the still-unidentified cause of the earlier IO disconnect.


## November 27, 13:38 Kaliningrad: four readers delivered

The concurrency setting is now4. All four ordinary reader jobs have been
delivered and reported model started: Gubin/Строкин, Glinski, Kunde des
Samlandes and Chronicle of the Prussian Land. Passive receipt counts are
63/63,185/185,99/99 and312/303 TEXT/SEM respectively. These are cumulative
book totals, not new-page counts. The fourth reader has not yet added a
new physical page; its initial activity points to earlier semantic review.
Latest native-verified Saved508 covers accepted prefix23847.

The fourth slot was initially delayed by an inherited8 November book
cycle. On re-enabling the slot, advanceBookRest assigned another35–90min
rest despite weeks without work. PR116 fixes this normal expiry rule:
when no issued turn or packet works and the existing maximum90min rest
has fully passed since last recorded work, that rest is already served.
No manual clock or queue edit is needed. Fresh rest and unfinished-turn
guards remain. Production net10 lines; fictional RED in two stale-cycle
cases,196 compatible and213 integration tests passed. Exact CI45990709712
passed; main0edcf208 contains compatible46175b0, installed at11:28:52UTC.

The ordinary driver then created the fourth job. Chrome stopped its first
submission before Send; the existing proven-unsent handler created one
fresh-chat successor, which was delivered and reported model started.
The original blocked receipt remains. No operator UI, Send, dispatch,
resubmit, notebook Stop or forced rotation was used.

Remaining acceptance: new fourth-reader TEXT/SEM, native-verified Saved
covering those operations, and continued operation across scheduled rests.
The API and running native worker were not changed by this relay release.


## November 27, 14:22 Kaliningrad: accepted-command drain defect fixed

All four readers produced accepted work. Chronicle added one new physical
page312 with TEXT23977/SEM23978 and filled semantic coverage for two older
pages. At the three-hour boundary, the controller requested ordinary
drain285 at11:52UTC. The API then rejected its accepted source-command
binding at kaggle_source_commands.py:414, closed WSS4403, and the exact
worker log reported server revoked worker authority. The exact controller
terminal receipt was ERROR at11:53:06UTC. This is a source-binding defect
during planned drain, not an inferred Kaggle restriction or CPU crash.

PR888 reuses existing allow_draining for the exact accepted-command lookup
and validation. Fresh public reads/import admission still closes; run,
incarnation, connection, database, writer, snapshot and schema checks remain.
Two production lines changed, net0. The distinguishing actual-queue test
failed on old code. Local affected checks had103PASS and four unrelated
PDF tests failed to import missing pymupdf; integration new test passed.
Full CI45996100028 passed on both Python3.12 and3.13, envelope45996099863
passed, and main9685b035 contains compatible51b7cf81.

The compatible host API was installed at12:21:32UTC, PID1402656, during
the existing exact terminal OFF interval. Health passed; controller and
Kaggle were not restarted, and13 retained publication packages were kept.
The real hourOFF ends12:53:06UTC/14:53Kaliningrad. No provider mutation,
manual reader, Stop, forced rotation or unknown resubmission was used.

Latest verified Saved510/base23899 does not yet cover Chronicle23977/23978.
Text23986 remains SENT_UNKNOWN; its original receipt must be reconciled,
not manually replayed. Remaining product checks are ordinary successor
restore/physical verification, new Saved covering the accepted suffix,
all four readers continuing, and the next drain completing its checkpoint
without this source-binding4403. Deployment alone does not complete those
checks.

## November 27, 15:10 Kaliningrad: retained archive exists, locator failed after pruning

The controller launched286 after3606.962 seconds of real OFF. Its restore
never read the archive: the host requested deleted Dataset version510 and
received404. Retention had published the identical four files as Ready511
before deleting older versions, but verification/promotion was interrupted.
Read-only reconciliation of511 passed, including the 4043663360-byte PG
archive; its download link passed the existing size/origin/ETag checks.
No archive body was downloaded to Dev and no provider mutation was sent.

PR889 resolves the existing retained prune operation when issuing the
immutable archive download. Database generation, accepted prefix and all
four name/size/SHA pins must match, and the existing publisher reconciles
the exact provider package. Native archive verification and Saved promotion
remain mandatory. This does not repeat a finalize or change unknown intents.
Production net22 lines, budget25; local125 checks passed. Full CI46004315227
on both Python versions is running; envelope46004315360 passed. Compatible
348378df descends from actual API51b7. Deployment has not run.

The controller sealed286 terminalERROR and ordinarily created287. All four
import tasks remain selected, with TEXT/SEM74/74,203/203,106/106,313/306.
Their new work after Saved510 is not yet covered by a fresh verified Saved.
Next steps are exact CI/main, compatible host delivery at a reconciled
lifecycle boundary, ordinary restore/READY and the actual new Saved.

## November27,17:38 Kaliningrad: archive locator repair deployed

PR889 full CI46004315227 passed both Python versions; envelope46004315360
passed. Main1607ac59 contains the tested change. Ancestry-only PR890 has
exactly the same tree and records compatible348378df in main4ada6eb2;
unchanged successful tests were reused.

Host API348378df was installed at15:38:03UTC, PID1535845, health passed.
This repaired the known pre-dispatch404 while295 had channel0 and only
an unexecuted prepare claim: no physical restore or capture was running.
The same current owner, controller, four readers and13 retained publication
packages were preserved. No notebook launch/Stop/rotation or provider
publication was issued by root. The ordinary controller must now complete
the dead old-code attempt and restore through the published511 copy.
New physical verification, READY and Saved are still outstanding.

## November27,18:09 Kaliningrad: recovery and a new verified Saved completed

The ordinary controller sealed295 ERROR and created296. Its physical prepare
completed at15:49:24UTC; all87 accepted suffix operations were restored,
finish/verify completed, and runtime296 became READY at15:53:57UTC.
TEXT23986 was reconciled from SENT_UNKNOWN to APPLIED without root replay;
SEM23987 then completed through the normal path.

The retained511 copy passed native readback and was promoted. The following
ordinary checkpoint512 passed native download SHA/size verification and was
observed as the verified pointer at16:09UTC, base23987. Its coverage includes
all four books and Chronicle physical312–313 with both TEXT and SEMANTIC.
The native producer performed its normal pg_verifybackup; no additional
independent restore of512 was launched merely to retest the same snapshot.

All four ordinary replacement readers were delivered and started. By16:09:19UTC
they had accepted25 new TEXT pages,24 of which also had SEMANTIC: Kunde3,
Gubin4, Glinski13, Chronicle4 complete pages. This excludes baseline TEXT
pages and semantic-only corrections. These newer operations are after the
Saved512 prefix and are being captured by the next ordinary checkpoint;
this report does not claim every current write is already in512.

No full hour has elapsed since the four replacements were created at
15:54:52UTC. No hourly speed or causal code uplift is claimed. Book rests,
the four-window cap and real notebook OFF policy remain in place; the next
planned drain still needs live verification with888 installed.


## 27 November, 17:01 UTC: first four-window interval and early runtime error

From 15:54:52 to 17:01:10 UTC (66.31 minutes), the same four assignments
accepted 78 new physical pages with both TEXT and SEMANTIC receipts:
Gubin 11, Glinski 38, Kunde 14, Chronicle 15. Baseline pages and old
semantic-only corrections are excluded. Including all waits and downtime,
this is 70.58 pages/hour total, or 17.64 per assigned window/hour.
This measurement does not establish causal code uplift.

Runtime296 ended early. The API logged peer_disconnect/1000 at16:28:25UTC;
the controller obtained the exact sealed provider ERROR at16:30:21UTC.
This was not the planned three-hour drain. The returned exact v296 output
and stream contain reconnect ConnectionError events but no terminal exception;
the reason for the provider ERROR has not been established. No speculative
source patch or manual restart was performed.

The controller enforced one hour OFF until17:30:21UTC (19:30:21 Kaliningrad).
All four book assignments remain selected and are waiting for the runtime.
Latest native-verified Saved514/base24030 covers23 of the78 new pages;
the remaining55 complete pages have APPLIED control receipts and still need
the next verified checkpoint. The accepted journal maximum is24147.
The next check is the ordinary successor, physical restore and fresh Saved.
A successful next planned drain with888 still remains to be demonstrated.


## 27 November, 18:09 UTC: Saved515 verified and import resumes

Runtime297 completed physical restore, all117 accepted suffix commands and
finish/verification, reaching READY at17:43:15UTC. Saved515/base24147 was
promoted after native verification at18:03:43UTC. Coverage of all four books
proves that the former55 journal-only complete pages are now in the verified
dataset; all78 new complete pages counted since512 are saved.

Chronicle subsequently accepted21 additional complete pages and one additional
TEXT awaiting SEMANTIC. These newer pages belong to the next checkpoint.
The ordinary driver created three replacement readers: Gubin SENT, Glinski
claimed and Kunde queued at18:09; Chronicle is SENT with model progress.
The four-window cap and normal delivery remain in effect. The cause of the
previous296 early ERROR and the next successful planned drain remain open.


## 27 November, 18:44 UTC: all four flows and first complete restored hour

During60.9946minutes from297 READY at17:43:15UTC, the four assignments accepted
118 new physical pages with TEXT and SEMANTIC receipts: Gubin27, Glinski11,
Kunde25, Chronicle55. The full-capacity rate is116.08pages/hour total and
29.02per window/hour, including waits and browser delivery. This is observed
throughput, not a causal code-uplift measurement.

Glinski now has an ordinary fresh-chat successor, SENT/modelSTARTED, and is
publishing. Chronicle has384TEXT pages and377SEM pages; finish is pending,
and seven historical semantic gaps remain separate. Kunde reports incomplete
entity enrichment on dense tables under the32entity bound; receipt coverage
alone does not establish semantic completeness.

Latest verified Saved517 remains atbase24152. The next ordinary capture
completed through24387 at18:44:19UTC; publication and native verification
are still running. Newer pages are not yet claimed as saved in517.


## 27 November, 19:02 UTC: bounded retention and fifth-window rollout

Read-only Kaggle metadata at18:52UTC listed just two retained Ready versions,
517 and518. The existing retention maximum is three old versions plus current;
verified pruning periodically removes the old payload versions. Version numbers
continue increasing and do not represent the number of retained copies. The
account moderation false-positive trigger remains unknown; bounded retention
does not guarantee that moderation will never recur.

Saved518/base24387 has now passed native verification. Native297 startup,
physical restore and117 accepted suffix operations took12minutes48seconds from
launch to READY. The three-hour work cycle and real one-hour OFF remain enabled.

The owner authorized a fifth window. Root first changed the setting before
checking the deployed upper bound; this caused a34second relay startup failure.
The setting was reverted to4 and the relay restored. PR117 then fixed the
existing constructor bound in one production line, preserving default4 and
rejecting6. Compatible197tests and integration214tests passed, exact CI46053207010
passed, and main37a5ddd contains compatible64f47a1. That compatible relay is now
live with actual environment capacity5; queues, credentials and rest clocks
were preserved. The ordinary fifth-reader job e8de6e77 for Armstedt/Fischer
Heimatkunde was claimed at19:01UTC. Its133TEXT/131SEM baseline is historical;
new fifth-reader pages and their verified saving are the next acceptance step.
