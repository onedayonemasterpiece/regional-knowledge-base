# M5 — owner-scoped research continuation from an accepted native book

**Source-only claim:** RKB-Импорт, 2026-10-22, dot thread 1792671044.883929.
**Exact base:** `integration/rkb-native-import-20261022` at `b932c15ef133d668a0eaa3570cc895e750876c77`.
**Changed production leaf:** `src/regional_knowledge/kaggle_importer.py::_native_research_status` and one post-native-`book_ingest(status)` output callsite. One NEW own test. No shared `server.py`, SQL, Kaggle materializer, WSS, provider client, archive uploader, new data store or credentials.

## Problem / implementation

The already-prepared native `NativeSourceIntake._output` produces
`state=finalized; next_action=done` for any durable source activation.
However the same accepted model reading may have persisted
`research_json={state:pending,continuation:{tool:book_pages,arguments:{ingestion_id,cursor?}}}`.
In a new authorized session, `book_ingest(status,document_id=...)`
previously hid that pending work, so the source was wrongly presented as a
completed research goal.

New behavior is strictly read-only. After the **existing native** status
returns finalized, the importer reads the same owner-scoped compact control
row, validates model-authored research, current owner/lease and exact
`ingestion_id`, and verifies the same original archive receipt still exists.
A pending, well-formed page continuation replaces only the **MCP output**
fields `next_action=continue_pages`, `next_cursor`, nonsecret message and
`model_authored_research_pending` warning. It never modifies the immutable
activation payload, source revision, facts, Story.body, accepted operation,
control spool, or native page renderer. Subsequent `book_pages` independently
checks actor/source/render provenance. A completed research state leaves the
original `done` output unchanged; contradictory or forged continuations fail
closed rather than inventing reachable work.

This makes a concrete *post-acceptance model continuation* reachable through
existing owner personal MCP across chat boundaries, for any source already
Saved by the native authority. It **does not** itself prove the actual first
real PDF/DjVu activation, model-authored facts/Story.body or Dataset readback,
which remain the joint M2–M4 acceptance owned by dot/Opus. In particular,
`DURABLE` and application-level `finalized` are not equivalent to the
separate accepted retrieval quality gate; no new `Saved` claim is minted.

## Verification / dependency

`tests/test_kaggle_native_frontier_status_20261022.py` uses real
`KaggleSourceImporter`, owner OAuth, SQLite queue and archived original
controller fixture, with **explicitly synthetic** native terminal status.
Tests cover repeated new-session-compatible `status(document_id)`,
current page cursor, preservation of archive operation count, completed
research, bad IDs/tools/cursors/batch/contradiction, and unavailable archive.
These do NOT demonstrate live book/image/revision2 acceptance.

Integration dependency: compatible 77-path native input and
`KaggleSourceImporter.native_intake` composition from the diagnostic baseline
`6d51bfd82132048bd4642ea4fabd613583505ce3`, merged by sole owner Opus.
For source-only combined tests Shkiper applies only the method/callsite and
own test to his isolated diagnostic candidate. No canonical integration,
provider actions or deploy by RKB-Импорт.

**Next selected independent priority:** native accepted reprocess/rechunk
revision2 SQL/original-receipt contract is separately blocked by the immutable
SQL013/014 `absent_source,revision=1` format and assigned only when current
SQL owner releases exact successor hunks; otherwise M5 actual research
readback with a first accepted book remains a dot/Opus live gate. Future
native evidence-bearing story/figure output requires existing M3/M4 owners;
do not duplicate them.
