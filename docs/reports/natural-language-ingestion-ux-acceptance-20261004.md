# Natural-language ingestion UX acceptance — 2026-10-04

Continuation of `abca00bfeccd31a7b5254dcb068cd24cbc7e162e`
(`Add natural-language archived book reprocessing`), against prompt base
`8364cd102ecc9289704352ab6ab75e920970c922`. Final implementation commit:
`69adf5a42616e7d6f278cd41a71916376be9f999`. Working branch remains
`chatgpt/natural-language-ingestion-ux-20261004`.
The initial implementation was already merged through PR #37; the verification,
fixes and report are delivered by [PR #38](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/38).
Final canonical merge SHA, deployed file identities and process readback are
recorded in `final-runtime-readback.json` and returned in the delivery response.

## Verified user/model contract

- New attachment: “Добавь эту книгу в базу знаний региона” + PDF/DjVu. Actual
  declarations describe start, model page review/stage, validate, finalize and
  later status through `next_action`. `file` retains `openai/fileParams` metadata.
- Known book: “Переимпортируй Гаузе, прошлый импорт был неполный”. Full-profile
  `book_find` resolves readable catalog metadata; `reprocess` opens the exact
  verified archive and starts/resumes a revision on that same logical document.
- Instructions require disambiguation with title/author/year when needed; no
  user-supplied UUID, lifecycle commands, duplicate policy, archive ref or vector
  configuration is required. Both ingestion tools remain absent from Live.
- Tests assert actual MCP declarations/input/output schemas/instructions, not a
  simulated LLM. Production acceptance invokes the real public OAuth MCP endpoint.
  It does not claim a full semantic reading of the book or a browser-chat UI test.

## Narrow corrections to the existing implementation

Complete staged page coverage now persists the validate action for status/restart;
synchronous finalization returns done. Empty completion cursors are not exposed as
an invalid page cursor. Pending archives direct recovery/wait before re-upload.
The transitional REST catalog scans subsequent actor-authorized pages while
retaining at most eight matches; it no longer loses matches beyond record 512.

A real PostgreSQL regression reproduced `historical_source_identity_ambiguous`
when reprocess explicitly selected an owned historical root sharing source SHA
with another root. Migration `018_selected_source_revision.sql` honors that exact
owned root using the existing RPC. Roots remain separate; unselected attached
starts still reject ambiguous SHA identities. No new API, queue or platform.

Changed files, including the original implementation relative to prompt base:
`docs/mcp.md`; `src/regional_knowledge/backend.py`, `contracts.py`, `server.py`,
`source_archive.py`, `supabase_backend.py`, `stage_service.py`;
`sql/018_selected_source_revision.sql`; `tests/test_natural_language_ingestion_ux.py`,
`test_server_contract.py`, `test_stage_service.py`; this report.
Unrelated untracked checkout `artifacts/` was neither deleted nor committed.

## Tests and CI

- Initial targeted run: **44 passed** — natural-language UX, server contract,
  ingestion/revision/staging, Telegram archive, Postgres, ACL/application identity,
  SQL policy and robust import/dedup.
- Expanded targeted run, including transitional REST: **63 passed**.
- Historical-root fix plus natural-language/server/archive/dedup regression:
  **23 passed**; the migration was applied twice in the actual PostgreSQL test.
- Final full suite with isolated pgvector PostgreSQL: **143 passed**, one existing
  Starlette deprecation warning, 25.96 seconds on local Python 3.14.
- [Full Python 3.12/3.13 CI at the final implementation commit](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/37226335477)
  passed both jobs. Report-only delivery CI is also checked before merge.

Coverage includes readable-only catalog lookup and stable literal ranking; shared
viewer denial for original-source reprocess; mandatory archived SHA comparison
before job allocation; archive-only reads despite available local cache; committed
RPC response loss and replay; unchanged active revision until real finalize;
a subsequent reprocess after real activation; old attached start default reuse and
explicit new_revision; wait/resume_finalize and complete MCP schema declarations.
No OCR, VLM, LLM or paid inference was added to the server.

## Actual bounded production acceptance

Acceptance first ran on `a9294eeecdaa87efe97b0084467770b1ff74514e` and was read
back again on final implementation `69adf5a42616e7d6f278cd41a71916376be9f999`.
The existing immutable-release/systemd production process deployed all five RKB
services. Public initialize instructions and all twelve tool declarations matched
local source. Migration 018 was applied twice in production; Gause's active
revision, job count and logical-root count stayed unchanged by the migration.

| Evidence | Observed result |
| --- | --- |
| Public book_find("Гаузе") | One book: Фриц Гаузе, Кёнигсберг в Пруссии. История одного европейского города, 1994 |
| document_id before and after | `7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9` |
| Active revision before and after | **1** |
| Control staged revision | **2**, state **staged**, inactive |
| Control ingestion | `b2067d3e-c345-43d2-a0c4-5a4e56d4603c` |
| Logical roots with this owner/source | **1 → 1** |
| Jobs after start/replay/restart | Original finalized job + exactly one control job |
| Public book_pages | Two JPEG ImageContent blocks, physical pages 0 and 1, next cursor 2 |
| Reprocess replay | Same ingestion identity, no third revision |
| Service restart, then reprocess/status | Same control identity, continue_pages |
| Active vectors after acceptance | **747/747 E5 and BGE**, zero missing |

The two returned pages were visually inspected: bilingual Gause title page and
publication/translation credits. Their private image artifacts stay outside Git.
No semantic pages/chunks were staged, validated or finalized for this real book.
The control job remains inactive and resumable through the existing workflow;
no destructive cleanup API was added. Retrieval continues using active revision 1.

The production archive verification event records:

```json
{"event":"source_archive_download_verified","document_id":"7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9","source_archive_ref":"pub_fc7a9ebff7324722a079d80efb857e79","sha256":"10328207e2664aabef78201fb997e18f3ea8e48f8d7d56a98f3d6832b21ddb65","size_bytes":12752172,"require_archive":true}
```

This is the verified Telegram source path with actual SHA comparison, not a local
object fallback. The original source cache metadata was already marked deleted.
Read-only book_pages recovered the same archived original normally. Replays reused
persisted state instead of downloading/allocating another revision.

## Retained receipts and scope limits

Evidence is retained, with restricted permissions, under
`/home/dev/artifacts/regional-knowledge-base/20261004T183239Z-natural-language-ingestion-ux-20261004`:
`targeted-initial.log`, `targeted-final.log`, `historical-before.log`,
`historical-fix.log`, `full-selected-root.log`, `production-mcp-surface.json`,
`production-book-find.json`, `production-acceptance.json`,
`archive-readback-events.jsonl`, `migration-018-production.json`,
`updated-implementation-readback.json`, `final-runtime-readback.json`.
Temporary acceptance OAuth families were revoked; credentials were not copied.

The full **174-page semantic Gause reimport was not performed**. The model still
must review and stage all source pages before validation/activation. The REST
compatibility path uses catalog pagination; production uses the existing
PostgreSQL actor/RLS path. No ingestion/storage/retrieval/embedding/graph redesign
was introduced.
