# Natural-language ingestion UX — hide MCP workflow from the user

Date: 2026-10-04

This is a small product-interface correction. Do not redesign ingestion, retrieval,
storage, embeddings or the graph.

## Product requirement

A normal user must be able to say only:

- with an attachment: "Добавь эту книгу в базу знаний региона";
- without an attachment, for an existing source: "Переимпортируй Гаузе, прошлый импорт был неполный".

The user must NOT need to know or type:

- document UUIDs;
- duplicate_policy;
- start/stage/validate/finalize/status;
- page counts;
- continuation cursors;
- E5/BGE;
- Telegram topics/sessions;
- storage layout;
- graph staging;
- recovery rules.

Those are model/tool implementation details.

## Current UX defects to fix

1. Current `book_ingest(start)` hard-requires an attached file even when the exact
   original is already verified in the source archive.
2. There is no small read-only document/catalog lookup that lets the model resolve
   "Гаузе" to an accessible logical document without already knowing its UUID.
3. MCP server instructions are too weak to teach the model the normal ingestion
   orchestration.
4. `docs/mcp.md` still contains stale pre-migration storage statements.

## Minimal tool changes

### 1. Add a bounded read-only document lookup

Expose a small tool such as `book_find(query, limit=8)`.

It returns only documents the caller may read, with compact fields:

- document_id;
- title;
- authors;
- publication_year;
- active_revision;
- source_format;
- source_archive_status.

Use deterministic catalog/title/author matching. No LLM/server semantic matching.

If several plausible documents remain, the model asks the user which one.

### 2. Add reprocess-existing-source lifecycle entry

Extend `book_ingest` with a model-facing operation such as:

`command="reprocess", document_id=<id>`

or an equivalently clear typed contract.

Behavior:

- authorize the document;
- require verified exact source archive;
- recover/download the exact archived source through the existing source adapter;
- verify stored SHA;
- create/resume the next staged revision on the SAME document_id;
- preserve source identity/archive entry;
- never create a second logical document;
- remain idempotent under replay/lost response.

Do not require the user to upload the source again.

Keep attached-file `start` for genuinely new books.

### 3. Improve MCP server instructions/tool descriptions

The model-facing instructions should say, compactly:

- If the user asks to add an attached PDF/DjVu book, carry the ingestion through
  pages -> model review -> stage -> validate -> finalize -> later status checks.
- If the user asks to reimport/reprocess an existing book, find it and use the
  verified archived source; do not ask for a re-upload unless the archive is
  genuinely unavailable.
- The model, not MCP, performs semantic reading/recognition.
- Resume existing ingestion state after interruptions.
- Do not expose internal workflow terms unless useful for explaining an error.

Do not put a giant runbook into the server instructions. Tool descriptions and
typed outputs should make the next action obvious.

### 4. Make ingestion status model-actionable

Without adding a scheduler, ensure status output contains enough compact state for
the model to know whether to:

- continue reading/staging pages;
- validate;
- finalize;
- wait for server-side finalization/indexing;
- report a real blocker.

Prefer a small `next_action`/progress field if current state is otherwise
ambiguous.

## Preserve the thin-MCP boundary

Do not add OCR, Tesseract, VLM, LLM parsing, semantic chunking or entity extraction
to the server.

PDF/DjVu adapters remain deterministic page transport only. ChatGPT performs the
book understanding.

## Acceptance

Test from the model/user perspective, not only low-level RPCs.

### New book

Given an attached small PDF/DjVu and only the user phrase:

"Добавь эту книгу в базу знаний региона"

the available tool descriptions must be sufficient for a capable ChatGPT agent
to select the correct workflow without requiring technical clarification.

### Existing book

Given no attachment and only:

"Переимпортируй Гаузе, прошлый импорт был неполный"

the flow must:

1. resolve the existing Gause logical document;
2. use its verified archived source;
3. create/resume a new revision on the same document;
4. never create another logical Gause document;
5. expose page batches to ChatGPT normally.

Do not perform the full 174-page semantic reimport as part of this engineering
task. Stop after proving the real new-revision ingestion can be started/resumed
from the archived Gause source, then cancel/archive the control revision if needed
without changing the active revision.

### Regression

Also prove:

- same attached source default import still deduplicates;
- explicit new book import still works;
- ACL isolation;
- archive unavailable -> clear actionable error, not request for hidden IDs;
- restart/replay idempotency;
- no paid inference;
- full tests/CI;
- exact production readback.

## Documentation

Update `docs/mcp.md` to the current Postgres-text / Telegram-source-archive
architecture and document the **user-level** examples separately from internal
tool workflow.

Create:

`docs/reports/natural-language-ingestion-ux-acceptance-20261004.md`

## Definition of Done

Done only when a normal user never needs the long technical prompt that triggered
this task. The expected user commands are one sentence plus an attachment for a
new book, or one sentence naming an existing book for reprocessing.
