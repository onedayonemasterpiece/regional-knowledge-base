# Book ingestion

Use `book_ingest(start, file, metadata)` with an attached source. Production
must accept at least **PDF and DjVu**. The exact downloaded source SHA and owner
identify one logical document; attachment IDs are resumable request identities.

The MCP does not recognize the book. It detects/validates the source container,
archives exact bytes, renders requested pages and exposes any native embedded text
as a hint. **ChatGPT itself** visually reads/transcribes pages and supplies the
semantic page graph, chunks, captions, footnotes, illustrations, people/events/POI
links and review notes.

No OCR engine, VLM, layout AI or server-side LLM parser is part of the MCP.

`metadata.duplicate_policy` is `reuse` by default. Another attachment containing
the same PDF returns the existing ingestion/document, including an in-progress or
finalized import, without replacing title/authors. `new_revision` starts the next
staged revision on that same document. Replaying an attachment/policy resumes its
job. Different owners retain separate roots. Concurrent starts are serialized in
the DB and a partial unique index protects new logical roots. Audited historical
duplicate roots remain separate; ambiguous source identity fails explicitly,
without silently picking or merging an old book.

Read `book_pages` source images and full native text, follow continuations and
stage every page as visually reviewed with a bounded review note. Figures require
real figure regions and illustration entries, or a reason in
`excluded_figure_regions`. A reviewed page may have no figures. Printed captions
must be source caption regions on the same page. Image-only pages may form chunks
with empty printed text when linked to a searchable illustration.

An illustration optionally carries `visual_description` (up to 2,000 characters),
fixed `visual_description_provenance=model_observation` and an optional language.
This is model-authored retrieval context, never a quotation or historical fact.
Use `illustration_fetch` to inspect the actual authorized crop when judging it.

Validate before finalize. Finalize materializes the existing crop/page/region
pipeline, preserves canonical source text separately and activates a revision.
The accepted automatic indexing owner reconciles E5/BGE against deterministic
search material: source text, linked printed captions and labelled observations.
`text_sha256` verifies source evidence; `search_material_sha256` identifies encoder
input. Ready vectors must match both, and partial/stale coverage degrades search
safely. No backend OCR, VLM, CLIP or paid inference is introduced.

Original source files and eligible active private illustrations are routed through
VibePublish using the dedicated Knowledge Base Telegram connection/session
`TELEGRAM_KNOWLEDGE_BASE`.

- sources -> `https://t.me/c/4368830579/2` as exact DOCUMENT bytes;
- illustrations -> `https://t.me/c/4368830579/4` as DOCUMENTs.

The separate Telegram session/connection is intentional: Knowledge Base binary
traffic must not occupy the ordinary VibePublish Telegram connection lane.
Provider/account-wide Telegram limits may still exist, but VibePublish's own lane
and pacing state are isolated by connection.

Activation does not need to wait for illustration delivery. A source may stay in
bounded temporary storage until its Telegram archive has verified exact provider
readback; after that the temporary binary is GC-eligible.
 Stable owner/document/revision/figure
keys survive restart and replay; native private-topic/DOCUMENT readback confirms
the mirror. The source image SHA is not a sameness gate. A configured resource
grant binds the eligible RKB owner to the VibePublish destination. Other owners
cannot use that grant. VibePublish owns the shared rolling provider budget.

For production, apply migrations 015/016 with the guarded isolated proof, then
configure the private `RKB_VIBEPUBLISH_GRANT_FILE` (mode 0600). The grant contains
the RKB owner UUID, VibePublish issuer/client/rotating OAuth credentials, destination
alias and private thread. Never commit it. OAuth refresh rotation is atomically
persisted by the sole existing indexing owner; a revoked/expired grant leaves
mirrors pending and imports/search independent.
