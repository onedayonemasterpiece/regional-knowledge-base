# Book ingestion

## SQLite / Supabase v2 implementation boundary (2026-10-05)

The canonical task is [SQLite corpus + Supabase vectors + on-demand proof](prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md).
The old v1 page archive and capacity prompt are superseded. No local PostgreSQL,
page archive/topic/spool, whole-book readiness gate or new IAM is introduced.

The v2 release selects persistent SQLite with `RKB_SQLITE_CORPUS_PATH`.
SQLite owns corpus text, catalog, rights/ACL, page/region mapping, ingestion,
graph/POI and publication state. Supabase owns only E5/BGE vectors and minimal
ID/revision/hash/scope anchors. The existing application actor bridge authorizes
local reads and writes; MCP bearers never enter the vector plane. Fetch/catalog
and FTS lexical-only search remain available without Supabase. See the
[deployment and recovery runbook](operations/sqlite-v2.md).

Use `book_ingest(start, file, metadata)` with an attached source. Production
must accept at least **PDF and DjVu**. The exact downloaded source SHA and owner
identify one logical document; attachment IDs are resumable request identities.

The MCP does not recognize the book. It detects/validates the source container,
archives exact bytes, renders requested pages and exposes any native embedded text
as a hint. **ChatGPT itself** visually reads/transcribes pages and supplies the
semantic page graph, chunks, captions, footnotes, illustrations, people/events/POI
links and review notes.

Ingestion uses no OCR engine or automatic semantic parser. The separate on-demand quote-proof helper may use bounded Flash-Lite localization; it never reimports or edits accepted source text.

`metadata.duplicate_policy` is `reuse` by default. Another attachment containing
the same PDF returns the existing ingestion/document, including an in-progress or
finalized import, without replacing title/authors. `new_revision` starts the next
staged revision on that same document. Replaying an attachment/policy resumes its
job. Different owners retain separate roots. Concurrent starts are serialized by short SQLite write transactions. Audited historical
duplicate roots remain separate; ambiguous source identity fails explicitly,
without silently picking or merging an old book.

Read `book_pages` source images and full native text, follow continuations and
stage every page as visually reviewed with a bounded review note. Figures require
real figure regions and illustration entries, or a reason in
`excluded_figure_regions`. A reviewed page may have no figures. Printed captions
must be source caption regions on the same page. Image-only pages may form chunks
with empty printed text when linked to a searchable illustration.

## Передача подготовленного моделью пакета

Для больших страниц `book_ingest(stage)` принимает тот же пакет через параметр
`file`: один UTF-8 JSON-файл с `ingestion_id`, `source_revision` и обычными полями
`pages/chunks/entity_candidates/story_candidates/poi_facts/poi_media_links`.
Ограничения: 1 МиБ, не более 8 страниц, 100 чанков и 12 историй. Пакет нельзя
смешивать с inline-полями или метаданными; PDF исходника передаётся только в
`start/reprocess`, не в `stage`. Сначала проверяются права на задание, затем
скачивается ограниченный файл. После разбора выполняется прежний stage-процесс
со всеми проверками исходных доказательств. Возвращается SHA-256 пакета.
Ошибки схемы возвращают не более восьми путей полей и типов ошибок, без значений
частного текста; неизвестные пользовательские имена полей маскируются.
Скачивание stage JSON явно отделено от проверки PDF/DjVu-контейнера: обычный
`start/reprocess` по-прежнему требует исходный документ, а JSON проходит строгую
схему StageFile. Это только транспорт: он не извлекает текст, не придумывает сущности и не
проставляет модельную проверку. Повтор использует прежние устойчивые ключи. Сопоставление задания принимает как строковый UUID, так и типизированный UUID из адаптера базы; проверка принадлежности и редакции остаётся обязательной.

Если у CID-шрифта PDF нет ToUnicode, `book_pages.native_text_info.encoding_notice`
предупреждает, что видимые страницы могут читаться лучше текстового слоя. Модель
сверяет символы с изображением и либо транскрибирует, либо восстанавливает
карты шрифтов в отдельном рабочем представлении. Оригинал не заменяется;
непроверенный текст нельзя принимать за точное свидетельство. Отсутствие
предупреждения также не отменяет визуальную проверку.

## Единый смысловой проход при обычном импорте

Модель читает страницу один раз и рассматривает истории, даты, людей, организации,
события, исторические нити, места, численные сведения, иллюстрации и сноски.
`pages[].knowledge_reviewed=true` фиксирует этот проход (и включает прежний
`story_candidates_reviewed`). Отметка допустима и при отсутствии подходящих
кандидатов, но не заменяет настоящую работу модели и не доказывает 100% recall.
Старые страницы не получают её автоматически. Нужные сущности создаются или
переиспользуются через существующий `entity_candidates`, а не только `poi_ref`.

В `story_candidates[]` теперь можно сразу передавать:

- `event_dates`: исходная дата, точность, календарь/`unspecified`, роль даты,
  допустимые year/month/day и модельное основание;
- `observations`: число, исходное написание, единица, предмет, период и метод;
- `entity_keys`: выбранные моделью ключи из накопленного `GraphBundle` книги.

Даты и числа используют `evidence_indexes` — позиции (с нуля, по умолчанию `[0]`)
в `evidence_refs` той же истории. Не требуются ещё не созданные assertion/evidence
UUID. Исходное написание даты/числа должно присутствовать в выбранной цитате.
Не добавлять фиктивный день к году, не угадывать календарь и не превращать
совпадение названий или близость координат в тождество объектов.

При активации исходника эти данные записываются существующими валидаторами
Story Registry, в той же базе и транзакции. Ошибка дополнительной даты, числа
или связи не уничтожает доказательный сюжет: корректная часть сохраняется,
а исходный кандидат получает `needs_review` с сохранённым `story_id` и причиной.
После активации тот же `book_ingest(stage)` принимает только метаданные:
`story_candidates` и `entity_candidates`. Повтор исправленного кандидата дополняет
существующий сюжет и закрывает его checkpoint; страницы, чанки и векторы не меняются.
`story_edit` остаётся обычным редакционным инструментом, но не заменяет этот
checkpoint импорта. Повторная активация не создаёт второй набор дат/историй.
При повторном импорте совпавший исходный сюжет переиспользуется; чужие
редакторские изменения не перезаписываются. Сущности той же книги сохраняют UUID,
выбранные POI, map_refs и контекст как при повторном source key, так и при явном
entity_id. Их новая редакция включается до привязки историй, в той же транзакции.
Присвоенные параллельно картографические ссылки не стираются. Для переключения
читаются только сущности/упоминания этой книги по индексу, не весь граф корпуса.

`book_ingest(status, document_id=...)` находит последнее задание принадлежащей
пользователю книги. `knowledge_extraction` показывает число просмотренных страниц,
следующую непросмотренную страницу и не более десяти кандидатов, требующих
доработки. `pass_completed` означает завершение заявленного прохода, **не**
полноту исторических знаний, приёмку поиска или завершение межкнижного исследования.
Состояние источника/векторов и смысловая работа различаются: `next_action=done`
не даёт права сообщить о полной обработке при незавершённом `knowledge_extraction`.

После активации модель продолжает существующий `story_reconcile(next)` и выбирает
нужные физические POI через `poi_registry`. Отсутствие идеальной карты или
недоступность Street Story не блокирует книгу. Поиск принимается отдельно на
конкретной активной редакции с естественными и перефразированными вопросами;
одни числа готовых векторов никогда не объявляются качественной приёмкой.

Новых сервисов, реестров или фоновых LLM нет. Пер-страничная отметка добавлена
в существующую `story_ingest_coverage`; выбор задания и чтение статуса индексированы,
объём модельного пакета ограничен прежними 8 страницами / 12 сюжетами. Чтение
статуса не загружает весь исходный граф книги и выполняется вне event loop.

## Автоматическое извлечение историй при обычном импорте

При команде пользователя «Добавь книгу» существующая модель **одним проходом**
через `book_pages` → `book_ingest(stage)` делает и смысловые чанки для RAG,
и оценку историй-кандидатов. Отдельный пользовательский промпт/модельный вызов
для историй не требуется. Это **не** скрытый серверный LLM-worker: модель,
которая уже читает книгу, поставляет кандидатов вместе со страницами.

Каждая `StagePageInput` содержит `story_candidates_reviewed=true`, когда
модель действительно просмотрела страницу на предмет эпизодов. Пустой список
`story_candidates` означает, что на просмотренных страницах новых кандидатов
не выделено. Не создавать искусственную «историю на каждый чанк».
Эпизод должен иметь узнаваемое содержание; допустимы быт, место, человек,
загадка, противоположные версии или короткий любопытный факт.

`book_ingest(stage, pages=[...], chunks=[...], story_candidates=[...])` принимает
не более 12 кандидатов на вызов. Каждый кандидат включает устойчивый
`candidate_key` в рамках импорта, нейтральный заголовок, краткую основу,
описание **позиции автора/источника**, тип истории, настоящие
`evidence_refs` с `page_id`, `region_key`, короткой исходной
`original_excerpt` и (при повторах) точными `start/end` по Unicode-символам.
Сервер сверяет цитату с уже принятой моделью просмотренной страницы,
не подменяет цитату интерпретацией модели и не выдаёт совпадение текста
за историческую истину.

Чекпоинты просмотра страниц и кандидатов хранятся в той же SQLite.
Переразметка страницы удаляет устаревшие кандидаты, связанные с ней.
До активации книги это **staged**, не редакционные истории.
При успешной активации точной исходной ревизии книги кандидаты
создаются в Story Registry **в той же короткой SQLite-транзакции**:
Story.state=candidate, Assertion.kind=attributed_account,
Evidence.relation=reports, text_match=exact, support ещё не оценён.
Повтор команды или активации не создаёт дубликаты.

Во всех ответах `book_ingest` с ingestion ID есть `story_extraction`:
`reviewed_pages/total_pages`, число подготовленных и сохранённых
кандидатов, `state`. **Важное ограничение:** отсутствие
`story_candidates_reviewed` не должно блокировать активацию корректно
проиндексированной книги. В таком случае `story_extraction.state=awaiting_agent`,
а не ложное «всё извлечено». Даже полный проход по страницам не равен
100% обнаружению всех возможных историй. Нет фоновой автоматической
обработки книг, загруженных сторонним сервисом *без* модели в его
ingestion-процессе — потребуется согласованный executor, его нельзя
изображать запущенным только потому, что есть задание.

Реестр не публикует кандидатов в соцсетях, не присваивает им
`publish_ready` и не отправляет на дополнительные платные OCR/LLM-обработки.
Предыдущие книжные и BGE-gates продолжают выполняться независимо.

## Evidence-backed extraction from already accepted books (no reimport)

For a previously accepted, indexed book with incomplete story coverage, **do not
reprocess the PDF or create another source revision just to extract stories**.
The same calling model that inspects the accepted passages can use the existing
Story Registry job:

1. `story_extract(start, document_id, source_revision=active_revision, batch_size=20)`
   creates a durable checkpoint with `awaiting_agent`. This does **not** start
   a server-side model.
2. `story_extract(claim, job_id, expected_job_revision)` leases the next batch
   and returns `start_cursor`, `batch_size`, `batch_id`, and `lease_token`.
3. `corpus_read(document_id, source_revision, cursor=start_cursor,
   limit=batch_size)` supplies source text and **accepted** `source_regions`
   (`page_id`, `region_id`, `physical_page_index`, exact `source_text`).
   Review the passage semantically; distinct stories may cover multiple chunks.
4. `story_extract(stage, ..., grounded_candidates=[...])` accepts up to
   10 model-authored episodes with material type, attributed assertion and
   1–4 exact `original_excerpt` references to those regions. A candidate must
   cite at least one region from the claimed batch. A scanned batch can have zero
   candidates — never invent an episode to reach a numerical target.
5. The service verifies exact document revision, region, quote/offset, batch
   scope, source ACL and replay fingerprint **in the same SQLite transaction**.
   It creates `Story.state=candidate`, `attributed_account` and evidence
   relation `reports`; no story is auto-approved or historically certified.
   Continue the same job until `done`, checking `story_job_get` for count
   of inspected source chunks, not just number of stories.

Legacy `story_extract(stage, candidates=[SeedInput(...)])` still creates raw
editorial seeds for callers that explicitly want unverified leads. Do not use
that path to claim evidence-backed extraction. A new book still uses **same-pass
`book_ingest(stage, pages, chunks, story_candidates)`** with the importing
model. The accepted-source path is for books already ingested before that pass.

For a later re-chunk/reprocess of byte-identical archived source, the server may
reuse the page-level visual review from a prior revision instead of requiring the
same scan page to be visually re-reviewed. Reuse is fail-closed: the source SHA-256
must be identical, the prior revision must already have reached
`revision_publication`, and the prior staged page must itself be
`visual_reviewed` with a non-empty review note. Only review completeness is
reused; current regions, chunks, captions, figures and token budgets are still
validated from the new staged revision.

Build retrieval chunks as coherent semantic passages, not as a mechanical copy of
native PDF text blocks. The existing chunk contract may reference ordered regions
from several pages. The measured operating target is about **256 encoder tokens**
per semantic passage. On the audited book corpus this was typically about
**800–1,000 characters**, with a practical observed range around **700–1,100**.
Those character figures are guidance, not a tokenizer substitute. Attach short body
fragments to their surrounding passage and keep a genuine sentence/paragraph
continuation together across a page boundary. Page boundaries are provenance, not
default embedding boundaries. Headings, captions and image-only evidence may be
shorter. Use bounded overlap only when it represents a real semantic boundary; do
not duplicate every neighboring chunk.

Both deployed encoders cap the **final augmented search material** at 512 tokens.
Validation obtains exact untruncated counts from the pinned E5 and BGE tokenizers,
including the E5 passage prefix and caption/model-observation augmentation. A chunk
above either hard limit blocks validation/finalization rather than being silently
truncated. The 256-token target is a quality diagnostic, not a hard semantic split:
a self-contained passage may be longer while remaining below both 512-token limits.
Character thresholds emitted by graph validation remain diagnostics only.

Validation also reports retrieval_quality:open_continuation_boundaries:N when
adjacent prose chunks retain strong signs of an unfinished source-order boundary
(for example an additive connector or split word). This warning is non-blocking:
it helps future imports keep continuations inside coherent chunks without
invalidating already accepted historical revisions.

If a source illustration is printed sideways, `display_rotation_degrees` may be
set to a model-reviewed cardinal **clockwise** turn (0/90/180/270). This affects
only the delivered/display rendition. The reviewed source bbox and source-crop
digest remain in the source-page coordinate frame and are preserved separately
from the display-crop digest.

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

Migration 017 is historical; v2 migrates exact chunk/region text to SQLite and preserves the dedicated
source archive. New revisions do not persist page renders or text-projection
objects. See [deployment, recovery and guarded GC](operations/telegram-archive.md).

`metadata.catalog` accepts book/journal_issue/article, optional parent, publication
edition/volume/issue/publisher/date, ordered contributor roles, identifiers with
explicit validity notes, expandable form/purpose/topics, source/user/model
annotation provenance and actual cover/title-page references. Unknown values
remain absent. These additions are stored only in SQLite and do not re-embed text.
Standalone article PDFs use the same start/stage/validate/finalize MCP flow.
Issue-internal article components share the original and exact semantic ranges;
activation waits for exact acknowledgments from the configured required vector spaces (BGE by default; E5 is optional unless explicitly required). Both tokenizer limits still apply to final search material.

## People, stories and locations in the same review

Follow [location-first ingestion](location-registry.md). The model searches the
existing `entity_list`/`graph_fetch` registry, reuses an ID or adds a distinct
source-side place, and includes `entity_candidates` with `story_candidates`
during ordinary staging. Coordinates, maps and a canonical external POI are
optional. Preserve each exact source mention; use `located_in` for source-backed
place hierarchy and `entity_refs` for existing parents. Current/historical names
from web research are separately attributed, not inserted into the book quote.
Do not run a second semantic pass solely because cartography is unavailable.
For accepted books, `graph_stage` enriches the same revision without reimport or
re-embedding. Link appropriate Story Registry episodes through existing
`link_entity` operations using these same IDs.

## Reading progress without losing the active revision

`catalog`/`book_find` report the active source revision.
`book_ingest(status, document_id=...)` reports the latest owned ingestion job,
which may be an unfinished replacement while an earlier revision remains searchable.
Do not treat that job as the active revision or restart it merely because its
checkpoint is incomplete. Inspect `next_action`, `story_extraction` and
`knowledge_extraction` separately. `awaiting_agent` requires model work;
`pass_completed` is a recorded review pass, not a completeness or retrieval certificate.
After activation, perform source-grounded retrieval checks on that exact revision
and report unfinished reconciliation explicitly.

## Local activation and issue articles

Finalization stores pages, regions, relations and exact chunks locally. It persists
a pending publication manifest; the existing E5/BGE workers acknowledge exact
ID/revision/hash/space matches before selecting the new revision. Retries resume
the same publication; incomplete vectors leave the previous revision active.

An article inside a journal issue can be registered without another attachment
using catalog `parent_id`, physical-page range and accepted `region_ids`. It shares
the issue original and exact semantic chunk ranges, carries its own catalog
metadata, and creates no duplicate pages or ingestion job. Partial-chunk ranges
are rejected. Standalone articles use the same attached-source workflow as books.