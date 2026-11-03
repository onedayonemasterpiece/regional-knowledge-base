# Book import marks in the Telegram originals topic

The first caption line is maintained by RKB using the existing owner-authorized
original-caption phase. The original caption text and archive marker remain.

| Mark | Meaning |
| --- | --- |
| ⏳ | Reading/import is in progress (including a normal rest). |
| ⏳⚠️ | Import continues; the journal contains rejected operations. |
| ✅ | Reading ended and every original physical page has TEXT and SEMANTIC APPLIED. |
| ✅⚠️ | The same complete receipt coverage, with earlier rejected operations. |
| 🏁⚠️ | Reading ended with missing text/semantic coverage or unfinished work. |
| ❌ | The importer reached its failed state. |

An import finish is not proof of full page coverage or Dataset Saved. Marks do
not certify the broader source-quality or search acceptance gates. A recovered
rejection remains visible, so users can distinguish a clean run from one with
errors. Client UI errors which never reach RKB are not inferred from the ledger.

Only existing native intakes with a verified original are projected. Files
merely prepared for future reading, and untouched Telegram documents, are not
enumerated or marked. Ordinary initial original captions retain their previous
behavior; the status line appears when the native import actually starts.

Normal owner book_ingest traffic updates changed captions, including previously
finished books, without a code agent, new daemon, timer or Telegram credential.
Each call considers at most two changed books and edits at most one; unchanged
metadata skips journal parsing and Telegram calls. The in-memory optimization
is discarded on restart; authoritative desired status is reconstructed from
the existing journal. The entire optional provider update has a two-second
bound and cannot fail an accepted import. The ordinary next import request
retries a missed update. When no importer is calling, the display is the last
confirmed projection; no separate status poller starts Kaggle just to edit a
caption.

Only the currently leased Telegram original owner may edit its existing file
message in the configured originals topic. No document is resent or downloaded.
Provider readback and existing lost-ACK reconciliation remain authoritative.
An uneditable/foreign message or a full-length caption is logged and left
intact rather than replacing the document or truncating user notes.
