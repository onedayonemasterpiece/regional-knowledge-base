# Import a newly uploaded Telegram book

An owner can upload a PDF/DjVu into the configured originals topic and start
the existing import using the message selector alone:

```json
{
  "command": "start",
  "metadata": {"title": "Title from the original filename"},
  "telegram_original": {
    "chat_id": -1001234567890,
    "topic_id": 2,
    "message_id": 10
  }
}
```

The reader does not need to calculate a hash or copy a Telegram document ID.
The existing leased Telegram client checks the configured chat/topic and actual
message, then downloads the original once **in Kaggle**. That download computes
SHA-256 and returns the observed exact document metadata. The existing verified
original receipt retains the resolved locator; repeating the same start reuses
the intake without another download or archive upload. Supplied `document_id`,
`size` or `sha256` remain assertions and must match.

Continue the returned ingestion with `book_pages` and `book_ingest(stage)`.
An import reader works in a long session, not a separate task per page. Correct
provisional bibliography from the actual title/imprint page using the existing
catalog-update command. Never resend accepted pages or treat an ordinary
processing/wait response as the end of the book.

This changes native acquisition code, not just the host schema. Installing only
the host change does not enable the short selector in an older notebook. The
operator must use the normal drained/checkpointed runtime deployment before
claiming live acceptance. No additional Telegram login, local original download,
public URL, ingestion queue or background topic scanner is introduced.
