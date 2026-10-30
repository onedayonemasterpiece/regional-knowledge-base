# Launcher import activity (local service API)

## Contract

The existing RKB HTTP process listens on **127.0.0.1:8000**. With the optional
`RKB_IMPORT_ACTIVITY_CONFIG` descriptor installed, a same-host Launcher calls:

```http
GET /internal/import/activity?ingestion_id=<canonical-UUID>
Authorization: Bearer <dedicated-service-token>
X-RKB-Service: launcher-import-watch
X-RKB-Service-Credential: <credential-UUID>
X-RKB-Service-Epoch: 1
```

Successful response contains **only** `state`, `next_action`,
`last_operation_at` (UTC ISO-8601 or null for pre-installation ingestions with
no subsequent reader operation), `accepted_pages` (distinct physical pages in
applied **and published** native stage packets), `next_cursor` (model's
saved numeric continuation or physical-page hint, otherwise null).
`state` is one of `processing`, `staged`, `finalized`, `failed`;
`next_action` is `wait`, `continue_pages`, `done`, or `blocker`.
`next_cursor` remains a *hint*, not a reader-order instruction.

This is a local snapshot of existing SQLite control/progress rows and accepted
command receipts. **No Kaggle access, background wakeup, status reconciliation,
reading of book text, participant lookup or mutation happens on GET.** If Kaggle
is cold/unverified, any otherwise runnable ingestion reports
`processing / wait` rather than a false ready state or HTTP 500. The
last reader operation timestamp is updated on the existing ingestion control
row after authorized `book_pages` and `book_ingest` operations (all supported
commands, including status and stage). Background packet processing does not
extend the reader activity clock. Historical ingestions have a null timestamp
until their next authorized operation.

Access denied: HTTP 403 for missing/invalid/revoked credential, wrong principal,
wrong epoch or non-loopback client. Invalid selector: 422. Unknown ingestion:
404. Local control unavailable: 503. All responses are no-store. HTTP error
bodies are fixed, without book text, URLs, user identifiers or credentials.

## Operator installation (not performed in PR)

1. On the **RKB host**, prepare an existing protected directory (0700).
   Create a 0600 JSON scope file containing exactly
   `{"operation":"import_activity"}`. The RKB owner/operator issues a *new*
   service identity using the existing encrypted OAuth authority:
   ```sh
   python -m regional_knowledge.personal_auth_cli service issue \
     --service-id launcher-import-watch \
     --scope-file /PRIVATE/launcher-scope.json \
     --output /PRIVATE/launcher-service.json \
     --reason "Local Launcher ingestion inactivity watcher"
   ```
   Output contains a private token: never paste it into Slack, GitHub or a
   URL. It is **not** an end-user OAuth bearer. Existing service audit/list/revoke
   commands remain the only credential lifecycle.
2. From returned *metadata* (not the token) write a separate private 0600
   descriptor `/PRIVATE/launcher-import-activity.json`:
   ```json
   {"codec":"rkb.import-activity-reader.v1",
    "service_id":"launcher-import-watch",
    "service_credential_id":"<credential-UUID>",
    "service_credential_epoch":1}
   ```
   Set `RKB_IMPORT_ACTIVITY_CONFIG` to this absolute path in the existing
   RKB service configuration, and restart **only at the operator-approved
   installation boundary**. Leave unset to disable this route entirely.
3. Configure Launcher with `http://127.0.0.1:8000`, the issued private token
   and the matching metadata headers. Keep the file under owner-only
   permissions; no remote DNS, reverse proxy, OAuth relay or extra service.
   Verify one authorized loopback GET, then a denial without credentials and
   with a different principal. Operator can revoke by credential ID and epoch
   using the existing `service revoke` command.
4. Poll only the known ingestion ID. An idle timer uses `last_operation_at`;
   **do not** treat `processing/wait` during cold Kaggle as a failed import,
   and never restart an ingestion from a watcher. Unknown outcome remains
   unknown. The correct ingestion is always resumed by its existing reader.

## Boundary and acceptance

Caller: local Launcher; receiver: existing RKB HTTP process; transport:
loopback HTTP GET. Principal: `launcher-import-watch`; scope:
`{"operation":"import_activity"}`; methods: GET only; target: an exact known
ingestion UUID. No idempotency/receipt lifecycle is needed for a read-only GET.
Database authority remains the current source intake control and accepted
packet journal. No second registry/table, tokens in query strings, new
provider dependencies, or public listener. Local CI is **not** live E2E
acceptance; Shkiper installs and verifies after RKB-Opus merges.
