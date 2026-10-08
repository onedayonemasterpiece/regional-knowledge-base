# Vector retrieval RLS v5 — statement-scoped policies

Version v5 optimizes PostgreSQL authorization checks for the compact
SQLite-authority architecture. The application authenticates a principal,
authorizes readable documents and active revisions in SQLite, and sets an
actor-scoped, transaction-local PostgreSQL document/revision map.
**The database must still enforce the resulting scope independently**:
v5 runs as `SECURITY INVOKER` under `rkb_app`, not as a table-owner bypass.

## Why

Under the prior policies, scanning roughly 4,900 vector anchors evaluated
`rkb_vector_scope(document_id)` for every row, then embedding-table RLS
ran a correlated EXISTS lookup for thousands of matching chunks.
A read-only production plan measured approximately 21.6 ms under the table
owner against about 294.3 ms under `rkb_app` with the same ranking SQL.
The roles differ, so the owner timing is diagnostic rather than acceptance.

## Mechanism

Migration `sql/023_vector_rls_v5.sql` is one transaction:

- `rkb_vector_readable_documents()` returns an empty array without a valid
  actor. Under the rkb_app anchor RLS policy it is evaluated through a
  noncorrelated scalar SELECT, so PostgreSQL can cache it as an InitPlan per
  statement rather than invoke a document-scope function per anchor.
- Both embedding tables permit reads only when their chunk IDs appear in the
  independently RLS-filtered vector-anchor table. The membership subquery
  is noncorrelated; PostgreSQL may implement it as a hashed subplan.
- `rkb_vector_candidates_v5` uses `SECURITY INVOKER`, checks that an actor
  and explicit revision map are present, validates document/revision scope
  consistency, and restricts returned candidates to those active revisions.
  The search space and model-revision checks match v4. Both BGE and E5 have
  the same rules.

**Trust boundary:** the `rkb_app` role is accessible only to the server's
PostgreSQL bridge. GUC scope values are *not* end-user claims; SQLite ACL
authorizes and constructs them before SQL execution. PostgreSQL RLS
independently enforces that already-admitted transaction scope, but does not
implement the full SQLite user/workspace/rights grant model itself.
Never expose the rkb_app connection string or allow users to set actor/scope
GUCs directly. Existing backend checks, revision/hash comparison and
actor-scoped evidence hydration remain mandatory.

## Safe rollout

1. Run unit and isolated real-PostgreSQL RLS tests, including v4/v5
   candidate parity, missing/unknown actor, wrong revision, invalid scope,
   and rollback.
2. Apply the migration using
   `python scripts/production/apply_vector_rls_v5.py`. The routine and
   policies are installed atomically, with no corpus/vector data mutation.
3. Leave `RKB_VECTOR_CANDIDATE_VERSION=v4` during the first production
   **shadow test**. Compare v4/v5 ranked IDs, hashes, branches and timing
   under exactly the same authorized actor/scope and query vectors.
4. Switch explicitly to `RKB_VECTOR_CANDIDATE_VERSION=v5` only if parity
   and performance checks pass. Restart the MCP/indexer services using a
   single immutable release SHA. Confirm public MCP timing and index status.
5. To revert, first restore `RKB_VECTOR_CANDIDATE_VERSION=v4`.
   Apply `python scripts/production/apply_vector_rls_v5.py --rollback`
   only when old policies are required. The v4 function remains installed;
   no vectors, books or source metadata are deleted.

The full corpus/vector maintenance path re-applies the v5 policy migration
after its legacy v2/v3/v4 migrations to avoid silent version drift.
The required dense-only Hit@5/10, RU→German slice, latency, ACL and
100k-chunk/Supabase-capacity gates remain unchanged. The two-book test does
not by itself prove mass-ingestion readiness.
