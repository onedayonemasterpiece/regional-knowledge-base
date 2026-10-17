# Article-scoped search (2026-10-17)

Catalog article IDs returned by book_find can be supplied to search and
knowledge_search document_ids. The local corpus resolves an article to its
readable, searchable parent and selected revision, then restricts retrieval to
the article's registered semantic chunks. This does not create a second source,
copy vectors, or widen ACL access.

A stale article revision, unreadable parent, hidden source/article, or unknown
ID is rejected. Multiple scopes are a union; explicitly selecting the entire
parent also includes its other articles. Lexical and exact-alias filtering runs
before LIMIT. Dense candidates remain bounded at the parent vector plane, then
are filtered before local fusion. Therefore very small article scopes can still
lose dense recall through parent-level candidate crowding; this change does not
claim an article-native remote top-k index. Continuations retain existing
component boundaries.

Validation: six new regressions cover before-LIMIT lexical filtering, dense
candidate hydration, mixed parent/article union, stale/private/hidden/missing
scope rejection, all three local RPC retrieval branches, public search and
continuation boundaries. Focused tests: 21 passed. Full suite: exit 0,
29 pre-existing isolated PostgreSQL/pgvector tests skipped. Uses existing
isolated PyMuPDF test dependency cache.

Relevant accepted-source snapshot gate: disposable copy of the 2026-10-17
19:43:32 UTC backup; original opened read-only. Re-registering the same article
in that disposable DB yields 18 chunks. All 18 query lexical rankings equal the
parent ranking projected onto that article; no outside chunk is returned.
54 measured scoped calls: median 4.03 ms, p95 16.40 ms, maximum 19.06 ms.
Every saved top-two query result has an in-article hit; one saved top-two
candidate outside the article is intentionally excluded. This is not a fresh
dense-quality or mass-import acceptance claim. Live article-scoped verification
must follow rollout.
