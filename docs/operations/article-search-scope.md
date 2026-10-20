# Article-scoped search

Updated 2026-10-20. Normative target: [implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), one PostgreSQL/pgvector corpus on Kaggle with BGE-only retrieval. This documentation update is not rollout evidence.

## Scope identity and retrieval

Article IDs returned by `book_find` may be passed to `search` and `knowledge_search.document_ids`. Resolve an article to one exact accepted active parent revision and its registered semantic chunks. Article scope is a projection of the source; it creates no second binary import, vector copy or evidence identity.

Reject unknown, stale, hidden or withdrawn source/article scopes. Accepted active book bibliography and text are searchable without a per-book owner filter. Revision/current-policy checks and genuine private resource authorization still apply. Multiple selected scopes form a union; selecting the parent also includes its other articles. The article boundary applies to chunks, footnotes and continuation expansion; a distant continuation needs its explicit link, while a neighboring article is not a continuation.

Apply eligible article/chunk scope before lexical and exact-alias LIMIT. For dense retrieval, use a scoped PG plan or a measured bounded candidate/refill strategy; filtering a small article from a parent/global ANN shortlist can crowd out relevant hits. Report retrieval mode and degraded/partial outcomes, and measure real scoped recall. ANN and bounded refill are not proofs of exhaustive semantic coverage.

Citations retain article title/contributors, containing issue/edition, exact physical page/printed label and source revision. A catalog article record is not the old book author's ownership ACL and does not grant proof pixels or the full PDF. Public illustration renditions are available to everyone; quote/page/crop scans use the existing `scan_proof_read` policy for owner/demo/expert/local_historian, with actual geometry constrained to scope. The current materialized grant, not a category label, authorizes rendering; apply verified-category creation, expiry/removal and manual-revocation rules in [rights.md](../rights.md). Recheck withdrawal, active revision and audience after a delayed query or stale reader.

## Fresh import and acceptance

Old article/chunk/vector projections do not migrate. Fresh MCP book/issue/article import rebuilds them from the verified original and newly accepted semantic graph. The sole data carryover is GEO under the [GEO cutover contract](../migrations/rkb-layers-only-cutover-20261019.md); its legacy source refs do not become searchable article text.

Target acceptance covers two articles on one page, remote continuation, stale revision, parent/article union, a narrow article among many parent chunks, hidden/withdrawn scopes, public text retrieval without a book grant and denied proof/full-original access. Preserve one usable search during import and the protected latency/quality gates from the handoff. Record BGE, lexical and fusion behavior separately.

## Historical evidence — 2026-10-17

The prior local-corpus implementation applied lexical/exact-alias filters before LIMIT, but selected dense candidates at the parent vector plane before article filtering. That was a known small-scope recall limitation, not an article-native top-k acceptance.

Its recorded focused suite had 21 passing tests; the full suite exited 0 with 29 pre-existing isolated PostgreSQL/pgvector tests skipped. A disposable copy of the 2026-10-17 19:43:32 UTC backup yielded 18 article chunks; all 18 lexical rankings equaled parent rankings projected into the article. Across 54 scoped calls, median was 4.03 ms, p95 16.40 ms and maximum 19.06 ms. These figures are unchanged historical measurements, not fresh dense-quality, mass-import or Kaggle acceptance. Full context: [immutable source document](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/article-search-scope.md).
