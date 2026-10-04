# Retained embedding benchmark evidence

PR #26 is superseded for implementation by the narrow source-completeness port
after E5 PR #27 and BGE PR #28. Its evidence is preserved, not discarded:

- [CPU benchmark](devcoveer-small-embedding-benchmark-20261004.md) and
  [original aggregates](devcoveer-small-embedding-benchmark-20261004.summary.json).
- [Retrieval validation](embedding-retrieval-validation-followup-20261004.md) and
  [original aggregates](embedding-retrieval-validation-followup-20261004.summary.json).
- [Source completeness audit](gause-import-coverage-audit-20261004.md).

These five files are byte-identical to PR #26 head
`d987eedac2396a4ecd65fd9acbbb0691c6c18bfb`. Historical source/benchmark links in
them refer to that experiment, not to a new canonical runtime. The full scripts,
fixtures and commit history remain accessible in the
[immutable evidence tree](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/d987eedac2396a4ecd65fd9acbbb0691c6c18bfb/scripts/benchmarks)
and [PR #26](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/26).
They were not rerun or imported into the production embedding path.

Accepted production behavior is documented in the
[E5 report](fast-e5-production-acceptance-20261004.md),
[BGE report](bge-kaggle-multilingual-hybrid-acceptance-20261004.md) and
[runtime decision](embedding-runtime-decision-20261004.md). Their historical
latencies are measurements, not new SLA guarantees.

Only source transport/provenance/validation and associated tests/docs were ported
from #26. The active incomplete source projection remains unchanged; a future
ChatGPT-led visual review/reimport is still required. Page IDs, native previews
and even complete native extraction cannot prove visual/semantic completeness.
