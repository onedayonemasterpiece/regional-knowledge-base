# RKB BGE compact-storage experiment — 2026-10-09

**Result:** measured on the authorized live vector PostgreSQL without changing permanent tables. This is a storage experiment, **not** search-quality, peak-reimport, 100k-corpus, or production-release acceptance.

## Scope and custody

- Observed 2026-10-09 13:34–13:37 UTC; current indexed RKB vector database, `pgvector 0.8.2`.
- Read-only size audit: `scripts/production/audit_storage_budget.py`; observed `pg_database_size=94,967,475 bytes`. Its obsolete `rkb_documents/rkb_chunks` remote probes returned `UndefinedTable` under SQLite authority and **must not** be interpreted as missing books.
- Bounded, rollback-only temp experiment: `scripts/benchmarks/vector_storage_design.py`. Original experiment was recovered from existing DevCoveer work, not rewritten as a new provider test. New canonical script fixes direct script imports and defaults to at most 6,000 sampled BGE rows; explicit cap may rise to 20,000 after operator capacity review.
- 5,813 existing BGE rows copied into two temporary relation/index alternatives. Includes historical/inactive rows; contrast current active chunk readiness 2,434 BGE-ready / 2,434 total. Temp table changes were rolled back. No private documents, vectors, source texts or identifiers are in this report.
- Current required retrieval space: **BGE**; E5 optional. Legacy `halfvec(768)` documentation is not the deployed BGE representation.

## Actual allocated bytes, not raw payload estimates

| Representation | Rows | Table bytes | Index bytes | Total bytes | Total MB (decimal) | Linear 100k MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Current `vector(1024)` + HNSW | 5,813 | 34,078,720 | 33,628,160 | 67,706,880 | 67.71 | 1,164.75 |
| Temp `halfvec(1024)` + HNSW | 5,813 | 16,605,184 | 11,411,456 | 28,016,640 | 28.02 | 481.97 |
| Temp `halfvec(1024)` + binary-quantized HNSW | 5,813 | 16,605,184 | 2,072,576 | 18,677,760 | 18.68 | 321.31 |

Current `rkb_vector_items`: 6,817 rows / 3,244,032 bytes, a **linear-only** estimate of 47.59 MB per 100k anchors. E5 table + indexes currently 10,682,368 bytes. Current entire PostgreSQL: 94.97 MB (about 90.57 MiB); this includes other public and system relations, and does not equate to actual free project quota.

## Scale/capacity conclusions

The protected requirement is **at least 100 book-equivalent sources, 100,000 active chunks**, with database steady ≤80% quota and largest-source reimport peak ≤90%. If the applicable quota is 500 decimal MB, those ceilings are 400 and 450 MB respectively. Confirm provider's exact quota/units before the final gate.

The current fp32 BGE representation plainly fails 100k on this linear extrapolation; even halfvec + HNSW is ~482 MB **before anchors and baseline**. Binary-quantized HNSW with halfvec payload is the first plausible candidate, not a proven fit: ~321 MB BGE plus ~48 MB anchors already consumes almost all of the 400 MB steady target before system baseline, E5 retention, story projections, WAL/rebuild, and reimport overhead. Index/payload growth need not remain linear; the required 100k peak must be measured rather than asserted from this table.

**No source truncation, dropping required retrieval quality, second Supabase account or hidden E5 requirement change is authorized by this result.** A separate geo database would not remove this same vector budget.

## Next experiment, with mandatory gates

1. Compare BGE-only exact/halfvec/binary HNSW + rerank **against the same held-out source-grounded ≥30 fact families** with Hit@5 ≥85%, Hit@10 ≥90%, critical language slices Hit@10 ≥85%, using no lexical prefilter. Also report lexical-only/E5-only/fusion ablations independently.
2. Check ANN candidate overfetch and exact halfvec rerank, permission scope before output, and p50/p95 backend/public MCP latency at representative concurrency (≤1,000/1,500 ms p95; hard 2,000 ms).
3. Quantify 100k active rows, 100 sources, retained inactive generations, largest-source reimport steady/peak, HNSW rebuild peak, index updates, story vector budget and WAL/TOAST/baseline. Fail the capacity gate explicitly if any required condition misses.
4. Only then plan reversible production index/table migration, dual-read/rollback and service schema validation. The temporary tables here were **not** installed as a replacement.

## Traceability

- Requirement authority: `.devcoveer/requirements.json` and `docs/mass-ingestion-readiness.md`.
- Canonical product continuation: `idea-hub/prompts/implementation/regional-knowledge-graph-and-geography-continuation-20261009.md`.
- Main story-phase deliveries: PR #87, #88 and #89 merged on 2026-10-09; these do not alter the required book-vector policy.
- Status dimensions: **measurement performed**; benchmark script versioned; compact ANN quality not accepted; 100k capacity not accepted; production migration not performed.
