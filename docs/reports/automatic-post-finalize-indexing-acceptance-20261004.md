# Automatic post-finalize indexing acceptance — 2026-10-04

A newly activated revision now automatically receives local E5 vectors and
existing durable Kaggle BGE document jobs. No manual backfill is required.
Missing active vectors and the accepted SQLite BGE jobs are recovery state;
no additional durable queue or model platform was introduced.

## Implementation and safe search

SQL014 adds a blank, commit-delivered activation notification and actor-RLS
coverage counts. A dedicated bounded owner listens, reconciles on startup and
every five seconds, hydrates source bytes through ordinary owner authorization,
checks exact source hash, and rechecks active revision/hash at installation.
E5 retains ordered per-document batch4; BGE retains single-document jobs and
query-first claims. At most two E5 batches and 16 missing BGE rows per document
are handled per pass; unfinished document jobs are capped at 64.

`indexing_status`, finalized ingestion status and search expose actor-scoped
ready/missing counts, indexing/worker state and effective retrieval mode.
Incomplete BGE uses complete E5; incomplete E5 uses lexical search. SQL snapshot
coverage guards also suppress partial semantic branches during activation races.
Paid embedding fallback is forbidden when automatic indexing is enabled.

Passage transport accepts the existing 40,000-character chunk bound; query
bounds remain unchanged. Encoder tokens, normalization, model revisions and
batch sizes are unchanged. No native encoder library loads in MCP/index owner.

## Actual production acceptance

Public OAuth MCP intake used an HTTPS attachment containing an operator-created
12-page synthetic PDF. All twelve returned source renders were visually
reviewed; native material was checked for truncation. Four-page stage batches
were replayed, then ordinary validate/finalize activated twelve chunks.

Measured from the database activation timestamp:

| Event | Seconds |
| --- | ---: |
| First E5 vector | 3.174 |
| All 12 E5 vectors | 11.353 |
| First durable BGE document job | 11.859 |
| All 12 BGE vectors | 201.111 |

The owner was stopped at E5 coverage 4/12, lexical search was verified while
stopped, then the owner was started with a different PID. It resumed remaining
work. E5 search/fetch passed while BGE was incomplete; final BGE lexical
search/fetch returned the control source. Finalized ingestion readback reported
zero missing vectors and removed the pending warning.

One actually claimed control BGE document job was interrupted through an
**accelerated liveness fault injection**: its run heartbeat was made overdue
and the existing queue expiration path reclaimed it. The accepted controller
started a replacement CPU-only Kaggle worker; the same job completed with at
least two attempts. This is a controlled recovery test, not a measured natural
provider outage. The completion time includes that replacement cold start.

Results: 12 new E5 vectors, 12 new BGE vectors, 12 unique document jobs;
finalize replay produced zero duplicate jobs. Existing corpus E5/BGE/source and
legacy vector digests remained unchanged. The synthetic control was archived,
leaving 747 active chunks, 747 valid E5 vectors and 747 valid BGE vectors.
Foreign-actor corpus counts were zero and scoped control status was denied.
Temporary acceptance OAuth families were revoked. Manual backfill and external
paid embedding calls: zero.

An initial operator invocation exceeded the existing eight-page staging bound;
it was rejected before activation. The runner was corrected to four-page
batches. No product limit was relaxed.

## Verification and delivery

118 tests passed locally with actual isolated PostgreSQL/RLS, including LISTEN
wake-up, restart recovery, exact-source validation, partial-index SQL guards,
source/revision staleness, queue query priority, replay and actor isolation.
PR #32 CI passed on Python 3.12 and 3.13. SQL014 was applied twice in the
isolated fixture before production; production migration preserved all baseline
corpus/vector digests. Delivery/readback of the final merge is retained with
its exact SHA and main CI in the operational receipt and PR description.

Private fixture IDs, source renders, queue recovery evidence, migration proof,
timings and exact-runtime receipt are retained under
`/home/dev/artifacts/regional-knowledge-base/20261004T110316Z-automatic-post-finalize-indexing-20261004`.
They are not part of the public repository.

## Boundary before corrected Gause import

Automatic indexing is ready for a new activated revision. Existing source
completeness issues were not repaired by indexing: old clipped text and omitted
image/caption material remain explicit. Corrected ChatGPT-led Gause import also
requires the separately requested robust dedup/illustration/media follow-up.
No historical source was reimported or semantically interpreted in this task.
