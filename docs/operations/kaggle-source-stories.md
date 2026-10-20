# Native source-bound Story activation

The new `source.import.v2` contract carries exact resolved source inputs and
model-authored findings/narratives together. V1 bytes and function semantics
remain unchanged. New writes require the ordered revision tuple
`rkb-v4-sql001-007-v1` / `rkb-v4-source-story-import-v2` /
`rkb-python-apply-v1`; a builder does not authorize or install that transition.

Each staged candidate remains an immutable source finding even when it produces
no Story. `basic_variant` supplies an actual body plus a complete Unicode-offset
evidence map; title and summary have no body fallback. A missing body is either
explicit `facts_only` or `needs_context` with the existing reachable research
frontier. Dates, observations and model-selected entity keys remain attached to
the finding. Source excerpts are checked against exact persisted region bytes
and an imported passage. These structural checks do not establish historical
truth or independently judge narrative quality.

The importer resolves stable source-bound IDs and gets vectors for the exact
body through the same Kaggle DOCUMENT admission as source passages. The accepted
command contains those results; recovery never reruns the model or substitutes a
new summary. Native activation must save source, findings, Story, actual default
Variant revision/body, evidence/projection, frontier and apply outcome atomically.
A successful saved Story requires actual ordinary native get/readback of those
exact bytes and reference.

This document tracks implementation in progress. The resolved contract has
synthetic validation checks; native SQL/read/search integration, rollback/replay
fixtures and actual target acceptance are not established by that result. The
existing target still rejects unsupported Story operations until the explicit
native handlers/proof/output path is installed through its ordered barrier.
No current controller, runtime configuration or public profile is changed here.
Legacy SQLite Story ingestion is not a substitute or fallback for this path.

## Synthetic native evidence, 2026-10-20

Six bounded PG17.10/pgvector0.8.2 tests passed in a network-isolated disposable
fixture. They verified exact saved body/default reference through the ordinary
native read function, history/variants, direct lexical+dense candidates/hydrate,
retry and a new connection, withdrawal, mid-Story failure rollback of the entire
source/frontier/applied receipt, unchanged accepted body/outcome replay into a
fresh generation, false-quote rejection, and unique full catalog identifiers.
They also verified accepted author/request/op/sequence receipts and rejected a
consistently forged saved body after a synthetic immutable-trigger bypass.
The fixture consumed 192MiB with 96MiB PGDATA tmpfs; the final six-test run lasted8.063s
and verified stop/removal/absence. No BGE, provider, real source or deployment was
involved. PG17.11 remains a candidate artifact pin rather than this observation.

The new long foreign keys exposed a catalog-observer bug: a CASE branch of
`pg_catalog.name` truncated combined internal trigger identifiers to63bytes.
Casting that branch to text preserves the actual unique identifiers. The
reference catalog has488 records/183951bytes, so the512-record/1MiB bounds
remain unchanged. Its base64 catalog occupies245268bytes; this is not a claimed
actual BOOTSTRAP_OBSERVATION frame size. Native definitions independently report
SECURITY INVOKER/bootstrap ownership. Safe role/ACL and catalog reference bytes
are retained separately from code under the central managed artifact task.

This is fresh-generation7 evidence only. All existing generation6 data and v1
handlers remain intact. An initialized generation6 must not receive007 through
adoption or latest-first replay. A future ordered migration barrier is a separate
unpassed gate. Native WSS routing, Story output/auth fences, full MCP get/readback,
private drafts, editing/merging/archive/review parity, and qualitative human
source-fidelity comparison remain incomplete and do not follow from these tests.


The schema7 history observer locks Story rows beside the source/applied prefix
and checks native rows against the exact captured applied v2 command payload.
Dev still checks the immutable remote journal and contiguous applied receipt
prefix; no body or SQL RPC is sent to Dev for that proof. Incremental extension
checks affected accepted import origins, while unchanged renewal retains the
same verified immutable prefix. The bounded native origin pass currently caps
history at10000 applied commands; larger histories require the retained-prefix
checkpoint improvement before claiming scalable recovery acceptance.

Fresh initialization selects artifacts through
`kaggle_owned_pg.sql_artifacts(schema_revision)`: schema6 remains001..006 and
schema7 selects001..007. The schema7 reader receives only the fixed four Story
read functions and their required read tables; the origin verifier uses the
separate trusted maintenance connection. No existing generation is adopted or
modified by this selection.
