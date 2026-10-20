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
