# Accepted import repair

Target2026-10-20: PG adapter, same existing book_ingest checkpoints and one research workflow. See [ingestion](../ingestion.md), [single handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Old source-specific repair reports remain history; do not replay old non-GEO imports as part of GEO cutover.

Metadata-only repair of a newly accepted source uses its exact ingestion/source revision, typed IDs and expected revisions. Source facts/dates/observations/entities do not require Story; sufficient episodes save existing Variant.body/default ref and normal readback. Invalid optional metadata returns a specific needs_review without losing valid evidence. Unexpected apply failure rolls back candidate checkpoint and domain changes together; no independently committed overwritten checkpoint. Retrying exact payload/key restores same result rather than new inference/Story/Variant.

Preserve existing accepted human edits, approvals, private drafts and parallel POI/map links. Wrong revision/source/quote/hash fails closed. Corrected metadata stage does not change accepted source pages/chunks/vector space; a real source/layout change uses a new revision. Fact/saved-body search projection updates only for changed semantic input.

Preliminary validation and final activation use the same semantic coverage kinds: heading/body/caption/footnote/table/marginalia. Running headers/page numbers retain provenance without standalone retrieval passages. Full visual review/source spans remain required. BGE validates full augmented input≤512; no E5 gate.

Catalog.update is owner-personal, allowlisted descriptive fields with expected_revision. Preserve source SHA, archive state, active revision, chunks, grants and story links. Unknown/identity mutation fields rejected. A fragmentary journal source is described honestly; article components have verified parent and exact region boundaries, no overlap or cut chunk, without new original/vectors solely for catalog change.

Focused checks: schema UUID/string normalization, semantic/layout coverage, atomic failure/replay, two corrected checkpoints without duplicates, facts without story, body/default readback, preservation of human edits and scope. Do not request the user to reimport the whole book just to fix a supported metadata checkpoint.
