# Regional Knowledge CPU scheduling for retrieval

On 8 GB RAM the host still has two vCPUs shared with development agents and other applications.
Postdeployment on 2026-10-08, a strict backend test on main b2d0b415 yielded p95
948.64 ms at concurrency 1 and 1348.03 ms at concurrency 2, with all quality checks passing.
Direct public MCP HTTPS search p95 was 987.8 ms (12 serial calls). CPU pressure (PSI
some 30–55%) is significant, while IO pressure is low.

**Current owner architecture (2026-10-17): BGE inference runs ONLY in the
Kaggle worker**, via the existing durable, owner-bound BGE queue on DevCoveer.
The legacy local ONNX BGE query unit must be **disabled and stopped**, not
started as a remedy for latency. The main MCP and story indexer enqueue
deduplicated BGE jobs and report Kaggle warming/pending truthfully; zero
local BGE model bytes should be loaded.
E5 remains a separate **local** CPU sidecar, restored and assessed independently.
The MCP unit receives CPUWeight=250 via the versioned override in ops.
The indexing unit retains CPUQuota=50% and has CPUWeight=70.
These are relative CPU weights, not absolute guarantees. They do not stop or
change unrelated applications. This preserves resumability of background indexing.
A previous local BGE service in 1.7 GiB RAM repeatedly reached ~1 GiB and was
OOM-killed; increasing VM memory is not authorization to violate Kaggle-only BGE.

## Verification

Deployment must use an immutable merged main SHA. Install/read back the service
priority configuration, check exact release SHAs, all service health, BGE readiness,
and authenticated search correctness. Re-run strict backend p95 <=1000 ms at
representative concurrency; public MCP p95 <=1500 ms and hard evidence deadline
<=2000 ms. Preserve a failed measurement rather than lower requirements.
If unstable with co-located developer jobs, a 4-vCPU or dedicated retrieval
allocation may be required. Do not claim 100-book/100k chunk readiness from this
two-book acceptance, which has separate capacity and retrieval-quality gates.

## Nearby source context (bounded, not a second search service)

When a top-ranked accepted chunk is contextually incomplete, the existing
retrieval post-pass may examine at most **two adjacent source-position chunks**
on either side of each of the top two hits. A two-position context hop is
allowed only within one authorized original document, active revision,
article/component and physical page, with its intervening chunk also on that
page and no illustration-bearing content crossed. The same actor/source
authorization is applied *before* fetching any neighbor.

Only **one** contextual source result can be added to the requested result
window; top-two ranked evidence is protected. Grammatical continuation retains
priority on the immediate hop. Otherwise at least three question cues and more
overlap than the anchor are required, with a passage-length normalization to
avoid over-favoring long general background. Context signals mark direction,
hop count and `same_page_context` distinctly from primary BGE ranking.
No user-specific titles, named places, entity IDs or question rewrites are used.

The original natural gold and independent questions must still be verified
against the **deployed** MCP and full source evidence. This bounded supplement
does not make weak BGE dense retrieval a passing general holdout.
