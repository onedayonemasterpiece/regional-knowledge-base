# Regional Knowledge CPU scheduling for retrieval

On 8 GB RAM the host still has two vCPUs shared with development agents and other applications.
Postdeployment on 2026-10-08, a strict backend test on main b2d0b415 yielded p95
948.64 ms at concurrency 1 and 1348.03 ms at concurrency 2, with all quality checks passing.
Direct public MCP HTTPS search p95 was 987.8 ms (12 serial calls). CPU pressure (PSI
some 30–55%) is significant, while IO pressure is low.

The BGE query unit is CPUWeight=300 and CPUQuota=100%, without single-core CPUAffinity.
The MCP unit receives CPUWeight=250 via the versioned override in ops.
The indexing unit retains CPUQuota=50% and has CPUWeight=70.
These are relative CPU weights, not absolute guarantees. They do not stop or
change unrelated applications. This preserves resumability of background indexing.

## Verification

Deployment must use an immutable merged main SHA. Install/read back the service
priority configuration, check exact release SHAs, all service health, BGE readiness,
and authenticated search correctness. Re-run strict backend p95 <=1000 ms at
representative concurrency; public MCP p95 <=1500 ms and hard evidence deadline
<=2000 ms. Preserve a failed measurement rather than lower requirements.
If unstable with co-located developer jobs, a 4-vCPU or dedicated retrieval
allocation may be required. Do not claim 100-book/100k chunk readiness from this
two-book acceptance, which has separate capacity and retrieval-quality gates.
