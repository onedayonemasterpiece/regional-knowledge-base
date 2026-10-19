# Existing executor and Mira boundary — 19 October 2026

The source interfaces do not yet prove an automatic RKB request → isolated
executor → original Mira dialog → RKB result-hash ACK cycle. Ordinary personal
RKB MCP search/journal use remains independent of dot. This gap calls for an
adapter to existing contracts, without a separate expert MCP, IAM or relay.

The owner-authorized Slack thread was used to ask dot for current interfaces,
with an explicit read-only scope. dot acknowledged receipt, then returned a
sourced report. Recipient, exact thread/message receipts and the report remain
in retained private evidence. The following source findings were independently checked locally at exact commits;
the report itself is not a production receipt or a claim of completed integration.

| Existing boundary | Verified source scope | Remaining integration |
| --- | --- | --- |
| RKB `research_request` | Personal submit/result/hash-ACK bound to authenticated person/client/resource | A real client's durable result retrieval and original conversation sink |
| RKB `expert_request_delivery` | Saved request, fair claim, generation/token, history, dependency checkpoint and immutable result | Dispatcher that starts a fresh isolated model context and resumes it correctly |
| RKB MCP Events | Correlated request wake, not a final-answer callback | Actual executor acceptance, durable context and completion |
| Projects Hub `RegionalKnowledgeAdapter` | An injected `RegionalKnowledgeProvider.search_evidence`, with actor/workspace-bound input and bounded output | Concrete RKB transport and supported authenticated binding; no request submit/result/ACK adapter exists here |
| Projects Hub `MCPBindings` | Bound PH request/event → `discussion_reply` or verified `analysis_result`; stored result and collaboration event | Verified mapping from RKB request/person/client/resource to PH actor/conversation/turn and its receipt |

Projects Hub evidence is pinned at
[`2196bf62`](https://github.com/onedayonemasterpiece/projects-hub/tree/2196bf62b937ec61b1fa00d46dfc20ddb2930743):
[`regional_knowledge.py`](https://github.com/onedayonemasterpiece/projects-hub/blob/2196bf62b937ec61b1fa00d46dfc20ddb2930743/src/projects_hub/regional_knowledge.py)
contains no HTTP/MCP transport or OAuth acquisition;
[`scripts/run_server.py`](https://github.com/onedayonemasterpiece/projects-hub/blob/2196bf62b937ec61b1fa00d46dfc20ddb2930743/scripts/run_server.py)
calls `create_app()` without a regional knowledge factory. This is the standard
launcher source, not inspection of the current process configuration.
[`mcp_bindings.py`](https://github.com/onedayonemasterpiece/projects-hub/blob/2196bf62b937ec61b1fa00d46dfc20ddb2930743/src/projects_hub/mcp_bindings.py)
requires its own request/event/binding and validated result kind. An arbitrary RKB
request UUID cannot be substituted for a PH request. These durable result writes
are a useful existing sink, not a reason to bypass that authorization.

RKB source is pinned at
[`aee83d35`](https://github.com/onedayonemasterpiece/regional-knowledge-base/tree/aee83d35c8066f33c2cb03f928ba58957f50d61f):
[`personal_request_mcp.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/aee83d35c8066f33c2cb03f928ba58957f50d61f/src/regional_knowledge/personal_request_mcp.py)
and
[`expert_mcp.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/aee83d35c8066f33c2cb03f928ba58957f50d61f/src/regional_knowledge/expert_mcp.py).
These are undeployed feature contracts. A live provider-session checkpoint is a
different object from a saved RKB research continuation. Slack send success,
Mira `brief_seen` and human reading are also different from a client ACK of the
exact immutable RKB result hash.

The adapter must retain the accepted request across disconnects, recover the same
result, store it under the original authorized conversation/turn, verify that
native storage receipt, and only then ACK its exact RKB result hash. Delayed reads,
writes and receipt replay must recheck current human/source/case authorization.
Local PH↔RKB traffic uses existing loopback/Unix transport and a dedicated narrow
service identity with verified person/resource binding, per the
[same-host policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md).
It must not forward the owner's or user's bearer. External personal AI clients
continue to use RKB's existing resource OAuth directly.

No new service/grant, production execution or provider publication was performed
for this inventory. Actual dot isolation/resume, Mira rendering/receipt and the
complete second-client E2E remain unverified. They remain part of the product
objective rather than being replaced by these source checks.
