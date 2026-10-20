# Private retrieval acceptance observation

This is an inert internal harness, not an MCP method, search mode or deployment
switch. Ordinary `QUERY` keeps mandatory lexical+BGE retrieval, its default
three hits and maximum five. No vector is returned to Dev. The private operation
always runs **both** branches once; “BGE only” means evaluating its recorded
dense rank list, not disabling lexical search in the product.

`ACCEPTANCE_READ` shares existing authenticated WSS, actor-scoped admission,
resource claim/release, generations, two-second absolute budget, pool and pinned
tokenizer/encoder. In one repeatable-read read-only transaction it executes the
same scoped lexical and dense functions, candidate budget100, HNSW strict order,
ef_search100/max_scan_tuples1000 and RRF60. It hydrates the union of the three
top-ten lists (at most30 passages), including fused hits outside branch top10.
Every hydrated passage passes the existing current source-history/revision/
eligibility gate. Pending changes, withdrawal, missing hydration, dense failure
or invalid proof withhold the entire observation rather than count as a miss.
No SQL, model, vector, rank limit or retrieval setting is caller-selectable.

## Freeze and authority

Only reviewed in-process composition may construct `AcceptanceObserver` with the
existing queue and SourceReadGate. Normal startup never constructs it. Neither
a serialized fixture nor a parsed request grants authority. Both admission and
the output gate require the installed exact `FrozenAcceptanceFixture`, a retained
current canonical owner authority, owner route and personal destination. A
copied authority, delegated service/non-owner identity, changed runtime binding,
different source scope, different gold hash or unlisted query is denied. Config
changes invalidate delayed issuance. No owner credential is written to fixtures,
results, source or documentation.

Before installing the fixture, independently verify and freeze:

1. Candidate source commit, loaded executor/transport hashes, native schema/
   function/catalog pins, actual run/incarnation/version/database/connection
   generation and search settings. Record these before opening any ranks.
2. Two independent licensed original source units through normal supported
   intake/import. Record exact original SHA/version, accepted source IDs and
   content/eligibility revisions, acceptance receipts and applied frontier.
3. Private gold bytes/hash, authored before retrieval. Independently bind every
   gold support span to complete native page/region proof and accepted passage
   IDs, recording an immutable support-map SHA. A PDF reader span alone is not
   native MCP provenance. Unsupported/unmatched support is explicit, not guessed.
4. The complete question SHA256 allowlist (not answers) and request order. The
   fixture contains owner UUID, the exact ordered two source UUIDs, gold SHA,
   support-map SHA, exact `TargetReadBinding`, accepted minimum sequence and
   exact source content/eligibility revisions and bounded question-hash tuple.
   Both accepted source revisions are checked natively before encoding, even
   when retrieval has zero hits. Its canonical `.sha256` binds these plus fixed
   retrieval/model/tokenizer settings. Persist and hash `.wire()` privately
   before the first call. Freeze persists outside ephemeral RAM; process restart
   requires explicit reinstallation/review, never automatic corpus discovery.

The trusted owner runner obtains authority from the existing authenticated
resolver and calls `await observer.observe(question, authority=authority,
request_id=unique_attempt_id, consume=private_synchronous_writer)`. Authority
ownership transfers to this attempt. The consumer must synchronously persist
bounded bytes and return `None`; the harness rechecks current output immediately
before calling it and then releases authority/tickets. It emits only ordinal
IDs, per-ID source revisions, observed frontier, fixture hash and explicit
bounded-retrieval status. It never emits gold, query text, source text or vectors.
The runner is an internal embedding seam; no production CLI, HTTP endpoint or
automatic benchmark trigger is installed by this change. An actual operator
invocation must reuse the API's retained admission owner rather than start a
second process competing for its queue lock or invent a synthetic owner.

## Runtime compatibility and activation

The frozen v93 transport does **not** recognize `ACCEPTANCE_READ`. Editing only
the read function cannot enable this operation. The inspected runtime constructs
its query handler once during `TargetRuntime.prepare`; transport dispatch has a
fixed envelope allowlist. There is no implemented authenticated stateless bundle
activation API that can replace these loaded objects with independently verified
readback. A proposed hot update is therefore **not supported** by this patch.

Do not publish/reload BGE merely to obtain these measurements. Keep the current
run useful for PG, first-snapshot drill, intake and ordinary read proofs. Parent
review may include this code in the next already-required controlled release,
after exact snapshot/tail recovery validation and UNKNOWN reconciliation. That
release must freeze the new worker bundle, transport and expected runtime hashes
and preserve accepted data/command interpreter history. Adding the small contract
module to the bundle is necessary because the shared query adapter imports it;
this changes bundle/source pins even when observation remains disabled. No SQL,
native package or model-space change is required. No notebook Save, service
restart or runtime activation is authorized by this document itself.

## Measurement and limits

After candidate/config/gold/support hashes are frozen, an independent evaluator
opens the private gold and maps each result list to already-bound known support.
Report BGE, lexical and configured fusion known-support Hit@5/@10 (optionally
Hit@1/MRR@10), both per source and pooled. Keep unavailable/denied/incomplete
attempts explicit and outside a fabricated zero-hit score. Family-level paired
uncertainty uses the30 independent fact families, not60 correlated paraphrases
as independent samples. Preserve all observations before computing metrics; no
post-gold synonym/config tuning on this held-out set. Do not claim exhaustive
precision/nDCG from positive-only support labels, exact ANN recall, German-slice
coverage, mass scale, protected capacity/latency or general product acceptance.

Local regressions use synthetic tokenizer/encoder/SQL data only. Their real
generated transport→ASGI→shared admission→gate→private issuance round trip proves
plumbing and current-source fencing, not actual Kaggle BGE quality or deployment.
