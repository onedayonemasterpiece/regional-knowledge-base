# Personal research drafts

The candidate stores private draft versions in the existing corpus SQLite
authority. They are separate from the shared case event stream, contributions,
conclusions and search/export material. Neither a case manager nor the owner can
read or rewrite another participant's draft through the product API.

Use the existing `disagreement_conclusion` tool on the same `/mcp`:

1. `action=draft_list` with `case_id` lists only your accessible draft IDs and
   current versions. Follow `next_after_id` using `after_id`; `limit` is bounded.
2. `draft_save` takes a stable client-generated `draft_id` UUID,
   `expected_version=0` for creation, a stable `operation_key`, and a
   `PersonalDraft` command: text, exact `based_on_seq`, existing case evidence and
   position IDs. Updates require the current private draft version.
3. `draft_get` returns your current draft; `expected_version` selects an exact
   historical private version. A new chat can recover the ID through `draft_list`.
4. `draft_publish` requires the current private `expected_version`, a new stable
   operation key and `DraftPublication`: exact half-open text offsets, contribution
   kind and selected evidence/position references. The selected substring becomes
   one ordinary contribution. The unselected analysis remains private; the
   original draft and older versions remain unchanged.

Both draft read/save require `personal_conclusion`, `case_read` and actual current
case/source access. Publishing additionally requires `case_contribute` and a case
contributor grant. Stored request generations, service revocation and group
audience restrictions also apply. Slack/dot uses `draft_list/get/save/publish`
operations of the existing `expert_research` tool, with the saved
request/generation/execution-token context; it does not acquire owner powers.

The private save and version/receipt commit atomically without changing shared
case revision or history. Publication calls the same contribution writer as a
direct contribution, in one transaction with its private publication link and
normal immutable operation receipt. A crash before commit leaves no contribution,
event or publication link; repeating the same key returns one committed result.
Authorization is rechecked before receipt replay. A changed draft version does
not invalidate an already committed publication receipt, or permit a new stale
publication. The ordinary `operation_status` action can recover its shared receipt.

Draft dependencies include existing case sources and server-known sources/cases
read in the saved request, including inherited history. Private edits retain
those dependencies. Revocation prevents disclosure of old versions and cached
receipts. For this first release, publication requires every retained dependency
to belong to the same case evidence boundary. Otherwise the API returns
`draft_publication_requires_case_evidence`; selecting a substring alone cannot
prove that a private derivative became shareable. Resolve the evidence/access
boundary explicitly rather than copying the whole private analysis into a case.

A draft and its published contribution remain model-mediated account actions.
Neither asserts that the person read a source or supported a conclusion. Actual
human support, exact-version confirmation and public case projection remain
separate requirements. The source candidate is not deployed; synthetic domain,
MCP and HTTP tests do not establish the real Slack/non-owner PMI scenario.
