# Native Story editorial path

The fresh schema 11 target registers existing `story_edit` and `story_access`
when its fixed editorial handlers and independent tokenizer are configured.
The supported edit is one complete `UpsertVariant`; its exact body is saved,
versioned and selected by the existing Story reads and indexed search. Editing
clears approval rather than publishing the changed text. Other operation kinds
remain explicitly unavailable in this target profile.

OAuth and canonical Participants authorize the current personal caller. The
fixed native edit-context reader checks the current per-Story role and complete
variant references. Editors, publishers and managers may upsert; only managers
may grant or revoke access. The native apply function repeats source scope,
role and revision preconditions atomically. No owner fallback grants a Story
role. This path does not create private drafts or Case authority.

The protected tokenizer counts the entire caller-authored body. A body above
512 tokens is explicitly rejected; it is never truncated or replaced with a
summary. A retained authorization object admits only that exact document job
to the existing DOCUMENT queue and shared compute resource, for at most the
original two-second source proof. An unrelated document queued by the same
caller gains no authorization. Polling can obtain a fresh proof for the same
durable pending job. Revocation, changed source history or a changed runtime
closes admission.

After encoding, the exact vector/body and resolved identities enter the same
ControlSpool, immutable accepted journal and SOURCE_COMMAND dispatch as source
imports. The original idempotency key reuses accepted bytes without another
model call. An operation remains processing until its exact native receipt is
remotely durable. Final own-operation status uses the existing current HTTP
issuance guard; narrative reads continue through SourceReadGate.

Verification includes real OAuth/Participants/spool/journal/admission objects
with synthetic native context/vector/receipt observations, plus the MCP tool
entry point. The independent native fixture covers SQL body/grant/version and
replay behavior. These tests do not claim a deployed target or actual BGE
acceptance. Remaining Story parity includes other existing editorial operations,
long-body encoding, contribution workflows and publication/export approvals.
