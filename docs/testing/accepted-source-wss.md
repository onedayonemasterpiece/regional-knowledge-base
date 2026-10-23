# Accepted source commands on the existing WSS channel

`AcceptedSourceDispatch` binds the existing ControlSpool, BgeQueue and shared admission.
`enqueue(owner_context, operation_key, writer_generation=...)` requires current canonical
owner import authority, an already sealed exact source import/withdraw intent, and a
real `RecoveryJournal.verify_accepted` read. Dispatch repeats that real acceptance check
and current run/incarnation/connection/database/writer binding. Public queue enqueue,
HTTP encoding completion and read QUERY cannot submit or complete these commands.

A `source_command` durable job retains only an immutable pointer/hash/generation in the
existing jobs table. It uses the same canonical-person scheduling and actual document
resource claim as existing durable work. The fixed DISPATCH carries exact command bytes,
singleton manifest/seal, registry fingerprint, run/nonce/incarnation, original dispatch
and compute claim, and database/writer generations. Only this dispatch subtype permits
up to 2 MiB. Read/control limits are unchanged. Native source receipts are at most 16 KiB.

TargetLaunchSpec's optional `command_handlers` list contains only import/withdraw route
names and independently observed exact native check/apply definition hashes. It is part
of the launch/source binding. After early control and resident preparation, TargetRuntime
opens one bounded `rkb_writer` pool alongside the reader pool. `WorkerSourceExecutor`
constructs a narrow ControllerAcceptedProof for the one captured immutable command/seal.
It trusts the authenticated controller's actual remote-membership verification; it has
no cloud journal credentials and cannot verify a different command or write a journal.
The existing `apply_accepted` still verifies exact function definitions, next sequence,
writer/database generation, atomic native apply ledger and duplicate outcome. No SQL,
model choice, arbitrary method name or inference request is supplied by the caller.

Native apply survives WSS reconnect in the same process. Its pending exact receipt is
resent without another apply or BGE forward. Dev checks the admitted resource identity
and native canonical outcome against the original command, then persists record_apply
with the spool's FULL durability. `apply_received` acknowledges only that local receipt;
it keeps the worker receipt and durable job pending. A bounded task belonging to that
same existing job publishes the remote outcome through the existing journal, with
bounded backoff on UNKNOWN/outage. Cloud latency does not serialize ordinary read/control
dispatch. Only remote durability permits queue completion and the normal RESULT_ACK.
A disconnect/lost ACK reuses the exact original operation; native duplicate receipts
and existing remote outcomes reconcile delivery without recomputing model output.
No user Saved or product completion is emitted by this transport.

Exact `PASSAGE_READ` uses the existing ephemeral read resource and deadline/generation
rules. Admission and the channel enforce the paired `PASSAGE_READ_RESULT` type. The
worker routes it to the exact passage handler and never invokes encode/token counting;
it has no search mode and uses no durable mutation receipt.

Verification uses actual local SQLite/journal/admission and ASGI WSS plus synthetic
native callbacks. It covers untrusted queue attempts, owner authority, exact accepted
proofs/function pins, generation and compute-claim rejection, durable-outcome ordering,
source result reconnect without encode/reapply, and exact-read type pairing. Actual
native SQL and Kaggle provider/bootstrap checks remain independently reported by the
root task. The bridge does not create a second broker, identity or scheduler.
