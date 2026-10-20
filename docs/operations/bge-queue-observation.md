# Compatibility queue observation

The existing HTTP worker path now distinguishes `stale_claim` (409) from
`revoked_run` (403). A stale assignment discards only that pending result; the
worker's existing 409 branch continues claiming work. Revocation stops the run.
Neither response proves provider compute termination or releases a Telegram
session-use lease.

Status, lookup, result and pending-count reads use query-only snapshot
transactions, without `BEGIN IMMEDIATE`. They project expired assignments as
pending without mutating the queue. Claims and maintenance still reconcile
persisted leases transactionally; observation does not renew them.

Local result waiters are notified only after completion commits, including
between queue instances in the same process. A bounded 250 ms reconciliation
path observes completions from other processes and recovers missed hints. A
wait has the caller's finite deadline and removes its subscription on exit or
cancellation. Notifications carry no authority or result bytes; each wake reads
the actor-bound durable row. The old 50 ms polling loop is retained only for
non-queue compatibility adapters.

This is a repair of the existing compatibility path. It does not implement
the target ephemeral QUERY/admission or claim PostgreSQL/WSS, remote durability,
mass-ingestion readiness, exact provider Stop or deployment acceptance.
