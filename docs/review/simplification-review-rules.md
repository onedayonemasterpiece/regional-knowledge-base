# Simplification review rules

These rules are for a reviewer whose only job is to remove unnecessary complexity.
They apply to any codebase. The review result is a list of deletions and blockers, not improvements.

## 0. Burden of proof

Every condition, limit, timer, pin, hash, state, mode, retry, special case, table and
protocol message must justify itself. If it cannot, it is deleted. Without a concrete
justification, the default verdict is DELETE.

"Defense in depth", "extra safety", "can't hurt" and "in case X changes" are not
justifications. A check that duplicates another guarantee has a cost: every gate on
the critical path is one more way for the system to stop.

## 1. Before reading the diff, inventory the existing guarantees

List, with file:line, the mechanisms that the system already relies on:
- transactions, uniqueness constraints and idempotency ledgers;
- leases, fencing tokens and heartbeats;
- locks and serial executors;
- immutable identifiers (version numbers, content hashes);
- **everything that is hashed, pinned, fingerprinted or contract-versioned**:
  `grep -n -i -E "fingerprint|sha256|digest|PIN|CONTRACT|version"`.
  A change to any input of such a hash is a **BLOCKER** until you prove that
  nothing persisted depends on the old value.

The review must quote this inventory. A verdict that ignores it is invalid.

## 2. For each element, the four-column test

| element | concrete failure it catches (observed or specifically reachable) | which existing guarantee from §1 already catches it | verdict |

- Column 3 is non-empty → DELETE (duplicate).
- Column 2 is "theoretically something" or "if the provider changes" → DELETE.
- Only a concrete, reachable failure with an empty column 3 keeps it.

## 3. Additions

A new message, state field, table, mode, flag or retry is accepted only if:
1. you searched §1 for an existing mechanism that already provides it, and wrote down
   where you looked (file:line); and
2. the addition removes more code than it adds.

Otherwise the verdict is "do not merge" plus the existing mechanism to use instead.
Finding *a* scenario the addition handles is not enough; ask whether an existing lock,
ledger or lease already handles the same scenario.

## 4. Smells that almost always mean DELETE

- **A limit equal to today's count** (raised every time something is added). It protects
  nothing. Delete it; real bounds are bytes or resources.
- **A special case** ("allow X only if Y"). The base rule is wrong. Delete the rule,
  don't widen the exception.
- **A pin of a third-party library version** used as a gate. Delete it: an upgrade
  silently stops the system.
- **Multiple independent proofs of one fact** (status + log + re-read + hash). Keep the
  single authoritative source.
- **A hash or digest of data whose integrity is already guaranteed** (DB, verified
  archive, immutable id).
- **Validation of our own just-produced output** with no untrusted boundary in between.
- **Shape validation inside SQL or storage.** Shape belongs at the input boundary;
  storage keeps integrity constraints.
- **One-shot immutability** (an immutable row or trigger making an error impossible to
  correct). Prefer append-only with "latest wins".

## 5. Liveness rules (deadlocks are bugs)

- **An "unknown outcome" must not stop the system.**
  - An error before commit means not applied.
  - An outcome is unknown only when the connection was lost at commit; resolve it by
    reading the ledger by key.
  - A command still not applied after a retry is rejected, and the queue moves on.

  Any design where "unknown" blocks all work, or waits for a human, is a BLOCKER.
- **A timer or lease must extend on the fact of a request** (any incoming search or
  import call), not on its successful result. A shutdown timer that can stop a
  component serving or retrying requests is a BLOCKER.
- **A successor or recovery must not wait for proof that the predecessor ended** if
  fencing already prevents two writers.
- **One failing item must not block unrelated items** (other users, other books, other
  queues). Only its own predecessor or a genuinely global inconsistency may block it.

## 6. Output format

For each case:
1. The §1 inventory (short, file:line).
2. The §2 table.
3. Verdict: "merge", "merge after deleting …" or "do not merge: <blocker>".
4. The minimal change, as deletions only.

No introductions, no restating the code. If unsure between KEEP and DELETE, write
DELETE and name the one test that would prove KEEP.
