# Saved-covered local operation bodies

`ControlSpool.raw_retention_seconds` controls redundant **local** command/outcome
bodies, not accepted identity, provenance or the PostgreSQL Dataset contents.
After a verified physical Saved, the existing checkpoint refresh retires at most
128 old, DURABLE, successfully applied native stage/packet bodies per pass.
The existing journal retains original commands in addressed sealed segments and
original outcomes. Exact duplicate reads use that journal without execution or
reinserting the retired local bodies. No new archive or cleanup worker exists.

Keys, sequence, SHA, actor/scope, database generation, dependencies, seals,
apply/durable receipts and compact progress facts remain. The pass preserves
pending/UNKNOWN, rejected, unsaved and young records, current/latest/referenced
stages and original models required by missing semantic pages. Other command
domains are unchanged. Progress and Telegram read compact facts; accepted-page
correction selects the matching TEXT-APPLIED stage before fetching its model.

The one-time transactional SQLite migration makes `command_bytes` nullable and
preserves existing records, indexes and triggers. Warm reopen never rebuilds
history. Freed SQLite pages are reusable; this is not an online VACUUM and the
main database file need not shrink immediately. Failure to reclaim redundant
bytes is logged and does not undo a verified Saved or block its planned stop.

## Release and recovery

This changes the local spool representation, not native executor or wire bytes.
Install only reviewed, tested `main`, with actual API/controller ancestry and
private-configuration checks. The production controller uses BgeQueue jobs and
does not consume `rkb_control_operations` bodies; the API owns ControlSpool.
No native rotation is required for this host-only change.

Once any raw body is NULL, do not automatically roll the API back to a release
that requires every local body to be present. Keep a compatible release and fix
forward. Never restore an older live SQLite copy or regenerate an accepted
outcome. Original bytes remain available through the existing journal. Physical
PostgreSQL restore still starts from verified Saved and only replays its unsaved
suffix. A supported rollback must itself understand the nullable local format.

## Redundant Dataset command bindings

Verified Dataset promotion also retires at most128 local `commands/` bindings
already excluded by the existing Dataset export policy. Their outcome, accepted
sequence locator and accepted manifest must all be Dataset-covered. This runs
after the existing promotion transaction commits; no additional worker or timer exists.
A reclamation failure is logged and cannot roll back verified Saved.
The sealed segments and exact outcomes stay unchanged, so original-command
lookup and duplicate/conflict checks use the already supported addressed journal
path. Pending or unindexed bindings remain. Lost promotion acknowledgment does
not run another collection pass. This reduces a second local copy; it does not
remove canonical journal provenance or shrink an existing SQLite file.
