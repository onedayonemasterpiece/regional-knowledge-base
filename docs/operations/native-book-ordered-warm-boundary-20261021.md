# Native book integration: ordered migration and warm-runtime boundary

Source baseline: b71f85dbb270724f464411cef99bd2d583a8b1b7. This is source-only work; no production or provider action is implied.

## Ordered accepted transition
- Retain exact historical SQL11 and SQL12 handlers, each with its explicit revision tuple. Never substitute current functions for old accepted commands.
- Preserve original GEO1–20 outcomes, including rejected operations. The SQL12 transition and repair retries must be settled by the existing operator before choosing the next book-upgrade sequence; a cursor alone is not GEO success.
- Fixed `source.native.upgrade.v1` is a schema-level maintenance command with no fabricated source/manifest dependency. Admission uses the current owner import authority and binds installer_person_id to that actor.
- Deployment pins contain the SHA256 of SQL013, SQL014, and both completion function definitions. The ordered artifact manifest hash is the existing MigrationTransition artifact hash.
- Native preparation requires the immediately preceding accepted frontier and the actual SQL12 migration predecessor. SQL013+014 install atomically; neither old SQL nor old function definitions are replaced. The existing accepted apply records the transition to SQL14.
- History for SQL14 requires both actual applied schema-transition receipts in increasing sequence order, unchanged original genesis, exact registry/catalog/ACL, and ordinary native history verification. A rejected transition does not prove the closure.
- An initialized SQL11 Dataset plus accepted ordered tail through SQL12 and SQL14 is supported by the source restore composition. A separate SQL12-origin checkpoint to SQL14 requires its own exact origin-closure pin contract and is not claimed here.

## Warm v105 at baseline b71f85
Existing `read_handler_stage/activate` replaces the read-handler slot with the same explicit compatibility tuple. It does not replace the source-command decoder, retained writer registry, recovery factory, or running WSS receiver.

Therefore:
- Dev-only admission/history composition can be installed on Dev without replacing native compute, but cannot make an old native process understand a new command.
- The existing SQL11→12 migration path remains the operator's path.
- The new SQL12→14 handler/prepare dispatcher and native book materializer/intake require corresponding native code. The baseline native executor does not register `source.native.upgrade.v1`.
- RKB1 REQUEST/GRANT/RESULT challenge protocol and native late binding likewise require the matching receiver code; a Dev-only API rollout is insufficient.
- No cold restart is prescribed here. No remote-exec substitute or second transport was added. The sole live operator must identify an explicitly supported native owner-update seam before claiming a warm rollout of these new write/intake/protocol capabilities.

## Evidence and remaining acceptance
Synthetic/source tests cover actual ControlSpool/registry/history composition, exact pins, null-dependency rejection, two ordered receipts, generation fencing, pending retries, challenge ordering and fast ACK.
Real PostgreSQL SQL12→14 execution, Kaggle packaging with Telethon dependency closure, and native Telegram authorization/import remain unverified. PostgreSQL must not be run on DevCoveer.
