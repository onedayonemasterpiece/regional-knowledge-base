# SQL11 to SQL12: existing warm-runtime boundary

Source checked: main `bd138b2449f68cabb2cbfa3df552df8471f40c78`, including the
native code present at `b71f85dbb270724f464411cef99bd2d583a8b1b7`.
This finding is source evidence, not a migration or live acceptance receipt.

Enabling `geo_dependency_upgrade` in the Dev source service constructs an
expanded **Dev** registry. `SourceGeoInstaller._dependency_repair` can retain
the ordered transition and retries, but `needs_native_upgrade` checks the
persisted native launch revisions. For a SQL11 launch it deliberately skips
dispatch. An API restart does not update that launch or the native registry.

The native `TargetRuntime.prepare` constructs `WorkerSourceExecutor` and its
immutable `HandlerRegistry` from the verified launch `command_handlers`.
`build_native_recovery` installs `NativeGeoMigration.prepare` only when that
registry already contains `geo.dependencies.upgrade.v2`. A SQL11 launch with
only the nine original domain routes has neither this transition handler nor
its preparation hook. Sending a transition would fail registry resolution
before acquiring a PostgreSQL connection.

Existing maintenance does not change this boundary:

- `read_handler_stage/activate/rollback` changes only `ReadHandlerSlot`, retaining
  its exact compatibility tuple and PG/model references. It does not replace
  the source executor, recovery factory or retained writer registry.
- Recovery `prepare` requires the factory's exact original database and runtime
  identity. It is physical restore, not a registry-update instruction.
- `expect_search_runtime` explicitly rejects changing an existing launch
  expectation. Editing its SQLite row would not install a native handler.

Therefore the existing protocol cannot perform this transition in a process
launched with the original SQL11-only registry. The next runtime decision must
select a supported owner/process update boundary with the reviewed expanded
registry and ordered preparation hook. This finding does not prescribe or
execute a cold restart, a new transport, arbitrary SQL or a read-handler write
patch. Preserve the accepted original outcomes and checkpoint while deciding
that boundary.

`tests/test_geo_warm_upgrade_boundary.py` exercises the real registry and queue:
the expanded API registry resolves the repair transition, the unchanged native
registry refuses it, the installer still requires native upgrade, and the
original launch expectation cannot be relabelled. SQL012 definition/rollback
evidence is independently covered by `tests/test_geo_upgrade_pins_pg.py` in
the existing GitHub PG17 fixture. Neither test attests an installed migration.
