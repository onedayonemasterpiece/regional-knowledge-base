# RKB-Импорт — M2 native reprocess/rechunk owner-only resume

**Scope:** This is an import-only source change authored on canonical integration base
`b932c15ef133d668a0eaa3570cc895e750876c77`; pinned compatible diagnostic
candidate for combined tests: `6d51bfd82132048bd4642ea4fabd613583505ce3`
and the moving Shkiper isolated diagnostic branch. Opus remains the only
integration merge/live/deploy owner. No SQL, server tool schema, materializer,
native provider, IAM, dataset, or existing archive/lease behavior was changed.

## Connected behavior

`KaggleSourceImporter.book_ingest` admits native `reprocess` and `rechunk`
commands into a **single owner-scoped safe preflight**, instead of always
returning `target_import_command_unavailable`. It uses the real owner OAuth
authority, existing intake lock, exact original receipt, same actor and source
IDs, and existing NativeSourceIntake state.

- For an **unfinished original** whose exact archive has already been verified,
  `reprocess` invokes the pre-existing NativeSourceIntake `status` to
  reconcile/resume *that same operation*, including its accepted intent or
  UNKNOWN; it does not create another intake, fetch/upload bytes, allocate a
  new source ID, or silently rename the same operation as a new revision.
- `rechunk` requires an already finalized source and never changes immutable
  accepted regions or pictures.
- For a finalized source, the preflight checks the exact
  `source.activate.v1` operation, owner, source ID, generation,
  `apply_receipt.status='applied'`, `outcome_state='DURABLE'`, exact remote
  outcome, the **real DatasetRecoveryStore** type, and its independently
  version-covered stored outcome bytes. A local finalization or S3 result is
  not called Saved. If any proof is missing/UNKNOWN it fails closed.
- Any attempted replacement file, changed metadata or stage payload on
  `reprocess/rechunk` is refused rather than silently discarding the user
  input or accepting another edition; a competing active intake from the same
  owner is refused. Caller-supplied source IDs do not bypass ownership.
- Even with a verified accepted original and covered Dataset outcome,
  the current native SQL013/014 **only accepts absent source/revision 1**. It
  rejects staged/activated revision 2 for the SAME canonical source ID, and
  `source_stage_sessions` and activation candidates are unique by source_id.
  The exact result for a *new* accepted revision is therefore
  `native_source_revision_upgrade_required`, without staging a fake new book
  or presenting revision 1 as a new reprocess. This is an actual hard product
  dependency, not a caller confirmation.

## Evidence and what remains

`tests/test_kaggle_native_reprocess_20261022.py` exercises the real
KaggleSourceImporter with existing authenticated Owner/SQLite queue fixtures
plus explicitly synthetic native journal/Dataset simulations, including
owner-only resume, stable archive operation/no reupload, simultaneous-owner
exclusion, invalid new file/payload, missing terminal intent, wrong storage
authority, uncovered remote outcome, and the fail-closed native revision
contract. None of the fixtures are real imported books or production Saved.

The next **implementation** dependency is an Opus-owned ordered SQL/native
revision-2 profile, original archived-receipt reuse for the same canonical
`source_id` across distinct `ingestion_id`, and accepted_reuse exact
source/page/figure proof for `rechunk`. This cannot be implemented solely by
changing `kaggle_importer.py`: SQL013/014 `absent_source` precondition and
the original controller's `receipt.revision==1`/operation identity would
reject it. It must be scoped and assigned to the SQL/native revision owner;
until then, no reprocess of an accepted source can truthfully return
`finalized` as a new revision.

For M2, dot performs model reading of the already provided Gause PDF and Opus
integrates stage/seal/materialize/activate from the prepared native source
candidate, with exact Dataset-version-covered Saved and ordinary readback. The
next actual full DjVu/figure/Story.body/continuation acceptance is separately
NOT RUN. No Dev vector round-trip, second backend model/PG instance, or new
original upload was introduced.

**Acceptance boundaries:** pinned full RKB v4.1, owner alignment, acceptance
matrix and current PR238/M2; application partial success is not product PASS.
