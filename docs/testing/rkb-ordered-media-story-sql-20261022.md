# RKB ordered SQL013–016 compatibility gate — isolated source handoff

**Status: source-only SQL compatibility fixture, not an activated migration, not a READY runtime, no Case or illustration provider effects.** The test invokes real PostgreSQL 17/pgvector 0.8.2 functions on GitHub's existing disposable CI service; it must not run in Dev or Kaggle. It does not authorize or perform 013–016 native accepted-command migration transitions.

## Exact immutable sources

| Ordinal | Path | Pin / provenance |
| --- | --- | --- |
| 013 | `sql/kaggle/013_source_stage.sql` | PR [#212](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/212) exact commit `cda234538dcd61880955a12c13bb6007885e97a8`, SHA256 `55f86fa1fdc2c2635c87f5edbc0f235cc629c6d59891ce199f30e264dce303b6`, Git blob `c4b4ef6179a0bdb328fee7f3d93de4a6a7d20fe9` |
| 014 | `sql/kaggle/014_source_stage_activation.sql` | Same PR212 commit, SHA256 `5f8257e9fe58e2e2bdd00f67042ad17b8db007c7873dcc04f52d53be3523e855`, Git blob `f79592b40b0abc6663841186cc7b2c14bce15406` |
| 015 | `sql/kaggle/015_source_illustrations.sql` | **Only media delta** in managed `handoff:src_ce236841a07e116e9ace41ce`. Immutable `.media-delta.patch` SHA256 `701c03106cb2a9436ddd814d08583008dd1065243fd6ce5e6b7a03e81f01a377`, **97,933 bytes**, verified by authorized `source_handoff_list` / `project_probe`. SQL015 separately checked SHA256 `7faacbf7fbb2b99f0dbdbd4339dea5138f134347e31a6be09577dff35be8c773`, **29,865 bytes**, Git blob `557972acc9ef9b66e89038cef3f24e4354ef434c`. Old seed `a120`/`src77` **not copied**. |
| 016 | `sql/kaggle/016_story_reconcile.sql` | RKB-Риф previously reviewed source in PR [#214](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/214), base `1384b76cc3f5b78ccc61ac75efdd98c165ced8b9`, Git blob `702794fcac7463da1445647c8a1ad6006ad351b2`; no SQL016 modification in this fixture. |

This branch adds only three exact SQL predecessors, one test (`tests/test_story_reconcile_ordered_013_016_pg.py`) and this manifest. The base provides prior SQL001–012 and reviewed SQL016. No media decoder, producer, registry, WSS, source history, source materializer or runtime source is imported. PR212 is not modified or merged.

## Runnable real PG test and exact negative guarantees

In existing CI pull-request jobs `RKB_KAGGLE_TEST_DSN` points to the already provisioned PG17/pgvector service; the test first verifies the fixed CI environment and file SHA256/blob pins, and fails if any SQL prefix ordinal 001–016 is missing/duplicated or if SQL015 is a placeholder. No standalone PG instance, local BGE or network credentials are created.

The test:
1. opens one explicit disposable transaction, installs **every actual SQL001–012** in increasing order, records all `pg_get_functiondef` hashes, and creates only NOLOGIN test roles;
2. opens a nested savepoint; installs **actual SQL013, SQL014, SQL015, SQL016 in that exact order**, proving each stage's tables/functions and old-function hash preservation after every boundary;
3. inspects real PostgreSQL constraints linking SQL015 media tables to SQL014 activation candidates and SQL016 reconciliation to existing Case/source tables; checks independent cross-book `open` and calls real empty-source media reader, not a fake stub;
4. rolls back the nested savepoint and proves SQL013–016 functions/tables disappeared **while exact SQL001–012 historical definitions remained**; then rolls back the outer transaction and verifies that the entire disposable `rkb_v4` namespace is gone.

**Boundaries of this test:** it validates DDL/catalog/function compatibility and transactional rollback on real PG. It does **not** apply the accepted migration journal or validate target role grant installation, actual source import, Telegram, image decoding, media output ACL, native archive binding, Dataset promotion, Case writes, warm in-place runtime updates or live Kaggle. A SQL-installed function does not mean its caller, registry and complete domain effect are deployed.

## Three highest-risk remaining integration blockers

1. **Media015 activation / accepted command inventory.** The media delta explicitly lacks a single composed decoder/registry/handler revision and complete pinned source inventory. SQL015 `check_source_activate_with_illustrations_v1` / `apply_source_activate_with_illustrations_v1` cannot be selected just by adding the schema. The production owner must lock the **ordered accepted source/handler revision chain 013→014→015→016**, calculate pins against actual installed PostgreSQL function definitions and preserve old replay handlers before issuing a domain effect. PR212's 161/160 packaging issue and its unmerged state are independent blockers, not test skip conditions.

2. **Public media read/output authority.** `read_source_illustrations_v1` needs explicit target composition and a gated output with current source visibility/active revision/withdrawal, private-versus-public artifact class, exact original/page/crop evidence, archive authority/lease and final response issuance. Current SQL015 contains only `REVOKE ... FROM PUBLIC`, **not** an approved media reader role/grant/output gate. An installed SQL function with no vetted caller is not public-media READY.

3. **Telegram topic-4 native send lifecycle and crash recovery.** Native photo/document locators need durable begin/send intent, server-issued exact archive/topic binding, explicit UNKNOWN/readback, immutable result and recovery before any HTTP200/Saved/public result. A media availability row or in-memory send success is not an accepted provider receipt; absence of a locator is not authorization to resend. The runtime owner must implement current lease + registered locator + no-duplicate proof and independently test the real provider pathway before activating illustrations. This fixture deliberately creates **no** Telegram commands, media locator or provider action.

**Handoff:** review exact committed source/file hashes and GitHub CI PostgreSQL result, then ask dot and media/runtime owners to integrate a *single* candidate with full native handler pins, current schema/revisions, trusted read/write gates, and separate live acceptance. Keep this fixture isolated from PR212 and installed Kaggle.
