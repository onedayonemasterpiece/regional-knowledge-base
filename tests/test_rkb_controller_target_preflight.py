"""H1: a one-shot empty Kaggle target cannot be reused after durable accepts.

Uses the real journal condition protocol with deterministic memory store. A
preflight must be read-only and never call Kaggle or allocate a fresh run.
"""
from dataclasses import replace

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_target_launch import preview_target_history, persist_empty_intent
from test_kaggle_target_runtime import spec, hashes
from test_recovery_journal import fixture as journal_fixture, command


@pytest.mark.asyncio
async def test_after_first_accepted_seal_empty_target_reports_verified_restore_required(tmp_path):
    queue=BgeQueue(tmp_path/'preflight.sqlite')
    with queue.connect() as db:run=queue._ensure(db,queue.clock(),useful=False)
    nonce=queue.prepare_channel(run)
    store,journal=journal_fixture()
    accepted=await journal.seal([command()],predecessor=None,writer_generation='writer-1')
    before=dict(store.objects)
    with queue.connect(write=False) as db:
        run_before=dict(db.execute('select * from runs where id=?',(run,)).fetchone())
    receipt=await preview_target_history(spec(fresh=True),journal)
    assert receipt==dict(status='restore_required',target_mode='NEW_EMPTY_GENERATION',
        accepted_head_sequence=1,can_push=False)
    assert store.objects==before
    with pytest.raises(ValueError,match='accepted history requires verified restore'):
        await persist_empty_intent(queue,run,spec(fresh=True),
            launch_nonce=nonce,recovery_journal=journal,sql_hashes=hashes())
    with queue.connect(write=False) as db:
        assert dict(db.execute('select * from runs where id=?',(run,)).fetchone())==run_before
        assert db.execute("select name from sqlite_master where name='bge_empty_generation_intents'").fetchone() is None
    assert store.objects==before


@pytest.mark.asyncio
async def test_empty_remote_head_is_only_start_condition_for_one_shot_genesis():
    store,journal=journal_fixture()
    assert await preview_target_history(spec(fresh=True),journal)==dict(
        status='empty_genesis_permitted',target_mode='NEW_EMPTY_GENERATION',
        accepted_head_sequence=0,can_push=False)
    assert not store.objects




@pytest.mark.asyncio
async def test_verified_restore_target_compiles_after_nonempty_head_without_genesis(tmp_path):
    # Existing REAL composition classes; synthetic pins intentionally cannot
    # authorize a production restore or report readiness.
    import ast
    from test_kaggle_generated_restore import target as restore_target
    from test_kaggle_target_runtime import generate, queue_run
    store,journal=journal_fixture()
    await journal.seal([command()],predecessor=None,writer_generation='writer-1')
    q,run=queue_run(tmp_path)
    restore=restore_target()
    assert restore.database.mode=='VERIFIED_INITIALIZED'
    assert restore.recovery is not None and restore.recovery.drill_empty is False
    assert restore.bootstrap_only is False
    source=generate(q,run,restore)
    tree=ast.parse(source)
    assignments=[node for node in tree.body if isinstance(node,ast.Assign)
        and any(isinstance(item,ast.Name) and item.id=='RUN_CONFIG' for item in node.targets)]
    config=ast.literal_eval(assignments[0].value)
    assert config['target_runtime']['database']['mode']=='VERIFIED_INITIALIZED'
    assert config['target_runtime']['initialization_intent'] is None
    assert config['target_runtime']['recovery']['drill_empty'] is False
    assert config['target_runtime']['initialized_receipt'] is not None
    with q.connect(write=False) as db:
        assert db.execute("select name from sqlite_master where name='bge_empty_generation_intents'").fetchone() is None
    assert await journal.head() is not None
    assert store.objects  # Accepted history not cleared to allow new code.

@pytest.mark.asyncio
async def test_declared_verified_target_still_requires_independent_recovery():
    store,journal=journal_fixture()
    await journal.seal([command()],predecessor=None,writer_generation='writer-1')
    receipt=await preview_target_history(spec(fresh=False),journal)
    assert receipt==dict(status='verify_restore_contract',
        target_mode='VERIFIED_INITIALIZED',accepted_head_sequence=1,can_push=False)
