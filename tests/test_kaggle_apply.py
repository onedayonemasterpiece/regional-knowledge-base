"""Synthetic real-PG apply/replay tests on the existing strictly verified fixture."""
import asyncio
import hashlib
import json
import os
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import psycopg
from psycopg.pq import TransactionStatus
import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import ApplyConflict, ApplyExecutionError, ApplyLimit, ApplyUnknown, HandlerRegistry, HandlerSpec, PgFunction, Revisions, apply_accepted, handover_writer
from pg_schema_metadata import initialize_fixture_database as initialize_database
from regional_knowledge.recovery_journal import JournalConflict, RecoveryJournal, StaleWriter
from test_kaggle_pg_driver import connect, database, wait_for_lock_wait
from test_recovery_journal import ConditionalMemoryStore, command

ROOT = Path(__file__).resolve().parents[1]
PG = pytest.mark.skipif(not (os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET") or os.environ.get("RKB_KAGGLE_TEST_DSN")),
                        reason="explicit checked disposable PG fixture required")
V1 = Revisions("s1", "f1", "e1")
V2 = Revisions("s2", "f2", "e2")
MIGRATION_ARTIFACT = hashlib.sha256(b"synthetic static v1-to-v2 SQL artifact").hexdigest()
DOMAIN_SQL = """
CREATE TABLE rkb_v4.apply_fixture_items (id text PRIMARY KEY, revision bigint NOT NULL,
    body text NOT NULL, logical_time text NOT NULL);
CREATE TABLE rkb_v4.apply_fixture_calls (n integer NOT NULL);
INSERT INTO rkb_v4.apply_fixture_calls VALUES (0);
CREATE FUNCTION rkb_v4.check_item_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
    SELECT expected->>'revision' = coalesce((SELECT revision::text
        FROM rkb_v4.apply_fixture_items WHERE id=ids->>'item_id'), '0')
$$;
CREATE FUNCTION rkb_v4.apply_item_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE b text := convert_from(p,'UTF8')::jsonb->>'body';
BEGIN
    INSERT INTO rkb_v4.apply_fixture_items VALUES (ids->>'item_id',1,b,logical_time)
    ON CONFLICT (id) DO UPDATE SET revision=apply_fixture_items.revision+1,
        body=excluded.body, logical_time=excluded.logical_time;
    UPDATE rkb_v4.apply_fixture_calls SET n=n+1;
    RETURN convert_to(jsonb_build_object('body',b,'id',ids->>'item_id',
        'logical_time',logical_time,'semantics','v1')::text,'UTF8');
END $$;
CREATE FUNCTION rkb_v4.check_transition_v1(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
    SELECT expected->>'schema'=schema_revision AND expected->>'function'=function_revision
        AND expected->>'executor'=executor_revision
        AND expected->>'migration'=coalesce(last_migration_id,'')
    FROM rkb_v4.recovery_state WHERE singleton
$$;
CREATE FUNCTION rkb_v4.transition_v1_v2(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
    ALTER TABLE rkb_v4.apply_fixture_items ADD COLUMN migrated boolean NOT NULL DEFAULT false;
    UPDATE rkb_v4.apply_fixture_items SET migrated=true;
    RETURN convert_to('{"migration":"v1-v2"}','UTF8');
END $$;
CREATE FUNCTION rkb_v4.apply_item_v2(p bytea, ids jsonb, expected jsonb, logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE b text := convert_from(p,'UTF8')::jsonb->>'body';
BEGIN
    INSERT INTO rkb_v4.apply_fixture_items (id,revision,body,logical_time,migrated)
        VALUES (ids->>'item_id',1,b,logical_time,true)
    ON CONFLICT (id) DO UPDATE SET revision=apply_fixture_items.revision+1,
        body=excluded.body, logical_time=excluded.logical_time, migrated=true;
    UPDATE rkb_v4.apply_fixture_calls SET n=n+1;
    RETURN convert_to(jsonb_build_object('body',b,'id',ids->>'item_id',
        'logical_time',logical_time,'semantics','v2')::text,'UTF8');
END $$;
"""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def item(sequence=1, *, revision="0", versions=V1, **changes):
    return command(sequence, route="fixture.item", schema_revision=versions.schema,
                   function_revision=versions.function, executor_revision=versions.executor,
                   ids={"item_id": "chosen-exact-id"}, expected_revisions={"revision": revision},
                   payload=canonical({"body": f"accepted exact body {sequence}"}), **changes)




async def pinned(connection, name):
    async with connection.transaction():
        definition = (await (await connection.execute("SELECT pg_get_functiondef(to_regprocedure(%s))",
                         (f"rkb_v4.{name}(bytea,jsonb,jsonb,text)",))).fetchone())[0]
    return PgFunction(name, hashlib.sha256(definition.encode()).hexdigest())


async def install(connection, generation="db-1"):
    async with connection.transaction():
        await connection.execute((ROOT / "sql/kaggle/002_recovery.sql").read_text(), prepare=False)
        await connection.execute(DOMAIN_SQL, prepare=False)
    check = await pinned(connection, "check_item_v1")
    registry = HandlerRegistry([
        HandlerSpec("domain_command", "fixture.item", V1, check,
                    await pinned(connection, "apply_item_v1"), frozenset({"revision"})),
    ])
    await initialize_database(connection, database_generation=generation, writer_generation="writer-1", revisions=V1)
    return registry


@pytest_asyncio.fixture
async def env(database):
    registry = await install(database)
    return {"connection": database, "registry": registry,
            "journal": RecoveryJournal(ConditionalMemoryStore(), stream="apply-synthetic"),
            "generation": "db-1"}


async def accepted(env, cmd):
    journal = env["journal"]
    return await journal.seal([cmd], predecessor=await journal.head(), writer_generation="writer-1")


async def apply(env, cmd, seal, *, connection=None, writer="writer-1", **changes):
    return await apply_accepted(connection or env["connection"], journal=env["journal"], seal=seal, command=cmd,
                                database_generation=env["generation"], writer_generation=writer,
                                registry=env["registry"], **changes)


async def facts(connection):
    assert connection.info.transaction_status == TransactionStatus.IDLE
    async with connection.transaction():
        state = await (await connection.execute("SELECT applied_seq,schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state")).fetchone()
        ledger = (await (await connection.execute("SELECT count(*) FROM rkb_v4.applied_ops")).fetchone())[0]
        calls = (await (await connection.execute("SELECT n FROM rkb_v4.apply_fixture_calls")).fetchone())[0]
        items = await (await connection.execute("SELECT id,revision,body,logical_time FROM rkb_v4.apply_fixture_items ORDER BY id")).fetchall()
    return state, ledger, calls, items


@PG
@pytest.mark.asyncio
async def test_atomic_effect_ledger_cursor_and_exact_duplicate(env):
    cmd = item()
    seal = await accepted(env, cmd)
    connection = env["connection"]
    original_verify = env["journal"].verify_accepted

    async def verify(actual_seal, actual_command):
        assert connection.info.transaction_status == TransactionStatus.IDLE
        return await original_verify(actual_seal, actual_command)

    env["journal"].verify_accepted = verify
    receipt = await apply(env, cmd, seal)
    duplicate = await apply(env, cmd, seal)
    assert not receipt.duplicate and duplicate.duplicate
    assert duplicate.outcome == receipt.outcome
    assert receipt.database_generation == "db-1"
    state, ledger, calls, rows = await facts(connection)
    assert state == (1, "s1", "f1", "e1") and ledger == calls == 1
    assert rows == [("chosen-exact-id", 1, "accepted exact body 1", cmd.logical_time)]


@PG
@pytest.mark.asyncio
async def test_failure_after_domain_effect_rolls_back_before_ledger_and_retry(env):
    connection = env["connection"]
    async with connection.transaction():
        await connection.execute("""CREATE FUNCTION rkb_v4.fixture_fail_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic failure after effect'; END $$;
            CREATE TRIGGER fail_ledger BEFORE INSERT ON rkb_v4.applied_ops
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.fixture_fail_ledger()""", prepare=False)
    cmd = item()
    seal = await accepted(env, cmd)
    with pytest.raises(ApplyExecutionError):
        await apply(env, cmd, seal)
    assert await facts(connection) == ((0, "s1", "f1", "e1"), 0, 0, [])
    async with connection.transaction():
        await connection.execute("DROP TRIGGER fail_ledger ON rkb_v4.applied_ops")
    await apply(env, cmd, seal)
    assert (await facts(connection))[1:3] == (1, 1)


@PG
@pytest.mark.asyncio
async def test_concurrent_duplicate_has_one_actual_handler_effect(env):
    cmd = item()
    seal = await accepted(env, cmd)
    first, second = await connect(), await connect()
    try:
        receipts = await asyncio.gather(apply(env, cmd, seal, connection=first), apply(env, cmd, seal, connection=second))
        assert sorted(r.duplicate for r in receipts) == [False, True]
        assert receipts[0].outcome == receipts[1].outcome
        assert (await facts(env["connection"]))[1:3] == (1, 1)
    finally:
        await first.close()
        await second.close()


@PG
@pytest.mark.asyncio
async def test_sequence_gap_unknown_handler_and_exact_ledger_conflicts(env):
    journal = env["journal"]
    one, two = item(), item(2, revision="1")
    seal = await journal.seal([one, two], predecessor=None, writer_generation="writer-1")
    with pytest.raises(ApplyConflict, match="next sequence"):
        await apply(env, two, seal)
    await apply(env, one, seal)
    # A separately verified remote stream cannot bypass this database's exact ledger.
    other = RecoveryJournal(ConditionalMemoryStore(), stream="other-synthetic")
    changed = replace(one, expected_revisions={"revision": "999"})
    other_seal = await other.seal([changed], predecessor=None, writer_generation="writer-1")
    with pytest.raises(ApplyConflict, match="different exact"):
        await apply_accepted(env["connection"], journal=other, seal=other_seal, command=changed,
            database_generation="db-1", writer_generation="writer-1", registry=env["registry"])
    with pytest.raises(JournalConflict):
        await apply(env, changed, seal)
    await apply(env, two, seal)
    wrong = replace(item(3, revision="2"),route="fixture.unregistered")
    wrong_seal = await accepted(env, wrong)
    with pytest.raises(ApplyConflict, match="no exact current route handler"):
        await apply(env, wrong, wrong_seal)
    assert (await facts(env["connection"]))[:3] == ((2, "s1", "f1", "e1"), 2, 2)


@PG
@pytest.mark.asyncio
async def test_preconditions_are_executed_and_rejection_is_a_terminal_exact_outcome(env):
    cmd = item(revision="9")
    seal = await accepted(env, cmd)
    receipt = await apply(env, cmd, seal)
    assert receipt.status == "rejected_precondition"
    assert (await apply(env, cmd, seal)).outcome == receipt.outcome
    assert await facts(env["connection"]) == ((1, "s1", "f1", "e1"), 1, 0, [])
    extra = replace(item(2), expected_revisions={"revision": "0", "ignored": "unsafe"})
    with pytest.raises(ApplyConflict, match="precondition key"):
        await apply(env, extra, await accepted(env, extra))
    assert (await facts(env["connection"]))[0][0] == 1


@PG
@pytest.mark.asyncio
async def test_writer_handover_fences_old_executor_but_replays_old_accepted_command(env):
    one, two = item(), item(2, revision="1")
    first_seal = await accepted(env, one)
    second_seal = await accepted(env, two)
    await apply(env, one, first_seal)
    connection = env["connection"]
    await handover_writer(connection, database_generation="db-1", expected_writer="writer-1", new_writer="writer-2")
    await handover_writer(connection, database_generation="db-1", expected_writer="writer-1", new_writer="writer-2")
    for cmd, seal in ((one, first_seal), (two, second_seal)):
        with pytest.raises(StaleWriter):
            await apply(env, cmd, seal)
    await apply(env, two, second_seal, writer="writer-2")
    with pytest.raises(ApplyConflict):
        await handover_writer(connection, database_generation="db-1", expected_writer="writer-2", new_writer="writer-1")
    with pytest.raises(ApplyConflict):
        await apply_accepted(connection, journal=env["journal"], seal=second_seal, command=two,
            database_generation="replacement-db", writer_generation="writer-2", registry=env["registry"])
    assert (await facts(connection))[1:3] == (2, 2)
    await handover_writer(connection, database_generation="db-1", expected_writer="writer-2", new_writer="writer-3")
    # A delayed ACK retry proves the earlier transition without reactivating W2.
    await handover_writer(connection, database_generation="db-1", expected_writer="writer-1", new_writer="writer-2")
    async with connection.transaction():
        assert (await (await connection.execute("SELECT writer_generation FROM rkb_v4.recovery_state")).fetchone()) == ("writer-3",)
    with pytest.raises(StaleWriter):
        await apply(env, two, second_seal, writer="writer-2")


@PG
@pytest.mark.asyncio
async def test_commit_ack_loss_reconciles_saved_exact_outcome_on_new_connection(env, monkeypatch):
    connection = await connect()
    original = connection._exec_command
    dropped = False

    def lose_ack(statement, *args, **kwargs):
        nonlocal dropped
        yield from original(statement, *args, **kwargs)
        if statement == b"COMMIT" and not dropped:
            dropped = True
            raise psycopg.OperationalError("synthetic lost commit response")

    monkeypatch.setattr(connection, "_exec_command", lose_ack)
    cmd = item()
    seal = await accepted(env, cmd)
    try:
        with pytest.raises(ApplyUnknown):
            await apply(env, cmd, seal, connection=connection)
    finally:
        await connection.close()
    fresh = await connect()
    try:
        receipt = await apply(env, cmd, seal, connection=fresh)
        assert dropped and receipt.duplicate
        assert (await facts(env["connection"]))[1:3] == (1, 1)
    finally:
        await fresh.close()




@PG
@pytest.mark.asyncio
@pytest.mark.parametrize("set_check", [False, True])
async def test_pinned_set_returning_functions_are_rejected_before_any_handler_effect(env, set_check):
    connection = env["connection"]
    async with connection.transaction():
        await connection.execute("""CREATE FUNCTION rkb_v4.set_check_v1
            (p bytea, ids jsonb, expected jsonb, logical_time text) RETURNS SETOF boolean
            LANGUAGE sql STABLE SECURITY INVOKER AS $$ SELECT true UNION ALL SELECT false $$;
            CREATE FUNCTION rkb_v4.set_apply_v1
            (p bytea, ids jsonb, expected jsonb, logical_time text) RETURNS SETOF bytea
            LANGUAGE plpgsql VOLATILE SECURITY INVOKER AS $$ BEGIN
                UPDATE rkb_v4.apply_fixture_calls SET n=n+1;
                RETURN NEXT 'first'::bytea; RETURN NEXT 'second'::bytea; END $$""", prepare=False)
    original = env["registry"].resolve(item())
    handler = replace(original, check=await pinned(connection, "set_check_v1")) if set_check else replace(
        original, apply=await pinned(connection, "set_apply_v1"))
    env["registry"] = HandlerRegistry([handler])
    cmd = item()
    with pytest.raises(ApplyConflict, match="stable boolean"):
        await apply(env, cmd, await accepted(env, cmd))
    assert await facts(connection) == ((0, "s1", "f1", "e1"), 0, 0, [])


@PG
@pytest.mark.asyncio
async def test_handover_queued_on_actual_state_lock_fences_a_later_old_writer(env):
    blocker, control, worker = await connect(), await connect(), await connect()
    handover, application = None, None
    cmd = item()
    seal = await accepted(env, cmd)
    try:
        await blocker.execute("SELECT * FROM rkb_v4.recovery_state FOR UPDATE")
        handover = asyncio.create_task(handover_writer(control, database_generation="db-1",
                    expected_writer="writer-1", new_writer="writer-2"))
        await wait_for_lock_wait(env["connection"], control.info.backend_pid)
        application = asyncio.create_task(apply(env, cmd, seal, connection=worker))
        await wait_for_lock_wait(env["connection"], worker.info.backend_pid)
        await blocker.rollback()
        await handover
        with pytest.raises(StaleWriter):
            await application
        assert await facts(env["connection"]) == ((0, "s1", "f1", "e1"), 0, 0, [])
        await apply(env, cmd, seal, connection=worker, writer="writer-2")
    finally:
        for task in (handover, application):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.rollback()
        await blocker.close()
        await control.close()
        await worker.close()


@PG
@pytest.mark.asyncio
async def test_connection_idle_required_and_lock_cancellation_rolls_back_before_reuse(env):
    connection, blocker = await connect(), await connect()
    cmd = item()
    seal = await accepted(env, cmd)
    task = None
    try:
        await blocker.execute("SELECT * FROM rkb_v4.recovery_state FOR UPDATE")
        task = asyncio.create_task(apply(env, cmd, seal, connection=connection))
        await wait_for_lock_wait(env["connection"], connection.info.backend_pid)
        with pytest.raises(ValueError, match="exclusively"):
            await apply(env, cmd, seal, connection=connection)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert connection.info.transaction_status == TransactionStatus.IDLE
        await blocker.rollback()
        assert not (await apply(env, cmd, seal, connection=connection)).duplicate
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await blocker.rollback()
        await blocker.close()
        await connection.close()


@PG
@pytest.mark.asyncio
async def test_initializer_never_silently_upgrades_and_outcome_overflow_rolls_back(env):
    with pytest.raises(ApplyConflict, match="already initialized"):
        await initialize_database(env["connection"], database_generation="replacement", writer_generation="new", revisions=V2)
    cmd = item()
    seal = await accepted(env, cmd)
    with pytest.raises(ApplyLimit):
        await apply(env, cmd, seal, max_outcome_bytes=32)
    assert await facts(env["connection"]) == ((0, "s1", "f1", "e1"), 0, 0, [])


def test_static_registry_rejects_sql_identifier_injection_and_is_immutable():
    with pytest.raises(ValueError):
        PgFunction("apply_v1; DROP SCHEMA rkb_v4 CASCADE", "0" * 64)
    fn = PgFunction("trusted_v1", "0" * 64)
    registry = HandlerRegistry([HandlerSpec("domain_command", "fixture.item", V1, fn, fn, frozenset())])
    with pytest.raises(TypeError):
        registry.handlers[("other",)] = None
    with pytest.raises(FrozenInstanceError):
        registry.handlers = {}


@PG
@pytest.mark.asyncio
async def test_native_token_rejection_is_retained_without_applying_effect(env):
    import base64
    reason='complete_native_input_exceeds_512_tokens:chunk_key=Page-0.body:tokens=513'
    cmd=item()
    seal=await accepted(env,cmd)
    receipt=await apply(env,cmd,seal,reject_reason=reason)
    assert receipt.status=='rejected_precondition'
    outcome=json.loads(receipt.outcome)
    assert json.loads(base64.b64decode(outcome['result_b64']))=={'reason':reason}
    assert await facts(env['connection'])==((1,'s1','f1','e1'),1,0,[])
    assert (await apply(env,cmd,seal,reject_reason=reason)).outcome==receipt.outcome
    for unsafe in (reason+':private text',reason.replace('Page-0.body','bad/key')):
        with pytest.raises(ValueError,match='fixed internal source rejection reason'):
            await apply(env,cmd,seal,reject_reason=unsafe)
