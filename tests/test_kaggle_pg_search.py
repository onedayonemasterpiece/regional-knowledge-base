"""Explicit opt-in, disposable PG17/pgvector fixture only; no production DSN."""
import asyncio
import os
from pathlib import Path
import subprocess
from contextlib import asynccontextmanager
from types import SimpleNamespace

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from regional_knowledge.kaggle_search import _PENDING_ENCODERS

import pytest

from regional_knowledge.kaggle_search import fused_search, vector_literal
from test_kaggle_pg_driver import catalog_database, database, connect

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "rkb-v4-fixture-20261020"


def test_vector_contract():
    assert vector_literal([1] + [0] * 1023).startswith("[1.0,0.0")
    for vector in ([0] * 1024, [1] * 1023, [float("nan")] * 1024, [1] * 1024,
                   [True] + [0] * 1023, ["1"] + [0] * 1023):
        with pytest.raises(ValueError):
            vector_literal(vector)


class FixtureConnection:
    def __init__(self):
        self.closed = False
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.row_factory = tuple_row
        self.cleaned = False
        self.statements = []

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        finally:
            self.cleaned = True

    @asynccontextmanager
    async def cursor(self):
        yield self

    async def execute(self, statement, params=()):
        self.statements.append((statement, params))
        self.statement = statement

    async def fetchall(self):
        return []


@pytest.mark.asyncio
@pytest.mark.parametrize("caller_cancel", [False, True])
async def test_deadline_or_caller_cancel_cleans_transaction(caller_cancel):
    connection = FixtureConnection()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def encode(query):
        entered.set()
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    task = asyncio.create_task(fused_search(connection, "fixture", encode=encode,
                                           timeout_seconds=.02 if not caller_cancel else 1))
    await entered.wait()
    if caller_cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if caller_cancel else TimeoutError):
        await task
    await asyncio.wait_for(cancelled.wait(), .1)
    await asyncio.sleep(0)
    assert connection.cleaned
    assert not _PENDING_ENCODERS


@pytest.mark.asyncio
async def test_resistant_encoder_returns_on_deadline_and_observes_late_failure():
    connection = FixtureConnection()
    finish = asyncio.Event()
    cancellation = asyncio.Event()

    async def encode(query):
        try:
            await finish.wait()
        except asyncio.CancelledError:
            cancellation.set()
            await finish.wait()  # Actual shared forward still owns its permit.
        raise RuntimeError("late inference failure")

    task = asyncio.create_task(fused_search(connection, "fixture", encode=encode,
                                           timeout_seconds=.02))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(task, .2)
    await cancellation.wait()
    assert connection.cleaned and len(_PENDING_ENCODERS) == 1
    finish.set()
    await asyncio.sleep(.01)
    assert not _PENDING_ENCODERS


@pytest.mark.asyncio
async def test_sql_cancel_rolls_back_before_return():
    class BlockedSQL(FixtureConnection):
        async def execute(self, statement, params=()):
            if "lexical_candidates" in statement:
                await asyncio.sleep(10)
            await super().execute(statement, params)

    async def encode(query):
        await asyncio.sleep(.01)
        return [1] + [0] * 1023

    connection = BlockedSQL()
    with pytest.raises(TimeoutError):
        await fused_search(connection, "fixture", encode=encode, timeout_seconds=.02)
    assert connection.cleaned


@pytest.mark.asyncio
async def test_input_and_connection_bounds():
    async def encode(query):
        raise AssertionError("invalid input reached encoder")

    for arguments in ({"top_k": True}, {"top_k": 1.5}, {"candidate_budget": True},
                      {"candidate_budget": 10.0}, {"top_k": 11}):
        with pytest.raises(ValueError):
            await fused_search(FixtureConnection(), "fixture", encode=encode, **arguments)
    for query in ("x" * 1001, "", None):
        with pytest.raises(ValueError):
            await fused_search(FixtureConnection(), query, encode=encode)
    for field, value in (("closed", True), ("row_factory", None)):
        connection = FixtureConnection()
        setattr(connection, field, value)
        with pytest.raises(ValueError):
            await fused_search(connection, "fixture", encode=encode)
    connection = FixtureConnection()
    connection.info.transaction_status = TransactionStatus.INTRANS
    with pytest.raises(ValueError):
        await fused_search(connection, "fixture", encode=encode)


def fixture_sql():
    vector = "[1," + ",".join(["0"] * 1023) + "]"
    ddl = (ROOT / "sql/kaggle/001_search.sql").read_text()
    return f"""\nBEGIN;
SET LOCAL statement_timeout = '10s';
SET LOCAL maintenance_work_mem = '8MB';
{ddl}
INSERT INTO rkb_v4.sources VALUES
 ('00000000-0000-0000-0000-000000000001','accepted shared book',true,false),
 ('00000000-0000-0000-0000-000000000002','withdrawn book',true,true),
 ('00000000-0000-0000-0000-000000000003','unaccepted book',false,false);
INSERT INTO rkb_v4.revisions VALUES
 ('00000000-0000-0000-0000-000000000001',1),
 ('00000000-0000-0000-0000-000000000001',2),
 ('00000000-0000-0000-0000-000000000002',1),
 ('00000000-0000-0000-0000-000000000003',1);
INSERT INTO rkb_v4.active_revision VALUES
 ('00000000-0000-0000-0000-000000000001',2),
 ('00000000-0000-0000-0000-000000000002',1),
 ('00000000-0000-0000-0000-000000000003',1);
INSERT INTO rkb_v4.passage_search
 (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
 SELECT ('10000000-0000-0000-0000-' || lpad(n::text,12,'0'))::uuid,
 source_id,revision,n,'page ' || n,
 CASE WHEN n = 2 THEN 'semantic harbour without keyword ' || repeat('x',5000) ELSE 'needle forbidden' END,
 'simple','pg_catalog.simple','{vector}'::public.vector
 FROM (SELECT row_number() OVER (ORDER BY source_id,revision)::int n, *
       FROM rkb_v4.revisions) x;
DO $$
DECLARE ids uuid[]; total integer; old_projection jsonb; new_projection jsonb;
BEGIN
 SELECT count(*) INTO total FROM rkb_v4.lexical_candidates('needle',200);
 IF total <> 0 THEN RAISE EXCEPTION 'inactive/private leakage lexical %',total; END IF;
 SELECT array_agg(passage_id) INTO ids FROM rkb_v4.dense_candidates('{vector}',200);
 IF cardinality(ids) <> 1 THEN RAISE EXCEPTION 'dense eligibility failed'; END IF;
 SELECT count(*) INTO total FROM rkb_v4.hydrate(ids);
 IF total <> 1 THEN RAISE EXCEPTION 'dense independent recall failed'; END IF;
 SELECT length(body) INTO total FROM rkb_v4.hydrate(ids);
 IF total <= 4000 THEN RAISE EXCEPTION 'body silently truncated'; END IF;
 SELECT to_jsonb(p) INTO old_projection FROM rkb_v4.passage_search p WHERE passage_id=ids[1];
 UPDATE rkb_v4.sources SET bibliography=repeat('b',5000)
 WHERE source_id='00000000-0000-0000-0000-000000000001';
 SELECT length(bibliography) INTO total FROM rkb_v4.hydrate(ids);
 IF total <> 5000 THEN RAISE EXCEPTION 'bibliography silently truncated'; END IF;
 SELECT to_jsonb(p) INTO new_projection FROM rkb_v4.passage_search p WHERE passage_id=ids[1];
 IF old_projection IS DISTINCT FROM new_projection THEN RAISE EXCEPTION 'metadata reembedded projection'; END IF;
 UPDATE rkb_v4.sources SET bibliography='accepted shared book'
 WHERE source_id='00000000-0000-0000-0000-000000000001';

 UPDATE rkb_v4.sources SET withdrawn=true WHERE bibliography='accepted shared book';
 SELECT count(*) INTO total FROM rkb_v4.hydrate(ids);
 IF total <> 0 THEN RAISE EXCEPTION 'hydrate did not recheck withdrawal'; END IF;
 UPDATE rkb_v4.sources SET withdrawn=false WHERE bibliography='accepted shared book';
 BEGIN
  UPDATE rkb_v4.passage_search SET body='replacement';
  RAISE EXCEPTION 'immutable update unexpectedly succeeded';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM NOT LIKE 'immutable revision/projection:%' THEN RAISE; END IF;
 END;
 UPDATE rkb_v4.active_revision SET revision=1
 WHERE source_id='00000000-0000-0000-0000-000000000001';
 SELECT count(*) INTO total FROM rkb_v4.lexical_candidates('needle',200);
 IF total <> 1 THEN RAISE EXCEPTION 'pointer activation failed'; END IF;
END $$;
ANALYZE rkb_v4.passage_search;
SET LOCAL hnsw.iterative_scan = strict_order;
SET LOCAL hnsw.max_scan_tuples = 1000;
PREPARE dense(public.vector, integer) AS SELECT * FROM rkb_v4.dense_candidates($1,$2);
EXECUTE dense('{vector}',10);
EXECUTE dense('{vector}',10);
EXECUTE dense('{vector}',10);
EXECUTE dense('{vector}',10);
EXECUTE dense('{vector}',10);
EXPLAIN (ANALYZE, BUFFERS) EXECUTE dense('{vector}',10);
-- A tiny fixture may rightly prefer sequential scan. Demonstrate indexability
-- separately without claiming this forced plan is a serving performance result.
SET LOCAL enable_seqscan=off;
EXPLAIN (ANALYZE, BUFFERS)
 SELECT p.passage_id FROM rkb_v4.passage_search p
 JOIN rkb_v4.active_revision a USING(source_id,revision)
 JOIN rkb_v4.sources s USING(source_id)
 WHERE s.accepted AND NOT s.withdrawn
 ORDER BY p.embedding <=> '{vector}'::public.vector LIMIT 10;
SELECT 'fixture_search_checks_passed';
ROLLBACK;
"""


@pytest.mark.skipif(not (os.environ.get('RKB_PG_DRIVER_FIXTURE_SOCKET') or
                           os.environ.get('RKB_KAGGLE_TEST_DSN')), reason='disposable PG17 required')
@pytest.mark.asyncio
async def test_hydrate_30_acceptance_union_and_top_ten(catalog_database):
    from regional_knowledge.kaggle_protocol import QueryFilters
    from regional_knowledge.kaggle_search import acceptance_search
    source = '00000000-0000-0000-0000-000000000001'
    other = '00000000-0000-0000-0000-000000000004'
    vector_b = '[' + ','.join(['0', '1'] + ['0'] * 1022) + ']'
    async with catalog_database.transaction():
        await catalog_database.execute((ROOT / 'sql/kaggle/043_hydrate_window.sql').read_text(), prepare=False)
        await catalog_database.execute("INSERT INTO rkb_v4.sources VALUES (%s,'second',true,false)", (other,))
        await catalog_database.execute("INSERT INTO rkb_v4.revisions VALUES (%s,1)", (other,))
        await catalog_database.execute("INSERT INTO rkb_v4.active_revision VALUES (%s,1)", (other,))
        await catalog_database.execute("""
            INSERT INTO rkb_v4.passage_search
                (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            SELECT ('10000000-0000-0000-0000-' || lpad(n::text,12,'0'))::uuid,
                CASE WHEN n<=15 THEN %s::uuid ELSE %s::uuid END,1,n,
                'pages ' || n, CASE WHEN n<=15 THEN 'other' ELSE 'needle' END,
                'simple','pg_catalog.simple',
                CASE WHEN n<=15 THEN %s::vector ELSE %s::vector END
            FROM generate_series(2,30) g(n)
        """, (source, other, vector_literal([1] + [0] * 1023), vector_b))
        ids = (await (await catalog_database.execute(
            'SELECT array_agg(passage_id ORDER BY passage_id) FROM rkb_v4.passage_search')).fetchone())[0]
        for sql, args in [
            ('SELECT count(*) FROM rkb_v4.hydrate(%s::uuid[])', (ids,)),
            ('SELECT count(*) FROM rkb_v4.hydrate_scoped(%s::uuid[],%s::uuid[])', (ids, [source, other])),
        ]:
            assert (await (await catalog_database.execute(sql, args)).fetchone())[0] == 30
    conn = await connect()
    try:
        async def encode(_): return [1] + [0] * 1023
        filters = QueryFilters(source_ids=(source, other))
        normal = await fused_search(conn, 'needle', encode=encode, filters=filters, top_k=10)
        assert len(normal.evidence) == 10
        diagnostic = await acceptance_search(conn, 'needle', encode=encode, filters=filters,
            timeout_seconds=2, snapshot_guard=None, snapshot_finish=None)
        union = {p for branch in diagnostic.branch_ranks for p in branch}
        assert 10 < len(union) <= 30
        assert {e.passage_id for e in diagnostic.evidence} == union
    finally:
        await conn.close()


@pytest.mark.skipif(os.environ.get("RKB_PG_SEARCH_FIXTURE_CONTAINER") != CONTAINER,
                    reason="explicit disposable-container opt-in required")
def test_actual_postgres_search_slice():
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-X", "-v", "ON_ERROR_STOP=1",
         "-U", "postgres", "-d", "rkb_fixture"],
        input=fixture_sql(), text=True, capture_output=True, timeout=25)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "fixture_search_checks_passed" in result.stdout
    assert "passage_dense" in result.stdout
    evidence = os.environ.get("RKB_PG_SEARCH_EVIDENCE")
    if evidence:
        destination = Path(evidence).resolve()
        assert destination.is_relative_to(Path("/home/dev/artifacts"))
        destination.write_text(result.stdout + result.stderr)


@pytest.mark.asyncio
async def test_fused_dense_only_evidence_and_single_batch_hydration():
    from uuid import UUID
    passage = UUID("10000000-0000-0000-0000-000000000001")
    source = UUID("00000000-0000-0000-0000-000000000001")
    statements = []

    class Connection(FixtureConnection):
        def transaction(self):
            return self

        def cursor(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, statement, params=()):
            statements.append((statement, params))
            self.statement = statement

        async def fetchall(self):
            if "lexical_candidates" in self.statement:
                return []
            if "dense_candidates" in self.statement:
                return [(passage, .1)]
            return [(passage, source, 2, "page 1", "harbour", "book")]

    async def encode(query):
        await asyncio.sleep(.01)
        return [1] + [0] * 1023

    result = await fused_search(Connection(), "no lexical match", encode=encode)
    assert result.evidence[0].passage_id == str(passage)
    assert result.incomplete
    hydration = [item for item in statements if "hydrate" in item[0]]
    assert len(hydration) == 1 and hydration[0][1] == ([passage],)
    dense = next(item for item in statements if "dense_candidates" in item[0])
    assert len(dense[1]) == 2  # query vector + budget, no lexical IDs
    timeouts = [int(params[0]) for sql, params in statements if "set_config" in sql]
    assert timeouts == sorted(timeouts, reverse=True)
    assert timeouts[-1] < timeouts[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("server_message,at_budget,expected_timeout", [
    ("canceling statement due to statement timeout", True, True),
    ("canceling statement due to statement timeout", False, False),
    ("canceling statement due to user request", False, False),
    ("canceling statement due to user request", True, False),
])
async def test_server_timeout_race_classifies_only_own_expired_budget(
        server_message, at_budget, expected_timeout, monkeypatch):
    # Real time.sleep(server_budget_ms/1000) at the same 20 ms deadline is
    # nondeterministic under CI load: asyncio's local timeout may fire BEFORE
    # the synthetic server raises QueryCanceled. Model server-first explicitly
    # using a virtual loop clock; keep the strict exception-cause assertion.
    loop = asyncio.get_running_loop()
    real_time = loop.time
    # The server-first scenario needs a virtual clock from the *start* of
    # the search. Freezing only when lexical_candidates raises is too late
    # under overloaded CI: an earlier yield may let the real 50 ms deadline
    # fire first, before that branch is reached.
    frozen = [real_time() if at_budget else None]
    monkeypatch.setattr(loop, "time",
                        lambda: real_time() if frozen[0] is None else frozen[0])
    deadline = [None]
    original_timeout_at = asyncio.timeout_at
    def capture_deadline(when):
        deadline[0] = when
        return original_timeout_at(when)
    monkeypatch.setattr(asyncio, "timeout_at", capture_deadline)
    from psycopg.errors import QueryCanceled
    from psycopg.pq import DiagnosticField

    server_error = QueryCanceled("synthetic server cancellation", info={
        DiagnosticField.MESSAGE_PRIMARY: server_message.encode()})

    class ServerFirst(FixtureConnection):
        async def execute(self, statement, params=()):
            if "set_config" in statement:
                self.server_budget_ms = int(params[0])
            if "lexical_candidates" in statement:
                if at_budget:
                    # Server QueryCanceled has arrived just before the async
                    # deadline, while the event-loop callback has NOT fired.
                    # Freeze until exception classification/rollback completes;
                    # never rely on operating-system sleep granularity.
                    assert deadline[0] is not None
                    frozen[0] = deadline[0] - .0005
                raise server_error
            await super().execute(statement, params)

    async def encode(query):
        return [1] + [0] * 1023

    connection = ServerFirst()
    try:
        with pytest.raises(TimeoutError if expected_timeout else QueryCanceled) as caught:
            await fused_search(connection, "fixture", encode=encode, timeout_seconds=.05)
    finally:
        frozen[0] = None
    assert connection.cleaned
    if expected_timeout:
        assert caught.value.__cause__ is server_error
    else:
        assert caught.value is server_error
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not _PENDING_ENCODERS
