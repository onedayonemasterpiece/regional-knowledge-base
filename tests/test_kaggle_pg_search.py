"""Explicit opt-in, disposable PG17/pgvector fixture only; no production DSN."""
import asyncio
import os
from pathlib import Path
import subprocess

import pytest

from regional_knowledge.kaggle_search import fused_search, vector_literal

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "rkb-v4-fixture-20261020"


def test_vector_contract():
    assert vector_literal([1] + [0] * 1023).startswith("[1.0,0.0")
    for vector in ([0] * 1024, [1] * 1023, [float("nan")] * 1024):
        with pytest.raises(ValueError):
            vector_literal(vector)


@pytest.mark.asyncio
async def test_deadline_cancels_encoder_and_exits_transaction():
    entered = asyncio.Event()
    cleaned = asyncio.Event()

    async def encode(query):
        entered.set()
        await asyncio.sleep(10)

    class Connection:
        def transaction(self):
            return self

        async def __aenter__(self):
            await entered.wait()
            await asyncio.sleep(10)

        async def __aexit__(self, *args):
            cleaned.set()

    with pytest.raises(TimeoutError):
        await fused_search(Connection(), "fixture", encode=encode, timeout_seconds=.02)
    # Cancellation during transaction entry does not enter its body.
    assert not cleaned.is_set()
    assert not [task for task in asyncio.all_tasks()
                if task is not asyncio.current_task() and not task.done()]


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
 CASE WHEN n = 2 THEN 'semantic harbour without keyword' ELSE 'needle forbidden' END,
 'simple','pg_catalog.simple','{vector}'::public.vector
 FROM (SELECT row_number() OVER (ORDER BY source_id,revision)::int n, *
       FROM rkb_v4.revisions) x;
DO $$
DECLARE ids uuid[]; total integer;
BEGIN
 SELECT count(*) INTO total FROM rkb_v4.lexical_candidates('needle',200);
 IF total <> 0 THEN RAISE EXCEPTION 'inactive/private leakage lexical %',total; END IF;
 SELECT array_agg(passage_id) INTO ids FROM rkb_v4.dense_candidates('{vector}',200);
 IF cardinality(ids) <> 1 THEN RAISE EXCEPTION 'dense eligibility failed'; END IF;
 SELECT count(*) INTO total FROM rkb_v4.hydrate(ids);
 IF total <> 1 THEN RAISE EXCEPTION 'dense independent recall failed'; END IF;
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

    class Connection:
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
        return [1] + [0] * 1023

    result = await fused_search(Connection(), "no lexical match", encode=encode)
    assert result.evidence[0].passage_id == str(passage)
    assert result.incomplete
    hydration = [item for item in statements if "hydrate" in item[0]]
    assert len(hydration) == 1 and hydration[0][1] == ([passage],)
    dense = next(item for item in statements if "dense_candidates" in item[0])
    assert len(dense[1]) == 2  # query vector + budget, no lexical IDs
