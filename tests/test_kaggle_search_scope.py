"""Bounded source filtering, synthetic fixture only; no model or provider calls."""
import os
from pathlib import Path
from uuid import UUID

import pytest

from regional_knowledge.kaggle_protocol import QueryFilters
from regional_knowledge.kaggle_search import fused_search
from test_kaggle_pg_search import FixtureConnection, fixture_sql
from test_kaggle_pg_driver import database, connect

ROOT = Path(__file__).resolve().parents[1]
SOURCE = '00000000-0000-0000-0000-000000000001'
OTHER = '00000000-0000-0000-0000-000000000004'


@pytest.mark.asyncio
@pytest.mark.parametrize('filters', [
    {}, QueryFilters(source_ids=('not-a-uuid',)), QueryFilters(kinds=('fact',)),
    QueryFilters(kinds=('source', 'story')),
    QueryFilters.model_construct(source_ids=(), kinds=None),
    QueryFilters.model_construct(source_ids=(SOURCE,) * 33, kinds=None),
    QueryFilters(source_ids=(SOURCE, SOURCE.replace('-', ''))),
])
async def test_invalid_scope_before_inference_or_sql(filters):
    connection = FixtureConnection()

    async def encode(query):
        raise AssertionError('invalid filter reached inference')

    with pytest.raises(ValueError):
        await fused_search(connection, 'needle', encode=encode, filters=filters)
    assert not connection.statements


@pytest.mark.asyncio
async def test_scope_bound_independently_in_every_branch():
    connection = FixtureConnection()

    async def encode(query):
        return [1] + [0] * 1023

    result = await fused_search(connection, 'needle', encode=encode,
                                filters=QueryFilters(source_ids=(SOURCE,), kinds=('source',)))
    queries = [(sql, params) for sql, params in connection.statements if '_scoped(' in sql]
    assert len(queries) == 3
    assert all(params[-1] == [UUID(SOURCE)] for _, params in queries)
    assert result.incomplete and result.limitation == 'bounded_candidate_fusion_unverified'


@pytest.mark.asyncio
@pytest.mark.parametrize('filters', [None, QueryFilters(), QueryFilters(kinds=('source',))])
async def test_no_source_scope_keeps_existing_sql(filters):
    connection = FixtureConnection()

    async def encode(query):
        return [1] + [0] * 1023

    result = await fused_search(connection, 'needle', encode=encode, filters=filters)
    assert not any('_scoped(' in sql for sql, _ in connection.statements)
    assert result.limitation == 'bounded_ann_recall_unverified'


def scoped_fixture_sql():
    # Reuse only synthetic rows, without executing the old test's assertions/rollback.
    initial = fixture_sql().split('DO $$')[0].replace('BEGIN;', '').replace(
        (ROOT / 'sql/kaggle/001_search.sql').read_text(), '')
    ddl = (ROOT / 'sql/kaggle/003_search_scope.sql').read_text()
    vector = '[1,' + ','.join(['0'] * 1023) + ']'
    return initial + ddl + f'''
INSERT INTO rkb_v4.sources VALUES ('{OTHER}', 'other active book', true, false);
INSERT INTO rkb_v4.revisions VALUES ('{OTHER}',1);
INSERT INTO rkb_v4.active_revision VALUES ('{OTHER}',1);
INSERT INTO rkb_v4.passage_search
 (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
 VALUES ('10000000-0000-0000-0000-000000000005','{OTHER}',1,1,
 'page 1','needle outside selected scope','simple','pg_catalog.simple','{vector}');
DO $$
DECLARE ids uuid[]; n integer; picked uuid; bad uuid[];
BEGIN
 SELECT count(*) INTO n FROM rkb_v4.lexical_candidates_scoped('needle',200,ARRAY['{SOURCE}']::uuid[]);
 IF n <> 0 THEN RAISE EXCEPTION 'scoped lexical leaked inactive/outside source'; END IF;
 SELECT array_agg(passage_id) INTO ids FROM rkb_v4.dense_candidates_scoped('{vector}',200,ARRAY['{SOURCE}']::uuid[]);
 IF cardinality(ids) <> 1 THEN RAISE EXCEPTION 'scoped dense lost independent semantic hit'; END IF;
 SELECT source_id INTO picked FROM rkb_v4.hydrate_scoped(ids,ARRAY['{SOURCE}']::uuid[]);
 IF picked IS DISTINCT FROM '{SOURCE}'::uuid THEN RAISE EXCEPTION 'incorrect hydration'; END IF;
 SELECT count(*) INTO n FROM rkb_v4.hydrate_scoped(ids,ARRAY['{OTHER}']::uuid[]);
 IF n <> 0 THEN RAISE EXCEPTION 'hydrate failed source scope'; END IF;
 SELECT count(*) INTO n FROM rkb_v4.dense_candidates_scoped('{vector}',200,ARRAY[
 '{SOURCE}','{OTHER}','00000000-0000-0000-0000-000000000002',
 '00000000-0000-0000-0000-000000000003']::uuid[]);
 IF n <> 2 THEN RAISE EXCEPTION 'mixed scope eligibility failure'; END IF;
 SELECT count(*) INTO n FROM rkb_v4.lexical_candidates_scoped('needle',200,ARRAY['{OTHER}']::uuid[]);
 IF n <> 1 THEN RAISE EXCEPTION 'selected lexical source absent'; END IF;
 UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id='{SOURCE}';
 SELECT count(*) INTO n FROM rkb_v4.hydrate_scoped(ids,ARRAY['{SOURCE}']::uuid[]);
 IF n <> 0 THEN RAISE EXCEPTION 'hydrate failed current withdrawal'; END IF;
 UPDATE rkb_v4.sources SET withdrawn=false,accepted=false WHERE source_id='{SOURCE}';
 SELECT count(*) INTO n FROM rkb_v4.hydrate_scoped(ids,ARRAY['{SOURCE}']::uuid[]);
 IF n <> 0 THEN RAISE EXCEPTION 'hydrate failed current acceptance'; END IF;
 UPDATE rkb_v4.sources SET accepted=true WHERE source_id='{SOURCE}';
 UPDATE rkb_v4.active_revision SET revision=1 WHERE source_id='{SOURCE}';
 SELECT count(*) INTO n FROM rkb_v4.hydrate_scoped(ids,ARRAY['{SOURCE}']::uuid[]);
 IF n <> 0 THEN RAISE EXCEPTION 'hydrate failed current revision'; END IF;
 SELECT count(*) INTO n FROM rkb_v4.lexical_candidates_scoped('needle',200,ARRAY['{SOURCE}']::uuid[]);
 IF n <> 1 THEN RAISE EXCEPTION 'activation not reflected'; END IF;
 FOREACH bad SLICE 1 IN ARRAY ARRAY[ARRAY['{SOURCE}','{SOURCE}']::uuid[],ARRAY['{SOURCE}',NULL]::uuid[]] LOOP
  BEGIN
   PERFORM rkb_v4.checked_source_scope(bad);
   RAISE EXCEPTION 'bad scope accepted';
  EXCEPTION WHEN invalid_parameter_value THEN NULL;
  END;
 END LOOP;
 BEGIN
  PERFORM rkb_v4.checked_source_scope(ARRAY[]::uuid[]);
  RAISE EXCEPTION 'empty scope accepted';
 EXCEPTION WHEN invalid_parameter_value THEN NULL;
 END;
END $$;
'''


@pytest.mark.skipif(not (os.environ.get('RKB_PG_DRIVER_FIXTURE_SOCKET')
                            or os.environ.get('RKB_KAGGLE_TEST_DSN')),
                    reason='explicit guarded disposable fixture required')
@pytest.mark.asyncio
async def test_actual_pg_scope_and_current_hydration(database):
    # Reuse the existing guarded fixture; never accept arbitrary DSNs.
    async with database.transaction():
        await database.execute('TRUNCATE rkb_v4.sources CASCADE')
        await database.execute(scoped_fixture_sql(), prepare=False)
        # SQL assertions above exercise SQL003; Python source hydration uses SQL009.
        for path in sorted((ROOT / 'sql/kaggle').glob('00[2-9]_*.sql')):
            if not path.name.startswith('003_'):
                await database.execute(path.read_text(), prepare=False)
        await database.execute('CREATE TABLE rkb_v4.source_catalog_corrections '
                               '(source_id uuid, accepted_sequence bigint, metadata jsonb)')
    connection = await connect()
    try:
        async def encode(query):
            return [1] + [0] * 1023

        # The SQL checks switched SOURCE to revision 1, whose body has a lexical hit.
        result = await fused_search(connection, 'needle', encode=encode,
                                    filters=QueryFilters(source_ids=(SOURCE,)))
        assert len(result.evidence) == 1
        assert result.evidence[0].source_id == SOURCE
        assert result.evidence[0].revision == 1
        assert connection.info.transaction_status.name == 'IDLE'
    finally:
        await connection.close()
