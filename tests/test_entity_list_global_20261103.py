"""Optional source entity_list contract and native SQL048 query on existing PG17 fixture."""
import pytest

from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest
from test_entity_list_prefilter_20261023 import selected, graph_sql22, long_graph_command
from test_source_import import SOURCE
from test_source_story_native import apply

SECOND = '00000000-0000-4000-8000-000000000002'


def test_optional_and_multiple_sources_are_valid_read_requests():
    assert selected(source_id=None, query='Кант').source_id is None
    assert selected(source_id=None, source_ids=(SOURCE, SECOND), query='Кант').source_ids == (SOURCE, SECOND)
    with pytest.raises(ValueError):
        selected(source_id=None, source_ids=(SOURCE, SOURCE))


@pytest.mark.asyncio
async def test_pg17_global_sql48_same_query_scoped_and_cursor(graph_sql22):
    native = graph_sql22
    assert (await apply(native, long_graph_command())).status == 'applied'
    connection = native['connection']
    # Second *synthetic* accepted source: preserve the exact canonical joins
    # and provenance while giving every source-owned row a fresh source UUID.
    async with connection.transaction():
        for table in ('sources', 'active_revision', 'source_visibility', 'source_graph_receipts',
                      'source_entity_representations'):
            await connection.execute(
                f'CREATE TEMP TABLE second_{table} AS SELECT * FROM rkb_v4.{table} WHERE source_id=%s', (SOURCE,))
            await connection.execute(f'UPDATE second_{table} SET source_id=%s', (SECOND,))
            await connection.execute(
                f'INSERT INTO rkb_v4.{table} SELECT * FROM second_{table}')
        # This query must include two sources, with stable keyset order.
        query = 'unrelated entry'
        sql = ('SELECT source_id,record_id FROM rkb_v4.read_source_graph_global_v1('
               '%s::uuid[],NULL,NULL,%s::text[],%s,4)')
        first = await (await connection.execute(sql, (None, ['person'], query))).fetchall()
        assert len(first) == 4
        assert [str(r[0]) for r in first] == sorted(str(r[0]) for r in first)
        after = first[-1]
        next_page = await (await connection.execute(
            'SELECT source_id,record_id FROM rkb_v4.read_source_graph_global_v1(%s::uuid[],%s::uuid,%s,%s::text[],%s,4)',
            (None, after[0], after[1], ['person'], query))).fetchall()
        assert next_page and (str(next_page[0][0]),next_page[0][1]) > (str(after[0]),after[1])
        scoped = await (await connection.execute(sql, ([SOURCE], ['person'], query))).fetchall()
        assert scoped and all(str(r[0])==SOURCE for r in scoped)
        multi = await (await connection.execute(sql, ([SECOND,SOURCE], ['person'], query))).fetchall()
        assert multi == first
        # Existing accepted/withdrawn source joins, never a client-side filter.
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SECOND,))
        remaining = await (await connection.execute(sql, (None, ['person'], query))).fetchall()
        assert remaining and all(str(r[0]) == SOURCE for r in remaining)
        plan = await (await connection.execute(
            'EXPLAIN (FORMAT TEXT) '+sql, (None, ['person'],query))).fetchall()
        assert plan and all(row[0] for row in plan)
