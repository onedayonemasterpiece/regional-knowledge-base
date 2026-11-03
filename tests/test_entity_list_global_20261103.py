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
        for table in ('sources', 'revisions', 'active_revision', 'source_graph_receipts',
                      'source_entity_representations'):
            await connection.execute(
                f'CREATE TEMP TABLE second_{table} AS SELECT * FROM rkb_v4.{table} WHERE source_id=%s', (SOURCE,))
            await connection.execute(f'UPDATE second_{table} SET source_id=%s', (SECOND,))
            await connection.execute(
                f'INSERT INTO rkb_v4.{table} SELECT * FROM second_{table}')
        # Source insertion creates its visibility row through the native trigger.
        # This query must include two sources, with stable keyset order.
        query = 'strasse'
        sql = ('SELECT source_id,record_id FROM rkb_v4.read_source_graph_global_v1('
               '%s::uuid[],NULL,NULL,%s::text[],%s,4)')
        first = await (await connection.execute(sql, (None, ['organization'], query))).fetchall()
        assert len(first) == 4
        assert {str(r[0]) for r in first} == {SOURCE, SECOND}
        assert [str(r[0]) for r in first] == sorted(str(r[0]) for r in first)
        after = first[-1]
        next_page = await (await connection.execute(
            'SELECT source_id,record_id FROM rkb_v4.read_source_graph_global_v1(%s::uuid[],%s::uuid,%s,%s::text[],%s,4)',
            (None, after[0], after[1], ['organization'], query))).fetchall()
        assert next_page and (str(next_page[0][0]),next_page[0][1]) > (str(after[0]),after[1])
        scoped = await (await connection.execute(sql, ([SOURCE], ['organization'], query))).fetchall()
        assert scoped and all(str(r[0])==SOURCE for r in scoped)
        multi = await (await connection.execute(sql, ([SECOND,SOURCE], ['organization'], query))).fetchall()
        assert multi == first
        # Existing accepted/withdrawn source joins, never a client-side filter.
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SECOND,))
        remaining = await (await connection.execute(sql, (None, ['organization'], query))).fetchall()
        assert remaining and all(str(r[0]) == SOURCE for r in remaining)
        # Inspect the inner join/filter, not just the non-inlineable SQL
        # function's outer Function Scan. Tiny synthetic data needs no index.
        plan = await (await connection.execute("""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT n.source_id,n.entity_key FROM rkb_v4.source_entity_representations n
            JOIN rkb_v4.sources s USING(source_id)
            JOIN rkb_v4.active_revision a USING(source_id)
            JOIN rkb_v4.source_visibility v USING(source_id)
            JOIN rkb_v4.source_graph_receipts r USING(source_id)
            WHERE s.accepted AND NOT s.withdrawn AND a.revision=1
            AND position(rkb_v4.source_graph_name_fold_v1(%s)
              IN rkb_v4.source_graph_name_fold_v1(n.data->>'canonical_label'))>0
            ORDER BY n.source_id,n.entity_key COLLATE "C" LIMIT 4
        """, (query,))).fetchall()
        assert plan and any('Execution Time:' in row[0] for row in plan)
        print('SQL048 synthetic inner plan: ' + '\\n'.join(row[0] for row in plan))


@pytest.mark.asyncio
async def test_global_native_uses_one_bounded_pg_read():
    from regional_knowledge.kaggle_source_graph_read import execute_source_graph_read
    from test_kaggle_fact_read import Native
    from test_kaggle_source_graph_read import row
    n=Native([row(2)],revision=('rkb-v4-sql001-048-v1','current'))
    result=await execute_source_graph_read(selected(source_id=None,query='recorded alias',limit=1),
                                           acquire=n.acquire)
    assert result.status=='ok' and len(result.records)==1
    calls=[(sql,params) for sql,params in n.calls if 'read_source_graph_global_v1' in sql]
    assert len(calls)==1
    assert calls[0][1]==(None,None,None,None,'recorded alias',2)
