"""Bounded fact wire/native executor behavior without model or PG effects."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from regional_knowledge.kaggle_fact_contracts import (FactReadRequest, canonical,
    cursor_scope, decode_cursor, encode_cursor)
from regional_knowledge.kaggle_fact_read import execute_fact_read, FACT_SCHEMA, FACT_FUNCTION
from regional_knowledge.story_contracts import StageEventDateInput, StageObservationInput

SOURCE = '00000000-0000-0000-0000-000000000001'
ASSERTION = '10000000-0000-0000-0000-000000000001'
FRONTIER = (9, 9, 4, True, False, 'native-db')


def request(**changes):
    values = dict(request_id='fact-call', connection_generation=2, turn_generation=3,
        action='calendar', month=2, day=29, calendar='julian', min_data_seq=9,
        deadline_ms=2000, actor=dict(person_id='actual-verified-person', audience='personal',
            auth_epoch=3, visibility_epoch=4))
    values.update(changes)
    return FactReadRequest.model_validate(values)


def date(**changes):
    value = dict(original_date_text='29 February 1900', precision='day', calendar='julian',
        date_role='event', year=1900, month=2, day=29, rationale='Exact stated source date.', evidence_indexes=[0])
    value.update(changes)
    return StageEventDateInput.model_validate(value).model_dump(mode='json')


def observation():
    return StageObservationInput.model_validate(dict(original_value_text='12.50', value='12.50',
        metric_key='population_count', unit_code='persons', subject_label='Reported village',
        period_text='1900', period_start_year=1900, period_end_year=1900,
        method='reported', method_note='As reported by the author.', precision='as_reported',
        rationale='Exact source-attributed number.', evidence_indexes=[0])).model_dump(mode='json')


def row(ordinal=0, *, assertion=ASSERTION, action='calendar'):
    return (UUID(assertion), UUID(SOURCE), 1, 7, 'The source reports the date and number.',
        'testimony', None, 'Source author', ordinal,
        date() if action == 'calendar' else observation(),
        [dict(region_id='20000000-0000-0000-0000-000000000001',
            passage_id='30000000-0000-0000-0000-000000000001', start=0, end=47,
            original_excerpt='29 February 1900: population 12.50 persons in 1900.')], 'Exact source citation', 1900 if action == 'calendar' else 0)


class Native:
    def __init__(self, rows=(), *, revision=(FACT_SCHEMA, FACT_FUNCTION), frontier=FRONTIER):
        self.rows, self.revision, self.frontier = rows, revision, frontier
        self.calls = []
        self.closed = False
        self.row_factory = tuple_row
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

    @asynccontextmanager
    async def acquire(self):
        yield self

    @asynccontextmanager
    async def transaction(self):
        yield self

    @asynccontextmanager
    async def cursor(self):
        yield self

    async def execute(self, statement, params=()):
        self.calls.append((statement, params))
        self.statement = statement

    async def fetchone(self):
        return self.revision if 'SELECT schema_revision' in self.statement else self.frontier

    async def fetchall(self):
        return self.rows


@pytest.mark.parametrize('changes', [dict(month=4, day=31), dict(calendar=None), dict(metric_key='count'),
    dict(source_id='caller-arbitrary'), dict(day=True), dict(limit=11), dict(action='unknown')])
def test_exact_filters_do_not_admit_arbitrary_sql_or_approximate_dates(changes):
    with pytest.raises(ValueError):
        request(**changes)


def test_stated_calendar_and_decimal_semantics_preserved():
    assert date()['day'] == 29  # Julian1900 is leap; Gregorian1900 is not.
    with pytest.raises(ValueError):
        date(calendar='gregorian')
    assert observation()['value'] == '12.50'
    assert request(action='observations', month=None, day=None, calendar=None,
                   metric_key='population_count').metric_key == 'population_count'


@pytest.mark.asyncio
async def test_native_calendar_reads_no_dense_encoder_and_uses_last_returned_lookahead():
    native = Native([row(0), row(1)])
    first = await execute_fact_read(request(limit=1), acquire=native.acquire)
    assert first.status == 'ok' and len(first.evidence) == 1
    assert first.records[0]['fact']['calendar'] == 'julian'
    assert first.records[0]['calendar_conversion'] == 'none'
    assert first.evidence[0].dependencies.variant_refs == ()  # No Story exists or is required.
    assert first.evidence[0].dependencies.source_revisions[0].eligibility_revision == 7
    state = dict(data=9, projection=9, visibility=4, database='native-db')
    assert decode_cursor(first.next_cursor, cursor_scope(request(limit=1), state)) == (ASSERTION, 0, 1900)
    native.rows = [row(1)]
    second = await execute_fact_read(request(limit=1, cursor=first.next_cursor), acquire=native.acquire)
    assert second.records[0]['id'].endswith(':d1') and second.next_cursor is None
    call = [v for v in native.calls if 'read_source_calendar_v1' in v[0]][-1]
    assert call[1][-4:] == (1900, ASSERTION, 0, 2)
    assert not any('dense_candidates' in statement or 'embedding' in statement for statement, _ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['actor', 'database', 'visibility', 'data'])
async def test_cursor_refuses_changed_actor_or_native_frontier_before_read_function(change):
    native = Native([row(0), row(1)])
    first = await execute_fact_read(request(limit=1), acquire=native.acquire)
    native.calls.clear()
    kwargs = dict(limit=1, cursor=first.next_cursor)
    if change == 'actor':
        kwargs['actor'] = request().actor.model_copy(update={'auth_epoch':4})
    elif change == 'database':
        native.frontier = (*FRONTIER[:-1], 'new-native-db')
    elif change == 'visibility':
        native.frontier = (9, 9, 5, True, False, 'native-db')
    else:
        native.frontier = (10, 10, 4, True, False, 'native-db')
    result = await execute_fact_read(request(**kwargs), acquire=native.acquire)
    assert result.status == 'invalid_query' and result.reason == 'invalid_input'
    assert not any('read_source_calendar_v1' in s for s, _ in native.calls)


@pytest.mark.asyncio
async def test_fresh7_cannot_silently_serve_fresh8_fact_semantics():
    native = Native(revision=('rkb-v4-sql001-007-v1', 'rkb-v4-source-story-import-v2'))
    result = await execute_fact_read(request(), acquire=native.acquire)
    assert result.status == 'unavailable' and result.reason == 'not_ready'
    assert not any('read_source_calendar_v1' in s for s, _ in native.calls)


@pytest.mark.asyncio
async def test_native_observation_is_source_attributed_decimal_without_unit_conversion():
    native = Native([row(action='observations')])
    result = await execute_fact_read(request(action='observations', month=None, day=None,
        calendar=None, metric_key='population_count', unit_code='persons'), acquire=native.acquire)
    assert result.status == 'ok'
    assert result.records[0]['fact']['value'] == '12.50'
    assert result.records[0]['subject_identity'] == 'unverified_label_only'
    assert result.records[0]['comparison_policy'] == 'no_automatic_unit_or_entity_coercion'


@pytest.mark.asyncio
async def test_projection_lag_and_total_pool_deadline_return_no_fact_payload():
    native = Native([row()], frontier=(10, 9, 4, True, True, 'native-db'))
    lag = await execute_fact_read(request(), acquire=native.acquire)
    assert lag.reason == 'projection_lag' and not lag.records

    @asynccontextmanager
    async def slow():
        await asyncio.sleep(.1)
        yield native
    timed = await execute_fact_read(request(deadline_ms=5), acquire=slow)
    assert timed.reason == 'deadline_exceeded' and not timed.evidence


@pytest.mark.asyncio
async def test_cancellation_drains_native_lease_and_never_becomes_success():
    released = asyncio.Event()

    @asynccontextmanager
    async def waiting():
        try:
            await asyncio.Event().wait()
            yield
        finally:
            released.set()
    task = asyncio.create_task(execute_fact_read(request(), acquire=waiting))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert released.is_set()


def test_cursor_is_canonical_and_cannot_inject_extra_filter_fields():
    cursor = encode_cursor('scope', ASSERTION, 0, 1900)
    assert decode_cursor(cursor, 'scope') == (ASSERTION, 0, 1900)
    with pytest.raises(ValueError):
        decode_cursor(cursor + '=', 'scope')
    with pytest.raises(ValueError):
        decode_cursor(cursor, 'another-scope')


def test_measured_metadata_budget_is_shared_by_runtime_and_native_history_constructors():
    from dataclasses import replace
    from regional_knowledge.kaggle_database_probe import native_probe_for_schema
    from regional_knowledge.kaggle_source_history_native import NativeSourceHistoryObserver
    from regional_knowledge.kaggle_target_runtime import TargetRuntime
    from regional_knowledge.kaggle_apply import HandlerRegistry
    from regional_knowledge.source_import_v3 import FACT_REVISIONS,source_import_v3_handler
    from test_kaggle_target_runtime import spec
    for revision, rows in (('rkb-v4-sql001-011-v1',1376),(FACT_SCHEMA,576),('rkb-v4-sql001-007-v1',512),
                           ('rkb-v4-sql001-006-v1',512),('unknown-later-schema',512)):
        base=spec(fresh=True).wire()
        base['database']=replace(spec(fresh=True).database,schema_revision=revision).wire()
        runtime=TargetRuntime({'target_runtime':base},executed_source='fixture',owner_factory=lambda database:None)
        observer=NativeSourceHistoryObserver(acquire=lambda:None,database=runtime.database,
            identity=('run','nonce','inc'),registry=HandlerRegistry([source_import_v3_handler(
                revisions=FACT_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)]),current=lambda binding:False)
        assert runtime.probe==observer.probe==native_probe_for_schema(revision)
        assert runtime.probe.timeout_seconds==2.0


@pytest.mark.asyncio
@pytest.mark.parametrize('revision',[
    ('rkb-v4-sql001-008-v1','rkb-v4-source-story-facts-v3'),
    ('rkb-v4-sql001-009-v1','rkb-v4-source-story-editorial-v4'),
    ('rkb-v4-sql001-010-v1','rkb-v4-source-story-cases-v5'),
    ('rkb-v4-sql001-011-v1','rkb-v4-source-story-cases-geo-v6'),
])
async def test_reviewed_fact_projection_deployments_execute_exact_native_reader(revision):
    native=Native([row()],revision=revision)
    result=await execute_fact_read(request(),acquire=native.acquire)
    assert result.status=='ok' and result.records[0]['fact']['calendar']=='julian'
    assert any('read_source_calendar_v1' in sql for sql,_ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('revision',[
    ('rkb-v4-sql001-011-v1','rkb-v4-source-story-facts-v3'),
    ('rkb-v4-sql001-012-v1','rkb-v4-source-story-cases-geo-v6'),
    ('rkb-v4-sql001-007-v1','rkb-v4-source-story-cases-geo-v6'),
])
async def test_unknown_or_mixed_fact_deployment_cannot_invoke_read_function(revision):
    native=Native([row()],revision=revision)
    result=await execute_fact_read(request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='not_ready'
    assert not any('read_source_calendar_v1' in sql for sql,_ in native.calls)
