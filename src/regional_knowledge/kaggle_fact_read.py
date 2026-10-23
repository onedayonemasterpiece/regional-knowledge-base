"""Native indexed facts reads. No encoding, SQL RPC or legacy corpus access."""
from __future__ import annotations

import asyncio
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .kaggle_fact_contracts import (FactReadRequest, FactReadResult, canonical,
    cursor_scope, decode_cursor, encode_cursor, observation_position)
from .kaggle_protocol import EvidenceItem, EvidenceMetadata, ItemDependencies, SourceDependency
from .kaggle_query import ReadUnavailable, _frontier, _identity
from .story_contracts import StageEventDateInput, StageObservationInput

FACT_READER_FUNCTIONS = (
    'read_source_calendar_v1(text,integer,integer,uuid,integer,uuid,integer,integer)',
    'read_source_observations_v1(text,text,uuid,uuid,integer,integer)',
    'read_source_assertions_v1(uuid,uuid,integer)',
)


OBSERVATION_PAIR_SQL = """
 SELECT a.assertion_id,a.source_id,a.source_revision,v.eligibility_revision,a.proposition,
 a.data->>'account_kind',a.data->>'attributed_to',a.data->>'reported_by',d.ordinal,d.data,d.proofs,s.bibliography,0
 FROM rkb_v4.source_observations d JOIN rkb_v4.story_assertion_versions a
 ON (a.assertion_id,a.revision)=(d.assertion_id,d.assertion_revision)
 JOIN rkb_v4.sources s ON s.source_id=a.source_id
 JOIN rkb_v4.active_revision r ON (r.source_id,r.revision)=(a.source_id,a.source_revision)
 JOIN rkb_v4.source_visibility v ON v.source_id=a.source_id
 WHERE (d.assertion_id,d.assertion_revision,d.ordinal) IN ((%s::uuid,1,%s),(%s::uuid,1,%s))
 AND a.source_id=%s::uuid AND s.accepted AND NOT s.withdrawn
"""


def _record(row, action):
    (assertion_id, source_id, revision, eligibility, proposition, account_kind,
     attributed, reported, ordinal, fact, proofs, bibliography, year) = row
    assertion_id, source_id = str(assertion_id), str(source_id)
    UUID(assertion_id); UUID(source_id)
    if (not isinstance(proposition, str) or not 1 <= len(proposition) <= 500 or
            type(ordinal) is not int or not 0 <= ordinal <= 7 or
            not isinstance(proofs, list) or not 1 <= len(proofs) <= 4):
        raise ReadUnavailable('internal_error')
    for proof in proofs:
        if (type(proof) is not dict or set(proof) != {'region_id', 'passage_id', 'start', 'end', 'original_excerpt'} or
                type(proof['original_excerpt']) is not str or not 1 <= len(proof['original_excerpt']) <= 500 or
                type(proof['start']) is not int or type(proof['end']) is not int or
                not 0 <= proof['start'] < proof['end'] <= 8000):
            raise ReadUnavailable('internal_error')
        UUID(proof['region_id']); UUID(proof['passage_id'])
    if action != 'assertions':
        model = StageEventDateInput if action == 'calendar' else StageObservationInput
        if model.model_validate(fact).model_dump(mode='json') != fact:
            raise ReadUnavailable('internal_error')
        if action == 'calendar' and (fact['precision'] != 'day' or fact['date_role'] != 'event'):
            raise ReadUnavailable('internal_error')
        literal = fact['original_date_text'] if action == 'calendar' else fact['original_value_text']
        if not any(literal in proof['original_excerpt'] for proof in proofs):
            raise ReadUnavailable('internal_error')
        if action == 'observations' and fact['period_text'] != 'unknown' and not any(
                fact['period_text'] in proof['original_excerpt'] for proof in proofs):
            raise ReadUnavailable('internal_error')
    identifier = assertion_id if action == 'assertions' else f'{assertion_id}:{"d" if action == "calendar" else "o"}{ordinal}'
    record = dict(id=identifier, source_id=source_id, source_revision=revision,
        assertion_id=assertion_id, assertion_revision=1, proposition=proposition,
        account_kind=account_kind, attributed_to=attributed, reported_by=reported,
        kind=action, fact=fact, proofs=proofs, bibliography_complete=len(bibliography) <= 1024,
        semantic_status='source_attributed_account_not_verified_historical_truth')
    if action == 'calendar':
        record.update(calendar_conversion='none')
    if action == 'observations':
        record.update(subject_identity='unverified_label_only',
            comparison_policy='no_automatic_unit_or_entity_coercion')
    dependency = SourceDependency(source_id=source_id, content_revision=revision,
                                  eligibility_revision=eligibility)
    item = EvidenceItem(id=identifier, kind='fact', text=proposition,
        metadata=EvidenceMetadata(citation=bibliography[:1024], locator=f'assertion:{assertion_id}:r1'),
        dependencies=ItemDependencies(source_ids=(source_id,), source_revisions=(dependency,),
            variant_refs=(), grant_refs=()))
    return record, item, (assertion_id, ordinal, year)


async def execute_fact_read(request: FactReadRequest, *, acquire):
    if type(request) is not FactReadRequest:
        raise TypeError('validated fixed fact read required')
    loop = asyncio.get_running_loop()
    deadline = loop.time() + min(request.deadline_ms, 2000) / 1000
    observed = {}

    def failed(reason, invalid=False):
        return FactReadResult(**_identity(request), status='invalid_query' if invalid else 'denied' if reason == 'access_denied' else 'unavailable',
            observed_data_seq=observed.get('data'), observed_projection_seq=observed.get('projection'),
            observed_visibility_epoch=observed.get('visibility'), observed_database_generation=observed.get('database'),
            reason=reason)

    try:
        async with asyncio.timeout_at(deadline):
            async with acquire() as connection:
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE or
                        connection.row_factory is not tuple_row):
                    raise ReadUnavailable('internal_error')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')

                        async def execute(statement, params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1, int((deadline - loop.time()) * 1000))),))
                            await cursor.execute(statement, params)

                        await _frontier(cursor, execute, request, observed, require_dense=False)
                        await execute('SELECT max(version) FROM rkb_v4.schema_migrations')
                        installed=await cursor.fetchone()
                        if not installed or type(installed[0]) is not int or installed[0]<8:
                            raise ReadUnavailable('not_ready')
                        scope = cursor_scope(request, observed)
                        after = ('00000000-0000-0000-0000-000000000000', -1, 0)
                        if request.cursor:
                            try:
                                after = decode_cursor(request.cursor, scope)
                            except ValueError:
                                raise ReadUnavailable('invalid_input') from None
                        assertion, ordinal, year = after
                        comparison_story = None
                        if request.action == 'observation_compare':
                            # Both indexed observation lookups share the exact Story snapshot.
                            statement = ("SELECT rkb_v4.read_story_v1(%s,'get',NULL,0,1)"
                                if installed[0]<9 else
                                "SELECT rkb_v4.read_editorial_story_v1(%s,NULL,'get',0,1,NULL)")
                            await execute(statement, (UUID(request.story_id),))
                            row = await cursor.fetchone()
                            comparison_story = row[0] if row else None
                            if not comparison_story:
                                raise ReadUnavailable('access_denied')
                            left, right = observation_position(request.left_id), observation_position(request.right_id)
                            if (comparison_story.get('story_id') != request.story_id or
                                    any(position[0] not in comparison_story['finding_ids'] for position in (left,right))):
                                raise ReadUnavailable('access_denied')
                            await execute(OBSERVATION_PAIR_SQL, (*left,*right,
                                comparison_story['source']['source_id']))
                        elif request.action == 'calendar':
                            await execute('SELECT * FROM rkb_v4.read_source_calendar_v1(%s,%s,%s,%s::uuid,%s,%s::uuid,%s,%s)',
                                (request.calendar, request.month, request.day, request.source_id,
                                 year, assertion, ordinal, request.limit + 1))
                        elif request.action == 'observations':
                            await execute('SELECT * FROM rkb_v4.read_source_observations_v1(%s,%s,%s::uuid,%s::uuid,%s,%s)',
                                (request.metric_key, request.unit_code, request.source_id,
                                 assertion, ordinal, request.limit + 1))
                        else:
                            await execute('SELECT * FROM rkb_v4.read_source_assertions_v1(%s::uuid,%s::uuid,%s)',
                                (request.source_id, assertion, request.limit + 1))
                        rows = await cursor.fetchall()
                        if len(rows) > request.limit + 1:
                            raise ReadUnavailable('internal_error')
                        if comparison_story is not None:
                            by_id = {f'{row[0]}:o{row[8]}':row for row in rows}
                            if len(rows) != 2 or set(by_id) != {request.left_id,request.right_id}:
                                raise ReadUnavailable('access_denied')
                            rows = [by_id[request.left_id],by_id[request.right_id]]
                        records, evidence, positions = [], [], []
                        for row in rows[:request.limit]:
                            record, item, position = _record(row,
                                'observations' if comparison_story is not None else request.action)
                            if comparison_story is not None:
                                if (record['source_id'] != comparison_story['source']['source_id'] or
                                        record['source_revision'] != comparison_story['source']['content_revision']):
                                    raise ReadUnavailable('internal_error')
                                record.update(story_id=request.story_id,story_revision=comparison_story['revision'])
                            if request.source_id is not None and record['source_id'] != request.source_id:
                                raise ReadUnavailable('internal_error')
                            if request.action=='calendar' and any(record['fact'][key]!=getattr(request,key)
                                    for key in ('month','day','calendar')):
                                raise ReadUnavailable('internal_error')
                            if request.action=='observations' and (record['fact']['metric_key']!=request.metric_key
                                    or request.unit_code is not None and record['fact']['unit_code']!=request.unit_code):
                                raise ReadUnavailable('internal_error')
                            records.append(record); evidence.append(item); positions.append(position)
                        result = FactReadResult(**_identity(request), status='ok',
                            records=tuple(records), evidence=tuple(evidence),
                            next_cursor=encode_cursor(scope, *positions[-1]) if len(rows) > request.limit else None,
                            observed_data_seq=observed['data'], observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'], observed_database_generation=observed['database'])
                        canonical(result.model_dump(mode='json'))
                if loop.time() >= deadline:
                    raise TimeoutError
                return result
    except ReadUnavailable as exc:
        return failed(exc.reason, exc.reason == 'invalid_input')
    except TimeoutError:
        return failed('deadline_exceeded')
    except asyncio.CancelledError:
        raise
    except psycopg.errors.QueryCanceled as exc:
        return failed('deadline_exceeded' if loop.time() + .001 >= deadline and
            exc.diag.message_primary == 'canceling statement due to statement timeout' else 'internal_error')
    except (psycopg.Error, ValueError, TypeError, KeyError):
        return failed('internal_error')
