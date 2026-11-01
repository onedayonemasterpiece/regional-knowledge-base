"""Fixed native Case reads on the existing bounded shared read resource."""
from __future__ import annotations
import asyncio
import base64
import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from .kaggle_case_contracts import CaseReadRequest,CaseReadResult,canonical
from .kaggle_query import ReadUnavailable,_identity

CASE_READER_FUNCTIONS=(
    'verify_disagreement_origins_v1(uuid[])',
    'read_disagreement_v1(uuid,uuid,text,bigint,integer,bigint,uuid,bigint,uuid)',
    'read_disagreement_operation_v1(uuid,uuid,text)',
)

 
async def _case_pg_frontier(cursor,execute,request,observed):
    # A PG MVCC snapshot plus the current native actor/Case role is sufficient
    # to read existing Case evidence. Unrelated source packet/projection lag
    # must not hide previously visible cases during the next page stage.
    await execute("""SELECT s.applied_seq,f.projection_seq,f.visibility_epoch,
                           s.database_generation
        FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
        WHERE s.singleton AND f.singleton""")
    row=await cursor.fetchone()
    if (row is None or row[0] is None or row[1] is None
            or row[2] is None or row[3] is None):
        raise ReadUnavailable('not_ready')
    observed.update(data=row[0],projection=row[1],visibility=row[2],database=row[3])
    if min(row[0],row[1])<request.min_data_seq:
        raise ReadUnavailable('projection_lag')


async def execute_case_read(request,*,acquire):
    if type(request) is not CaseReadRequest:raise TypeError('fixed Case read required')
    loop=asyncio.get_running_loop();deadline=loop.time()+min(request.deadline_ms,2000)/1000;observed={}
    def failed(reason):
        return CaseReadResult(**_identity(request),status='denied' if reason=='access_denied' else 'unavailable',reason=reason,
            observed_data_seq=observed.get('data'),observed_projection_seq=observed.get('projection'),
            observed_visibility_epoch=observed.get('visibility'),observed_database_generation=observed.get('database'))
    try:
        async with asyncio.timeout_at(deadline):
            async with acquire() as connection:
                if connection.closed or connection.info.transaction_status!=TransactionStatus.IDLE or connection.row_factory is not tuple_row:
                    raise ReadUnavailable('internal_error')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        async def execute(statement,params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",(str(max(1,int((deadline-loop.time())*1000))),))
                            await cursor.execute(statement,params)
                        await _case_pg_frontier(cursor,execute,request,observed)
                        await execute('SELECT max(version) FROM rkb_v4.schema_migrations')
                        installed=await cursor.fetchone()
                        if not installed or type(installed[0]) is not int or installed[0]<10:
                            raise ReadUnavailable('not_ready')
                        if request.section=='search':
                            # Existing Case ACL/source authority, not a new model or
                            # backend dedup decision. PG candidates are for the
                            # calling model to evaluate by question/place/source.
                            await execute("""
                                SELECT c.case_id, c.revision, c.acl_revision, c.event_seq, c.state,
                                  left(c.data->>'question',2000),left(coalesce(c.data->>'context',''),1000),
                                  rkb_v4.case_sources_v1(c.case_id)
                                FROM rkb_v4.disagreement_cases c
                                WHERE rkb_v4.case_role_v1(c.case_id,%s::uuid)>=1
                                  AND rkb_v4.case_sources_v1(c.case_id) IS NOT NULL
                                  AND c.case_id>%s::uuid
                                  AND (%s='' OR position(lower(%s) in lower(
                                      coalesce(c.data->>'question','')||' '||
                                      coalesce(c.data->>'context',''))) > 0)
                                  AND (%s='' OR position(lower(%s) in lower(
                                      coalesce(c.data->>'question','')||' '||
                                      coalesce(c.data->>'context',''))) > 0)
                                  AND (%s::uuid IS NULL OR EXISTS(
                                      SELECT 1 FROM rkb_v4.disagreement_evidence e
                                      WHERE e.case_id=c.case_id AND e.source_id=%s::uuid))
                                ORDER BY c.case_id LIMIT %s
                            """,(request.actor.person_id,request.after_case_id or '00000000-0000-0000-0000-000000000000',
                                 request.search_query or '',request.search_query or '',
                                 request.place or '',request.place or '',
                                 request.source_id,request.source_id,request.limit+1))
                            rows=await cursor.fetchall()
                            selected=rows[:request.limit]
                            candidates=[dict(case_id=str(row[0]),case_revision=row[1],acl_revision=row[2],
                                event_seq=row[3],state=row[4],question=row[5],context=row[6],
                                source_dependencies=row[7])
                                for row in selected]
                            record=dict(section='search',items=candidates,has_more=len(rows)>request.limit,
                                next_after_case=candidates[-1]['case_id'] if candidates else None)
                            result=CaseReadResult(**_identity(request),status='ok',
                                record_b64=base64.b64encode(canonical(record)).decode(),
                                observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                                observed_visibility_epoch=observed['visibility'],
                                observed_database_generation=observed['database'])
                            if loop.time()>=deadline:raise TimeoutError
                            return result
                        if request.section=='operation':
                            await execute('SELECT rkb_v4.read_disagreement_operation_v1(%s::uuid,%s::uuid,%s)',
                                (request.case_id,request.actor.person_id,request.operation_key))
                        else:
                            await execute('SELECT rkb_v4.read_disagreement_v1(%s::uuid,%s::uuid,%s,%s,%s,%s,%s::uuid,%s,%s::uuid)',
                                (request.case_id,request.actor.person_id,'case' if request.section=='edit_context' else request.section,request.after_seq,request.limit,
                                 request.revision,request.draft_id,request.draft_version,request.after_draft))
                        row=await cursor.fetchone()
                        if row is None or row[0] is None:raise ReadUnavailable('access_denied')
                        record=row[0]
                        if request.section=='edit_context':
                            record={k:record[k] for k in ('case_id','case_revision','acl_revision','event_seq','state',
                                'current_conclusion_id','source_dependencies','case_role')}
                            record.update(section='edit_context',candidate_dependency=None)
                            if request.candidate_source_id is not None:
                                await execute('SELECT a.revision,v.eligibility_revision FROM rkb_v4.active_revision a '
                                    'JOIN rkb_v4.sources s USING(source_id) JOIN rkb_v4.source_visibility v USING(source_id) '
                                    'WHERE a.source_id=%s::uuid AND s.accepted AND NOT s.withdrawn',(request.candidate_source_id,))
                                source=await cursor.fetchone()
                                if source is None:raise ReadUnavailable('evidence_withdrawn')
                                record['candidate_dependency']=dict(source_id=request.candidate_source_id,
                                    content_revision=source[0],eligibility_revision=source[1])
                        result=CaseReadResult(**_identity(request),status='ok',record_b64=base64.b64encode(canonical(record)).decode(),
                            observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
                if loop.time()>=deadline:raise TimeoutError
                return result
    except ReadUnavailable as exc:return failed(exc.reason)
    except TimeoutError:return failed('deadline_exceeded')
    except asyncio.CancelledError:raise
    except psycopg.errors.QueryCanceled as exc:
        return failed('deadline_exceeded' if loop.time()+.001>=deadline and exc.diag.message_primary=='canceling statement due to statement timeout' else 'internal_error')
    except (psycopg.Error,ValueError,TypeError,KeyError):return failed('internal_error')
