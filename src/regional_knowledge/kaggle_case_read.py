"""Fixed native Case reads on the existing bounded shared read resource."""
from __future__ import annotations
import asyncio
import base64
import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from .kaggle_case_contracts import CaseReadRequest,CaseReadResult,canonical
from .kaggle_query import ReadUnavailable,_frontier,_identity
from .disagreement_native import CASE_REVISIONS
from .kaggle_domain_bindings import DOMAIN_REVISIONS

CASE_READER_FUNCTIONS=(
    'verify_disagreement_origins_v1(uuid[])',
    'read_disagreement_v1(uuid,uuid,text,bigint,integer,bigint,uuid,bigint,uuid)',
    'read_disagreement_operation_v1(uuid,uuid,text)',
)


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
                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        await execute('SELECT schema_revision,function_revision,executor_revision FROM rkb_v4.recovery_state WHERE singleton')
                        if tuple(await cursor.fetchone() or ()) not in (CASE_REVISIONS.wire(),DOMAIN_REVISIONS.wire()):
                            raise ReadUnavailable('not_ready')
                        await execute('SELECT rkb_v4.verify_disagreement_origins_v1(%s::uuid[])',([request.case_id],))
                        if await cursor.fetchone()!=(True,):raise ReadUnavailable('not_ready')
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
