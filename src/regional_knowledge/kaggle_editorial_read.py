"""Fixed native editorial reads and scoped edit context; no model or corpus scan."""
import asyncio
from uuid import UUID
import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from .kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
from .kaggle_story_read import story_item
from .kaggle_query import _frontier,_identity,ReadUnavailable

EDITORIAL_SCHEMAS=frozenset({'rkb-v4-sql001-009-v1','rkb-v4-sql001-011-v1'})


def editorial_failure(request,reason,observed):
    return EditorialReadResult(**_identity(request),status='unavailable',reason=reason,record=None,evidence=(),
        observed_data_seq=observed.get('data'),observed_projection_seq=observed.get('projection'),
        observed_visibility_epoch=observed.get('visibility'),observed_database_generation=observed.get('database'))


async def execute_editorial_read(request,*,acquire):
    if type(request) is not EditorialReadRequest:raise TypeError('validated fixed editorial read required')
    observed={};loop=asyncio.get_running_loop();deadline=loop.time()+min(request.deadline_ms,2000)/1000
    try:
        async with asyncio.timeout_at(deadline):
            async with acquire() as connection:
                if connection.closed or connection.row_factory is not tuple_row or connection.info.transaction_status!=TransactionStatus.IDLE:
                    raise ReadUnavailable('not_ready')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        await cursor.execute('SET LOCAL search_path=pg_catalog')
                        async def execute(statement,args=()):
                            remaining=deadline-loop.time()
                            if remaining<=0:raise TimeoutError
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",(str(max(1,int(remaining*1000))),))
                            await cursor.execute(statement,args)
                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        await execute('SELECT schema_revision FROM rkb_v4.recovery_state WHERE singleton')
                        if await cursor.fetchone() not in [(s,) for s in EDITORIAL_SCHEMAS]:raise ReadUnavailable('not_ready')
                        if request.view=='edit_context':
                            await execute('SELECT rkb_v4.read_story_edit_context_v1(%s,%s)',(UUID(request.story_id),UUID(request.actor.person_id)))
                        else:
                            await execute('SELECT rkb_v4.read_editorial_story_v1(%s,%s,%s,%s,%s,%s)',
                                (UUID(request.story_id),request.revision,request.view,request.after_revision,request.limit,
                                 UUID(request.after_variant_id) if request.after_variant_id else None))
                        row=await cursor.fetchone()
                        if row is None or row[0] is None:raise ReadUnavailable('context_gap')
                        result=EditorialReadResult(**_identity(request),status='ok',reason=None,record=row[0],
                            evidence=(story_item(row[0]),),observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
                        if len(result.model_dump_json().encode())>262144:raise ReadUnavailable('internal_error')
                        if loop.time()>=deadline:raise TimeoutError
                        return result
    except ReadUnavailable as exc:return editorial_failure(request,exc.reason,observed)
    except TimeoutError:return editorial_failure(request,'deadline_exceeded',observed)
    except psycopg.errors.UndefinedTable:return editorial_failure(request,'not_ready',observed)
    except (psycopg.Error,ValueError,TypeError,KeyError):return editorial_failure(request,'internal_error',observed)
