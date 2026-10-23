"""Fixed native editorial reads and scoped edit context; no model or corpus scan."""
import asyncio
from uuid import UUID
import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from .kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
from .kaggle_story_read import story_item
from .kaggle_query import _frontier,_identity,ReadUnavailable


async def _accepted_story_passages(record, execute, cursor):
    """Return bounded source-owned passage/page refs from accepted Story evidence.

    This runs inside the existing authenticated native Story read transaction.
    The refs are *data*, not bearer grants. The API's current source gate
    rechecks eligibility before final output and ordinary fetch checks it again.
    """
    if record['view'] != 'get':
        return record
    source = record['source']
    await execute(
        """SELECT DISTINCT p.passage_id::text, page.page_id::text, page.physical_page_index
           FROM rkb_v4.story_assertion_versions AS assertion
           CROSS JOIN LATERAL jsonb_array_elements(assertion.data->'evidence') AS evidence(value)
           JOIN rkb_v4.passage_search AS p
             ON p.passage_id=(evidence.value->>'passage_id')::uuid
            AND p.source_id=assertion.source_id AND p.revision=assertion.source_revision
           JOIN rkb_v4.source_regions AS region
             ON region.region_id=(evidence.value->>'region_id')::uuid
            AND region.source_id=assertion.source_id AND region.source_revision=assertion.source_revision
           JOIN rkb_v4.source_stage_pages AS page
             ON page.source_id=region.source_id AND page.page_id=region.page_id
           JOIN rkb_v4.sources AS original ON original.source_id=assertion.source_id
           JOIN rkb_v4.active_revision AS active
             ON active.source_id=assertion.source_id AND active.revision=assertion.source_revision
           JOIN rkb_v4.source_visibility AS visibility ON visibility.source_id=assertion.source_id
           WHERE assertion.assertion_id=ANY(%s::uuid[])
             AND assertion.source_id=%s::uuid AND assertion.source_revision=%s
             AND original.accepted AND NOT original.withdrawn
             AND visibility.eligibility_revision=%s
           ORDER BY p.passage_id::text, page.page_id::text, page.physical_page_index
           LIMIT 49""",
        (list(map(UUID, record['finding_ids'])), UUID(source['source_id']),
         source['content_revision'], source['eligibility_revision']))
    rows = await cursor.fetchall()
    # At most 12 findings with four accepted evidence spans each. A truncated
    # response is never presented as the complete set of story source refs.
    if len(rows) > 48:
        raise ReadUnavailable('context_gap')
    refs = []
    for passage_id, page_id, index in rows:
        if (type(passage_id) is not str or type(page_id) is not str
                or type(index) is not int or not 0 <= index <= 2499
                or str(UUID(passage_id)) != passage_id or str(UUID(page_id)) != page_id):
            raise ReadUnavailable('internal_error')
        refs.append(dict(passage_id=passage_id, page_id=page_id,
                         physical_page_index=index))
    return {**record, 'passage_refs': refs}




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
                        await execute('SELECT max(version) FROM rkb_v4.schema_migrations')
                        installed=await cursor.fetchone()
                        if not installed or type(installed[0]) is not int or installed[0]<9:
                            raise ReadUnavailable('not_ready')
                        if request.view=='edit_context':
                            await execute('SELECT rkb_v4.read_story_edit_context_v1(%s,%s)',(UUID(request.story_id),UUID(request.actor.person_id)))
                        else:
                            await execute('SELECT rkb_v4.read_editorial_story_v1(%s,%s,%s,%s,%s,%s)',
                                (UUID(request.story_id),request.revision,request.view,request.after_revision,request.limit,
                                 UUID(request.after_variant_id) if request.after_variant_id else None))
                        row=await cursor.fetchone()
                        if row is None or row[0] is None:raise ReadUnavailable('context_gap')
                        record = (await _accepted_story_passages(row[0], execute, cursor)
                                  if installed[0] >= 13 and request.view == 'get' else row[0])
                        result=EditorialReadResult(**_identity(request),status='ok',reason=None,record=record,
                            evidence=(story_item(record),),observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
                        if len(result.model_dump_json().encode())>262144:raise ReadUnavailable('internal_error')
                        if loop.time()>=deadline:raise TimeoutError
                        return result
    except ReadUnavailable as exc:return editorial_failure(request,exc.reason,observed)
    except TimeoutError:return editorial_failure(request,'deadline_exceeded',observed)
    except psycopg.errors.UndefinedTable:return editorial_failure(request,'not_ready',observed)
    except (psycopg.Error,ValueError,TypeError,KeyError,IndexError):return editorial_failure(request,'internal_error',observed)
