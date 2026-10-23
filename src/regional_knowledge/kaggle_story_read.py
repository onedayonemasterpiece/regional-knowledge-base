"""Fixed native Story read on the existing idle read-pool lease; no inference."""
from __future__ import annotations

import asyncio
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .kaggle_protocol import (StoryReadRequest,StoryReadResult,EvidenceItem,EvidenceMetadata,
    ItemDependencies,SourceDependency,encode_envelope)
from .kaggle_query import _frontier,_identity,ReadUnavailable


def story_item(record):
    ref=record['default_variant_ref']
    variant_ref=f"variant:{ref['variant_id']}:r{ref['revision']}"
    source=SourceDependency(**record['source'])
    return EvidenceItem(id=record['story_id'],kind='story',text=record['body'],
        metadata=EvidenceMetadata(title=record['title'],citation=record['bibliography'][:1024],
            locator=f"story:{record['story_id']}:r{record['revision']}"),
        dependencies=ItemDependencies(source_ids=(source.source_id,),variant_refs=(variant_ref,),
            grant_refs=(),source_revisions=(source,)),exact_variant_ref=variant_ref)


def story_failure(request,reason,observed,*,invalid=False):
    return StoryReadResult(**_identity(request),status='invalid_query' if invalid else 'unavailable',
        reason=reason,evidence=(),record=None,observed_data_seq=observed.get('data'),
        observed_projection_seq=observed.get('projection'),observed_visibility_epoch=observed.get('visibility'),
        observed_database_generation=observed.get('database'))


async def execute_story_read(request,*,acquire):
    if type(request) is not StoryReadRequest:raise TypeError('validated Story read required')
    observed={};loop=asyncio.get_running_loop()
    deadline=loop.time()+min(request.deadline_ms,2000)/1000
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
                        await execute('SELECT rkb_v4.read_story_v1(%s,%s,%s,%s,%s)',
                            (UUID(request.story_id),request.view,request.revision,request.after_revision,request.limit))
                        row=await cursor.fetchone()
                        if row is None or row[0] is None:raise ReadUnavailable('context_gap')
                        record=row[0]
                        result=StoryReadResult(**_identity(request),status='ok',reason=None,evidence=(story_item(record),),
                            record=record,observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
                        encode_envelope(result)
                        if loop.time()>=deadline:raise TimeoutError
                        return result
    except ReadUnavailable as exc:return story_failure(request,exc.reason,observed)
    except TimeoutError:return story_failure(request,'deadline_exceeded',observed)
    except psycopg.errors.UndefinedTable:return story_failure(request,'not_ready',observed)
    except psycopg.Error:return story_failure(request,'internal_error',observed)
