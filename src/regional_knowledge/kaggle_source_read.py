"""Fixed native catalogue read using the existing ephemeral read channel."""
from __future__ import annotations

import asyncio
import json
from typing import Annotated, Literal
from uuid import UUID

import psycopg
from pydantic import Field, model_validator
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .kaggle_protocol import ReadIdentity, TrustedActorContext, Nonnegative, Identifier
from .kaggle_protocol import EvidenceItem, EvidenceMetadata, ItemDependencies, SourceDependency
from .kaggle_query import ReadUnavailable, _frontier, _identity


class SourceReadRequest(ReadIdentity):
    type: Literal['SOURCE_READ'] = 'SOURCE_READ'
    protocol_version: Literal[2] = 2
    operation: Literal['source_catalog'] = 'source_catalog'
    operation_version: Literal[1] = 1
    query: Annotated[str, Field(min_length=1,max_length=1000)]
    limit: Annotated[int, Field(ge=1,le=8)] = 8
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int, Field(gt=0)]
    compute_claim: Identifier | None = None

    @model_validator(mode='after')
    def bounded_query(self):
        if not self.query.strip() or len(self.query.encode())>4096:
            raise ValueError('bounded nonempty catalogue query required')
        return self


class SourceReadResult(ReadIdentity):
    type: Literal['SOURCE_READ_RESULT'] = 'SOURCE_READ_RESULT'
    protocol_version: Literal[2] = 2
    status: Literal['ok','unavailable','invalid_query','denied','cancelled']
    records: tuple[dict,...] = Field(default=(),max_length=8)
    evidence: tuple[EvidenceItem,...] = Field(default=(),max_length=8)
    observed_data_seq: Nonnegative | None
    observed_projection_seq: Nonnegative | None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal['projection_lag','deadline_exceeded','not_ready','disconnected','caller_cancelled',
        'turn_replaced','query_too_large','invalid_input','access_denied','prior_write_pending',
        'evidence_withdrawn','internal_error'] | None = None
    prior_write_pending: bool = False

    @model_validator(mode='after')
    def exact_records(self):
        if self.status=='ok':
            if self.reason is not None or self.prior_write_pending or any(v is None for v in (
                self.observed_data_seq,self.observed_projection_seq,self.observed_visibility_epoch,self.observed_database_generation)):
                raise ValueError('exact native catalogue frontier required')
        elif self.evidence or self.records:
            raise ValueError('failed catalogue read cannot carry data')
        if len(self.records)!=len(self.evidence):
            raise ValueError('catalogue records require exact dependencies')
        seen=set()
        for record,item in zip(self.records,self.evidence):
            if (set(record)!={'document_id','title','authors','publication_year','active_revision','source_format','source_archive_status'}
                    or item.kind!='source' or record['document_id']!=item.id or item.id in seen
                    or item.dependencies.source_ids!=(item.id,) or len(item.dependencies.source_revisions)!=1
                    or record['active_revision']!=item.dependencies.source_revisions[0].content_revision
                    or record['source_format']!='pdf' or record['source_archive_status']!='verified'
                    or type(record['title']) is not str or not 1<=len(record['title'])<=500
                    or item.text!=record['title'] or type(record['authors']) is not list or len(record['authors'])>50
                    or any(type(a) is not str or len(a)>4000 for a in record['authors'])
                    or (record['publication_year'] is not None and (type(record['publication_year']) is not int
                        or not 1<=record['publication_year']<=3000))):
                raise ValueError('exact bounded catalogue record required')
            if str(UUID(item.id))!=item.id:raise ValueError('canonical source UUID required')
            seen.add(item.id)
        if len(json.dumps(self.model_dump(mode='json'),ensure_ascii=False).encode())>128*1024:
            raise ValueError('bounded catalogue result required')
        return self


async def execute_source_read(request,*,acquire):
    if type(request) is not SourceReadRequest:
        raise TypeError('fixed catalogue read required')
    loop=asyncio.get_running_loop();deadline=loop.time()+min(request.deadline_ms,2000)/1000;observed={}
    def failed(reason):
        return SourceReadResult(**_identity(request),status='unavailable',reason=reason,
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
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1,int((deadline-loop.time())*1000))),))
                            await cursor.execute(statement,params)
                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        await execute('SELECT * FROM rkb_v4.find_source_catalog_v1(%s,%s)',(request.query,request.limit))
                        rows=await cursor.fetchall()
                        if len(rows)>request.limit:raise ReadUnavailable('internal_error')
                        records=[];evidence=[]
                        for source,revision,eligibility,metadata in rows:
                            source=str(source)
                            record=dict(document_id=source,title=metadata['title'],authors=metadata['authors'],
                                publication_year=metadata['publication_year'],active_revision=revision,
                                source_format='pdf',source_archive_status='verified')
                            item=EvidenceItem(id=source,kind='source',text=record['title'],
                                metadata=EvidenceMetadata(title=record['title'],locator='source:'+source),
                                dependencies=ItemDependencies(source_ids=(source,),variant_refs=(),grant_refs=(),source_revisions=(SourceDependency(
                                    source_id=source,content_revision=revision,eligibility_revision=eligibility),)))
                            records.append(record);evidence.append(item)
                        result=SourceReadResult(**_identity(request),status='ok',records=tuple(records),evidence=tuple(evidence),
                            observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
                if loop.time()>=deadline:raise TimeoutError
                return result
    except ReadUnavailable as error:return failed(error.reason)
    except TimeoutError:return failed('deadline_exceeded')
    except asyncio.CancelledError:raise
    except psycopg.errors.QueryCanceled as error:
        return failed('deadline_exceeded' if loop.time()+.001>=deadline and
            error.diag.message_primary=='canceling statement due to statement timeout' else 'internal_error')
    except (psycopg.Error,ValueError,TypeError,KeyError):return failed('internal_error')
