"""Fixed native catalogue read using the existing ephemeral read channel."""
from __future__ import annotations

import asyncio
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
    operation: Literal['source_catalog', 'accepted_chunks', 'source_illustration', 'source_units'] = 'source_catalog'
    operation_version: Literal[1] = 1
    query: Annotated[str, Field(min_length=1, max_length=1000)] | None = None
    source_id: Identifier | None = None
    page_start: Annotated[int, Field(strict=True, ge=0, le=2499)] | None = None
    page_end: Annotated[int, Field(strict=True, ge=0, le=2499)] | None = None
    after_ordinal: Annotated[int, Field(strict=True, ge=0)] | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=8)] = 8
    actor: TrustedActorContext
    min_data_seq: Nonnegative
    deadline_ms: Annotated[int, Field(gt=0)]
    compute_claim: Identifier | None = None

    @model_validator(mode='after')
    def bounded_query(self):
        if self.operation=='source_illustration':
            if (self.query is None or self.source_id is None
                    or any(x is not None for x in (self.page_start,self.page_end,self.after_ordinal))):
                raise ValueError('exact accepted illustration selector required')
            if any(str(UUID(x))!=x for x in (self.source_id,self.query)):
                raise ValueError('canonical illustration UUIDs required')
            return self
        if self.operation == 'source_units':
            if (self.query is not None or self.source_id is None
                    or self.page_start is not None or self.page_end is not None):
                raise ValueError('unit read needs one exact accepted source ID')
            try:
                if str(UUID(self.source_id)) != self.source_id:
                    raise ValueError
            except (TypeError, ValueError, AttributeError):
                raise ValueError('canonical unit source ID required') from None
            return self
        if self.operation == 'source_catalog':
            if (self.query is None or not self.query.strip() or len(self.query.encode()) > 4096
                    or any(value is not None for value in
                           (self.source_id, self.page_start, self.page_end, self.after_ordinal))):
                raise ValueError('bounded nonempty catalogue query required')
        elif (self.query is not None or self.source_id is None
                or self.page_start is None or self.page_end is None
                or self.page_end < self.page_start or self.page_end - self.page_start > 3):
            raise ValueError('bounded accepted source page range required')
        else:
            try:
                if str(UUID(self.source_id)) != self.source_id:
                    raise ValueError('canonical accepted source UUID required')
            except (TypeError, ValueError, AttributeError):
                raise ValueError('canonical accepted source UUID required') from None
        return self


class SourceReadResult(ReadIdentity):
    type: Literal['SOURCE_READ_RESULT'] = 'SOURCE_READ_RESULT'
    protocol_version: Literal[2] = 2
    operation: Literal['source_catalog', 'accepted_chunks', 'source_illustration', 'source_units'] = 'source_catalog'
    status: Literal['ok','unavailable','invalid_query','denied','cancelled']
    records: tuple[dict,...] = Field(default=(), max_length=8)
    evidence: tuple[EvidenceItem,...] = Field(default=(), max_length=8)
    next_after_ordinal: Annotated[int, Field(strict=True, ge=0)] | None = None
    observed_data_seq: Nonnegative | None
    observed_projection_seq: Nonnegative | None
    observed_visibility_epoch: Nonnegative | None = None
    observed_database_generation: Identifier | None = None
    reason: Literal['projection_lag','deadline_exceeded','not_ready','disconnected','caller_cancelled',
        'turn_replaced','query_too_large','invalid_input','access_denied','prior_write_pending',
        'evidence_withdrawn','internal_error'] | None = None
    prior_write_pending: bool = False


async def _accepted_chunks(request, cursor, execute):
    """One bounded source-scoped accepted projection read; never stage/private originals."""
    await execute('''SELECT p.passage_id,p.ordinal,p.body,prov.data,v.eligibility_revision
        FROM rkb_v4.passage_search p
        JOIN rkb_v4.source_passage_provenance prov ON prov.passage_id=p.passage_id
            AND prov.source_id=p.source_id AND prov.source_revision=p.revision
        JOIN rkb_v4.sources s ON s.source_id=p.source_id
        JOIN rkb_v4.active_revision active ON active.source_id=p.source_id
            AND active.revision=p.revision
        JOIN rkb_v4.source_visibility v ON v.source_id=p.source_id
        WHERE p.source_id=%s::uuid AND p.revision=1 AND s.accepted AND NOT s.withdrawn
          AND (%s::bigint IS NULL OR p.ordinal>%s::bigint)
          AND EXISTS (SELECT 1 FROM rkb_v4.source_stage_pages pg
              WHERE pg.source_id=p.source_id
                AND pg.physical_page_index BETWEEN %s AND %s
                AND prov.data->'page_ids' @> jsonb_build_array(pg.page_id::text))
        ORDER BY p.ordinal,p.passage_id LIMIT %s''',
        (request.source_id, request.after_ordinal, request.after_ordinal,
         request.page_start, request.page_end, request.limit+1))
    fetched = await cursor.fetchall()
    has_more = len(fetched) > request.limit
    rows = fetched[:request.limit]
    if not rows:
        return (), (), None
    # Accepted manifest shape and span offsets were validated at activation.
    page_ids = {page for _, _, _, provenance, _ in rows
                for page in provenance['page_ids']}
    region_ids = {region for _, _, _, provenance, _ in rows
                  for region in provenance['region_ids'] + provenance['footnote_region_ids']}
    # The production rkb_reader role can SELECT only these three page
    # identity columns; model_page/image_receipt are intentionally private.
    await execute('''SELECT page_id,physical_page_index
        FROM rkb_v4.source_stage_pages
        WHERE source_id=%s::uuid AND page_id=ANY(%s::uuid[])''',
        (request.source_id, sorted(page_ids)))
    pages = {str(pid):dict(page_id=str(pid),physical_page_index=physical)
             for pid, physical in await cursor.fetchall()}
    await execute('''SELECT region_id,page_id,source_text
        FROM rkb_v4.source_regions WHERE source_id=%s::uuid AND source_revision=1
            AND region_id=ANY(%s::uuid[])''', (request.source_id, sorted(region_ids)))
    regions = {str(rid):(str(pid),len(source_text))
               for rid, pid, source_text in await cursor.fetchall()}
    if pages.keys() != page_ids or regions.keys() != region_ids:
        raise ReadUnavailable('evidence_withdrawn')
    records, evidence = [], []
    for passage_id, ordinal, body, provenance, eligibility in rows:
        selected_pages = [pages[page] for page in provenance['page_ids']]
        spans = provenance['source_spans']
        if spans:
            refs = [(span['region_id'],span['start'],span['end']) for span in spans]
        else:
            refs = [(rid,0,regions[rid][1]) for rid in provenance['region_ids']]
        refs.extend((rid,0,regions[rid][1]) for rid in provenance['footnote_region_ids'])
        selected_regions = [dict(page_id=regions[rid][0],region_id=rid,
            start=start,end=end,
            role='footnote' if rid in provenance['footnote_region_ids'] else 'body')
            for rid, start, end in refs]
        chunk = str(passage_id)
        record = dict(source_id=request.source_id,chunk_id=chunk,ordinal=ordinal,
            text=body,pages=selected_pages,regions=selected_regions)
        dependency = SourceDependency(source_id=request.source_id,content_revision=1,
                                      eligibility_revision=eligibility)
        item = EvidenceItem(id=chunk,kind='source',text=body,
            metadata=EvidenceMetadata(locator='source:'+request.source_id+':chunk:'+chunk),
            dependencies=ItemDependencies(source_ids=(request.source_id,),
                variant_refs=(),grant_refs=(),source_revisions=(dependency,)))
        records.append(record)
        evidence.append(item)
    next_ordinal = records[-1]['ordinal'] if has_more else None
    return tuple(records), tuple(evidence), next_ordinal


async def _source_units(request, cursor, execute):
    """Only text-published original page projections, never unaccepted stage."""
    from .source_units import resolve_source_units, bounded_unit_records
    await execute('''SELECT physical_page_index,page,page_count,read_complete
        FROM rkb_v4.read_source_unit_pages_v1(%s::uuid)
        ORDER BY physical_page_index''', (request.source_id,))
    rows = await cursor.fetchall()
    if not rows:
        return (), (), None
    if len(rows) > 2500 or len({row[2] for row in rows}) != 1 or len({row[3] for row in rows}) != 1:
        raise ReadUnavailable('internal_error')
    # Validate the complete accepted boundary chain, but only materialize
    # spans for this requested page plus one lookahead unit for next_cursor.
    units = resolve_source_units([row[1] for row in rows],source_id=request.source_id,
        page_count=rows[0][2],read_complete=rows[0][3],
        after_ordinal=request.after_ordinal,limit=request.limit + 1,
        span_preview_limit=16)
    records, next_ordinal = bounded_unit_records(
        units,after_ordinal=request.after_ordinal,limit=request.limit)
    return records, (), next_ordinal


async def execute_source_read(request, *, acquire):
    if type(request) is not SourceReadRequest:
        raise TypeError('fixed native source read required')
    loop = asyncio.get_running_loop()
    deadline = loop.time() + min(request.deadline_ms, 2000)/1000
    observed = {}

    def failed(reason):
        return SourceReadResult(**_identity(request),operation=request.operation,
            status='unavailable',reason=reason,
            observed_data_seq=observed.get('data'),observed_projection_seq=observed.get('projection'),
            observed_visibility_epoch=observed.get('visibility'),
            observed_database_generation=observed.get('database'))

    try:
        async with asyncio.timeout_at(deadline):
            async with acquire() as connection:
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                        or connection.row_factory is not tuple_row):
                    raise ReadUnavailable('internal_error')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')

                        async def execute(statement, params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1,int((deadline-loop.time())*1000))),))
                            await cursor.execute(statement,params)

                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        if request.operation=='source_illustration':
                            from .kaggle_passage_illustrations import read_source_illustration
                            records,evidence,next_ordinal=await read_source_illustration(request,cursor,execute)
                        elif request.operation == 'accepted_chunks':
                            records, evidence, next_ordinal = await _accepted_chunks(request,cursor,execute)
                        elif request.operation == 'source_units':
                            records, evidence, next_ordinal = await _source_units(request,cursor,execute)
                        else:
                            await execute('SELECT * FROM rkb_v4.find_source_catalog_v1(%s,%s)',
                                (request.query,request.limit))
                            rows = await cursor.fetchall()
                            if len(rows) > request.limit:
                                raise ReadUnavailable('internal_error')
                            records, evidence = [], []
                            for source, revision, eligibility, metadata in rows:
                                source = str(source)
                                record = dict(document_id=source,title=metadata['title'],
                                    authors=metadata['authors'],publication_year=metadata['publication_year'],
                                    active_revision=revision,source_format='pdf',
                                    source_archive_status='verified')
                                item = EvidenceItem(id=source,kind='source',text=record['title'],
                                    metadata=EvidenceMetadata(title=record['title'],locator='source:'+source),
                                    dependencies=ItemDependencies(source_ids=(source,),variant_refs=(),
                                        grant_refs=(),source_revisions=(SourceDependency(
                                            source_id=source,content_revision=revision,
                                            eligibility_revision=eligibility),)))
                                records.append(record)
                                evidence.append(item)
                            next_ordinal = None
                        result = SourceReadResult(**_identity(request),operation=request.operation,
                            status='ok',records=tuple(records),evidence=tuple(evidence),
                            next_after_ordinal=next_ordinal,
                            observed_data_seq=observed['data'],
                            observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],
                            observed_database_generation=observed['database'])
                if loop.time() >= deadline:
                    raise TimeoutError
                return result
    except ReadUnavailable as error:
        return failed(error.reason)
    except TimeoutError:
        return failed('deadline_exceeded')
    except asyncio.CancelledError:
        raise
    except psycopg.errors.QueryCanceled as error:
        return failed('deadline_exceeded' if loop.time()+.001 >= deadline and
            error.diag.message_primary == 'canceling statement due to statement timeout'
            else 'internal_error')
    except (psycopg.Error,ValueError,TypeError,KeyError,IndexError):
        return failed('internal_error')
