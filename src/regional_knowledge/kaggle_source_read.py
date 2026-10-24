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
    operation: Literal['source_catalog', 'accepted_chunks'] = 'source_catalog'
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
    operation: Literal['source_catalog', 'accepted_chunks'] = 'source_catalog'
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

    @model_validator(mode='after')
    def exact_records(self):
        if self.status == 'ok':
            if self.reason is not None or self.prior_write_pending or any(value is None for value in (
                    self.observed_data_seq, self.observed_projection_seq,
                    self.observed_visibility_epoch, self.observed_database_generation)):
                raise ValueError('exact native source frontier required')
        elif self.evidence or self.records or self.next_after_ordinal is not None:
            raise ValueError('failed native source read cannot carry data or continuation')
        if len(self.records) != len(self.evidence):
            raise ValueError('every source record requires exact dependencies')
        if self.operation == 'source_catalog':
            if self.next_after_ordinal is not None:
                raise ValueError('catalogue cannot return a chunk continuation')
            seen = set()
            for record, item in zip(self.records, self.evidence):
                if (set(record) != {'document_id','title','authors','publication_year',
                                   'active_revision','source_format','source_archive_status'}
                        or item.kind != 'source' or record['document_id'] != item.id or item.id in seen
                        or item.dependencies.source_ids != (item.id,)
                        or len(item.dependencies.source_revisions) != 1
                        or record['active_revision'] != item.dependencies.source_revisions[0].content_revision
                        or record['source_format'] != 'pdf' or record['source_archive_status'] != 'verified'
                        or type(record['title']) is not str or not 1 <= len(record['title']) <= 500
                        or item.text != record['title']
                        or type(record['authors']) is not list or len(record['authors']) > 50
                        or any(type(author) is not str or len(author) > 4000 for author in record['authors'])
                        or (record['publication_year'] is not None
                            and (type(record['publication_year']) is not int
                                 or not 1 <= record['publication_year'] <= 3000))):
                    raise ValueError('exact bounded catalogue record required')
                if str(UUID(item.id)) != item.id:
                    raise ValueError('canonical source UUID required')
                seen.add(item.id)
        else:
            previous = -1
            for record, item in zip(self.records, self.evidence):
                if (type(record) is not dict
                        or set(record) != {'source_id','chunk_id','ordinal','text','pages','regions'}
                        or type(record['ordinal']) is not int or record['ordinal'] <= previous
                        or record['chunk_id'] != item.id or item.kind != 'source'
                        or record['text'] != item.text or item.text_form != 'complete'
                        or item.dependencies.source_ids != (record['source_id'],)
                        or item.dependencies.grant_refs or item.dependencies.variant_refs
                        or len(item.dependencies.source_revisions) != 1
                        or item.dependencies.source_revisions[0].source_id != record['source_id']
                        or item.dependencies.source_revisions[0].content_revision != 1
                        or type(record['pages']) is not list or not 1 <= len(record['pages']) <= 20
                        or type(record['regions']) is not list or not 1 <= len(record['regions']) <= 150):
                    raise ValueError('exact ordered accepted chunk required')
                if (str(UUID(record['source_id'])) != record['source_id']
                        or str(UUID(item.id)) != item.id):
                    raise ValueError('canonical chunk/source UUID required')
                pages = set()
                for page in record['pages']:
                    if (type(page) is not dict
                            or set(page) != {'page_id','physical_page_index','printed_page_number'}
                            or str(UUID(page['page_id'])) != page['page_id']
                            or page['page_id'] in pages
                            or type(page['physical_page_index']) is not int
                            or not 0 <= page['physical_page_index'] <= 2499
                            or (page['printed_page_number'] is not None and
                                (type(page['printed_page_number']) is not str
                                 or len(page['printed_page_number']) > 80))):
                        raise ValueError('accepted chunk page binding required')
                    pages.add(page['page_id'])
                for region in record['regions']:
                    if (type(region) is not dict
                            or set(region) != {'page_id','region_id','start','end','role'}
                            or str(UUID(region['region_id'])) != region['region_id']
                            or region['page_id'] not in pages
                            or region['role'] not in ('body','footnote')
                            or type(region['start']) is not int or type(region['end']) is not int
                            or not 0 <= region['start'] < region['end'] <= 1000000):
                        raise ValueError('accepted region/Unicode offset binding required')
                previous = record['ordinal']
            if self.next_after_ordinal is not None and (
                    not self.records or self.next_after_ordinal != self.records[-1]['ordinal']):
                raise ValueError('chunk continuation must match last returned ordinal')
        if len(json.dumps(self.model_dump(mode='json'),ensure_ascii=False).encode()) > 128*1024:
            raise ValueError('bounded native source result required')
        return self


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
    if len(fetched) > request.limit + 1:
        raise ReadUnavailable('internal_error')
    has_more = len(fetched) > request.limit
    rows = fetched[:request.limit]
    if not rows:
        return (), (), None
    page_ids, region_ids = set(), set()
    for passage_id, ordinal, body, provenance, eligibility in rows:
        if (type(ordinal) is not int or ordinal < 0
                or type(body) is not str or not 1 <= len(body) <= 8000
                or type(eligibility) is not int or eligibility < 1
                or type(provenance) is not dict
                or set(provenance) != {'passage_id','region_ids','footnote_region_ids',
                                      'page_ids','source_spans'}
                or provenance['passage_id'] != str(passage_id)
                or type(provenance['page_ids']) is not list
                or not 1 <= len(provenance['page_ids']) <= 20
                or type(provenance['region_ids']) is not list
                or not 1 <= len(provenance['region_ids']) <= 100
                or type(provenance['footnote_region_ids']) is not list
                or len(provenance['footnote_region_ids']) > 50
                or type(provenance['source_spans']) is not list
                or len(provenance['source_spans']) > 150):
            raise ReadUnavailable('internal_error')
        page_ids.update(provenance['page_ids'])
        region_ids.update(provenance['region_ids'])
        region_ids.update(provenance['footnote_region_ids'])
    if len(page_ids) > 160 or len(region_ids) > 1200:
        raise ReadUnavailable('internal_error')
    await execute('''SELECT page_id,physical_page_index,model_page->>'printed_page_number'
        FROM rkb_v4.source_stage_pages
        WHERE source_id=%s::uuid AND page_id=ANY(%s::uuid[])''',
        (request.source_id, sorted(page_ids)))
    pages = {str(pid):dict(page_id=str(pid),physical_page_index=physical,
                           printed_page_number=printed)
             for pid, physical, printed in await cursor.fetchall()}
    await execute('''SELECT region_id,page_id,source_text,source_text_sha256
        FROM rkb_v4.source_regions WHERE source_id=%s::uuid AND source_revision=1
            AND region_id=ANY(%s::uuid[])''', (request.source_id, sorted(region_ids)))
    regions = {str(rid):(str(pid), source_text, text_hash)
               for rid, pid, source_text, text_hash in await cursor.fetchall()}
    if pages.keys() != page_ids or regions.keys() != region_ids:
        raise ReadUnavailable('evidence_withdrawn')
    records, evidence = [], []
    for passage_id, ordinal, body, provenance, eligibility in rows:
        selected_pages = [pages[page] for page in provenance['page_ids']]
        if len({page['page_id'] for page in selected_pages}) != len(selected_pages):
            raise ReadUnavailable('internal_error')
        refs = []
        spans = provenance['source_spans']
        if spans:
            for span in spans:
                if type(span) is not dict or set(span) != {
                        'region_id','start','end','source_text_sha256'}:
                    raise ReadUnavailable('internal_error')
                rid = span['region_id']
                if rid not in provenance['region_ids']:
                    raise ReadUnavailable('internal_error')
                refs.append((rid,span['start'],span['end'],span['source_text_sha256']))
            for rid in provenance['footnote_region_ids']:
                if rid not in [ref[0] for ref in refs]:
                    refs.append((rid,None,None,None))
        else:
            refs = [(rid,None,None,None) for rid in (
                provenance['region_ids']+provenance['footnote_region_ids'])]
        selected_regions = []
        for rid, start, end, expected_hash in refs:
            if rid not in regions:
                raise ReadUnavailable('internal_error')
            page_id, source_text, text_hash = regions[rid]
            if (page_id not in provenance['page_ids'] or type(source_text) is not str
                    or len(source_text) > 1000000 or type(text_hash) is not str
                    or expected_hash is not None and expected_hash != text_hash):
                raise ReadUnavailable('internal_error')
            if start is None and end is None:
                start, end = 0, len(source_text)
            if (type(start) is not int or type(end) is not int
                    or not 0 <= start < end <= len(source_text)):
                raise ReadUnavailable('internal_error')
            selected_regions.append(dict(page_id=page_id,region_id=rid,
                start=start,end=end,
                role='footnote' if rid in provenance['footnote_region_ids'] else 'body'))
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
                        if request.operation == 'accepted_chunks':
                            records, evidence, next_ordinal = await _accepted_chunks(request,cursor,execute)
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
