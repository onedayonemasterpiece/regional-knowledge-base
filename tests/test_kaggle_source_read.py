"""Catalogue lookup remains an authorized bounded native read, not a corpus scan."""
import asyncio
from uuid import UUID

import pytest

from regional_knowledge.kaggle_protocol import decode_envelope,encode_envelope
from regional_knowledge.kaggle_source_read import SourceReadRequest,execute_source_read
from test_kaggle_fact_read import Native,SOURCE,FRONTIER


def request(**changes):
    return SourceReadRequest.model_validate(dict(dict(request_id='catalog-1',connection_generation=2,turn_generation=0,
        query='Exact title',limit=8,actor=dict(person_id='verified-person',audience='personal',auth_epoch=3,
            visibility_epoch=0,context_ref='trusted-context'),min_data_seq=9,deadline_ms=2000),**changes))


def row():
    return UUID(SOURCE),1,7,dict(title='Exact title',authors=['A; B','C, D'],publication_year=1910)


@pytest.mark.asyncio
async def test_fixed_catalogue_native_path_no_encoding_no_original_receipt_and_exact_wire():
    native=Native([row()]);result=await execute_source_read(request(),acquire=native.acquire)
    assert result.status=='ok' and result.records[0]['authors']==['A; B','C, D']
    assert result.evidence[0].dependencies.source_revisions[0].eligibility_revision==7
    assert decode_envelope(encode_envelope(request()))==request()
    assert decode_envelope(encode_envelope(result))==result
    assert any('find_source_catalog_v1' in sql for sql,_ in native.calls)
    assert not any('embedding' in sql or 'original_receipt' in sql for sql,_ in native.calls)


@pytest.mark.parametrize('changes',[dict(query=' '),dict(query='x'*1001),dict(limit=9),dict(limit=True)])
def test_reject_unbounded_or_empty_catalogue_query(changes):
    with pytest.raises(ValueError):request(**changes)


@pytest.mark.asyncio
async def test_frontier_lag_never_returns_stale_catalogue_rows():
    native=Native([row()],frontier=(9,8,4,True,True,'native-db'))
    result=await execute_source_read(request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='projection_lag' and not result.records


@pytest.mark.asyncio
async def test_native_row_limit_refuse_output():
    native=Native([row(),row()]);result=await execute_source_read(request(limit=1),acquire=native.acquire)
    assert result.status=='unavailable' and not result.records


# Accepted chunk review is a second *operation* on SOURCE_READ, never the
# old stage/original API. All examples use synthetic already accepted rows.
PAGE_A = '40000000-0000-0000-0000-000000000001'
PAGE_B = '40000000-0000-0000-0000-000000000002'
REGION_A = '50000000-0000-0000-0000-000000000001'
REGION_B = '50000000-0000-0000-0000-000000000002'
CHUNK_A = '60000000-0000-0000-0000-000000000001'
CHUNK_B = '60000000-0000-0000-0000-000000000002'
PAGE_ZERO = '40000000-0000-0000-0000-000000000003'
PAGE_THREE = '40000000-0000-0000-0000-000000000004'
REGION_PORTRAIT = '50000000-0000-0000-0000-000000000003'
REGION_CAPTION = '50000000-0000-0000-0000-000000000004'
REGION_THREE = '50000000-0000-0000-0000-000000000005'
CHUNK_ZERO = '60000000-0000-0000-0000-000000000003'
CHUNK_THREE = '60000000-0000-0000-0000-000000000004'
CAPTION_TEXT = 'Подпись к портрету'
THREE_TEXT = 'Последняя страница'


def chunk_request(**changes):
    values=dict(request_id='chunks-1',connection_generation=2,turn_generation=0,
        operation='accepted_chunks',source_id=SOURCE,page_start=1,page_end=2,
        actor=dict(person_id='verified-person',audience='personal',auth_epoch=3,
            visibility_epoch=0,context_ref='trusted-context'),
        min_data_seq=9,deadline_ms=2000)
    values.update(changes)
    return SourceReadRequest.model_validate(values)


def provenance(chunk, pages, regions, spans):
    return dict(passage_id=chunk,page_ids=pages,region_ids=regions,
        footnote_region_ids=[],source_spans=spans)


class ChunkNative(Native):
    def __init__(self, *, frontier=FRONTIER, accepted=True, portrait=False, missing_region=False):
        super().__init__((),frontier=frontier)
        self.accepted=accepted
        self.missing_region=missing_region
        self.rows=[
            (UUID(CHUNK_A),0,'Первый мыслительный фрагмент',provenance(
                CHUNK_A,[PAGE_A,PAGE_B],[REGION_A,REGION_B],[
                    dict(region_id=REGION_A,start=0,end=6,source_text_sha256='a'*64),
                    dict(region_id=REGION_B,start=2,end=8,source_text_sha256='b'*64)]),7),
            (UUID(CHUNK_B),1,'Второй фрагмент',provenance(
                CHUNK_B,[PAGE_B],[REGION_B],[
                    dict(region_id=REGION_B,start=8,end=16,source_text_sha256='b'*64)]),7),
        ]
        if portrait:
            before=self.rows
            # Accepted image page 0: its visual region has no transcription;
            # the caption supplies the whole passage text (no source spans).
            self.rows=[
                (UUID(CHUNK_ZERO),0,CAPTION_TEXT,provenance(
                    CHUNK_ZERO,[PAGE_ZERO],[REGION_PORTRAIT,REGION_CAPTION],[]),7),
                (before[0][0],1,*before[0][2:]),
                (before[1][0],2,*before[1][2:]),
                (UUID(CHUNK_THREE),3,THREE_TEXT,provenance(
                    CHUNK_THREE,[PAGE_THREE],[REGION_THREE],[]),7),
            ]

    async def fetchall(self):
        if 'FROM rkb_v4.passage_search p' in self.statement:
            if not self.accepted:
                return []
            params=self.calls[-1][1]
            start=params[3];end=params[4];cursor=params[1];take=params[-1]
            entries=[row for row in self.rows
                     if (cursor is None or row[1]>cursor)
                     and any(start<=({PAGE_ZERO:0,PAGE_A:1,PAGE_B:2,PAGE_THREE:3}[pid])<=end
                             for pid in row[3]['page_ids'])]
            return entries[:take]
        if 'FROM rkb_v4.source_stage_pages' in self.statement:
            requested=set(self.calls[-1][1][1])
            return [row for row in [(UUID(PAGE_ZERO),0),(UUID(PAGE_A),1),
                                   (UUID(PAGE_B),2),(UUID(PAGE_THREE),3)]
                    if str(row[0]) in requested]
        if 'FROM rkb_v4.source_regions' in self.statement:
            requested=set(self.calls[-1][1][1])
            return [row for row in [
                (UUID(REGION_A),UUID(PAGE_A),'Привет читателю'),
                (UUID(REGION_B),UUID(PAGE_B),'Книга продолжается'),
                (UUID(REGION_PORTRAIT),UUID(PAGE_ZERO),''),
                (UUID(REGION_CAPTION),UUID(PAGE_ZERO),CAPTION_TEXT),
                (UUID(REGION_THREE),UUID(PAGE_THREE),THREE_TEXT)]
                if str(row[0]) in requested
                and not (self.missing_region and str(row[0])==REGION_B)]
        return await super().fetchall()


@pytest.mark.asyncio
async def test_accepted_chunks_order_cross_page_unicode_offsets_and_exact_wire():
    native=ChunkNative()
    req=chunk_request()
    result=await execute_source_read(req,acquire=native.acquire)
    assert result.status=='ok',result.model_dump(mode='json')
    assert [r['chunk_id'] for r in result.records]==[CHUNK_A,CHUNK_B]
    assert [r['ordinal'] for r in result.records]==[0,1]
    assert result.records[0]['pages']==[
        {'page_id':PAGE_A,'physical_page_index':1},
        {'page_id':PAGE_B,'physical_page_index':2}]
    assert result.records[0]['regions']==[
        {'page_id':PAGE_A,'region_id':REGION_A,'start':0,'end':6,'role':'body'},
        {'page_id':PAGE_B,'region_id':REGION_B,'start':2,'end':8,'role':'body'}]
    assert result.records[0]['text']=='Первый мыслительный фрагмент'
    assert all(i.dependencies.source_revisions[0].eligibility_revision==7
               for i in result.evidence)
    assert result.next_after_ordinal is None
    assert decode_envelope(encode_envelope(req))==req
    assert decode_envelope(encode_envelope(result))==result
    assert any('ORDER BY p.ordinal' in query for query,_ in native.calls)
    assert any('REPEATABLE READ READ ONLY' in query for query,_ in native.calls)
    assert not any('original_receipt' in q or 'source_stage_batches' in q
                   or 'model_page' in q or 'embedding' in q for q,_ in native.calls)


@pytest.mark.asyncio
async def test_accepted_chunk_limit_lookahead_continuation_on_same_ordinal():
    native=ChunkNative()
    first=await execute_source_read(chunk_request(limit=1),acquire=native.acquire)
    assert first.status=='ok' and len(first.records)==1
    assert first.next_after_ordinal==0
    second=await execute_source_read(chunk_request(limit=1,after_ordinal=first.next_after_ordinal),
                                     acquire=native.acquire)
    assert second.status=='ok' and [r['chunk_id'] for r in second.records]==[CHUNK_B]
    assert second.next_after_ordinal is None


@pytest.mark.asyncio
async def test_chunk_pages_exclude_unmatched_pages_and_never_return_pending_material():
    native=ChunkNative()
    result=await execute_source_read(chunk_request(page_start=1,page_end=1),acquire=native.acquire)
    assert [r['chunk_id'] for r in result.records]==[CHUNK_A]
    native.accepted=False
    empty=await execute_source_read(chunk_request(),acquire=native.acquire)
    assert empty.status=='ok' and not empty.records and empty.next_after_ordinal is None


@pytest.mark.parametrize('kwargs', [
    {'query':'catalogue'}, {'source_id':'not-a-uuid'}, {'source_id':None},
    {'page_start':-1}, {'page_start':2,'page_end':1},
    {'page_start':0,'page_end':4}, {'page_end':2500},
    {'page_start':True}, {'limit':9}, {'after_ordinal':-1},
    {'page_start':None}, {'page_end':None},
])
def test_accepted_chunks_reject_unbounded_or_noncanonical_selector(kwargs):
    with pytest.raises(ValueError):
        chunk_request(**kwargs)


@pytest.mark.asyncio
async def test_accepted_portrait_caption_page_zero_range_singles_and_ordinal_cursor():
    native=ChunkNative(portrait=True)
    full=await execute_source_read(chunk_request(page_start=0,page_end=3),acquire=native.acquire)
    assert full.status=='ok',full.model_dump(mode='json')
    assert [row['chunk_id'] for row in full.records]==[CHUNK_ZERO,CHUNK_A,CHUNK_B,CHUNK_THREE]
    assert [row['ordinal'] for row in full.records]==[0,1,2,3]
    assert full.records[0]['pages']==[dict(page_id=PAGE_ZERO,physical_page_index=0)]
    assert full.records[0]['text']==CAPTION_TEXT
    assert full.records[0]['regions']==[
        dict(page_id=PAGE_ZERO,region_id=REGION_PORTRAIT,start=0,end=0,role='body'),
        dict(page_id=PAGE_ZERO,region_id=REGION_CAPTION,start=0,end=len(CAPTION_TEXT),role='body')]
    assert full.next_after_ordinal is None
    assert len(full.evidence)==4
    for physical,expected in enumerate([CHUNK_ZERO,CHUNK_A,CHUNK_B,CHUNK_THREE]):
        reader=ChunkNative(portrait=True)
        single=await execute_source_read(chunk_request(page_start=physical,page_end=physical),
                                         acquire=reader.acquire)
        assert single.status=='ok' and expected in [item['chunk_id'] for item in single.records]
    first=await execute_source_read(chunk_request(page_start=0,page_end=3,limit=2),
                                    acquire=native.acquire)
    assert [item['chunk_id'] for item in first.records]==[CHUNK_ZERO,CHUNK_A]
    assert first.next_after_ordinal==1
    second=await execute_source_read(chunk_request(page_start=0,page_end=3,limit=2,
        after_ordinal=first.next_after_ordinal),acquire=native.acquire)
    assert [item['chunk_id'] for item in second.records]==[CHUNK_B,CHUNK_THREE]
    assert second.next_after_ordinal is None
    assert not any('model_page' in sql or 'image_receipt' in sql
                   for sql,_ in native.calls)


@pytest.mark.asyncio
async def test_chunk_read_frontier_and_missing_accepted_region_fail_closed():
    native=ChunkNative(frontier=(9,8,4,True,True,'native-db'))
    result=await execute_source_read(chunk_request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='projection_lag'
    assert not result.records
    native=ChunkNative(missing_region=True)
    result=await execute_source_read(chunk_request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='evidence_withdrawn'
    assert not result.records and not result.evidence


