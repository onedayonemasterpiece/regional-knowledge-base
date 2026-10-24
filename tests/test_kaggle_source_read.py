"""Catalogue lookup remains an authorized bounded native read, not a corpus scan."""
import asyncio
from uuid import UUID

import pytest

from regional_knowledge.kaggle_protocol import decode_envelope,encode_envelope
from regional_knowledge.kaggle_source_read import SourceReadRequest,SourceReadResult,execute_source_read
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
async def test_native_row_limit_and_record_dependency_corruption_refuse_output():
    native=Native([row(),row()]);result=await execute_source_read(request(limit=1),acquire=native.acquire)
    assert result.status=='unavailable' and not result.records
    native.rows=[row()];result=await execute_source_read(request(),acquire=native.acquire)
    value=result.model_dump(mode='json');value['records'][0]['document_id']='other-source'
    with pytest.raises(ValueError):SourceReadResult.model_validate_json(__import__('json').dumps(value))


# Accepted chunk review is a second *operation* on SOURCE_READ, never the
# old stage/original API. All examples use synthetic already accepted rows.
PAGE_A = '40000000-0000-0000-0000-000000000001'
PAGE_B = '40000000-0000-0000-0000-000000000002'
REGION_A = '50000000-0000-0000-0000-000000000001'
REGION_B = '50000000-0000-0000-0000-000000000002'
CHUNK_A = '60000000-0000-0000-0000-000000000001'
CHUNK_B = '60000000-0000-0000-0000-000000000002'


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
    def __init__(self, *, corrupt=False, frontier=FRONTIER, accepted=True):
        super().__init__((),frontier=frontier)
        self.corrupt=corrupt
        self.accepted=accepted
        self.rows=[
            (UUID(CHUNK_A),0,'Первый мыслительный фрагмент',provenance(
                CHUNK_A,[PAGE_A,PAGE_B],[REGION_A,REGION_B],[
                    dict(region_id=REGION_A,start=0,end=6,source_text_sha256='a'*64),
                    dict(region_id=REGION_B,start=2,end=8,source_text_sha256='b'*64)]),7),
            (UUID(CHUNK_B),1,'Второй фрагмент',provenance(
                CHUNK_B,[PAGE_B],[REGION_B],[
                    dict(region_id=REGION_B,start=8,end=16,source_text_sha256='b'*64)]),7),
        ]

    async def fetchall(self):
        if 'FROM rkb_v4.passage_search p' in self.statement:
            if not self.accepted:
                return []
            params=self.calls[-1][1]
            start=params[3];end=params[4];cursor=params[1];take=params[-1]
            entries=[row for row in self.rows
                     if (cursor is None or row[1]>cursor)
                     and any(start<=({PAGE_A:1,PAGE_B:2}[pid])<=end
                             for pid in row[3]['page_ids'])]
            return entries[:take]
        if 'FROM rkb_v4.source_stage_pages' in self.statement:
            requested=set(self.calls[-1][1][1])
            return [row for row in [(UUID(PAGE_A),1,'12'),(UUID(PAGE_B),2,'13')]
                    if str(row[0]) in requested]
        if 'FROM rkb_v4.source_regions' in self.statement:
            requested=set(self.calls[-1][1][1])
            return [row for row in [
                (UUID(REGION_A),UUID(PAGE_A),'Привет читателю','a'*64),
                (UUID(REGION_B),UUID(PAGE_B),'Книга продолжается','b'*64)]
                if str(row[0]) in requested]
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
        {'page_id':PAGE_A,'physical_page_index':1,'printed_page_number':'12'},
        {'page_id':PAGE_B,'physical_page_index':2,'printed_page_number':'13'}]
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
                   or 'embedding' in q for q,_ in native.calls)


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
async def test_chunk_read_frontier_and_corrupt_provenance_fail_closed():
    native=ChunkNative(frontier=(9,8,4,True,True,'native-db'))
    result=await execute_source_read(chunk_request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='projection_lag'
    assert not result.records
    native=ChunkNative()
    native.rows[0]=(*native.rows[0][:3],provenance(CHUNK_A,[PAGE_A],[REGION_A],[
        dict(region_id=REGION_A,start=0,end=999,source_text_sha256='a'*64)]),7)
    result=await execute_source_read(chunk_request(),acquire=native.acquire)
    assert result.status=='unavailable' and result.reason=='internal_error'
    assert not result.records and not result.evidence


def test_chunk_wire_rejects_fabricated_region_or_nonmonotonic_ordinals():
    import json
    from regional_knowledge.kaggle_source_read import SourceReadResult
    from regional_knowledge.kaggle_protocol import SourceDependency, EvidenceItem, ItemDependencies
    dependency=SourceDependency(source_id=SOURCE,content_revision=1,eligibility_revision=7)
    evidence=EvidenceItem(id=CHUNK_A,kind='source',text='Accepted text',
        dependencies=ItemDependencies(source_ids=(SOURCE,),variant_refs=(),grant_refs=(),
                                      source_revisions=(dependency,)))
    record=dict(source_id=SOURCE,chunk_id=CHUNK_A,ordinal=0,text='Accepted text',
        pages=[dict(page_id=PAGE_A,physical_page_index=1,printed_page_number=None)],
        regions=[dict(page_id=PAGE_A,region_id=REGION_A,start=0,end=6,role='body')])
    result=SourceReadResult(request_id='r',connection_generation=1,turn_generation=0,
        operation='accepted_chunks',status='ok',records=(record,),evidence=(evidence,),
        observed_data_seq=9,observed_projection_seq=9,observed_visibility_epoch=4,
        observed_database_generation='native-db')
    data=result.model_dump(mode='json')
    data['records'][0]['regions'][0]['page_id']=PAGE_B
    with pytest.raises(ValueError):
        SourceReadResult.model_validate_json(json.dumps(data))
    data=result.model_dump(mode='json')
    data['next_after_ordinal']=17
    with pytest.raises(ValueError):
        SourceReadResult.model_validate_json(json.dumps(data))
