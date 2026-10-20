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
