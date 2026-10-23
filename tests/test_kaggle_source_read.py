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


def get_request(**changes):
    return SourceReadRequest.model_validate(dict(dict(request_id='catalog-get',
        connection_generation=2,turn_generation=0,action='get',query='',
        document_id=SOURCE,limit=1,
        actor=dict(person_id='verified-person',audience='personal',auth_epoch=3,
            visibility_epoch=0,context_ref='trusted-context'),
        min_data_seq=9,deadline_ms=2000),**changes))


def _accepted_book_manifest(*, kind='book', source=SOURCE):
    import base64
    from regional_knowledge.contracts import StartMetadataInput
    from regional_knowledge.original_intake import VerifiedOriginalReceipt, canonical
    receipt=VerifiedOriginalReceipt(source_id=SOURCE,
        ingestion_id='10000000-0000-0000-0000-000000000001',
        request_id='10000000-0000-0000-0000-000000000002',
        revision=1,sha256='a'*64,size=1024,key='originals/synthetic.pdf',
        native_version='test',format='pdf',page_count=4,
        parser_sha256='b'*64,acquisition_sha256='c'*64,native_verification_sha256='d'*64)
    metadata=StartMetadataInput(title='Exact accepted book',authors=['Source author'],
        publication_year=1901,language='de',
        catalog=dict(kind=kind,publisher='Source publisher',editors=['Editor'],
            cover_page=0,cover_kind='title_page',identifiers=[dict(kind='ISBN',value='test-id')]))
    return dict(source_id=source,source_revision=1,
        metadata=metadata.model_dump(mode='json'),
        original_receipt_b64=base64.b64encode(canonical(receipt.wire())).decode())


class CatalogGetNative(Native):
    async def fetchall(self):
        return [(UUID(SOURCE),1,7,self.rows[0])] if self.rows else []


@pytest.mark.asyncio
async def test_catalog_get_exact_pg_read_reuses_native_source_request_and_fences():
    native=CatalogGetNative([_accepted_book_manifest()])
    output=await execute_source_read(get_request(),acquire=native.acquire)
    assert output.status=='ok' and len(output.records)==1
    item=output.records[0]
    assert (item['document_id'],item['kind'],item['title'],item['publication_year']) == (
        SOURCE,'book','Exact accepted book',1901)
    assert item['catalog']['cover_page']==0 and item['catalog']['cover_kind']=='title_page'
    assert item['catalog']['publisher']=='Source publisher'
    assert item['source_archive_status']=='accepted_receipt'
    assert output.evidence[0].dependencies.source_revisions[0].eligibility_revision==7
    assert any('read_source_manifest_v1' in sql for sql,_ in native.calls)
    assert not any('download' in sql or 'archive_client' in sql for sql,_ in native.calls)
    assert decode_envelope(encode_envelope(get_request()))==get_request()
    assert decode_envelope(encode_envelope(output))==output


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['missing','foreign','article','journal_issue'])
async def test_catalog_get_never_returns_missing_foreign_or_nonbook_record(case):
    manifest=[] if case=='missing' else [_accepted_book_manifest(
        source='10000000-0000-0000-0000-000000000003' if case=='foreign'
        else SOURCE,kind=case if case in ('article','journal_issue') else 'book')]
    native=CatalogGetNative(manifest)
    output=await execute_source_read(get_request(),acquire=native.acquire)
    assert output.status=='unavailable' and not output.records


@pytest.mark.parametrize('change',[dict(action='get',document_id=None,query='',limit=1),
    dict(action='get',document_id=SOURCE,query='title',limit=1),
    dict(action='get',document_id=SOURCE,query='',limit=2),
    dict(action='get',document_id='wrong',query='',limit=1),
    dict(action='find',document_id=SOURCE),
    dict(action='unknown')])
def test_catalog_get_and_find_selector_stay_strict(change):
    with pytest.raises(ValueError):
        request(**change)
