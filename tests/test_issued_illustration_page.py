"""Synthetic archived page bytes; no corpus, model, provider or live runtime."""
from contextlib import contextmanager
from io import BytesIO
import json
import sqlite3
from types import SimpleNamespace as N
from uuid import UUID, uuid5

from PIL import Image
import pytest

from regional_knowledge.original_intake import OriginalIntakeError, digest, issued_image_receipt
from regional_knowledge.original_intake_controller import OriginalIntakeController
from test_source_stage import original


def archived_owner(tmp_path, *, issued=True, corrupt=False, changed_binding=False):
    receipt=original(); path=tmp_path/'issued.sqlite'; calls=[]
    image=BytesIO(); Image.new('RGB',(20,30),'red').save(image,format='JPEG'); pixels=image.getvalue()
    prior=issued_image_receipt(receipt,dict(page_id=str(uuid5(UUID(receipt.source_id),'page:1:0')),
        physical_page_index=0,original_sha256=receipt.sha256,parser_sha256=receipt.parser_sha256,
        width=20.,height=30.,image_sha256=digest(pixels),mime_type='image/jpeg',
        preview_render=dict(width=20,height=30,preview_only=True)))
    @contextmanager
    def connect(**kwargs):
        assert kwargs.get('write') is False
        db=sqlite3.connect(path); db.row_factory=sqlite3.Row
        try:yield db
        finally:db.close()
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE rkb_original_intakes(ingestion_id TEXT,actor TEXT,spec_json TEXT,receipt_json TEXT,storage_sha256 TEXT,acquisition_json TEXT)')
        db.execute('CREATE TABLE rkb_original_image_issuance(ingestion_id TEXT,physical_page_index INTEGER,receipt_json TEXT)')
        db.execute('INSERT INTO rkb_original_intakes VALUES(?,?,?,?,?,?)',(receipt.ingestion_id,'owner',
            json.dumps(dict(source_id=receipt.source_id)),json.dumps(receipt.wire()),'store',
            json.dumps(dict(handle='evicted-native-original',sha256=receipt.sha256,spec=dict(source_id=receipt.source_id,ingestion_id=receipt.ingestion_id)))))
        if issued:db.execute('INSERT INTO rkb_original_image_issuance VALUES(?,?,?)',(receipt.ingestion_id,0,json.dumps(prior)))
    current=[True]; owner=object.__new__(OriginalIntakeController)
    owner.queue=N(connect=connect); owner._storage='store'; owner.binding_current=lambda binding:current[0]
    owner.spool=N(journal=N(stream='synthetic-stream'))
    key='originals/v1/'+digest(b'synthetic-stream')+'/'+receipt.source_id+'/'+receipt.sha256+'/pages/0/'+prior['image_sha256']
    def head_object(**kwargs):
        assert kwargs==dict(Bucket='synthetic',Key=key); calls.append('head')
        return dict(ETag='exact-version',ContentLength=len(pixels))
    def get_object(**kwargs):
        assert kwargs==dict(Bucket='synthetic',Key=key,IfMatch='exact-version'); calls.append('get')
        if changed_binding:current[0]=False
        return dict(ContentLength=len(pixels),Body=BytesIO(b'x'*len(pixels) if corrupt else pixels))
    owner.store=N(bucket='synthetic',client=N(head_object=head_object,get_object=get_object))
    async def unavailable(*args,**kwargs):
        calls.append('native-render'); raise OriginalIntakeError('original_retained_bytes_unavailable')
    async def forbidden_restore(*args,**kwargs):
        calls.append('restore'); raise AssertionError('issued archived page must not restore the whole original')
    owner._phase=unavailable; owner._restore_handle=forbidden_restore
    return owner,N(source_id=receipt.source_id,ingestion_id=receipt.ingestion_id),pixels,calls


@pytest.mark.asyncio
async def test_issued_illustration_uses_exact_archived_bytes_without_restoring_original(tmp_path):
    owner,figure,pixels,calls=archived_owner(tmp_path)
    assert await owner.illustration_page(dict(vars(figure),physical_page_index=0),object())==pixels
    assert calls==['head','get']


@pytest.mark.asyncio
@pytest.mark.parametrize('corrupt,changed_binding,reason',[
    (True,False,'original_render_storage_invalid'),
    (False,True,'original_current_binding_required'),
])
async def test_issued_illustration_keeps_byte_and_current_binding_fences(tmp_path,corrupt,changed_binding,reason):
    owner,figure,_,calls=archived_owner(tmp_path,corrupt=corrupt,changed_binding=changed_binding)
    with pytest.raises(OriginalIntakeError,match=reason):
        await owner.illustration_page(dict(vars(figure),physical_page_index=0),object())
    assert calls==['head','get']


@pytest.mark.asyncio
async def test_unissued_illustration_keeps_existing_native_page_route(tmp_path):
    owner,figure,_,calls=archived_owner(tmp_path,issued=False)
    async def page_data(binding,record,index,cursor,*,restore):
        calls.append('existing-page-route'); assert record['handle']=='evicted-native-original'
        return record,{},b'existing-route-bytes'
    owner._page_data=page_data
    assert await owner.illustration_page(dict(vars(figure),physical_page_index=0),object())==b'existing-route-bytes'
    assert calls==['existing-page-route']


@pytest.mark.asyncio
async def test_changed_issued_receipt_cannot_authorize_archived_pixels(tmp_path):
    owner,figure,_,calls=archived_owner(tmp_path)
    with sqlite3.connect(tmp_path/'issued.sqlite') as db:
        prior=json.loads(db.execute('SELECT receipt_json FROM rkb_original_image_issuance').fetchone()[0])
        prior['original_receipt_sha256']='f'*64
        db.execute('UPDATE rkb_original_image_issuance SET receipt_json=?',(json.dumps(prior),))
    with pytest.raises(OriginalIntakeError,match='original_source_image_receipt_changed'):
        await owner.illustration_page(dict(vars(figure),physical_page_index=0),object())
    assert calls==[]


@pytest.mark.asyncio
async def test_missing_issued_archive_remains_an_archive_error_without_original_restore(tmp_path):
    from test_recovery_s3 import BotoFailure
    owner,figure,_,calls=archived_owner(tmp_path)
    def missing(**kwargs):
        calls.append('head');raise BotoFailure('NoSuchKey',404)
    owner.store.client.head_object=missing
    with pytest.raises(OriginalIntakeError,match='original_render_storage_invalid'):
        await owner.illustration_page(dict(vars(figure),physical_page_index=0),object())
    assert calls==['head']
