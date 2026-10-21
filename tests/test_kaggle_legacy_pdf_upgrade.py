"""Read-only historical PDF parser/receipt upgrade tests; no live provider.

An old receipt must replay exactly with original parser bytes after eviction.
"""
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest

from regional_knowledge.original_intake import VerifiedOriginalReceipt, digest
from regional_knowledge.kaggle_importer import KaggleSourceImporter
from test_kaggle_original_intake import (
    SOURCE, INGEST, REQUEST, PDF, native, acquire, frame, ticket, spec,
)

LEGACY_SCRIPT_SHA256 = "f614e3acbfc32f1bf3ca339a11a8edbf08cd3c3de3d72b0d5fcba2db8b09e159"


def test_exact_preupgrade_parser_script_and_failclosed_dispatch(native):
    n=native.handler
    historical=Path(__file__).parents[1]/"src/regional_knowledge/kaggle_original_parser_pdf_v1.py"
    assert digest(historical.read_bytes())==LEGACY_SCRIPT_SHA256
    assert n.legacy_parser_sha256 != n.parser_sha256
    assert n._parser_script(n.legacy_parser_sha256).name==historical.name
    assert n._parser_script(n.parser_sha256).name=="kaggle_original_parser.py"
    with pytest.raises(ValueError):
        n._parser_script("b"*64)


@pytest.mark.asyncio
async def test_exact_old_pdf_receipt_restores_after_eviction_and_retains_archive_and_page_pin(native):
    n=native
    _, first=await acquire(n)
    a=dict(handle=first["handle"],original_sha256=first["sha256"])
    up=frame("original_archive",a)
    up["original_transfer"]={"ticket":ticket(up,first)}
    assert (await n.handler(up))["value"]["native_version"]=="native-etag"
    original=VerifiedOriginalReceipt(
        SOURCE,INGEST,REQUEST,1,digest(PDF),len(PDF),
        ticket(up,first)["key"],"native-etag","pdf",1,
        n.handler.legacy_parser_sha256,"4"*64,"5"*64)
    n.handler._records.clear()
    data=dict(spec=spec().wire(),restore_receipt=original.wire(),restore_nonce="a"*48)
    restore=frame("original_acquire",data)
    restore["original_transfer"]={"ticket":ticket(restore,first,"GET")}
    result=(await n.handler(restore))["value"]
    assert result["format"]=="pdf"
    assert result["parser_sha256"]==original.parser_sha256
    assert result["acquisition_sha256"]==original.acquisition_sha256
    assert result["restored_from_receipt_sha256"]==original.receipt_sha256
    identity=dict(handle=result["handle"],original_sha256=result["sha256"])
    ver=frame("original_verify",identity)
    ver["original_transfer"]={"ticket":ticket(ver,result,"GET")}
    readback=(await n.handler(ver))["value"]
    assert readback["parser_sha256"]==original.parser_sha256
    assert readback["acquisition_sha256"]==original.acquisition_sha256
    page=(await n.handler(frame("original_pages",dict(identity,index=0))))["value"]
    assert page["parser_sha256"]==original.parser_sha256
    assert page["physical_page_index"]==0
    assert n.parser[-2:]==["inspect","page"]


@pytest.mark.asyncio
async def test_unrecognized_preupgrade_pin_cannot_restore(native):
    n=native
    _, first=await acquire(n)
    a=dict(handle=first["handle"],original_sha256=first["sha256"])
    up=frame("original_archive",a)
    up["original_transfer"]={"ticket":ticket(up,first)}
    await n.handler(up)
    bad=VerifiedOriginalReceipt(SOURCE,INGEST,REQUEST,1,digest(PDF),len(PDF),
                ticket(up,first)["key"],"native-etag","pdf",1,"b"*64,"4"*64,"5"*64)
    n.handler._records.clear()
    data=dict(spec=spec().wire(),restore_receipt=bad.wire(),restore_nonce="b"*48)
    restore=frame("original_acquire",data)
    restore["original_transfer"]={"ticket":ticket(restore,first,"GET")}
    result=(await n.handler(restore))["value"]
    assert result["error"]=="original_restore_receipt_mismatch"


@pytest.mark.asyncio
async def test_importer_accepts_verified_historical_pdf_pin_without_relabeling(native):
    async def _check(authority,context,binding):return None
    old=native.handler.legacy_parser_sha256
    request_id=str(uuid5(UUID(INGEST),"original:source-original:"+INGEST))
    receipt=VerifiedOriginalReceipt(SOURCE,INGEST,request_id,1,digest(PDF),len(PDF),
                                     "originals/test","etag","pdf",1,old,"4"*64,"5"*64)
    class Owned:
        async def verified(self,context,ingestion_id):
            assert ingestion_id==INGEST
            return receipt
    importer=SimpleNamespace(originals=Owned(),_check=_check,
                             parser_sha256=native.handler.parser_sha256)
    access=SimpleNamespace(context=object(),authority=object(),binding=object())
    got=await KaggleSourceImporter._receipt(importer,access,{"ingestion_id":INGEST,"source_id":SOURCE})
    assert got is receipt and got.parser_sha256==old
