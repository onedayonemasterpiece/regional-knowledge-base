"""B4 native DjVu page bridge: exact original and independent render receipt.

All test images/archives are synthetic, real DjVuLibre and Kaggle not involved.
"""
import base64
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_original_native import NativeOriginalIntake
from regional_knowledge.original_intake import OriginalIntakeSpec, canonical, digest
from regional_knowledge.kaggle_djvu_inspect import inspect_djvu
from test_kaggle_djvu_inspect import fixture as djvu_fixture
from test_kaggle_native_bundle import fixture as native_fixture, prepare
from test_kaggle_original_intake import (
    SOURCE, INGEST, REQUEST, native, frame, ticket, spec,
)


@pytest.fixture
def synthetic_decoder_bundle(native_fixture):
    value=native_fixture()
    cap=prepare(value)
    assert cap.current()
    return cap


def _fake_result(original_path, index):
    from regional_knowledge.kaggle_djvu_inspect import inspect_djvu
    inv=inspect_djvu(original_path)
    p=inv.pages[index]
    rgb=bytes([200,0,0] if index==0 else [0,0,200])
    jpeg=b"\xff\xd8"+sha256(rgb).digest()+b"\xff\xd9"
    render=dict(policy="rkb.djvu-preview.v1",width=2,height=2,scale=.5,
                max_edge=1600,jpeg_quality=82,byte_limit=256*1024,
                preview_only=True)
    return dict(format="djvu",page_count=inv.page_count,physical_page_index=index,
                width=p.width,height=p.height,
                native_cursor=dict(block=0,offset=0,text=0),next_native=None,
                native_text="",native_blocks=[],
                native_text_info=dict(original_length=0,offset=0,end=0,truncated=False,
                    complete=True,native_text_sha256=digest(b""),
                    native_blocks_sha256=digest(canonical([])),
                    requires_visual_review=True,
                    encoding_notice="image_only_djvu_no_native_layer",
                    block_count=0,blocks_truncated=False,preview_render=render),
                preview_render=render,image_b64=base64.b64encode(jpeg).decode(),
                image_sha256=digest(jpeg))


def _djvu_native(native, *,decoder_bundle, monkeypatch):
    from regional_knowledge.kaggle_original_parser import parse
    from regional_knowledge.file_ingress import DownloadedFile
    from regional_knowledge import kaggle_original_native as module
    import regional_knowledge.kaggle_djvu_pixels as pixels
    data=djvu_fixture(2,dictionaries=1)
    async def download(url,folder,*,max_bytes):
        assert max_bytes>=len(data)
        path=folder/"private.djvu"
        path.write_bytes(data)
        return DownloadedFile(path,digest(data),len(data))
    async def parser(path,action,index=0,*,cursor=None):
        if action=="inspect":return parse(path,action,index,cursor=cursor)
        raise AssertionError("DjVu page must not call legacy PDF parser")
    native.handler=NativeOriginalIntake(config=native.handler.config,
        database=native.handler.database,identity=native.handler.identity,
        workspace=native.handler.root,current=native.handler.current,
        downloader=SimpleNamespace(download=download),parser=parser,
        transfer=native.transfer,decoder_bundle=decoder_bundle)
    native.handler.attached(1)
    monkeypatch.setattr(pixels,"PRIVATE_ROOT",native.handler.root.parent)
    return data


async def _archive_verified(n,data):
    original=OriginalIntakeSpec(SOURCE,INGEST,REQUEST,"djvu-file",
                   mime_type="application/vnd.djvu",max_bytes=len(data))
    request=frame("original_acquire",{"spec":original.wire()},
                  private={"download_url":"https://attachment.invalid/private"})
    record=(await n.handler(request))["value"]
    assert record["format"]=="djvu" and record["page_count"]==2
    assert record["sha256"]==digest(data)
    args=dict(handle=record["handle"],original_sha256=record["sha256"])
    upload=frame("original_archive",args)
    upload["original_transfer"]={"ticket":ticket(upload,record)}
    assert (await n.handler(upload))["value"]["native_version"]=="native-etag"
    verify=frame("original_verify",args)
    verify["original_transfer"]={"ticket":ticket(verify,record,"GET")}
    assert (await n.handler(verify))["value"]["sha256"]==record["sha256"]
    return record,args


@pytest.mark.asyncio
async def test_verified_djvu_original_page_uses_one_native_bundle_and_distinct_render_receipt(
        native, synthetic_decoder_bundle, monkeypatch):
    cap=synthetic_decoder_bundle
    raw=_djvu_native(native,decoder_bundle=cap,monkeypatch=monkeypatch)
    calls=[]
    async def render(*,bundle,original_path,original_bytes,inventory,index):
        calls.append(index)
        assert bundle is cap and original_bytes==len(raw)
        assert inventory.page_count==2 and inventory.byte_length==len(raw)
        assert digest(Path(original_path).read_bytes())==digest(raw)
        return _fake_result(original_path,index)
    import regional_knowledge.kaggle_djvu_pixels as pixels
    monkeypatch.setattr(pixels,"render_verified_djvu_page",render)
    receipt,args=await _archive_verified(native,raw)
    parser_pin=receipt["parser_sha256"]
    for i in (1,0,1):
        result=(await native.handler(frame("original_pages",dict(args,index=i))))["value"]
        assert result["format"]=="djvu"
        assert result["page_id"]==__import__("uuid").uuid5(
                    __import__("uuid").UUID(SOURCE),f"page:1:{i}").__str__()
        assert result["original_sha256"]==digest(raw)
        assert result["parser_sha256"]==parser_pin
        assert result["preview_render"]["decoder_manifest_sha256"]==cap.manifest_sha256
        assert len(result["preview_render"]["render_software_sha256"])==64
        assert result["preview_render"]["preview_only"]
        assert result["native_text"]=="" and not result["native_blocks"]
        assert result["native_text_info"]["complete"]
        assert result["page_receipt_sha256"]==digest(canonical({
            k:v for k,v in result.items() if k!="page_receipt_sha256"}))
        assert result["image_sha256"]==digest(
            b"\xff\xd8"+sha256(bytes([200,0,0] if i==0 else [0,0,200])).digest()+b"\xff\xd9")
    assert calls==[1,0]
    assert receipt["parser_sha256"]==parser_pin


@pytest.mark.asyncio
async def test_without_exact_verified_native_bundle_archive_still_works_but_pages_unavailable(native,monkeypatch):
    raw=_djvu_native(native,decoder_bundle=None,monkeypatch=monkeypatch)
    record,args=await _archive_verified(native,raw)
    reply=(await native.handler(frame("original_pages",dict(args,index=0))))["value"]
    assert reply=={"error":"original_djvu_decoder_required"}
    assert record["format"]=="djvu" and record["sha256"]==digest(raw)


def test_unverified_or_wrong_object_cannot_be_installed_as_native_decoder(native):
    with pytest.raises(Exception,match="djvu|verified|bundle"):
        NativeOriginalIntake(config=native.handler.config,
            database=native.handler.database,identity=native.handler.identity,
            workspace=native.handler.root,current=native.handler.current,
            decoder_bundle=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("spoof",["wrong_index","wrong_format","fake_native_text"])
async def test_djvu_renderer_may_not_relabel_physical_page_or_model_text(
        native,synthetic_decoder_bundle,monkeypatch,spoof):
    cap=synthetic_decoder_bundle
    raw=_djvu_native(native,decoder_bundle=cap,monkeypatch=monkeypatch)
    import regional_knowledge.kaggle_djvu_pixels as pixels
    async def corrupted(*,bundle,original_path,original_bytes,inventory,index):
        data=_fake_result(original_path,index)
        if spoof=="wrong_index":data["physical_page_index"]=0 if index==1 else 1
        elif spoof=="wrong_format":data["format"]="pdf"
        else:data["native_text"]="invented transcription"
        return data
    monkeypatch.setattr(pixels,"render_verified_djvu_page",corrupted)
    record,args=await _archive_verified(native,raw)
    result=(await native.handler(frame("original_pages",dict(args,index=1))))["value"]
    assert result=={"error":"original_djvu_render_provenance_invalid"}
    assert native.handler._records[record["handle"]]["pages"]=={}
