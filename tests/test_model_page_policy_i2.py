"""I2: one dense synthetic spread crosses the existing page-image boundaries."""
import base64
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from uuid import UUID,uuid5
from io import BytesIO
import random
import sqlite3
from types import SimpleNamespace

import pytest

from regional_knowledge import kaggle_djvu_pixels as djvu
from regional_knowledge.backend import RenderedPage, RenderedPageBatch
from regional_knowledge.kaggle_djvu_inspect import DjvuInventory, DjvuPage
from regional_knowledge.kaggle_importer import KaggleSourceImporter
from regional_knowledge.kaggle_original_parser import (
    MODEL_PAGE_BYTE_LIMIT, MODEL_PAGE_QUALITIES, parse,
)
from regional_knowledge.original_intake import (
    OriginalIntakeError, VerifiedOriginalReceipt, canonical, digest,
)
from regional_knowledge.original_intake_controller import OriginalIntakeController
from test_kaggle_original_intake import INGEST, REQUEST, SOURCE, frame, native


@pytest.mark.asyncio
async def test_dense_spread_pdf_djvu_native_controller_book_pages_and_immutable_receipt(
        native, tmp_path, monkeypatch):
    fitz = pytest.importorskip("pymupdf")
    texture = fitz.Pixmap(fitz.csRGB, 60, 42,
                          random.Random(1).randbytes(60 * 42 * 3), False)
    path = tmp_path / "i2-synthetic-spread.pdf"
    with fitz.open() as document:
        page = document.new_page(width=4680, height=3310)
        page.insert_image(page.rect, pixmap=texture)
        next_page = document.new_page(width=4680, height=3310)
        next_page.insert_image(next_page.rect, pixmap=texture)
        document.save(path)

    # Explicit pre-change comparator: the 256 KiB pilot forced small pixels.
    with fitz.open(path) as document:
        source = document[0]
        for old_edge, old_quality in ((1600, 82), (1600, 70), (1600, 55),
                                      (1280, 70), (1280, 55)):
            old_scale = min(2., old_edge / 4680)
            old_pixmap = source.get_pixmap(
                matrix=fitz.Matrix(old_scale, old_scale), alpha=False)
            old_image = old_pixmap.tobytes("jpeg", jpg_quality=old_quality)
            if len(old_image) <= 256 * 1024:
                break
        else:
            pytest.fail("synthetic pilot comparator did not fit 256 KiB")
    assert max(old_pixmap.width, old_pixmap.height) <= 1600

    pdf = parse(path, "page", 0)
    pdf_image = base64.b64decode(pdf["image_b64"], validate=True)
    render = pdf["preview_render"]
    assert (3000 <= max(render["width"], render["height"]) <= 3200
            and render["jpeg_quality"] in MODEL_PAGE_QUALITIES
            and render["jpeg_quality"] >= 80)
    assert 256 * 1024 < len(pdf_image) == render["image_bytes"] <= MODEL_PAGE_BYTE_LIMIT
    assert render["byte_limit"] == MODEL_PAGE_BYTE_LIMIT
    assert pdf["native_text_info"]["preview_render"] == render
    assert fitz.Pixmap(pdf_image).n == 3  # no grayscale conversion

    # Synthetic DjVu decoder boundary: exercise PPM validation and real JPEG
    # encoding; only the pinned executable process is replaced by a fixture.
    import regional_knowledge.kaggle_djvu_pixels as pixels
    djvu_path = tmp_path / "i2-synthetic-spread.djvu"
    djvu_path.write_bytes(b"AT&TFORM" + b"0" * 32)
    length = djvu_path.stat().st_size
    inventory = DjvuInventory(1, 0, 0, length,
                              (DjvuPage(0, 4680, 3310, length, 0),))
    monkeypatch.setattr(pixels, "PRIVATE_ROOT", tmp_path)
    monkeypatch.setattr(pixels, "_verified_decoder",
                        lambda bundle: ("decoder", "loader", "lib", {}))

    async def decoded_ppm(binary, loader, library_path, environment, original, index, edge):
        with fitz.open(path) as document:
            pix = document[0].get_pixmap(matrix=fitz.Matrix(edge / 4680, edge / 4680),
                                        alpha=False)
        return f"P6\n{pix.width} {pix.height}\n255\n".encode() + pix.samples

    monkeypatch.setattr(pixels, "_decode_single", decoded_ppm)
    page = await djvu.render_verified_djvu_page(
        bundle=SimpleNamespace(current=lambda: True),
        original_path=djvu_path, original_bytes=length, inventory=inventory, index=0)
    djvu_image = base64.b64decode(page["image_b64"], validate=True)
    meta = page["preview_render"]
    assert 3000 <= max(meta["width"], meta["height"]) <= 3200
    assert meta["jpeg_quality"] >= 80
    assert 256 * 1024 < len(djvu_image) == meta["image_bytes"] <= MODEL_PAGE_BYTE_LIMIT
    assert fitz.Pixmap(djvu_image).n == 3

    # A real native one-page dispatch persists the JPEG once and reuses it for
    # a distinct operation; no rerender, new key, or changed receipt.
    n = native.handler
    original_sha = digest(path.read_bytes())
    handle = "i2-spread"
    n._records[handle] = dict(path=path, sha256=original_sha, size=path.stat().st_size,
                              format="pdf", page_count=2, pages={},
                              verified={"synthetic": True}, parser_sha256=n.parser_sha256,
                              spec={"source_id": SOURCE, "ingestion_id": INGEST})
    parser_calls = []

    async def selected_parser(selected, action, index=0, *, cursor=None):
        parser_calls.append(action)
        assert selected == path and action == "page" and index == 0
        return dict(pdf)

    n.parser = selected_parser
    args = dict(handle=handle, original_sha256=original_sha, index=0)
    first = (await n(frame("original_pages", args, key="i2-first")))["value"]
    second = (await n(frame("original_pages", args, key="i2-second")))["value"]
    assert first == second and parser_calls == ["page"]
    assert first["image_size"] == len(pdf_image)
    assert n._records[handle]["pages"][0]["path"].read_bytes() == pdf_image

    controller = object.__new__(OriginalIntakeController)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE rkb_original_image_issuance (
        ingestion_id TEXT NOT NULL, physical_page_index INTEGER NOT NULL,
        receipt_json TEXT NOT NULL, PRIMARY KEY(ingestion_id, physical_page_index))""")
    @contextmanager
    def connect(write=True):
        yield db
        if write:
            db.commit()
    controller.queue = SimpleNamespace(connect=connect)
    controller.spool = SimpleNamespace(journal=SimpleNamespace(stream="fixture"))
    async def head(key, size, version=None):
        assert size == len(pdf_image)
        return "etag"
    controller._head = head
    archived = await controller._archive(
        SimpleNamespace(), dict(handle=handle, sha256=original_sha, size=path.stat().st_size,
                                spec={"source_id": SOURCE, "ingestion_id": INGEST}), page=first)
    assert archived["native_version"] == "etag"
    controller.store = SimpleNamespace(bucket="fixture", client=SimpleNamespace(
        get_object=lambda **kw: dict(ContentLength=len(pdf_image), Body=BytesIO(pdf_image))))
    async def phase(binding, name, phase_args, private=None):
        return first
    controller._phase = phase
    binding = SimpleNamespace(run_id="fixture")
    _, returned_page, returned_data = await controller._page_data(
        binding, dict(handle=handle, sha256=original_sha, size=path.stat().st_size,
                      spec={"source_id": SOURCE, "ingestion_id": INGEST}), 0,
        dict(block=0, offset=0, text=0), restore=lambda prior: None)
    assert returned_page == first and returned_data == pdf_image

    receipt = VerifiedOriginalReceipt(
        SOURCE, INGEST, str(uuid5(UUID(INGEST), 'original:source-original:'+INGEST)), 1, original_sha, path.stat().st_size,
        "originals/i2/fixture", "etag", "pdf", 2, n.parser_sha256,
        "a" * 64, "b" * 64)
    issued = controller._record_issued_image(receipt, first)
    assert controller._record_issued_image(receipt, second) == issued
    with pytest.raises(OriginalIntakeError, match="original_issued_image_changed"):
        controller._record_issued_image(receipt, dict(first, image_sha256="f" * 64))

    importer = object.__new__(KaggleSourceImporter)
    @asynccontextmanager
    async def access(principal):
        yield SimpleNamespace(authority=SimpleNamespace(person_id="fixture"), context="fixture", binding=object())
    importer._access = access
    importer._row = lambda person_id, ingestion_id: dict(ingestion_id=INGEST, source_id=SOURCE)
    async def verified(access_value, row):
        return receipt
    async def check(authority, context, binding):return None
    importer._check = check
    importer.parser_sha256 = receipt.parser_sha256
    async def model_pages(context, ingestion_id, cursor=None, batch_size=1):
        assert batch_size == 1
        return RenderedPageBatch((RenderedPage(
            first["page_id"], 0, None, "image/jpeg", pdf_image,
            first["native_text"], tuple(first["native_blocks"]),
            first["native_text_info"]),), None)
    importer.originals = SimpleNamespace(pages=model_pages, verified=verified)
    async def reply(access_value, result, *, render=None):
        return result
    importer._reply = reply
    batch = await KaggleSourceImporter._pages(
        importer, principal="fixture", ingestion_id=INGEST, cursor=None)
    assert batch.pages[0].data == pdf_image
    print(f"I2_IMAGE_MEASURE pilot={old_pixmap.width}x{old_pixmap.height},"
          f"q{old_quality},{len(old_image)}B; PDF={render['width']}x{render['height']},"
          f"q{render['jpeg_quality']},{len(pdf_image)}B; "
          f"DjVu={meta['width']}x{meta['height']},q{meta['jpeg_quality']},"
          f"{len(djvu_image)}B")

    # Legacy DjVu pin: genuine importer _receipt and a previously unissued
    # physical page, using the already pinned native page path (no stub _receipt).
    from regional_knowledge.kaggle_original_native import NativeOriginalIntake,OriginalIntakeConfig
    from mcp.server.mcpserver.exceptions import ToolError
    import regional_knowledge.kaggle_original_native as native_module
    monkeypatch.setattr(native_module.importlib.metadata,'version',lambda name:'1.28.2')
    previous_root=tmp_path/'previous-native'
    previous_root.mkdir(mode=0o700)
    restored=NativeOriginalIntake(
        config=OriginalIntakeConfig(('https://archive.invalid',),(('pymupdf','1.28.2'),)),
        database=n.database,identity=n.identity,workspace=previous_root,current=n.current,
        downloader=n.downloader,parser=n.parser,transfer=n.transfer,
        native_bundle=SimpleNamespace(current=lambda:True))
    restored.attached(1)
    old_pin=restored.previous_parser_sha256
    assert old_pin=='adca0115e43da61713a048f8a816c1b40a885925320b916b68f3daaec0aa961b'
    assert restored._parser_script(old_pin).name=='kaggle_original_parser.py'
    previous_inventory=DjvuInventory(2,0,0,length,
        (DjvuPage(0,4680,3310,length,0),DjvuPage(1,4680,3310,length,0)))
    old_sha=digest(djvu_path.read_bytes())
    restored._records['prior-djvu']=dict(path=djvu_path,sha256=old_sha,size=length,format='djvu',
        page_count=2,inventory=previous_inventory,pages={},verified={'source':'fixture'},
        parser_sha256=old_pin,spec={'source_id':SOURCE,'ingestion_id':INGEST})
    page1=(await restored(frame('original_pages',
        dict(handle='prior-djvu',original_sha256=old_sha,index=1),key='i2-djvu-first-page1')))['value']
    assert page1['physical_page_index']==1 and page1['parser_sha256']==old_pin
    assert 256*1024<page1['image_size']<=MODEL_PAGE_BYTE_LIMIT
    old_receipt=replace(receipt,sha256=old_sha,size=length,format='djvu',
        page_count=2,parser_sha256=old_pin)
    # Index 1 has never been issued in this ingestion; real controller records it.
    assert controller._record_issued_image(old_receipt,page1)['image_sha256']==page1['image_sha256']
    async def prior_verified(context,ingestion_id):
        assert ingestion_id==INGEST
        return old_receipt
    async def prior_page(context,ingestion_id,cursor=None,batch_size=1):
        assert cursor=='1' and batch_size==1
        blob=restored._records['prior-djvu']['pages'][1]['path'].read_bytes()
        return RenderedPageBatch((RenderedPage(
            page1['page_id'],1,None,'image/jpeg',blob,
            page1['native_text'],tuple(page1['native_blocks']),page1['native_text_info']),),None)
    importer.originals=SimpleNamespace(verified=prior_verified,pages=prior_page)
    access_value=SimpleNamespace(authority=object(),context=object(),binding=object())
    assert await KaggleSourceImporter._receipt(importer,access_value,
        dict(ingestion_id=INGEST,source_id=SOURCE)) == old_receipt
    resumed=await KaggleSourceImporter._pages(importer,principal='fixture',
        ingestion_id=INGEST,cursor='1')
    assert resumed.pages[0].physical_page_index==1
    assert resumed.pages[0].data==restored._records['prior-djvu']['pages'][1]['path'].read_bytes()
    async def forged_verified(context,ingestion_id):
        return replace(old_receipt,parser_sha256='f'*64)
    importer.originals=SimpleNamespace(verified=forged_verified,pages=prior_page)
    with pytest.raises(ToolError,match='verified_small_original_required'):
        await KaggleSourceImporter._receipt(importer,access_value,dict(ingestion_id=INGEST,source_id=SOURCE))

    # Two-page same-ingestion continuation across a changed native handle: the
    # already issued first page must replay its exact archived JPEG and proof,
    # while never-issued page 2 must retain q85 under the upgraded policy.
    new_native=dict(first)
    new_native['preview_render']=dict(first['preview_render'],jpeg_quality=70)
    new_native['native_text_info']=dict(first['native_text_info'],
        preview_render=new_native['preview_render'])
    new_native.update(image_sha256=digest(b'changed-render'),image_size=len(b'changed-render'))
    new_native['page_receipt_sha256']=digest(canonical(
        {k:v for k,v in new_native.items() if k!='page_receipt_sha256'}))
    second_native=parse(path,'page',1)
    second_jpeg=base64.b64decode(second_native.pop('image_b64'),validate=True)
    second_native.update(page_id=str(uuid5(UUID(SOURCE),'page:1:1')),
        original_sha256=original_sha,parser_sha256=n.parser_sha256,
        image_size=len(second_jpeg),mime_type='image/jpeg')
    second_native['page_receipt_sha256']=digest(canonical(second_native))
    async def after_restart(binding_value,name,args,private=None):
        assert name=='original_pages' and args['handle']=='restored-new-handle'
        return new_native if args['index']==0 else second_native
    controller._phase=after_restart
    async def saved_head(key,size,version=None):
        assert key.endswith('/pages/0/'+digest(pdf_image)) and size is None
        return ('etag',len(pdf_image))
    controller._head=saved_head
    newly_archived=[]
    async def archive_new(binding_value,record,*,page=None):
        newly_archived.append(page['physical_page_index'])
        assert page['physical_page_index']==1
        return dict(key='originals/synthetic/new-page1',native_version='etag')
    controller._archive=archive_new
    def read_jpeg(**args):
        assert args.get('IfMatch')=='etag'
        blob=pdf_image if args['Key'].endswith('/pages/0/'+digest(pdf_image)) else second_jpeg
        return dict(ContentLength=len(blob),Body=BytesIO(blob))
    controller.store=SimpleNamespace(bucket='synthetic',client=SimpleNamespace(get_object=read_jpeg))
    rebound=dict(handle='restored-new-handle',sha256=original_sha,
        size=path.stat().st_size,spec={'source_id':SOURCE,'ingestion_id':INGEST})
    native_cursor=dict(block=0,offset=0,text=0)
    rebound_binding=SimpleNamespace(run_id='new-native-runtime')
    _,reissued_page,reissued_image=await controller._page_data(
        rebound_binding,rebound,0,native_cursor,restore=lambda old:None)
    assert reissued_image==pdf_image
    assert reissued_page['page_receipt_sha256']==first['page_receipt_sha256']
    assert reissued_page['preview_render']==first['preview_render']
    assert controller._record_issued_image(receipt,reissued_page)==issued
    # Same-size corrupt archive bytes may not masquerade as the saved image.
    def corrupted(**kwargs):
        return dict(ContentLength=len(pdf_image),Body=BytesIO(b'X'+pdf_image[1:]))
    controller.store.client.get_object=corrupted
    with pytest.raises(OriginalIntakeError,match='original_render_storage_invalid'):
        await controller._page_data(rebound_binding,rebound,0,native_cursor,restore=lambda old:None)
    controller.store.client.get_object=read_jpeg
    _,new_page,new_image=await controller._page_data(
        rebound_binding,rebound,1,native_cursor,restore=lambda old:None)
    assert new_image==second_jpeg and newly_archived==[1]
    assert new_page['preview_render']['jpeg_quality']==85
    assert 3000<=max(new_page['preview_render']['width'],new_page['preview_render']['height'])<=3200
    assert controller._record_issued_image(receipt,new_page)['image_sha256']==digest(second_jpeg)
