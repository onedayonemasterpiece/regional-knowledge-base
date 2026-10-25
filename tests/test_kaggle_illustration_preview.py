"""Direct preview through real auth, admission, source issuer and MCP middleware."""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import base64
from io import BytesIO
import json
import sqlite3
import time
import random
from types import SimpleNamespace as N

from PIL import Image,ImageDraw
import pytest

from regional_knowledge.kaggle_illustration_preview import configure_illustration_preview
from regional_knowledge.kaggle_current_registry import current_registry
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_protocol import EvidenceItem,ItemDependencies,SourceDependency
from regional_knowledge.kaggle_source_read import SourceReadResult
from regional_knowledge.server import build_server
from test_kaggle_public_media_wiring import call_http
from test_kaggle_public_illustration_gate import SOURCE,ILLUSTRATION,PAGE,REGION
from test_kaggle_service_startup import service
from test_kaggle_public_backend import target,work,release
from test_read_authority import auth
from test_participants import people
from test_control_spool import handler,withdraw as withdraw_command


@pytest.mark.asyncio
@pytest.mark.parametrize('slow',[False,True])
@pytest.mark.parametrize('noisy',[False,True])
async def test_direct_preview_mcp_crops_only_figure_and_rechecks_source(service,monkeypatch,slow,noisy):
    s=service;s.adapter.available=True
    s.startup.backend.read_telemetry=s.target.telemetry
    product=[]
    monkeypatch.setattr('regional_knowledge.product_telemetry.emit_product_event',product.append)
    withdrawal=handler()
    s.config.spool.registry=HandlerRegistry([
        *[h for h in current_registry().handlers.values() if h.route!=withdrawal.route],withdrawal])
    image=Image.new('RGB',(1000,1000),'blue')
    ImageDraw.Draw(image).rectangle((100,200,299,499),fill='red')
    if noisy:image=Image.frombytes('RGB',(900,900),random.Random(173).randbytes(900*900*3))
    content=BytesIO();image.save(content,format='JPEG',quality=20 if noisy else 75)
    elapsed=[0]
    s.startup.backend.source_gate.clock=lambda:time.monotonic()+elapsed[0]
    async def illustration_page(figure,binding):
        assert figure['source_id']==SOURCE and figure['physical_page_index']==0
        if slow:elapsed[0]=31
        return content.getvalue()
    configure_illustration_preview(N(config=N(import_originals=N(illustration_page=illustration_page)),
        backend=s.startup.backend))
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    server=build_server(target_startup=s.startup)
    async with s.startup.lifespan(None):
        pending=asyncio.create_task(call_http(N(startup=s.startup,server=server)))
        dispatched=await work(s.target)
        assert dispatched.query.operation=='source_illustration'
        figure=dict(source_id=SOURCE,illustration_id=ILLUSTRATION,page_id=PAGE,
            ingestion_id='internal-import-identity',
            source_region_id=REGION,physical_page_index=0,caption_text='Accepted caption',
            bbox=dict(left=100,top=200,right=300,bottom=500),display_rotation_degrees=90)
        if noisy:figure.update(bbox=dict(left=0,top=0,right=1000,bottom=1000),display_rotation_degrees=0)
        result=SourceReadResult(request_id=dispatched.query.request_id,
            connection_generation=s.target.generation,turn_generation=dispatched.query.turn_generation,
            operation='source_illustration',status='ok',records=(figure,),
            evidence=(EvidenceItem(id=ILLUSTRATION,kind='source',text='Accepted caption',
                dependencies=ItemDependencies(source_ids=(SOURCE,),variant_refs=(),grant_refs=(),
                    source_revisions=(SourceDependency(source_id=SOURCE,content_revision=1,eligibility_revision=1),))),),
            observed_data_seq=dispatched.query.min_data_seq,observed_projection_seq=dispatched.query.min_data_seq,
            observed_visibility_epoch=0,observed_database_generation='synthetic-db')
        assert await s.target.admission.finish_read(s.target.run,s.target.token,s.target.generation,result)
        data,wire=await pending
        await release(s.target,dispatched)
    await asyncio.to_thread(s.target.telemetry._queue.join)
    terminal=[r for r in s.target.records if r.get('event')=='read_attempt_terminal']
    assert len(terminal)==1
    assert (terminal[0]['status'],terminal[0]['reason'])==('ok',None)
    projected=[r for r in product if r.get('event')=='read.terminal']
    assert len(projected)==1 and projected[0]['status']==terminal[0]['status']
    result=data['result'];assert result['isError'] is False
    assert json.loads(result['content'][0]['text'])['metadata']['physical_page_index']==0
    assert result['content'][1]['mimeType']=='image/jpeg'
    with Image.open(BytesIO(base64.b64decode(result['content'][1]['data']))) as crop:
        assert crop.size==((900,900) if noisy else (300,200))
        if not noisy:
            red,green,blue=crop.getpixel((150,100))
            assert red>220 and green<40 and blue<40
    from regional_knowledge.read_issuance import MAX_RESPONSE_BYTES
    assert len(wire)<=MAX_RESPONSE_BYTES
    assert b'crop_binding' not in wire
    assert b'internal-import-identity' not in wire


@pytest.mark.asyncio
async def test_preview_uses_exact_accepted_ingestion_when_source_has_stale_receipt(tmp_path):
    from regional_knowledge.original_intake_controller import OriginalIntakeController
    from test_source_stage import original
    receipt=original();stale=replace(receipt,ingestion_id='90000000-0000-0000-0000-000000000009')
    path=tmp_path/'originals.sqlite'
    @contextmanager
    def connect(**kwargs):
        db=sqlite3.connect(path);db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()
    with connect() as db:
        db.execute('''CREATE TABLE rkb_original_intakes(ingestion_id TEXT,actor TEXT,
            spec_json TEXT,receipt_json TEXT,storage_sha256 TEXT,acquisition_json TEXT)''')
        for item,handle in ((stale,'stale'),(receipt,'current')):
            db.execute('INSERT INTO rkb_original_intakes VALUES(?,?,?,?,?,?)',(
                item.ingestion_id,'import-owner',json.dumps(dict(source_id=item.source_id)),
                json.dumps(item.wire()),'store',json.dumps(dict(handle=handle))))
    owner=object.__new__(OriginalIntakeController)
    owner.queue=N(connect=connect);owner._storage='store';owner.binding_current=lambda binding:True
    async def render(binding,record,index,cursor,*,restore):
        assert record['handle']=='current' and index==0
        return record,{},b'only-internal-page-bytes'
    owner._page_data=render
    assert await owner.illustration_page(dict(source_id=receipt.source_id,
        ingestion_id=receipt.ingestion_id,physical_page_index=0),object())==b'only-internal-page-bytes'


@pytest.mark.asyncio
async def test_noisy_jpeg_preview_fits_existing_mcp_response_budget():
    from mcp.types import CallToolResult,TextContent,ImageContent
    from regional_knowledge.read_issuance import MAX_RESPONSE_BYTES
    from regional_knowledge.kaggle_protocol import QUERY_MAX_BYTES
    image=Image.frombytes('RGB',(900,900),random.Random(173).randbytes(900*900*3))
    page=BytesIO();image.save(page,format='JPEG',quality=20)
    assert len(page.getvalue())<256*1024  # Valid existing book_pages preview.
    async def illustration_page(figure,binding):return page.getvalue()
    backend=N();configure_illustration_preview(N(config=N(import_originals=N(
        illustration_page=illustration_page)),backend=backend))
    figure=dict(bbox=dict(left=0,top=0,right=1000,bottom=1000))
    def frame(pixels):
        result=CallToolResult(content=[TextContent(type='text',text=json.dumps(
            dict(status='ready',metadata=figure))),ImageContent(type='image',mimeType='image/jpeg',
            data=base64.b64encode(pixels).decode())])
        return json.dumps(dict(jsonrpc='2.0',id=1,result=result.model_dump(
            mode='json',exclude_none=True)),ensure_ascii=False,separators=(',',':')).encode()
    recoded=BytesIO()
    with Image.open(BytesIO(page.getvalue())) as decoded:decoded.save(recoded,format='JPEG',quality=90)
    assert len(frame(recoded.getvalue()))>MAX_RESPONSE_BYTES  # Original regression.
    budget=((MAX_RESPONSE_BYTES-len(frame(b''))-QUERY_MAX_BYTES)//4)*3
    pixels=await backend.illustration_renderer(figure,object(),max_bytes=budget)
    assert len(frame(pixels))<=MAX_RESPONSE_BYTES
    with Image.open(BytesIO(pixels)) as preview:
        assert preview.format=='JPEG' and preview.size==(900,900)
