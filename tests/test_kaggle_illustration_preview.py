"""Direct preview through real auth, admission, source issuer and MCP middleware."""
import asyncio
import base64
from io import BytesIO
import json
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
@pytest.mark.parametrize('withdraw',[False,True])
async def test_direct_preview_mcp_crops_only_figure_and_rechecks_source(service,monkeypatch,withdraw):
    s=service;s.adapter.available=True
    withdrawal=handler()
    s.config.spool.registry=HandlerRegistry([
        *[h for h in current_registry().handlers.values() if h.route!=withdrawal.route],withdrawal])
    image=Image.new('RGB',(1000,1000),'blue')
    ImageDraw.Draw(image).rectangle((100,200,299,499),fill='red')
    content=BytesIO();image.save(content,format='JPEG')
    async def illustration_page(figure,binding):
        assert figure['source_id']==SOURCE and figure['physical_page_index']==0
        if withdraw:
            await s.config.spool.stage_source_withdraw(s.target.auth.p.admin,
                withdraw_command(s.target.auth.p.owner.subject),database_generation='synthetic-db')
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
            source_region_id=REGION,physical_page_index=0,caption_text='Accepted caption',
            bbox=dict(left=100,top=200,right=300,bottom=500),display_rotation_degrees=90)
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
    if withdraw:
        assert b'"type":"image"' not in wire
    else:
        result=data['result'];assert result['isError'] is False
        assert json.loads(result['content'][0]['text'])['metadata']['physical_page_index']==0
        assert result['content'][1]['mimeType']=='image/jpeg'
        with Image.open(BytesIO(base64.b64decode(result['content'][1]['data']))) as crop:
            assert crop.size==(300,200)
            red,green,blue=crop.getpixel((150,100))
            assert red>220 and green<40 and blue<40
        assert b'crop_binding' not in wire
