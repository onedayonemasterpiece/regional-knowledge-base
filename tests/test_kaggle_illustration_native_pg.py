"""Actual packet PG/reader-role/mirror composition; synthetic source and Telegram only."""
import base64
from copy import deepcopy
from contextlib import asynccontextmanager
from dataclasses import asdict
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace as N

from PIL import Image, ImageDraw
import pytest

from regional_knowledge.kaggle_illustration_native import NativeIllustrationService
from regional_knowledge.kaggle_owned_pg import configure_native_roles
from regional_knowledge.kaggle_passage_illustrations import read_passage_illustrations
from regional_knowledge.kaggle_public_illustration_native_read import NativeIllustrationReadUnavailable
from regional_knowledge.kaggle_read_binding import TargetReadBinding
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database, connect
from test_native_image_projection_20261022 import illustrated_batch
from test_source_activation_pg import image_pg_sql18, accepted, stage_command, DB, WRITER
from test_source_batch_pg import native, packet
from test_source_stage import SOURCE
from test_source_activation import PERSON

ROOT = Path(__file__).resolve().parents[1]


class Telegram:
    def __init__(self): self.sent=[];self.messages={};self.data={}
    async def send_file(self,chat,stream,**kwargs):
        data=stream.read();self.sent.append(data);number=len(self.sent)
        with Image.open(BytesIO(data)) as image: width,height=image.size
        media=N(id=number,sizes=[N(w=width,h=height)])
        message=N(id=number,chat_id=chat,reply_to=N(reply_to_top_id=kwargs['reply_to']),photo=media)
        self.messages[number]=message;self.data[number]=data
        return message
    async def get_messages(self,chat,*,ids): return self.messages[ids]
    def iter_download(self,media,**kwargs):
        data=self.data[media.id]
        class Chunks:
            def __aiter__(self):
                async def parts():
                    for start in range(0,len(data),65536):yield data[start:start+65536]
                return parts()
            async def close(self): pass
        return Chunks()


@PG
@pytest.mark.asyncio
async def test_packet_real_reader_crop_mirror_repeat_and_withdrawal(image_pg_sql18):
    env=image_pg_sql18;connection=env['connection'];doc=json.loads(illustrated_batch())
    page=doc['model_payload']['pages'][0]
    page['regions'].extend([
        dict(region_key='other-figure',kind='figure',reading_order=3,
             bbox=dict(left=600,top=600,right=800,bottom=800),source_text=''),
        dict(region_key='other-caption',kind='caption',reading_order=4,
             bbox=dict(left=600,top=820,right=800,bottom=860),source_text='An unrelated illustration.')])
    other=deepcopy(page['illustrations'][0])
    other.update(illustration_key='other',source_region_key='other-figure',
        caption_region_keys=['other-caption'])
    page['illustrations'].append(other)
    chunk=deepcopy(doc['model_payload']['chunks'][0])
    chunk.update(chunk_key='zz-unrelated',
        region_refs=[dict(page_id=page['page_id'],region_key='other-caption')],
        illustration_refs=[dict(page_id=page['page_id'],illustration_key='other')])
    doc['model_payload']['chunks'].append(chunk)
    prepare,_=native(env)
    assert (await accepted(env,stage_command(doc,1))).status=='applied'
    for part in ('text','semantic'):
        command=packet(env,doc,part);await prepare(command)
        assert (await accepted(env,command)).status=='applied'
    async with connection.transaction():
        await connection.execute('CREATE ROLE rkb_bootstrap NOLOGIN')
        await configure_native_roles(connection,selected_sql=sorted(
            path.name for path in (ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql')),
            dbname=connection.info.dbname,create_roles=False)
        figure=(await (await connection.execute("SELECT illustration_id FROM rkb_v4.source_illustrations WHERE data->>'illustration_key'='picture'")).fetchone())[0]
        passage=(await (await connection.execute('SELECT passage_id FROM rkb_v4.passage_search ORDER BY ordinal LIMIT 1')).fetchone())[0]
    @asynccontextmanager
    async def role(name):
        conn=await connect()
        try:
            async with conn.transaction():await conn.execute('SET ROLE '+name)
            yield conn
        finally:await conn.close()
    image=Image.new('RGB',(1000,1000),'blue')
    ImageDraw.Draw(image).rectangle((110,115,319,364),fill='red')
    encoded=BytesIO();image.save(encoded,format='JPEG',quality=95)
    async def parser(*args,**kwargs):return dict(image_b64=base64.b64encode(encoded.getvalue()).decode())
    telegram=Telegram()
    intake=N(identity=('fixture-run','nonce','fixture-inc'),generation=1,closed=False,
        current=lambda binding:True,database=N(database_generation=DB,writer_generation=WRITER),
        telegram_downloader=N(_client=telegram,_chat_id=-100234,_current=lambda:True),
        _parse_selected=parser)
    service=NativeIllustrationService(intake,acquire=lambda:role('rkb_reader'),
        writer_acquire=lambda:role('rkb_writer'))
    binding=TargetReadBinding('fixture-run','fixture-inc',1,DB,env['sequence'],'fixture:1')
    record=dict(spec=dict(source_id=SOURCE),verified={'synthetic':True},
        parser_sha256='fixture',path='fixture.pdf',sha256='a'*64)
    args=dict(handle='retained',original_sha256='a'*64,source_id=SOURCE,
        illustration_id=str(figure),actor_id=PERSON,binding=asdict(binding),pixels=True,nonce='first',recovery=None)
    try:
        async with role('rkb_reader') as conn:
            async with conn.transaction():
                async with conn.cursor() as cursor:
                    descriptors=await read_passage_illustrations(cursor,cursor.execute,
                        source_id=SOURCE,passage_id=str(passage))
        assert len(descriptors)==1 and descriptors[0]['illustration_id']==str(figure)
        assert descriptors[0]['physical_page_index']==0
        # The other accepted figure shares the page, but neither passage nor
        # caption/figure evidence; it must not become this passage's descriptor.
        result=await service(dict(arguments=args),record)
        assert result['snapshot']['archive_state']['status']=='ready'
        assert len(telegram.sent)==1
        with Image.open(BytesIO(base64.b64decode(result['image_b64']))) as crop:
            assert crop.size==(250,210)
            red,green,blue=crop.getpixel((125,105))
            assert red>220 and green<40 and blue<40
        assert (await service(dict(arguments=dict(args,nonce='second')),record))['mime_type']=='image/jpeg'
        assert len(telegram.sent)==1  # Re-read the registered locator, never upload again.
        # A Dataset can predate the mirror. Recover the registered provider
        # receipt with current native presence, without rerendering/reuploading.
        async with connection.transaction():
            await connection.execute('DELETE FROM rkb_v4.source_illustration_availability')
        resumed=await service(dict(arguments=dict(args,nonce='restored',
            recovery=result['snapshot']['archive_state'])),record)
        assert resumed['snapshot']['archive_state']['status']=='ready'
        assert len(telegram.sent)==1
        async with connection.transaction():
            await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        with pytest.raises(NativeIllustrationReadUnavailable):
            await service(dict(arguments=dict(args,nonce='withdrawn')),record)
        assert len(telegram.sent)==1
    finally:
        async with connection.transaction():
            await connection.execute('DROP OWNED BY rkb_bootstrap')
            await connection.execute('DROP ROLE rkb_bootstrap')
