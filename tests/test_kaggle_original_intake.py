"""Real private file hashes/dispatch binding, synthetic parser/HTTP peers only."""
import asyncio
import base64
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID,uuid5

import pytest

from regional_knowledge.kaggle_original_native import NativeOriginalIntake,OriginalIntakeConfig
from regional_knowledge.native_snapshot_transfer import SnapshotTicket
from regional_knowledge.original_intake import OriginalIntakeSpec,OriginalIntakeError,canonical,digest
from test_kaggle_djvu_pixels import archive as djvu_archive, bundle as djvu_bundle, native_fixture

SOURCE='10000000-0000-0000-0000-000000000001'
INGEST='20000000-0000-0000-0000-000000000002'
REQUEST='30000000-0000-0000-0000-000000000003'
PDF=b'%PDF-1.7\nsynthetic bounded file fixture'
IMAGE=b'synthetic-rendered-derivative'


class Downloader:
    calls=0
    async def download(self,url,folder,*,max_bytes):
        assert url=='https://attachment.invalid/private' and max_bytes==len(PDF)
        self.calls+=1;path=folder/'original.pdf';path.write_bytes(PDF)
        return SimpleNamespace(path=path,sha256=digest(PDF),size_bytes=len(PDF))


class Transfer:
    def __init__(self):self.objects={};self.calls=[];self.lost=False
    async def upload(self,ticket,path,*,operation_key):
        self.calls.append('upload');data=path.read_bytes()
        assert len(data)==ticket.size and digest(data)==ticket.sha256
        self.objects[ticket.key]=data
        if self.lost:raise TimeoutError('private provider detail')
        return 'native-etag'
    async def download(self,ticket,path,*,operation_key):
        self.calls.append('download');data=self.objects[ticket.key]
        if digest(data)!=ticket.sha256:raise ValueError('corrupt')
        path.write_bytes(data);return path


@pytest.fixture
def native(tmp_path,monkeypatch):
    import sys
    import regional_knowledge.kaggle_original_native as module
    monkeypatch.setitem(sys.modules,'telethon.tl.types',SimpleNamespace(
        DocumentAttributeFilename=lambda file_name:SimpleNamespace(file_name=file_name)))
    root=tmp_path/'native';root.mkdir(mode=0o700)
    monkeypatch.setattr(module,'PRIVATE_ROOT',tmp_path)
    monkeypatch.setattr(module.importlib.metadata,'version',lambda name:'pinned-version')
    download=Downloader();transfer=Transfer();parser_calls=[]
    async def parser(path,action,index=0,*,cursor=None):
        assert path.read_bytes()==PDF;parser_calls.append(action)
        if action=='inspect':return dict(format='pdf',page_count=1)
        return dict(format='pdf',page_count=1,physical_page_index=index,width=100,height=200,
            preview_render=dict(width=100,height=200,scale=1,jpeg_quality=82,preview_only=True),
            image_b64=base64.b64encode(IMAGE).decode(),image_sha256=digest(IMAGE),native_text='actual fixture span',
            native_cursor=dict(block=0,offset=0,text=0),next_native=None,
            native_text_info=dict(original_length=len('actual fixture span'),offset=0,end=len('actual fixture span'),
                truncated=False,requires_visual_review=True,encoding_notice=None,block_count=0,blocks_truncated=False,
                complete=True,native_text_sha256=digest(b'actual fixture span'),native_blocks_sha256=digest(canonical([]))),native_blocks=[])
    handler=NativeOriginalIntake(config=OriginalIntakeConfig(('https://archive.invalid',),(('PyMuPDF','pinned-version'),)),
        database=SimpleNamespace(database_generation='db',writer_generation='writer'),identity=('run','nonce','inc'),
        workspace=root,current=lambda b:b==('run','inc',1),downloader=download,parser=parser,transfer=transfer)
    handler.attached(1)
    return SimpleNamespace(handler=handler,download=download,transfer=transfer,parser=parser_calls)


def frame(phase,args,*,key=None,private=None):
    value=dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job',claim='claim',kind='recovery_phase',
        run_id='run',launch_nonce='nonce',runtime_incarnation='inc',database_generation='db',writer_generation='writer',
        phase=phase,operation_key=key or digest(canonical([phase,args])),arguments=args,
        request_sha256=digest(canonical(dict(phase=phase,arguments=args))),compute_claim='actual-shared-slot',dispatch_generation=1)
    if private is not None:value['original_transfer']=private
    return value


def spec(**changes):return replace(OriginalIntakeSpec(SOURCE,INGEST,REQUEST,'file',max_bytes=len(PDF)),**changes)


def ticket(f,record,method='PUT',*,page=False):
    sha256,size=(digest(IMAGE),len(IMAGE)) if page else (record['sha256'],record['size'])
    headers=(('Content-Length',str(size)),('Content-Type','application/octet-stream'),('If-None-Match','*')) if method=='PUT' else (('If-Match','native-etag'),)
    return SnapshotTicket(method,digest(f['operation_key'].encode()),'originals/private/'+sha256,size,sha256,
        9999999999.,'https://archive.invalid/private',headers).wire()


async def acquire(n,s=None):
    f=frame('original_acquire',{'spec':(s or spec()).wire()},private={'download_url':'https://attachment.invalid/private'})
    return f,(await n.handler(f))['value']


@pytest.mark.asyncio
@pytest.mark.parametrize('index',[0,1])
@pytest.mark.parametrize('background',[False,True])
async def test_original_pages_dispatch_uses_pinned_djvu_pixels_and_retains_same_page(
        native,djvu_archive,djvu_bundle,monkeypatch,index,background):
    import regional_knowledge.kaggle_djvu_pixels as pixels
    path,inventory=djvu_archive
    encoded=[]
    def jpeg(rgb,width,height):
        encoded.append(rgb)
        return b'\xff\xd8'+rgb+b'\xff\xd9',82
    monkeypatch.setattr(pixels,'_jpeg',jpeg)
    n=native;n.handler.native_bundle=djvu_bundle
    if background:
        from regional_knowledge.kaggle_snapshot_io import SnapshotIo
        n.handler.snapshot_io=SnapshotIo(factory=SimpleNamespace(_current=lambda b:None,
            limits=SimpleNamespace(timeout_seconds=10)),tickets=None,transfer=None)
    async def resolved(request):
        value=(await n.handler(request))['value']
        if background:
            io=n.handler.snapshot_io
            assert value=={'status':'pending','handle':digest(request['operation_key'].encode())}
            assert (await n.handler(request))['value']==value
            await io.operations[value['handle']].task
            value=io.status(value['handle'],('run','inc',1))['value']
        return value
    source=path.read_bytes();handle='synthetic-djvu-handle'
    n.handler._records[handle]=dict(path=path,sha256=digest(source),size=len(source),
        format='djvu',page_count=inventory.page_count,pages={},verified={'retained':True},
        parser_sha256=n.handler.parser_sha256,spec=spec(max_bytes=len(source)).wire())
    args=dict(handle=handle,original_sha256=digest(source),index=index)
    request=frame('original_pages',args)
    result=await resolved(request)
    assert 'error' not in result
    assert result['format']=='djvu' and result['physical_page_index']==index
    assert result['page_id']==str(uuid5(UUID(SOURCE),f'page:1:{index}'))
    assert result['original_sha256']==digest(source)
    assert result['parser_sha256']==n.handler.parser_sha256
    assert result['native_text']=='' and result['native_blocks']==[]
    assert result['native_text_info']['complete'] and result['native_text_info']['requires_visual_review']
    assert result['next_native'] is None and n.parser==[]
    assert encoded==[bytes([240,0,0] if index==0 else [0,0,240])*4]
    assert await n.handler(request)==await n.handler(request)
    # A distinct read operation still reuses the retained exact page pixels.
    assert await resolved(frame('original_pages',args,key='read-again'))==result
    assert len(encoded)==1
    artifact=n.handler._records[handle]['pages'][index]
    assert artifact['sha256']==digest(artifact['path'].read_bytes())==result['image_sha256']
    bad=frame('original_pages',dict(args,cursor=dict(block=1,offset=0,text=0)))
    assert await resolved(bad)=={'error':'original_page_cursor_invalid'}
    if background:await n.handler.snapshot_io.aclose()


@pytest.mark.asyncio
async def test_three_djvu_pages_inspect_original_once(native,monkeypatch):
    import regional_knowledge.kaggle_djvu_inspect as inspector
    import regional_knowledge.kaggle_djvu_pixels as pixels
    from test_kaggle_djvu_inspect import fixture
    source=fixture(3,dictionaries=1);n=native
    path=n.handler.root/'three-pages.djvu';path.write_bytes(source)
    inspect=inspector.inspect_djvu;inspections=[];rendered=[]
    def counted_inspect(original):
        inspections.append(original)
        return inspect(original)
    async def render(*,bundle,original_path,original_bytes,inventory,index):
        assert original_path==path and original_bytes==len(source) and inventory.page_count==3
        rendered.append(index)
        return dict(format='djvu',page_count=3,physical_page_index=index,width=100,height=200,
            preview_render=dict(width=100,height=200,scale=1,jpeg_quality=82,preview_only=True),
            image_b64=base64.b64encode(IMAGE).decode(),image_sha256=digest(IMAGE),
            native_cursor=dict(block=0,offset=0,text=0),next_native=None,
            native_text='',native_blocks=[],native_text_info=dict(complete=True,requires_visual_review=True))
    monkeypatch.setattr(inspector,'inspect_djvu',counted_inspect)
    monkeypatch.setattr(pixels,'render_verified_djvu_page',render)
    handle='three-pages'
    n.handler._records[handle]=dict(path=path,sha256=digest(source),size=len(source),
        format='djvu',page_count=3,pages={},verified={'retained':True},
        parser_sha256=n.handler.parser_sha256,spec=spec(max_bytes=len(source)).wire())
    try:
        for index in range(3):
            result=(await n.handler(frame('original_pages',dict(
                handle=handle,original_sha256=digest(source),index=index))))['value']
            assert result['physical_page_index']==index
            assert result['page_id']==str(uuid5(UUID(SOURCE),f'page:1:{index}'))
        assert inspections==[path] and rendered==[0,1,2]
    finally:await n.handler.aclose()


@pytest.mark.asyncio
async def test_djvu_dispatch_without_verified_decoder_does_not_use_ambient_tools(native,djvu_archive):
    path,inventory=djvu_archive;n=native;source=path.read_bytes();handle='no-decoder'
    n.handler._records[handle]=dict(path=path,sha256=digest(source),size=len(source),format='djvu',
        page_count=inventory.page_count,pages={},verified={'retained':True},
        parser_sha256=n.handler.parser_sha256,spec=spec(max_bytes=len(source)).wire())
    result=await n.handler(frame('original_pages',dict(handle=handle,original_sha256=digest(source),index=0)))
    assert result['value']=={'error':'original_djvu_decoder_required'}
    assert n.handler._records[handle]['pages']=={} and n.parser==[]


@pytest.mark.asyncio
async def test_native_hash_archive_conditional_readback_and_page_identity(native):
    n=native;f,r=await acquire(n)
    assert r['sha256']==digest(PDF) and n.download.calls==1 and n.parser==['inspect']
    assert await n.handler(f)==await n.handler(f) and n.download.calls==1
    args=dict(handle=r['handle'],original_sha256=r['sha256'])
    pending=await n.handler(frame('original_pages',dict(args,index=0)))
    assert pending['value']['error']=='original_archive_verification_required'
    upload=frame('original_archive',args);upload['original_transfer']={'ticket':ticket(upload,r)}
    assert (await n.handler(upload))['value']['native_version']=='native-etag'
    verify=frame('original_verify',args);verify['original_transfer']={'ticket':ticket(verify,r,'GET')}
    receipt=(await n.handler(verify))['value']
    assert receipt['sha256']==digest(PDF) and receipt['acquisition_sha256']==r['acquisition_sha256']
    pages=frame('original_pages',dict(args,index=0),key='after-verified')
    page=(await n.handler(pages))['value']
    assert page['page_id']==str(uuid5(UUID(SOURCE),'page:1:0'))
    assert page['original_sha256']==digest(PDF) and 'image_b64' not in page
    assert n.parser==['inspect','page']
    pageargs=dict(args,page_index=0)
    u=frame('original_archive',pageargs);u['original_transfer']={'ticket':ticket(u,r,page=True)}
    assert (await n.handler(u))['value']['sha256']==digest(IMAGE)
    await n.handler(pages)
    assert n.parser==['inspect','page']
    await n.handler.aclose();assert not n.handler.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('mime_type', ['application/pdf', 'text/html', None])
async def test_unsupported_bytes_are_rejected_independently_of_mime(native, monkeypatch, mime_type):
    import sys
    monkeypatch.setattr(sys.modules[__name__], 'PDF', b'<html>not a PDF or DjVu original</html>')
    _, result = await acquire(native, spec(mime_type=mime_type))
    assert result == {'error': 'original_format_unsupported', 'terminal': True}
    assert native.download.calls == 1
    assert not native.parser and not native.transfer.calls


@pytest.mark.asyncio
async def test_expected_hash_and_page_bound_fail_before_archive(native):
    _,bad=await acquire(native,spec(expected_sha256='f'*64))
    assert bad['error']=='original_expected_bytes_mismatch' and not native.transfer.calls
    async def too_many(*a):return dict(format='pdf',page_count=33)
    native.handler.parser=too_many
    f=frame('original_acquire',{'spec':spec(max_pages=32).wire()},key='over-page-bound',private={'download_url':'https://attachment.invalid/private'})
    assert (await native.handler(f))['value']=={'error':'original_page_count_limit','terminal':True}
    assert not native.transfer.calls


@pytest.mark.asyncio
async def test_small_ten_page_original_is_admitted_without_rendering_every_page(native):
    async def ten_pages(*a):return dict(format='pdf',page_count=10)
    native.handler.parser=ten_pages
    _,receipt=await acquire(native)
    assert receipt['page_count']==10 and receipt['size']==len(PDF)
    assert not native.transfer.calls


@pytest.mark.asyncio
async def test_dispatch_forgery_and_changed_operation_refuse(native):
    f,_=await acquire(native)
    for key,value in (('writer_generation','other'),('runtime_incarnation','other'),('compute_claim',''),('dispatch_generation',0)):
        with pytest.raises(OriginalIntakeError):await native.handler({**f,key:value})
    changed=frame('original_acquire',{'spec':spec(file_id='different').wire()},key=f['operation_key'],private=f['original_transfer'])
    with pytest.raises(OriginalIntakeError,match='operation_changed'):await native.handler(changed)
    native.handler.detached(1)
    with pytest.raises(OriginalIntakeError):await native.handler(f)


@pytest.mark.asyncio
async def test_cancelled_acquisition_retains_unknown_and_removes_owned_partial(native):
    entered=asyncio.Event()
    async def blocked(*a,**kw):entered.set();await asyncio.Event().wait()
    native.handler.downloader.download=blocked
    f=frame('original_acquire',{'spec':spec().wire()},private={'download_url':'https://attachment.invalid/private'})
    task=asyncio.create_task(native.handler(f));await entered.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    with pytest.raises(OriginalIntakeError,match='unknown_reconcile'):await native.handler(f)
    assert not list(native.handler.root.iterdir())


@pytest.mark.asyncio
async def test_prepared_intake_uses_actual_transport_document_resource_and_replays_without_download():
    from regional_knowledge.kaggle_bootstrap import PreparedRuntime
    from test_bge_shared_worker import Peer,result
    from test_bge_wss_worker import module,CONFIG
    from test_kaggle_bootstrap import candidate
    transport=module('bge_wss_transport');peer=Peer();stop=asyncio.Event();calls=[]
    class Intake:
        def attached(self,generation):calls.append(('attached',generation))
        def detached(self,generation):calls.append(('detached',generation))
        async def __call__(self,frame):
            calls.append(('acquire',frame['compute_claim']))
            return dict(phase=frame['phase'],request_sha256=frame['request_sha256'],value={'sha256':digest(PDF)})
    async def read(frame,*,encode):return result(frame)
    async def prepare(encoder,**identity):return PreparedRuntime(read,candidate(),original_intake_handler=Intake())
    def load():
        def forbidden(text):raise AssertionError('original intake invoked BGE')
        return forbidden
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),load,lambda:{},prepare_runtime=prepare,
        connector=peer.connect,stop=stop,heartbeat_seconds=.01,maximum_seconds=3)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        request=dict(type='DISPATCH',id='original',claim='claim',kind='recovery_phase',phase='original_acquire',
            operation_key='original-op',request_sha256='a'*64,arguments={'spec':{}},run_id=CONFIG['run_id'],
            launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,database_generation='db',
            writer_generation='writer',compute_claim='actual-document-slot',dispatch_generation=1,
            original_transfer={'download_url':'https://attachment.invalid/private'})
        await peer.put(request)
        receipt=await peer.wait('RECOVERY_PHASE_RESULT');await peer.wait('RESOURCE_RELEASED')
        assert receipt['receipt']['value']['sha256']==digest(PDF)
        await peer.disconnect()
        assert (await peer.wait('RECOVERY_PHASE_RESULT',generation=2))['receipt']==receipt['receipt']
        assert [c for c in calls if c[0]=='acquire']==[('acquire','actual-document-slot')]
        await peer.put(dict(type='RESULT_ACK',job_id='original',claim='claim',status='accepted'),generation=2)
    finally:stop.set();await task
