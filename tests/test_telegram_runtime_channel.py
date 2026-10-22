"""Real NativeOriginalIntake and downloader; synthetic SDK, never network/credentials."""
import base64,json
from dataclasses import replace
from types import SimpleNamespace
import pytest
from test_kaggle_original_intake import native,frame,spec,PDF
from test_telegram_original import Client,locator
from test_telegram_runtime_binding import seeded,RUN_A,INC_A,DEVICE,SYNTHETIC_SESSION
from regional_knowledge.telegram_runtime_channel import NativeRkb1Channel,Rkb1ChannelSource
from regional_knowledge.telegram_runtime_binding import TelegramBindingUnavailable
from regional_knowledge.original_intake import OriginalIntakeError,canonical,digest

class AuthorizedClient(Client):
    def __init__(self):super().__init__();self.connected=False;self.connects=0;self.disconnects=0
    def is_connected(self):return self.connected
    async def connect(self):self.connected=True;self.connects+=1
    async def disconnect(self):self.connected=False;self.disconnects+=1
    async def is_user_authorized(self):return True

def bind_channel(n,*,dependency=True):
    h=n.handler;h.identity=(RUN_A,'nonce',INC_A)
    h.current=lambda b:b==(RUN_A,INC_A,h.generation) and h.generation is not None
    client=AuthorizedClient();created=[]
    def factory(*a,**kw):created.append((a,kw));return client
    channel=NativeRkb1Channel(intake=h,dependency_current=lambda:dependency,
        session_factory=lambda v:('synthetic-session',v),client_factory=factory)
    h.telegram_channel=channel
    return channel,client,created

def grant(generation=1,*,retained=False):
    result=dict(type='RKB1_BIND_GRANT',protocol_version=2,connection_generation=generation,
        run_id=RUN_A,runtime_incarnation=INC_A,source_name='TELEGRAM_RKB1',locator=locator().wire())
    if not retained:result.update(bundle_b64=base64.b64encode(json.dumps(
        dict(session=SYNTHETIC_SESSION,**DEVICE)).encode()).decode(),api_id=12345,api_hash='a'*32)
    return result

def acquire_frame(*,loc=None,key='exact-original'):
    loc=loc or locator()
    s=spec(file_id=loc.file_id,file_name=loc.file_name,mime_type=loc.mime_type,
           expected_sha256=loc.sha256,expected_size=loc.size)
    f=frame('original_acquire',dict(spec=s.wire(),telegram_locator=loc.wire()),key=key)
    f.update(run_id=RUN_A,runtime_incarnation=INC_A)
    return f

@pytest.mark.asyncio
async def test_actual_intake_prebinding_same_key_then_late_bind_no_reload(native):
    channel,client,created=bind_channel(native);f=acquire_frame()
    assert (await native.handler(f))['value']['error']=='original_telegram_binding_required'
    assert client.fetches==client.downloads==0
    changed=acquire_frame(loc=replace(locator(),message_id=11))
    with pytest.raises(OriginalIntakeError,match='operation_changed'):await native.handler(changed)
    g=grant();await channel.receive(g)
    assert not {'bundle_b64','api_id','api_hash'}&g.keys()
    assert client.connects==1 and client.fetches==1 and client.downloads==0
    result=await native.handler(f)
    assert result['value']['sha256']==locator().sha256
    assert client.downloads==1 and native.parser==['inspect']
    assert await native.handler(f)==result and client.downloads==1 and len(created)==1

@pytest.mark.asyncio
async def test_same_client_reconnect_requires_new_grant_and_detach_fences_io(native,tmp_path):
    channel,client,created=bind_channel(native);await channel.receive(grant())
    native.handler.detached(1)
    with pytest.raises(OriginalIntakeError,match='lease_changed'):
        await channel.downloader.download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
    assert client.downloads==0
    native.handler.attached(2)
    assert not channel.current()
    with pytest.raises(TelegramBindingUnavailable,match='binding_changed'):await channel.receive(grant(1,retained=True))
    await channel.receive(grant(2,retained=True))
    assert channel.current() and len(created)==client.connects==1
    assert client.fetches==2 and native.handler.telegram_downloader is channel.downloader
    await native.handler.aclose()
    assert client.disconnects==1 and not channel.current()

@pytest.mark.asyncio
async def test_dependency_and_wrong_document_fail_without_acquire(native):
    channel,client,created=bind_channel(native,dependency=False)
    with pytest.raises(TelegramBindingUnavailable):await channel.receive(grant())
    assert created==[] and client.connects==0 and native.handler.telegram_downloader is None

@pytest.mark.asyncio
async def test_bad_topic_never_late_binds_or_downloads(native):
    channel,client,created=bind_channel(native)
    client.message.reply_to.reply_to_top_id=4
    with pytest.raises(TelegramBindingUnavailable):await channel.receive(grant())
    assert native.handler.telegram_downloader is None and client.downloads==0
    assert not channel.current() and channel.failed_generation==1

@pytest.mark.asyncio
async def test_dev_supplier_waits_for_outer_binding_and_does_not_repeat_on_reconnect(seeded):
    q,_=seeded;calls=[];eligible=[False];generation=[1]
    def supplier():calls.append(1);return {k:v for k,v in grant().items() if k in ('bundle_b64','api_id','api_hash')}
    source=Rkb1ChannelSource(queue=q,locator=locator(),credentials=supplier)
    startup=SimpleNamespace(config=SimpleNamespace(queue=q,writer_generation='writer'),
        binding=SimpleNamespace(database_generation='db'),imports_current=lambda:eligible[0],
        command_binding_current=lambda **k:k==dict(run_id=RUN_A,runtime_incarnation=INC_A,
            connection_generation=generation[0],database_generation='db',writer_generation='writer'))
    source.bind(startup)
    assert await source.grant(RUN_A,INC_A,1,retained_client=False) is None and calls==[]
    eligible[0]=True
    assert 'bundle_b64' in await source.grant(RUN_A,INC_A,1,retained_client=False)
    generation[0]=2
    assert await source.grant(RUN_A,INC_A,1,retained_client=False) is None and len(calls)==1
    answer=await source.grant(RUN_A,INC_A,2,retained_client=True)
    assert 'bundle_b64' not in answer and len(calls)==1

@pytest.mark.asyncio
@pytest.mark.parametrize('boundary',['probe','late_bind'])
async def test_reconnect_cannot_use_downloader_until_probe_and_bind_complete(native,tmp_path,boundary):
    import asyncio
    channel,client,_=bind_channel(native)
    await channel.receive(grant())
    native.handler.detached(1);native.handler.attached(2)
    entered=asyncio.Event();release=asyncio.Event()
    if boundary=='probe':
        original=client.get_messages
        async def delayed(*args,**kwargs):
            entered.set();await release.wait()
            return await original(*args,**kwargs)
        client.get_messages=delayed
    else:
        original=native.handler.bind_telegram_downloader
        async def delayed(*args,**kwargs):
            entered.set();await release.wait()
            return await original(*args,**kwargs)
        native.handler.bind_telegram_downloader=delayed
    task=asyncio.create_task(channel.receive(grant(2,retained=True)))
    await asyncio.wait_for(entered.wait(),1)
    try:
        assert not channel.current()
        with pytest.raises(OriginalIntakeError,match='lease_changed'):
            await channel.downloader.download(locator(),tmp_path,max_bytes=len(PDF),current=lambda:True)
        assert client.downloads==0
    finally:
        release.set();await task
    assert channel.current() and channel.authorized_generation==2

@pytest.mark.asyncio
async def test_archive_auth_confirmation_is_separate_and_rechecks_after_lease_await(seeded):
    q,_=seeded;eligible=[True];generation=[1]
    source=Rkb1ChannelSource(queue=q,locator=locator(),credentials=lambda:None)
    source.bind(SimpleNamespace(config=SimpleNamespace(queue=q,writer_generation='writer'),
        binding=SimpleNamespace(database_generation='db'),imports_current=lambda:eligible[0],
        command_binding_current=lambda **kw:kw['connection_generation']==generation[0]))
    source.lease.claim(RUN_A,INC_A)
    assert not source.archive_auth_confirmed(RUN_A,INC_A,1)
    assert await source.confirm_archive_auth(RUN_A,INC_A,1,True)
    assert source.archive_auth_confirmed(RUN_A,INC_A,1)
    generation[0]=2
    assert not source.archive_auth_confirmed(RUN_A,INC_A,2)
    original=source.lease.current
    def changed(*args):
        answer=original(*args);eligible[0]=False;return answer
    source.lease.current=changed
    assert not await source.confirm_archive_auth(RUN_A,INC_A,2,True)
    assert not source.archive_auth_confirmed(RUN_A,INC_A,2)
