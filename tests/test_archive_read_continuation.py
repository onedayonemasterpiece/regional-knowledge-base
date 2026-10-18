"""Slow/uncertain archive reads reuse one provider operation across restarts."""
import hashlib
import asyncio
from types import SimpleNamespace

import httpx
import pytest

from regional_knowledge import source_archive
from regional_knowledge.original_cache import OriginalCache


class Provider:
    owner='owner-a'
    issuer='https://archive.example'

    def __init__(self):
        self.admissions={}
        self.calls=[]
        self.pending=True
        self.lose_response=False
        self.asset_status=None
        self.payload=b'original bytes'
        self.state='verified'

    async def call(self, name, args):
        assert name=='vibepublish_media_store'
        assert args['command']['kind']=='get'
        key=args['request_key']
        identity=(self.owner,key)
        self.calls.append((identity,args['command']['entry_ref']))
        op=self.admissions.setdefault(identity,'op_'+str(len(self.admissions)))
        if self.lose_response:
            self.lose_response=False
            raise httpx.ReadTimeout('admitted, response lost')
        return {'operation_id':op}

    async def receipt(self, operation):
        assert operation in self.admissions.values()
        if self.pending:return {'operation_complete':False,'state':'running'}
        return {'operation_complete':True,'state':self.state,'items':[{
            'media':[{'source':{'kind':'asset','id':'asset-original'}}],
            'media_evidence':[{'media_kind':'document','sha256':hashlib.sha256(self.payload).hexdigest()}]}]}

    async def request(self, method, url):
        assert method=='GET' and url==self.issuer+'/v1/assets/asset-original'
        if self.asset_status:
            response=httpx.Response(self.asset_status,request=httpx.Request(method,url))
            response.raise_for_status()
        return SimpleNamespace(content=self.payload)


@pytest.fixture
def cache_dir(tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_ORIGINAL_CACHE_DIR',str(tmp_path/'originals'))
    monkeypatch.setattr(source_archive,'_ARCHIVE_READ_POLLS',1)
    async def no_sleep(_):pass
    monkeypatch.setattr(source_archive.asyncio,'sleep',no_sleep)
    return tmp_path/'originals'


@pytest.mark.asyncio
async def test_pending_read_survives_new_cache_and_client_instance(cache_dir):
    provider=Provider()
    with pytest.raises(source_archive.ArchiveReadPending) as pending:
        await source_archive.archive_payload(provider,'entry-a')
    assert pending.value.operation_id=='op_0'
    assert len(provider.calls)==1
    resumed=Provider()
    resumed.admissions=provider.admissions
    resumed.pending=False
    data,digest=await source_archive.archive_payload(resumed,'entry-a')
    assert data==provider.payload and digest==hashlib.sha256(data).hexdigest()
    assert len(resumed.calls)==1 and len(resumed.admissions)==1
    with OriginalCache(cache_dir).db() as db:
        assert db.execute('select count(*) from archive_reads').fetchone()[0]==1


@pytest.mark.asyncio
async def test_lost_admission_response_replays_same_key(cache_dir):
    provider=Provider();provider.lose_response=True
    with pytest.raises(httpx.ReadTimeout):
        await source_archive.archive_payload(provider,'entry-a')
    provider.pending=False
    await source_archive.archive_payload(provider,'entry-a')
    assert len(provider.calls)==2 and provider.calls[0]==provider.calls[1]
    assert len(provider.admissions)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[404,410])
async def test_verified_expired_asset_allows_new_attempt_only_after_definitive_response(cache_dir,status):
    provider=Provider();provider.pending=False;provider.asset_status=status
    with pytest.raises(RuntimeError,match='archive_source_cache_expired'):
        await source_archive.archive_payload(provider,'entry-a')
    provider.asset_status=None
    await source_archive.archive_payload(provider,'entry-a')
    assert len(provider.admissions)==2 and provider.calls[0][0]!=provider.calls[1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[401,403,500])
async def test_asset_auth_and_unknown_failures_retain_operation(cache_dir,status):
    provider=Provider();provider.pending=False;provider.asset_status=status
    with pytest.raises(httpx.HTTPStatusError):
        await source_archive.archive_payload(provider,'entry-a')
    provider.asset_status=None
    await source_archive.archive_payload(provider,'entry-a')
    assert len(provider.calls)==2 and len(provider.admissions)==1


@pytest.mark.asyncio
async def test_terminal_provider_failure_is_not_replaced_by_new_get(cache_dir):
    provider=Provider();provider.pending=False;provider.state='failed'
    for _ in range(2):
        with pytest.raises(RuntimeError,match='archive_source_unavailable'):
            await source_archive.archive_payload(provider,'entry-a')
    assert len(provider.calls)==2 and len(provider.admissions)==1


@pytest.mark.asyncio
async def test_owner_and_entry_scopes_do_not_share_pending_operations(cache_dir):
    provider=Provider()
    for owner,entry in [('owner-a','entry-a'),('owner-b','entry-a'),('owner-a','entry-b')]:
        provider.owner=owner
        with pytest.raises(source_archive.ArchiveReadPending):
            await source_archive.archive_payload(provider,entry)
    assert len(provider.calls)==3 and len(provider.admissions)==3


def test_retiring_old_attempt_cannot_remove_new_attempt(cache_dir):
    cache=OriginalCache(cache_dir);scope='a'*64
    old,_=cache.archive_attempt(scope)
    cache.archive_finished(scope,old)
    new,_=cache.archive_attempt(scope)
    cache.archive_finished(scope,old)
    assert cache.archive_attempt(scope)==(new,None)


def test_late_admission_cannot_overwrite_concurrently_retired_attempt(cache_dir):
    cache=OriginalCache(cache_dir);scope='a'*64
    old,_=cache.archive_attempt(scope)
    cache.archive_finished(scope,old)
    new,_=cache.archive_attempt(scope)
    assert cache.archive_admitted(scope,old,'op_old') is False
    assert cache.archive_attempt(scope)==(new,None)
    assert cache.archive_admitted(scope,new,'op_new') is True
    with pytest.raises(RuntimeError,match='archive_read_operation_conflict'):
        cache.archive_admitted(scope,new,'op_different')


@pytest.mark.asyncio
async def test_crash_after_payload_return_before_install_keeps_verified_receipt(cache_dir):
    provider=Provider();provider.pending=False
    await source_archive.archive_payload(provider,'entry-a')
    # Simulate caller/process loss before persisting downloaded original bytes.
    resumed=Provider();resumed.admissions=provider.admissions;resumed.pending=False
    await source_archive.archive_payload(resumed,'entry-a')
    assert len(resumed.admissions)==1 and provider.calls[0]==resumed.calls[0]


@pytest.mark.asyncio
async def test_resume_rechecks_current_get_authorization(cache_dir):
    provider=Provider();provider.pending=False
    await source_archive.archive_payload(provider,'entry-a')
    async def denied(name,args):
        raise PermissionError('binding revoked')
    provider.call=denied
    with pytest.raises(PermissionError,match='binding revoked'):
        await source_archive.archive_payload(provider,'entry-a')
    with OriginalCache(cache_dir).db() as db:
        assert db.execute('select count(*) from archive_reads').fetchone()[0]==1


@pytest.mark.asyncio
async def test_delayed_concurrent_admission_reuses_completed_operation(cache_dir):
    admitted=asyncio.Event();release=asyncio.Event()
    class DelayedProvider(Provider):
        async def call(self,name,args):
            result=await super().call(name,args)
            if len(self.calls)==1:
                admitted.set()
                await release.wait()
            return result
    provider=DelayedProvider();provider.pending=False
    first=asyncio.create_task(source_archive.archive_payload(provider,'entry-a'))
    await admitted.wait()
    second=await source_archive.archive_payload(provider,'entry-a')
    release.set()
    assert await first==second
    assert len(provider.calls)==2 and len(provider.admissions)==1
