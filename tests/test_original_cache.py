import asyncio,hashlib
import pytest
from regional_knowledge.original_cache import OriginalCache

@pytest.mark.asyncio
async def test_idle_renewal_in_use_and_integrity(tmp_path):
    now=[0];cache=OriginalCache(tmp_path,idle_seconds=10,max_bytes=100,clock=lambda:now[0]);data=b'original';sha=hashlib.sha256(data).hexdigest();calls=[]
    async def loader(path):calls.append(path);path.write_bytes(data)
    async with cache.access(sha,loader) as path:
        assert path.read_bytes()==data;now[0]=20;assert cache.cleanup()['deleted']==0
    now[0]=21
    async with cache.access(sha,loader):pass
    assert len(calls)==1;now[0]=30;assert cache.cleanup()['deleted']==0
    now[0]=32;assert cache.cleanup()['deleted']==1
    async def bad(path):path.write_bytes(b'wrong')
    with pytest.raises(ValueError):
        async with cache.access(sha,bad):pass
    assert not path.exists()

@pytest.mark.asyncio
async def test_simultaneous_access_downloads_once(tmp_path):
    data=b'original';sha=hashlib.sha256(data).hexdigest();calls=[];cache=OriginalCache(tmp_path)
    async def loader(path):calls.append(1);await asyncio.sleep(.02);path.write_bytes(data)
    async def access():
        async with cache.access(sha,loader) as path:assert path.read_bytes()==data
    await asyncio.gather(access(),access());assert len(calls)==1

@pytest.mark.asyncio
async def test_capacity_evicts_idle_lru_but_never_live(tmp_path):
    cache=OriginalCache(tmp_path,max_bytes=10)
    first=b'123456';second=b'abcdef';a=hashlib.sha256(first).hexdigest();b=hashlib.sha256(second).hexdigest()
    async def one(path):path.write_bytes(first)
    async def two(path):path.write_bytes(second)
    async with cache.access(a,one):
        with pytest.raises(RuntimeError,match='capacity_in_use'):
            async with cache.access(b,two):pass
    async with cache.access(b,two) as path:assert path.read_bytes()==second
    assert not (tmp_path/a).exists();assert cache.cleanup()['bytes']==6


def test_crash_partial_cleanup(tmp_path):
    cache=OriginalCache(tmp_path);sha='a'*64;partial=tmp_path/(sha+'.partial-123');partial.write_bytes(b'partial')
    with cache.lock(sha):cache.cleanup();assert partial.exists()
    cache.cleanup();assert not partial.exists()
