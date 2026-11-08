"""Real dispatch polling with concurrent fictional transport and background work."""
import asyncio
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch


@pytest.mark.asyncio
async def test_six_private_page_reads_return_completed_work_within_existing_deadline():
    bridge=SimpleNamespace(timeout=.5)
    ready=asyncio.Event();calls=[0]*6;active=0;peak=0
    async def finish():
        await asyncio.sleep(.08);ready.set()
    async def read(slot,args,handle):
        nonlocal active,peak
        calls[slot]+=1;active+=1;peak=max(peak,active)
        try:await asyncio.sleep(.02)  # The existing asynchronous WSS round trip.
        finally:active-=1
        return {'status':'complete','value':{'page':slot}} if ready.is_set() else {'status':'pending','handle':handle}
    completion=asyncio.create_task(finish())
    try:
        result=await asyncio.gather(*(RecoveryPhaseDispatch._resolve_io(
            bridge,None,'original_pages',{},str(i),{'status':'pending','handle':str(i)},
            page_read=lambda args,handle,i=i:read(i,args,handle)) for i in range(6)))
        assert result==[{'page':i} for i in range(6)]
        assert peak==6 and all(2<=n<=6 for n in calls)
    finally:await completion


@pytest.mark.asyncio
async def test_private_pending_read_keeps_original_timeout_and_propagates_authority_failure():
    bridge=SimpleNamespace(timeout=.08);calls=0
    async def pending(args,handle):
        nonlocal calls
        calls+=1;await asyncio.sleep(.02)
        return {'status':'pending','handle':handle}
    with pytest.raises(TimeoutError):
        await RecoveryPhaseDispatch._resolve_io(bridge,None,'original_pages',{},'same',
            {'status':'pending','handle':'same'},page_read=pending)
    assert 1<=calls<=5
    async def denied(args,handle):raise PermissionError('fictional-current-authority-withdrawn')
    with pytest.raises(PermissionError,match='withdrawn'):
        await RecoveryPhaseDispatch._resolve_io(bridge,None,'original_pages',{},'same',
            {'status':'pending','handle':'same'},page_read=denied)


@pytest.mark.asyncio
async def test_snapshot_poll_retains_existing_two_second_pause(monkeypatch):
    waits=[];calls=0;real_sleep=asyncio.sleep
    async def observe_sleep(seconds):
        waits.append(seconds);await real_sleep(0)
    async def request(*args,**kwargs):
        nonlocal calls
        calls+=1
        return {'status':'pending'} if calls==1 else {'status':'complete','value':{'saved':True}}
    monkeypatch.setattr(asyncio,'sleep',observe_sleep)
    result=await RecoveryPhaseDispatch._resolve_io(SimpleNamespace(timeout=1,request=request),
        None,'capture',{},'saved',{'status':'pending','handle':'same'})
    assert result=={'saved':True} and calls==2 and waits==[2]
