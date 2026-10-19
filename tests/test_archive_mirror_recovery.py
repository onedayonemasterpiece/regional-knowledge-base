"""Lost GET replies and unrelated polling failures cannot duplicate native reads."""
import httpx
from datetime import datetime,timedelta,timezone
from uuid import uuid4
import pytest
from test_illustration_mirror import Vibe
from test_illustration_mirror_sqlite import mirror_source
from regional_knowledge.illustration_mirror import IllustrationMirror


@pytest.mark.asyncio
async def test_first_read_lost_reply_reuses_admission_key(mirror_source):
    source=mirror_source
    class LostReply(Vibe):
        lost=False
        async def call(self,name,args):
            result=await super().call(name,args)
            if args['command']['kind']=='get' and not self.lost:
                self.lost=True
                raise httpx.ReadTimeout('admitted reply lost')
            return result
    client=LostReply(source.owner);client.fail=False
    try:
        worker=IllustrationMirror(source.backend,client)
        await worker.tick();client.advance(0)
        await worker.tick()
        assert await IllustrationMirror(source.backend,client).tick()==1
        assert len(client.recovered_reads)==1
        assert len(client.operations)==1
    finally:await source.backend.aclose()


@pytest.mark.asyncio
async def test_four_cooling_reads_do_not_starve_a_ready_crop(mirror_source):
    source=mirror_source;client=Vibe(source.owner);client.fail=False
    backend=source.backend
    item=backend.corpus.one('rkb_illustrations',source.illustration)
    now=datetime.now(timezone.utc)
    rows=[{**item,'id':str(uuid4()),'mirror_read_operation_id':'blocked-'+str(n),
           'mirror_error_type':'MirrorReadTransient','mirror_attempt_at':(now-timedelta(minutes=1)).isoformat()}
          for n in range(4)]
    backend.corpus.put('rkb_illustrations',rows+[{**item,'mirror_attempt_at':now.isoformat()}])
    try:
        await IllustrationMirror(backend,client).tick()
        ready=backend.corpus.one('rkb_illustrations',source.illustration)
        assert ready['mirror_operation_id']
    finally:await backend.aclose()


@pytest.mark.asyncio
async def test_poll_failure_preserves_transient_read_cooldown(mirror_source):
    source=mirror_source;client=Vibe(source.owner);client.fail=False
    worker=IllustrationMirror(source.backend,client)
    try:
        await worker.tick();client.advance(0)
        item=source.backend.corpus.one('rkb_illustrations',source.illustration)
        read_id='old-blocked-read'
        source.backend.corpus.put('rkb_illustrations',[{**item,'mirror_read_operation_id':read_id}])
        client.blocked_reads[read_id]='provider_read_deadline'
        await worker.tick()
        item=source.backend.corpus.one('rkb_illustrations',source.illustration)
        retry_id=item['mirror_read_operation_id']
        original=client.receipt
        async def broken(operation):raise httpx.ReadTimeout('poll unavailable')
        client.receipt=broken
        await worker.tick()
        client.receipt=original
        client.blocked_reads[retry_id]='provider_read_deadline'
        await IllustrationMirror(source.backend,client).tick()
        after=source.backend.corpus.one('rkb_illustrations',source.illustration)
        assert after['mirror_read_operation_id']==retry_id
        assert len(client.recovered_reads)==1
        assert after['mirror_attempt_at']==item['mirror_attempt_at']
    finally:await source.backend.aclose()
