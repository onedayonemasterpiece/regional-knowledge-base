"""Fixed GEO native reads without a model, native PG or owner-provider effect."""
import asyncio
import base64
from contextlib import asynccontextmanager
from uuid import UUID

import pytest

from regional_knowledge.geo_manifest_commands import GEO_REVISIONS
from regional_knowledge.geo_migration import canonical
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_geo_contracts import GeoReadRequest,GeoReadResult,decode_cursor,cursor_scope
from regional_knowledge.kaggle_geo_read import execute_geo_read
from test_geo_migration import snapshot,uid
from test_kaggle_fact_read import Native


def request(**changes):
    args=dict(request_id='geo-read',connection_generation=2,turn_generation=3,entity_id=uid(10),
        min_data_seq=9,deadline_ms=2000,actor=dict(person_id=uid(1),audience='personal',auth_epoch=1,visibility_epoch=4))
    args.update(changes);return GeoReadRequest.model_validate(args)


def row(entity=10,*,owner=1):
    manifest=build_geo_whitelist(**snapshot()).require_ready()
    item=next(item for item in manifest.items if item.key=='place:'+uid(entity))
    item=item.model_copy(update={'owner_person_id':uid(owner)})
    return (UUID(uid(entity)),item.key,canonical(item.wire()),None,manifest.sha256,5,'activation','a'*64)


def native(rows=(),**kwargs):return Native(rows,revision=(GEO_REVISIONS.schema,GEO_REVISIONS.function),**kwargs)


@pytest.mark.parametrize('change',[dict(entity_id='forged'),dict(poi_ref='http://arbitrary'),dict(name='x'),
    dict(entity_id=None),dict(limit=4),dict(year=1900),dict(action='members'),dict(category='arbitrary')])
def test_fixed_selector_scope_and_small_result_budget(change):
    with pytest.raises(ValueError):request(**change)


@pytest.mark.asyncio
async def test_private_indexed_identity_read_uses_no_encoder_or_fake_source_dependency():
    n=native([row()]);result=await execute_geo_read(request(),acquire=n.acquire)
    assert result.status=='ok' and len(result.records)==1 and result.evidence==()
    assert result.records[0].item_key=='place:'+uid(10)
    sql,params=next(call for call in n.calls if 'read_geo_identity_v1' in call[0])
    assert params[:2]==(uid(1),uid(10)) and params[-1]==4
    assert not any('embedding' in sql or 'dense_candidates' in sql for sql,_ in n.calls)


@pytest.mark.asyncio
async def test_cursor_scope_binds_actor_and_native_frontier_before_fixed_lookup():
    n=native([row(),row(11)])
    first=await execute_geo_read(request(limit=1),acquire=n.acquire)
    assert first.next_cursor
    state=dict(data=9,projection=9,visibility=4,database='native-db')
    assert decode_cursor(first.next_cursor,cursor_scope(request(limit=1),state))==uid(10)
    n.calls.clear()
    changed=await execute_geo_read(request(limit=1,cursor=first.next_cursor,
        actor=request().actor.model_copy(update={'person_id':uid(99)})),acquire=n.acquire)
    assert changed.reason=='invalid_input' and not changed.records
    assert not any('read_geo_identity_v1' in sql for sql,_ in n.calls)


@pytest.mark.asyncio
async def test_wrong_owner_and_old_revision_never_return_private_bytes():
    n=native([row(owner=99)])
    result=await execute_geo_read(request(),acquire=n.acquire)
    assert result.reason=='access_denied' and not result.records
    n.revision=('rkb-v4-sql001-008-v1','rkb-v4-source-story-facts-v3')
    result=await execute_geo_read(request(),acquire=n.acquire)
    assert result.reason=='not_ready' and not result.records


@pytest.mark.asyncio
async def test_total_pool_deadline_and_cancel_release_actual_read_context():
    @asynccontextmanager
    async def slow():
        await asyncio.sleep(.05);yield native()
    result=await execute_geo_read(request(deadline_ms=1),acquire=slow)
    assert result.reason=='deadline_exceeded' and not result.records
    done=asyncio.Event()
    @asynccontextmanager
    async def waiting():
        try:await asyncio.Event().wait();yield native()
        finally:done.set()
    task=asyncio.create_task(execute_geo_read(request(),acquire=waiting))
    await asyncio.sleep(.01);task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert done.is_set()
