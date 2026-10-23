"""Restored Dataset cursor does not depend on lifetime history verification."""
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_public_backend import TargetReadBinding
from test_control_spool import env,people,accept,receipt,handler,SOURCE
from test_kaggle_service_startup import service,target,auth


@pytest.mark.asyncio
async def test_dataset_ready_routes_without_opening_history(service):
    s=service;startup=s.startup;spool=s.config.spool
    # Model an already verified, empty Dataset. Actual queue/runtime/writer
    # binding and the canonical spool still come from the service fixture.
    spool.journal.store.latest_checkpoint=SimpleNamespace(base_sequence=0,dataset_version=1)
    startup.restorer=SimpleNamespace(complete=True,proof=SimpleNamespace(
        applied_sequence=0,control=json.dumps(dict(sequence=0,heads=[])).encode()))
    async def no_checkpoint(binding):return False
    startup.dataset_checkpoints=SimpleNamespace(refresh=no_checkpoint)
    startup.running=True
    assert not s.adapter.available
    assert await startup.refresh()
    assert startup.reads_current() and startup.imports_current()
    first=startup.coverage
    assert (first.accepted_sequence,first.applied_sequence)==(0,0)
    assert await startup.refresh() and startup.coverage is first
    assert not s.adapter.calls  # no restore(full), adopt or delta renewal
    # API-only reconnect has no retained physical-recovery capability. Its
    # existing Dataset control object is sufficient; no history fallback.
    raw=json.dumps(dict(sequence=0,heads=[])).encode()
    digest=hashlib.sha256(raw).hexdigest()
    await spool.journal._immutable(spool.journal.prefix+'/source-base-control/'+digest,raw,4096)
    spool.journal.store.latest_checkpoint=SimpleNamespace(base_sequence=0,dataset_version=1,
        control=SimpleNamespace(sha256=digest,size=len(raw)))
    startup.restorer.proof=None
    startup.restorer.physical=SimpleNamespace(limits=SimpleNamespace(max_control_bytes=4096))
    startup.binding=None;startup.coverage=None;spool.invalidate_source_history()
    assert await startup.refresh() and startup.reads_current()
    assert startup.coverage.applied_sequence==0 and not s.adapter.calls
    s.writer[0]=False
    assert not startup.reads_current() and not first.current()
    assert not await startup.refresh()
    startup.running=False


@pytest.mark.asyncio
async def test_cursor_unknown_apply_then_original_receipt_and_api_reconnect(env):
    e=env;current=[True]
    binding=TargetReadBinding('run','runtime',1,'db-source',0,'1')
    guard=lambda b:current[0] and b==binding
    await accept(e)
    empty=dict(sequence=0,heads=[])
    first=await e.spool.dataset_cursor(binding,control=empty,current=guard)
    assert first.accepted_sequence==1 and first.applied_sequence==0 and first.pending
    await e.spool.mark_apply_sent(e.cmd.operation_key)
    unknown=await e.spool.dataset_cursor(binding,control=empty,current=guard)
    assert unknown.applied_sequence==0 and unknown.pending
    await e.spool.record_apply(e.cmd.operation_key,receipt(e.cmd))
    applied=await e.spool.dataset_cursor(binding,control=empty,current=guard)
    assert applied.accepted_sequence==applied.applied_sequence==1 and not applied.pending
    # A subsequent checkpoint must retain the suffix's per-source restriction,
    # rather than export only the old Dataset's heads.
    with e.spool._db() as db:
        row=db.execute('SELECT sequence,restriction,pending,unknown FROM rkb_control_source_heads '
            'WHERE source_id=?',(SOURCE,)).fetchone()
    assert tuple(row)==(1,1,1,0)
    # Dataset promotion already preserves the old command. Reconnecting the
    # controller must not resolve/revalidate its historical handler again.
    control=dict(sequence=1,heads=[[SOURCE,1,hashlib.sha256(e.cmd.encode()).hexdigest(),1,0,0]])
    e.spool.invalidate_source_history()
    e.spool.registry=HandlerRegistry([replace(handler(None),route='current.route')])
    restored=await e.spool.dataset_cursor(binding,control=control,current=guard)
    assert restored.current() and restored.applied_sequence==1
    assert not restored.source_heads[0].pending
    facts=await e.spool.read_facts((SOURCE,),(SOURCE,))
    assert facts.changes[0].restriction and e.spool.facts_current(facts)
    current[0]=False
    assert not restored.current() and not e.spool.facts_current(facts)
    with pytest.raises(SpoolConflict):
        await e.spool.dataset_cursor(binding,control=control,current=guard)
