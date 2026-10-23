"""Native packet visibility does not misreport Dataset Saved state."""
import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.source_batch import handler,resolved_command,CODEC
from test_control_spool import env,receipt,SOURCE
from test_participants import people
from test_source_stage import INGESTION


@pytest.mark.asyncio
async def test_known_packet_apply_clears_read_barrier_but_keeps_durability_pending(env):
    e=env
    e.spool.registry=HandlerRegistry([handler(check_sha256='1'*64,apply_sha256='2'*64)])
    c=resolved_command(source_id=SOURCE,payload=dict(codec=CODEC,ingestion_id=INGESTION,
        batch_index=0,batch_count=1,last_batch_sha256='a'*64,materializer_sha256='f'*64),
        sequence=1,operation_key='packet:1',person_id=e.cmd.person_id,request_id=e.cmd.request_id,
        logical_time=e.cmd.logical_time)
    frozen,data,h=e.spool._validate(c,'db-source')
    await e.spool._write(e.spool._stage,frozen,data,h,'db-source')
    await e.spool.seal(e.p.admin,c.operation_key,predecessor=None,writer_generation='writer-1')
    assert (await e.spool.read_facts(None)).pending
    await e.spool.mark_apply_sent(c.operation_key)
    assert (await e.spool.read_facts(None)).unknown
    await e.spool.record_apply(c.operation_key,receipt(c))
    for scope in (None,(SOURCE,)):
        facts=await e.spool.read_facts(scope,(SOURCE,))
        assert not facts.pending and not facts.unknown
        assert not facts.changes[0].pending
    assert (await e.spool.scope_state())['pending']
    assert (await e.spool.get(c.operation_key)).outcome_state=='NOT_SENT'
