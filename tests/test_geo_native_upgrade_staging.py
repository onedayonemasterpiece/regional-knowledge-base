"""Run A stages+seals the schema14 transition right after the GEO repair, frozen."""
from types import SimpleNamespace

import pytest

from regional_knowledge.control_spool import SpoolConflict, SpoolError
from regional_knowledge.kaggle_geo_install import SourceGeoInstaller
from regional_knowledge.kaggle_native_source_upgrade import NativeSourceUpgradePins, ROUTE
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.geo_manifest_commands import GEO_REVISIONS, GEO_V2_REVISIONS
from regional_knowledge.kaggle_readiness import RUNTIME_HASHES, SearchRuntimeExpectation
from regional_knowledge.source_activation import ACTIVATION_REVISIONS


PINS = NativeSourceUpgradePins('1' * 64, '2' * 64, '3' * 64, '4' * 64)
REPAIR = (
    SimpleNamespace(sequence=21, operation_key='geo-repair12:manifest-1:migration',
        request_id='6f1c2b3a-0d4e-4f5a-8b6c-7d8e9f0a1b2c', person_id='3a2b1c0d-9e8f-4a7b-8c6d-5e4f3a2b1c0d', logical_time='2026-10-21T01:00:00Z'),
    SimpleNamespace(sequence=22), SimpleNamespace(sequence=25))


class Spool:
    def __init__(self):
        self.entries, self.calls = {}, []
        self.journal = SimpleNamespace(head=self._head)

    async def _head(self):
        return 'journal-head'

    async def get(self, key):
        if key not in self.entries:
            raise SpoolError('control operation not found')
        return self.entries[key]

    async def stage_native_source_upgrade(self, context, command, *, database_generation):
        self.calls.append(('stage', command.operation_key, database_generation))
        self.entries[command.operation_key] = SimpleNamespace(command_bytes=command.encode(),
            accepted=None, seal_state='NOT_SENT')

    async def seal(self, context, key, *, writer_generation, predecessor=None):
        self.calls.append(('seal', key, writer_generation, predecessor))
        entry = self.entries[key]
        entry.accepted, entry.seal_state = 'accepted', 'SEALED'


def installer(spool, pins=PINS):
    value = object.__new__(SourceGeoInstaller)
    value.spool, value.native_pins = spool, pins
    value.startup = SimpleNamespace(config=SimpleNamespace(writer_generation='writer-a'))
    return value


@pytest.mark.asyncio
async def test_native_upgrade_is_staged_and_sealed_once_after_geo_repair():
    spool = Spool()
    owner = installer(spool)
    binding = SimpleNamespace(database_generation='db-1')
    command = owner.native_upgrade_command(REPAIR)
    assert command.route == ROUTE and command.sequence == 26
    assert command.operation_key == 'geo-repair12:manifest-1:native-source-upgrade'
    assert command.person_id == REPAIR[0].person_id
    assert await owner._seal_native_upgrade(binding, REPAIR, 'ctx') == 26
    assert spool.calls == [('stage', command.operation_key, 'db-1'),
                           ('seal', command.operation_key, 'writer-a', 'journal-head')]
    # Restart: same deterministic intent, nothing staged or sealed again.
    assert await installer(spool)._seal_native_upgrade(binding, REPAIR, 'ctx') == 26
    assert len(spool.calls) == 2


@pytest.mark.asyncio
async def test_changed_native_intent_is_refused_and_absent_pins_stage_nothing():
    spool = Spool()
    binding = SimpleNamespace(database_generation='db-1')
    command = installer(spool).native_upgrade_command(REPAIR)
    spool.entries[command.operation_key] = SimpleNamespace(command_bytes=b'other', accepted=None, seal_state='NOT_SENT')
    with pytest.raises(SpoolConflict, match='native source upgrade intent changed'):
        await installer(spool)._seal_native_upgrade(binding, REPAIR, 'ctx')
    empty = Spool()
    assert await installer(empty, pins=None)._seal_native_upgrade(binding, REPAIR, 'ctx') == 25
    assert empty.calls == []


@pytest.mark.parametrize('revisions,pins,needs_upgrade',[
    (GEO_REVISIONS,PINS,True),(GEO_V2_REVISIONS,PINS,False),
    (ACTIVATION_REVISIONS,PINS,False),(ACTIVATION_REVISIONS,None,True)])
def test_pinned_final14_launch_is_compatible_with_geo_continuation(tmp_path,revisions,pins,needs_upgrade):
    queue=BgeQueue(tmp_path/'queue.sqlite')
    queue.enqueue(REPAIR[0].person_id,'fixture-demand',['synthetic'],kind='document')
    with queue.connect(write=False) as db:
        run=db.execute('select current_run from control where id=1').fetchone()[0]
    queue.expect_search_runtime(run,SearchRuntimeExpectation(
        {name:'a'*64 for name in RUNTIME_HASHES},'db-1',*revisions.wire(),0))
    owner=installer(Spool(),pins=pins)
    owner.queue=queue;owner.upgrade_pending=True
    assert owner.needs_native_upgrade(SimpleNamespace(run_id=run)) is needs_upgrade
