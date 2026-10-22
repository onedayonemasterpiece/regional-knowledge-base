"""Preserve an actual old closure when future handlers are registered for replay."""
from dataclasses import replace
from pathlib import Path

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.geo_manifest_commands import GEO_REVISIONS,GEO_V2_REVISIONS
from regional_knowledge.kaggle_apply import HandlerRegistry,PgFunction
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from regional_knowledge.kaggle_native_source_upgrade import NativeSourceUpgradePins
from regional_knowledge.kaggle_owned_pg import OwnedDatabase,canonical
from regional_knowledge.kaggle_source_commands import fixed_registry
from regional_knowledge.source_activation import ACTIVATION_REVISIONS
from regional_knowledge.source_history_wss import WssSourceHistoryVerifier
from first_book_registry_fixture import first_book_entries
from test_control_spool import env
from test_participants import people


def context(e):
    database=OwnedDatabase('VERIFIED_INITIALIZED','db-source','writer-1',*GEO_REVISIONS.wire(),
        '/kaggle/temp/fixture/socket','rkb',ownership_evidence='synthetic-origin11')
    entries=first_book_entries(Path(__file__).resolve().parents[1])
    e.spool.registry=fixed_registry(database,entries)
    pins=GeoUpgradePins(*('a'*64 for _ in range(5)),origin_catalog_sha256='b'*64,origin_acl_sha256='c'*64)
    routes={key:(lambda:None) for key in ('source.search','source.fetch','source.withdraw')}
    def verifier(db=database,**changes):
        options=dict(spool=e.spool,acquire=None,binding_current=lambda b:True,routes=routes,
            expected_catalog_sha256='b'*64,expected_acl_sha256='c'*64,writer_generation='writer-1',
            database=db,observer_sha256='d'*64,geo_upgrade_pins=pins)
        options.update(changes)
        return WssSourceHistoryVerifier(**options)
    return database,entries,pins,routes,verifier


@pytest.mark.asyncio
async def test_origin11_full33_preserves_original_genesis_and_rejects_changed_original_handler(env):
    database,entries,pins,routes,make=context(env)
    original=pins.origin_closure(env.spool.registry,routes)
    raw=canonical(dict(codec='rkb.source-genesis.v2',stream=env.journal.stream,
        database_generation=database.database_generation,sequence=0,origin_closure=original,
        producer={'verifier_sha256':'e'*64}))
    verifier=make()
    assert len(env.spool.registry.handlers)==33
    assert verifier.registry_hash==verifier._registry_hash()!=original['registry_sha256']
    assert verifier._verify_genesis(raw,'db-source',expected_closure=verifier.origin_closure)
    assert verifier._closure_transition({}, {}, {}, None) is None
    # The existing snapshot/remote genesis must still reject changed origin bytes.
    handlers=list(env.spool.registry.handlers.values())
    index=next(i for i,h in enumerate(handlers) if h.revisions==GEO_REVISIONS)
    handlers[index]=replace(handlers[index],apply=PgFunction(handlers[index].apply.name,'f'*64))
    env.spool.registry=HandlerRegistry(handlers)
    changed=make()
    assert changed._verify_genesis(raw,'db-source',expected_closure=None)


@pytest.mark.asyncio
async def test_legacy_catalog_acl_pins_are_ignored_by_version_based_history(env):
    database,entries,pins,routes,make=context(env)
    for changes in ({'expected_catalog_sha256':'f'*64},{'expected_acl_sha256':'f'*64}):
        assert make(**changes).schema_version==database.schema_version
    assert make()._closure_transition({}, {}, {},None) is None
