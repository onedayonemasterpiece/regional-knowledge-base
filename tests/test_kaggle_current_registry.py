"""Current executable contracts are code-owned, outside launch metadata."""
from dataclasses import replace

import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry,Revisions
from regional_knowledge.kaggle_current_registry import current_registry,registry_sha256
from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES
from regional_knowledge.kaggle_launch_config import target_launch_from_wire
from test_kaggle_target_runtime import spec


def test_registry_contains_one_handler_per_current_route_and_no_historical_copy():
    registry=current_registry()
    assert set(registry.handlers)==NATIVE_ROUTES
    assert len(registry.handlers)==len(NATIVE_ROUTES)
    assert {'source.batch.v1','source.finish.v1'}<=registry.handlers.keys()
    handler=next(iter(registry.handlers.values()))
    historical=replace(handler,revisions=Revisions('old','old','old'))
    with pytest.raises(ValueError,match='duplicate current route'):
        HandlerRegistry([handler,historical])


def test_launch_has_only_the_current_registry_hash_and_rejects_external_entries():
    target=replace(spec(fresh=True),registry_sha256=registry_sha256())
    wire=target.wire()
    assert 'command_handlers' not in wire
    assert target_launch_from_wire(wire).registry_sha256==registry_sha256()
    with pytest.raises(ValueError,match='configuration fields'):
        target_launch_from_wire(wire|{'command_handlers':[]})
    with pytest.raises(ValueError,match='current code registry hash'):
        replace(target,registry_sha256='0'*64)
    upgraded=replace(target,database=replace(target.database,schema_version=target.database.schema_version+1))
    assert upgraded.registry_sha256==target.registry_sha256


def test_worker_checks_code_registry_hash_before_opening_database_ownership():
    from regional_knowledge.kaggle_target_runtime import TargetRuntime
    from regional_knowledge.kaggle_bootstrap import BootstrapUnavailable
    target=replace(spec(fresh=True),registry_sha256=registry_sha256()).wire()
    def forbidden(_):pytest.fail('invalid registry reached the PG owner')
    for changed in (target|{'registry_sha256':'0'*64},target|{'command_handlers':[]}):
        with pytest.raises(BootstrapUnavailable,match='current_registry_hash_differs'):
            TargetRuntime({'target_runtime':changed},executed_source='fixture',owner_factory=forbidden)
