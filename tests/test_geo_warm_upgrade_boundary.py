"""An API-only SQL12 composition cannot replace a schema11 native registry."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.geo_manifest_commands import (
    GEO_REVISIONS, GEO_V2_REVISIONS, geo_dependency_repair_commands,
    geo_manifest_handler, plan_geo_manifest,
)
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_apply import ApplyConflict, HandlerRegistry
from regional_knowledge.kaggle_geo_install import SourceGeoInstaller
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from test_bge_runtime_readiness import expected
from test_geo_manifest_commands import commands
from test_geo_migration import snapshot


def test_api_upgrade_keeps_old_native_registry_and_launch_expectation(tmp_path):
    queue = BgeQueue(tmp_path / "boundary.sqlite", clock=lambda: 1000.)
    queue.enqueue("synthetic-owner", "synthetic-demand", ["fixture"], kind="document")
    with queue.connect(write=False) as db:
        run_id = db.execute("SELECT id FROM runs").fetchone()[0]
    initial = replace(expected(), schema_revision=GEO_REVISIONS.schema,
                      function_revision=GEO_REVISIONS.function,
                      executor_revision=GEO_REVISIONS.executor)
    queue.expect_search_runtime(run_id, initial)
    native_registry = HandlerRegistry([geo_manifest_handler(
        revisions=GEO_REVISIONS, check_sha256="a" * 64, apply_sha256="b" * 64)])
    pins = GeoUpgradePins(*("c" * 64 for _ in range(7)))
    api_registry = pins.registry(native_registry)
    original = commands(plan_geo_manifest(build_geo_whitelist(**snapshot())))
    outcomes = {command.operation_key: "applied" for command in original}
    outcomes[original[1].operation_key] = "rejected_precondition"
    outcomes[original[-1].operation_key] = "rejected_precondition"
    transition = geo_dependency_repair_commands(
        original, settled_outcomes=outcomes, artifact_sha256=pins.artifact_sha256)[0]
    assert api_registry.resolve(transition).transition.target == GEO_V2_REVISIONS
    with pytest.raises(ApplyConflict, match="no exact semantic handler"):
        native_registry.resolve(transition)

    # Exercise the existing installer against a real queue's pinned launch,
    # without starting services, making grants, sealing or dispatching anything.
    installer = SourceGeoInstaller.__new__(SourceGeoInstaller)
    installer.queue = queue
    installer.upgrade_pending = True
    assert installer.needs_native_upgrade(SimpleNamespace(run_id=run_id))
    with pytest.raises(ValueError, match="initial runtime expectation is immutable"):
        queue.expect_search_runtime(run_id, replace(
            initial, schema_revision=GEO_V2_REVISIONS.schema,
            function_revision=GEO_V2_REVISIONS.function,
            executor_revision=GEO_V2_REVISIONS.executor))
