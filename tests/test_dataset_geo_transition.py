"""GEO data repair keeps Dataset pending semantics and creates no DDL command."""
import pytest
from regional_knowledge.geo_manifest_commands import geo_dependency_repair_commands,plan_geo_manifest
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_pg_snapshot_native import encode,sha
from test_dataset_recovery_store import prepared
from test_geo_manifest_commands import commands
from test_geo_migration import snapshot


@pytest.mark.asyncio
async def test_ordinary_repair_has_new_keys_without_mutating_originals_or_saving_pending_outcome(tmp_path):
    p=prepared(tmp_path)
    originals=commands(plan_geo_manifest(build_geo_whitelist(**snapshot())))
    before=[c.encode() for c in originals]
    outcomes={c.operation_key:('applied' if n==0 else 'rejected_precondition') for n,c in enumerate(originals)}
    repair=geo_dependency_repair_commands(originals,settled_outcomes=outcomes)
    assert [c.sequence for c in repair]==list(range(originals[-1].sequence+1,originals[-1].sequence+len(repair)+1))
    assert all(c.kind=='domain_command' and c.route=='geo.manifest.install.v1' for c in repair)
    assert [c.payload for c in repair]==[c.payload for c in originals[1:]]
    assert [c.encode() for c in originals]==before
    command=repair[0];body=encode({'operation_key':command.operation_key,'status':'applied'})
    key=p.prefix+'outcomes/'+sha(command.operation_key.encode())
    assert await p.store.compare_and_swap(key,body,expected_token=None,writer_generation='writer')
    assert not p.store.remote_durable(key,body)
