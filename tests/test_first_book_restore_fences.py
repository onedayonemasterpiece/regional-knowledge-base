"""Dataset2 metadata is provenance; native result identity remains required."""
import pytest
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError
from test_first_book_restore import scenario
from test_control_spool import env
from test_participants import people


@pytest.mark.asyncio
async def test_wrong_native_result_version_cannot_create_restored_capability(env):
    s=await scenario(env)
    original=s.native.verify_initialized
    async def behind(*args,**kwargs):
        value=await original(*args,**kwargs)
        return dict(value,schema_version=11)
    s.native.verify_initialized=behind
    with pytest.raises(SnapshotError):
        await s.physical.recover(s.manifest,s.binding,database=s.database,expected_versions=s.versions,
            expected_schema_version=s.database.schema_version,
            identity=('fixture-run','fixture-nonce','fixture-incarnation'),operation_key='behind-version')
    assert s.store.latest_checkpoint.dataset_version==2
