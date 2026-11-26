"""Resume an independently committed publication without another provider effect."""
import pytest

from test_kaggle_dataset_checkpoint import control_cycle, put_control


@pytest.mark.asyncio
async def test_newer_pages_do_not_hide_a_committed_snapshot(tmp_path, monkeypatch):
    from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
    p = control_cycle(tmp_path, monkeypatch)
    await put_control(p)
    p.clock[0] = 1801
    p.fail_finalize = True
    with pytest.raises(CheckpointPending):
        await p.owner.refresh(p.binding)
    assert p.calls == ['capture', 'finalize']
    assert p.store.latest_checkpoint.dataset_version == 1
    # Kaggle has committed the first request; ordinary page work has advanced.
    p.proof.applied_sequence = p.proof.accepted_sequence = 8
    p.startup.config.spool._source_covered_sequence = 8
    with p.q.connect() as db:
        db.execute('UPDATE rkb_control_spool_meta SET sequence=8')
    def published_version(*, operation):
        return next((version for version, saved in p.saved.items() if saved[0] == operation), None)
    p.store.publisher.published_version = published_version
    assert await p.owner.refresh(p.binding)
    assert p.calls == ['capture', 'finalize', 'reconcile', 'verify']
    assert p.store.latest_checkpoint.dataset_version == 2
    assert p.store.latest_checkpoint.base_sequence == 7  # New page8 remains pending.
    with p.q.connect(write=False) as db:
        row = db.execute("SELECT state,version FROM rkb_dataset_seed WHERE operation_key LIKE 'dataset-checkpoint:%'").fetchone()
        assert tuple(row) == ('verified', 2)


@pytest.mark.asyncio
async def test_background_log_exposes_cause_class_without_exception_payload(caplog):
    from regional_knowledge.kaggle_dataset_checkpoint import SourceDatasetCheckpoints, CheckpointPending
    owner = SourceDatasetCheckpoints.__new__(SourceDatasetCheckpoints)
    owner._force_after = False
    async def refresh(binding, **kwargs):
        try:
            raise ValueError('PRIVATE-PROVIDER-TOKEN')
        except ValueError as error:
            raise CheckpointPending('ValueError', 'PRIVATE-PROVIDER-TOKEN') from error
    owner.refresh = refresh
    await owner._background(None, planned_stop=False)
    assert 'cause_type=ValueError' in caplog.text
    assert 'PRIVATE-PROVIDER-TOKEN' not in caplog.text
