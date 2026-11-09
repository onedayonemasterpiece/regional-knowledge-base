"""Fictional provider races; no corpus, runtime, model or external effects."""
import json
from dataclasses import replace
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_publisher import KaggleDatasetPublisher
from regional_knowledge.kaggle_dataset_checkpoint import SourceDatasetCheckpoints
from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_pg_snapshot_native import encode, sha
from regional_knowledge.recovery_journal import ObjectRef, JournalConflict
from test_dataset_recovery_store import prepared
from test_kaggle_dataset_publisher import setup


@pytest.mark.parametrize('old_first', [False, True])
def test_late_old_publication_cannot_hide_new_exact_snapshot(old_first):
    api, publisher, files = setup()
    api.version = 7
    history = {}
    mutations = []

    def commit(operation, description):
        api.version += 1
        api.description = description
        history[api.version] = dict(operation=operation, description=description)
        mutations.append(operation)

    def effect(request):
        if old_first:
            commit('old-unknown', 'old package')
        commit(request.body.version_notes, request.body.description)
        if not old_first:
            commit('old-unknown', 'old package')

    def metadata(request):
        return NS(current_version_number=api.version, is_private=True,
            versions=[NS(version_number=v, version_notes=d['operation'], status='ready')
                      for v, d in history.items()])

    def listing(ref, **kwargs):
        version = int(ref.rsplit('/', 1)[1])
        assert history[version]['operation'] == 'new-snapshot'
        return NS(files=[NS(name=f.name, total_bytes=f.size, description=f.description)
                         for f in files], next_page_token=None)

    api.create_dataset_version = effect
    api.get_dataset = metadata
    api.dataset_list_files = listing
    version = publisher.finalize(files=files, operation='new-snapshot', previous_version=7)
    assert version == (9 if old_first else 8)
    assert history[version]['operation'] == 'new-snapshot'
    assert mutations.count('new-snapshot') == 1
    assert mutations.count('old-unknown') == 1  # Provider completes its original request.
    if not old_first:
        assert publisher.reconcile(files=files, operation='new-snapshot', expected_version=version)
        api.dataset_list_files = lambda *a, **k: NS(files=[NS(name=f.name,
            total_bytes=f.size, description='wrong hash') for f in files], next_page_token=None)
        assert not publisher.reconcile(files=files, operation='new-snapshot', expected_version=version)


@pytest.mark.asyncio
@pytest.mark.parametrize('writer_current,publication_finishes', [(True, True), (False, True), (True, False)])
async def test_new_prefix_saves_without_repeating_or_clearing_old_unknown(tmp_path, monkeypatch, writer_current, publication_finishes):
    p = prepared(tmp_path)
    old_operation = 'dataset-checkpoint:old-unknown'
    old_package = json.dumps(dict(base_sequence=8, previous_version=1, files=['retained-old-grant']))
    control = encode(dict(database_generation='db', sequence=9))
    archive = replace(p.checkpoint.archive, name='pg-' + 'a'*64 + '.blob')
    manifest = encode(dict(database_generation='db', applied_sequence=9,
        blob=dict(size=archive.size, sha256=archive.sha256),
        source_control=dict(key=p.prefix+'source-base-control/'+sha(control), sha256=sha(control))))
    journal = p.store.export_control()
    def pin(name, data): return DatasetFile(name, len(data), sha(data))
    fresh = replace(p.checkpoint, dataset_version=2, base_sequence=9, archive=archive,
        manifest=pin('base-manifest.json', manifest), control=pin('source-control.json', control),
        journal=pin('journal-control.json', journal))
    class Publisher:
        ref = p.publisher.ref
        def published_version(self, *, operation): return None
        def version_created_at(self, version): return 0.
        def read_control(self, file, *, version):
            assert version == 2
            return {'base-manifest.json': manifest, 'source-control.json': control,
                    'journal-control.json': journal}[file.name]
    p.store.publisher = Publisher()
    with p.q.connect() as db:
        db.execute('CREATE TABLE rkb_control_spool_meta(id INTEGER PRIMARY KEY,sequence INTEGER)')
        db.execute('INSERT INTO rkb_control_spool_meta VALUES(1,9)')
        db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,sequence INTEGER,seal_state TEXT,apply_state TEXT,outcome_state TEXT)')
        db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',
            (old_operation, Publisher.ref, 'publishing', old_package, None))
    versions = dict(schema_version=16, server_version_num=170010, pgvector_version='0.8.2',
        model_sha256='b'*64, tokenizer_sha256='c'*64, schema_revision='schema11',
        function_revision='geo6', executor_revision='executor1')
    startup = NS(config=NS(queue=p.q, spool=NS(journal=NS(store=p.store,
        prefix=p.prefix.rstrip('/'))), source_restore=NS(versions=versions),
        history_adapter=NS(database=NS(**versions))), coverage=NS(applied_sequence=9,
        accepted_sequence=9), checkpoint_binding_current=lambda b: writer_current)
    clock = [1801.]
    owner = SourceDatasetCheckpoints(startup, clock=lambda: clock[0], wall_clock=lambda: clock[0])
    calls = []
    async def capture(binding, operation, latest, **kwargs):
        assert operation != old_operation, 'Unknown old operation must not block or be resubmitted'
        calls.append(operation)
        if not publication_finishes:
            with p.q.connect() as db:
                db.execute('INSERT INTO rkb_dataset_seed VALUES(?,?,?,?,?)',
                    (operation, Publisher.ref, 'publishing', json.dumps(dict(base_sequence=9)), None))
            from regional_knowledge.kaggle_dataset_publisher import DatasetPublicationError
            raise DatasetPublicationError('dataset_publication_pending_readback')
        ref = ObjectRef(p.prefix+'snapshots/manifests/'+fresh.manifest.sha256,
            fresh.manifest.sha256, fresh.manifest.size, 9)
        observed = dict(dataset_ref=Publisher.ref, dataset_version=2, base_sequence=9,
            archive_sha256=archive.sha256, archive_size=archive.size)
        return fresh, observed, ref, 2
    monkeypatch.setattr(owner, '_capture_verify', capture)
    if not writer_current:
        from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
        with pytest.raises(CheckpointPending, match='writer_changed'):
            await owner.refresh('fictional-binding')
        assert calls == [] and p.store.latest_checkpoint == p.checkpoint
        return
    if not publication_finishes:
        from regional_knowledge.kaggle_dataset_checkpoint import CheckpointPending
        with pytest.raises(CheckpointPending):
            await owner.refresh('fictional-binding')
        startup.coverage = NS(applied_sequence=10, accepted_sequence=10)
        clock[0] += 1
        assert not await owner.refresh('fictional-binding')
        assert len(calls) == 1, 'New pages must not create another upload before the existing interval'
        assert p.store.latest_checkpoint == p.checkpoint
        return
    assert await owner.refresh('fictional-binding')
    assert p.store.latest_checkpoint == fresh and len(calls) == 1
    with p.q.connect(write=False) as db:
        row = db.execute('SELECT state,package_json,version FROM rkb_dataset_seed WHERE operation_key=?',
            (old_operation,)).fetchone()
        assert tuple(row) == ('publishing', old_package, None)
    # Even if the old provider request finishes later as version3, existing
    # verified-pointer fencing rejects its older accepted prefix.
    late_old = replace(fresh, dataset_version=3, base_sequence=8)
    observed = dict(dataset_ref=Publisher.ref, dataset_version=3, base_sequence=8,
        archive_sha256=archive.sha256, archive_size=archive.size)
    with pytest.raises(JournalConflict, match='promotion_predecessor'):
        p.store.promote(late_old, observed, manifest_ref=p.ref, current=lambda: True)
    assert p.store.latest_checkpoint == fresh
