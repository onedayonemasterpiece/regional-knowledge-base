"""Restart chooses existing verified metadata, without provider or archive I/O."""
from dataclasses import replace
import json

import pytest

from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_pg_snapshot_native import compatible_versions, encode, sha
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig
from regional_knowledge.kaggle_source_owners import _verified_restart_restore
from regional_knowledge.recovery_journal import JournalConflict, ObjectRef
from test_dataset_recovery_store import prepared


def setup(tmp_path,*,base_vector='0.8.2',selected_vector='0.8.2',base_schema=42):
    p=prepared(tmp_path)
    versions=dict(server_version_num=170011,pgvector_version=base_vector,
                  model_sha256='a'*64,tokenizer_sha256='b'*64,schema_version=base_schema)
    config=SourceRestoreConfig(p.ref,'restore-fixture',versions|{'pgvector_version':selected_vector},'c'*64,
        ('https://storage.googleapis.com',),dataset_checkpoint=p.checkpoint)
    raw=encode(dict(database_generation='db',applied_sequence=9,versions=versions))
    latest=replace(p.checkpoint,dataset_version=2,base_sequence=9,
        manifest=DatasetFile('base-manifest.json',len(raw),sha(raw)))
    key=p.prefix+'snapshots/manifests/'+sha(raw)
    with p.q.connect() as db:
        db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=?',
                   (json.dumps(latest.wire()),))
        db.execute('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,?)',
                   (p.store.storage_identity,key,raw,'verified-manifest',2))
    return p,config,latest,key


@pytest.mark.parametrize('base_vector,selected_vector',[
    ('0.8.2','0.8.2'),('0.8.2','0.8.4'),('0.8.4','0.8.4'),
])
def test_newest_saved_preserves_selected_runtime_without_rewriting_base(tmp_path,base_vector,selected_vector):
    p,config,latest,key=setup(tmp_path,base_vector=base_vector,selected_vector=selected_vector)
    before=list(p.reads)
    resumed=_verified_restart_restore(p.q,config,'stream')
    assert resumed.dataset_checkpoint==latest and resumed.versions['pgvector_version']==selected_vector
    assert resumed.manifest.key==key and p.reads==before
    with p.q.connect(write=False) as db:
        raw=db.execute('SELECT data FROM rkb_dataset_recovery_objects WHERE key=?',(key,)).fetchone()[0]
    assert json.loads(raw)['versions']['pgvector_version']==base_vector
    assert _verified_restart_restore(p.q,resumed,'stream') is resumed


@pytest.mark.parametrize('already_selected',[False,True])
def test_newest_saved_cannot_downgrade_selected_runtime(tmp_path,already_selected):
    p,config,latest,key=setup(tmp_path,base_vector='0.8.4',selected_vector='0.8.2')
    if already_selected:
        config=replace(config,dataset_checkpoint=latest,
            manifest=ObjectRef(key,latest.manifest.sha256,latest.manifest.size,latest.base_sequence))
    with pytest.raises(JournalConflict,match='pgvector_selection_changed'):
        _verified_restart_restore(p.q,config,'stream')


def test_stale_restart_selects_exact_verified_checkpoint_without_provider_io(tmp_path):
    p,config,latest,key=setup(tmp_path)
    before=list(p.reads)
    resumed=_verified_restart_restore(p.q,config,'stream')
    assert resumed.dataset_checkpoint==latest
    assert resumed.manifest.key==key and resumed.manifest.last_sequence==9
    assert resumed.operation_key==config.operation_key
    assert resumed.launch_config_sha256==config.launch_config_sha256
    assert p.reads==before
    # Selection is read-only. The existing store constructor still owns promotion.
    with p.q.connect() as db:
        row=db.execute('SELECT checkpoint_json FROM rkb_dataset_recovery_store').fetchone()
        assert json.loads(row['checkpoint_json'])==config.dataset_checkpoint.wire()
    assert _verified_restart_restore(p.q,resumed,'stream') is resumed


@pytest.mark.parametrize('bad',['bytes','coverage','missing','database','cursor'])
@pytest.mark.parametrize('already_selected',[False,True])
def test_unverified_or_inconsistent_manifest_cannot_promote(tmp_path,bad,already_selected):
    p,config,latest,key=setup(tmp_path)
    if already_selected:
        config=replace(config,dataset_checkpoint=latest,
            manifest=ObjectRef(key,latest.manifest.sha256,latest.manifest.size,latest.base_sequence))
    with p.q.connect() as db:
        if bad=='bytes':
            db.execute('UPDATE rkb_dataset_recovery_objects SET data=? WHERE key=?',(b'corrupt',key))
        elif bad=='coverage':
            db.execute('UPDATE rkb_dataset_recovery_objects SET covered_version=NULL WHERE key=?',(key,))
        elif bad=='missing':
            db.execute('DELETE FROM rkb_dataset_recovery_objects WHERE key=?',(key,))
        else:
            changed=replace(latest,**({'database_generation':'foreign-db'} if bad=='database' else {'base_sequence':10}))
            db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=?',(json.dumps(changed.wire()),))
    with pytest.raises(JournalConflict):_verified_restart_restore(p.q,config,'stream')


def test_no_verified_advance_and_other_stream_leave_config_unchanged(tmp_path):
    p,config,latest,key=setup(tmp_path)
    assert _verified_restart_restore(p.q,config,'other-stream') is config
    newer=replace(config,dataset_checkpoint=replace(config.dataset_checkpoint,dataset_version=3))
    assert _verified_restart_restore(p.q,newer,'stream') is newer
    with p.q.connect() as db:db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=NULL')
    assert _verified_restart_restore(p.q,config,'stream') is config


def test_verified_advance_cannot_change_pinned_model(tmp_path):
    p,config,latest,key=setup(tmp_path)
    changed=replace(config,versions={**config.versions,'model_sha256':'d'*64})
    with pytest.raises(JournalConflict,match='versions_changed'):
        _verified_restart_restore(p.q,changed,'stream')


@pytest.mark.parametrize('selected_vector',['0.8.2','0.8.4'])
def test_already_selected_checkpoint_refreshes_stale_template_schema_from_verified_manifest(tmp_path,selected_vector):
    """Actual target-binding shape: latest base selected, old template pins kept."""
    p,config,latest,key=setup(tmp_path,base_schema=44,selected_vector=selected_vector)
    selected=replace(config,dataset_checkpoint=latest,
        manifest=ObjectRef(key,latest.manifest.sha256,latest.manifest.size,latest.base_sequence),
        versions={**config.versions,'schema_version':42})
    before=list(p.reads)
    with p.q.connect(write=False) as db:
        raw=db.execute('SELECT data FROM rkb_dataset_recovery_objects WHERE key=?',(key,)).fetchone()[0]
    base=json.loads(raw)['versions']
    assert not compatible_versions(base,dict(selected.versions))
    resumed=_verified_restart_restore(p.q,selected,'stream')
    assert compatible_versions(base,dict(resumed.versions))
    assert resumed.versions['schema_version']==44
    assert resumed.versions['pgvector_version']==selected_vector
    assert resumed.dataset_checkpoint==latest and resumed.manifest==selected.manifest
    assert resumed.operation_key==selected.operation_key
    assert resumed.launch_config_sha256==selected.launch_config_sha256
    assert selected.versions['schema_version']==42 and p.reads==before
    assert _verified_restart_restore(p.q,resumed,'stream') is resumed
    with p.q.connect(write=False) as db:
        assert db.execute('SELECT data FROM rkb_dataset_recovery_objects WHERE key=?',(key,)).fetchone()[0]==raw


def test_equal_dataset_version_cannot_select_different_checkpoint_identity(tmp_path):
    p,config,latest,key=setup(tmp_path)
    changed=replace(config,dataset_checkpoint=replace(config.dataset_checkpoint,dataset_version=latest.dataset_version))
    with pytest.raises(JournalConflict,match='checkpoint_identity_changed'):
        _verified_restart_restore(p.q,changed,'stream')


def test_already_selected_older_base_keeps_target_schema_migration_separate(tmp_path):
    p,config,latest,key=setup(tmp_path,base_schema=42)
    selected=replace(config,dataset_checkpoint=latest,
        manifest=ObjectRef(key,latest.manifest.sha256,latest.manifest.size,latest.base_sequence),
        versions={**config.versions,'schema_version':44})
    resumed=_verified_restart_restore(p.q,selected,'stream')
    assert resumed.versions['schema_version']==42
    assert resumed.dataset_checkpoint==latest and resumed.manifest==selected.manifest
    assert selected.versions['schema_version']==44
    # The independent target schema remains the native migration/readiness
    # requirement. An older verified base remains physically compatible.
    assert compatible_versions(dict(resumed.versions),dict(selected.versions))


@pytest.mark.parametrize('field,value',[
    ('model_sha256','d'*64),('tokenizer_sha256','d'*64),
    ('protocol_version',2),('postgis_version','3.5'),
])
def test_equal_checkpoint_schema_refresh_cannot_change_runtime_abi(tmp_path,field,value):
    p,config,latest,key=setup(tmp_path,base_schema=42)
    selected=replace(config,dataset_checkpoint=latest,
        manifest=ObjectRef(key,latest.manifest.sha256,latest.manifest.size,latest.base_sequence),
        versions={**config.versions,'schema_version':44,field:value})
    with pytest.raises(JournalConflict,match='versions_changed'):
        _verified_restart_restore(p.q,selected,'stream')


@pytest.mark.parametrize('legacy',[False,True])
def test_store_constructor_can_initialize_or_upgrade_metadata(tmp_path,legacy):
    p,config,latest,key=setup(tmp_path)
    with p.q.connect() as db:
        db.execute('ALTER TABLE rkb_dataset_recovery_store DROP COLUMN latest_json' if legacy
                   else 'DROP TABLE rkb_dataset_recovery_store')
    assert _verified_restart_restore(p.q,config,'stream') is config
