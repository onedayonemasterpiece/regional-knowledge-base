"""Restart chooses existing verified metadata, without provider or archive I/O."""
from dataclasses import replace
import json

import pytest

from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_pg_snapshot_native import encode, sha
from regional_knowledge.kaggle_restore_startup import SourceRestoreConfig
from regional_knowledge.kaggle_source_owners import _verified_restart_restore
from regional_knowledge.recovery_journal import JournalConflict
from test_dataset_recovery_store import prepared


def setup(tmp_path):
    p=prepared(tmp_path)
    versions=dict(server_version_num=170011,pgvector_version='0.8.2',
                  model_sha256='a'*64,tokenizer_sha256='b'*64,schema_version=42)
    config=SourceRestoreConfig(p.ref,'restore-fixture',versions,'c'*64,
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
def test_unverified_or_inconsistent_manifest_cannot_promote(tmp_path,bad):
    p,config,latest,key=setup(tmp_path)
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
