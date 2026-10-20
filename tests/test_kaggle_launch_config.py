"""Target metadata parsing only; controller rejects absent authority before effects."""
import json

import pytest

from regional_knowledge.kaggle_launch_config import load_target_launch,target_launch_from_wire
from test_bge_wss_worker import module
from test_kaggle_target_runtime import bundled_spec,stage0
from dataclasses import replace


def test_target_metadata_roundtrip_retains_all_typed_pins(tmp_path):
    target=bundled_spec();target=replace(target,stage0=stage0(target))
    path=tmp_path/'target.json';path.write_text(json.dumps(target.wire()))
    loaded=load_target_launch(path)
    assert loaded.wire()==target.wire()
    assert type(loaded.stage0) is type(target.stage0)
    assert type(loaded.native_bundle) is type(target.native_bundle)
    assert type(loaded.database) is type(target.database)
    with pytest.raises(ValueError,match='exact target'):
        target_launch_from_wire(dict(target.wire(),sql='SELECT 1'))
    path.write_text('{"database":{},"database":{}}')
    with pytest.raises(ValueError,match='duplicate'):load_target_launch(path)
    path.write_bytes(b'x'*65537)
    with pytest.raises(ValueError,match='bounded regular'):load_target_launch(path)
    alias=tmp_path/'symlink.json';alias.symlink_to(path)
    with pytest.raises(OSError):load_target_launch(alias)


def test_existing_controller_target_file_needs_real_journal_before_provider(tmp_path,monkeypatch):
    controller=module('bge_kaggle_controller')
    target=bundled_spec();target=replace(target,stage0=stage0(target))
    path=tmp_path/'target.json';path.write_text(json.dumps(target.wire()))
    monkeypatch.setenv('RKB_BGE_TRANSPORT','wss')
    monkeypatch.setenv('RKB_BGE_TARGET_CONFIG',str(path))
    with pytest.raises(TypeError,match='existing real RecoveryJournal'):
        controller.main()
    with pytest.raises(ValueError,match='one explicit'):
        controller.main(target_spec=target)
