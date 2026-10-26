"""Session-artifact root regressions: synthetic bytes only; no BGE, PG, Kaggle or network."""
import ast
from dataclasses import replace
import os
from pathlib import Path
import textwrap
import time
from types import SimpleNamespace

import pytest

import regional_knowledge.kaggle_stage0 as stage0
import regional_knowledge.kaggle_native_bundle as native
from regional_knowledge.kaggle_stage0 import Stage0Spec, prepare_stage0, Stage0Error
from regional_knowledge.kaggle_native_bundle import NativeBundleError
from regional_knowledge.kaggle_target_runtime import TargetLaunchSpec
from test_kaggle_stage0 import Opener, download, prepared
from test_kaggle_target_runtime import spec, generate, queue_run


def selected_root(tmp_path, monkeypatch):
    provider = tmp_path/'kaggle'
    temp = provider/'temp'
    root = temp/'rkb-artifacts'
    for directory in (provider, temp, root):
        directory.mkdir(mode=0o711)
        directory.chmod(0o711)
    monkeypatch.setattr(stage0, 'TEMP_ARTIFACT_ROOT', root)
    monkeypatch.setattr(native, 'TEMP_ARTIFACT_ROOT', root)
    return root


def test_selected_temp_download_and_native_root(tmp_path, monkeypatch):
    root = selected_root(tmp_path, monkeypatch)
    item = download(name='model/pytorch_model.bin', data=b'synthetic weight')
    plan = Stage0Spec((item,), (), {'fixture-package': '1.2.3'}, 0)
    opener = Opener(b'synthetic weight')
    monkeypatch.setattr(stage0.urllib.request, 'build_opener', lambda *args: opener)
    monkeypatch.setattr(stage0, '_versions', lambda packages: dict(packages))
    result = prepare_stage0(prepared(root, plan), plan.wire())
    assert result['downloaded_bytes'] == len(b'synthetic weight')
    assert (root/item.name).read_bytes() == b'synthetic weight'
    assert list(root.glob('.rkb-stage0-*.json'))
    fd = native._open_root(root)
    os.close(fd)



def test_selected_temp_model_directory_uses_same_verified_root(tmp_path, monkeypatch):
    from regional_knowledge.kaggle_bootstrap import PreparedArtifact, verify_artifacts
    from regional_knowledge.kaggle_readiness import ArtifactRequirement
    import regional_knowledge.kaggle_bootstrap as bootstrap
    import hashlib
    root = selected_root(tmp_path, monkeypatch)
    bodies = {
        'model/tokenizer.json': (b'fixture tokenizer', 'tokenizer'),
        'model/config.json': (b'fixture config', 'model_config'),
        'model/pytorch_model.bin': (b'fixture weight', 'weight'),
        'native/pinned.so': (b'fixture native', 'code'),
    }
    entries = []
    for name, (data, role) in bodies.items():
        path = root/name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(data)
        entries.append(PreparedArtifact(ArtifactRequirement(name,len(data),hashlib.sha256(data).hexdigest()),role))
    monkeypatch.setattr(bootstrap, 'TOKENIZER_SHA256', hashlib.sha256(b'fixture tokenizer').hexdigest())
    verified = verify_artifacts(root, entries, max_total_bytes=sum(len(data) for data,_ in bodies.values()))
    assert verified.root == root and verified.model_directory == root/'model'
    assert (root/'model/pytorch_model.bin').read_bytes() == b'fixture weight'

def test_selected_temp_pip_target_is_private_and_legacy_unchanged(tmp_path, monkeypatch):
    root = selected_root(tmp_path, monkeypatch)
    wheel = download(name='wheels/fixture-1.2.3-py3-none-any.whl', data=b'wheel')
    plan = Stage0Spec((wheel,), (wheel.name,), {'fixture': '1.2.3'}, 0)
    file = root/wheel.name
    file.parent.mkdir()
    file.write_bytes(b'wheel')
    inventory = stage0._inventory(prepared(root, plan))
    installed = [False]
    monkeypatch.setattr(stage0, '_versions', lambda packages: dict(packages) if installed[0] else {'fixture':'old'})
    calls = []
    def pip(command, **options):
        calls.append(command)
        assert command[command.index('--target')+1] == str(root/'.rkb-site')
        assert '--break-system-packages' not in command and '--no-index' in command
        installed[0] = True
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(stage0.subprocess, 'run', pip)
    monkeypatch.setattr(stage0.sys, 'path', stage0.sys.path.copy())
    fd = os.open(root, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        assert stage0._install(fd, root, plan, inventory, time.monotonic()+30)
    finally:
        os.close(fd)
    assert calls and (root/'.rkb-site').is_dir()
    assert str(root/'.rkb-site') in stage0.sys.path
    # The legacy global pip path remains covered by test_kaggle_stage0.py.


def test_invalid_arbitrary_symlink_and_writable_temp_roots(tmp_path, monkeypatch):
    root = selected_root(tmp_path, monkeypatch)
    plan = Stage0Spec((download(),), (), {'fixture-package':'1.2.3'}, 0)
    arbitrary = tmp_path/'arbitrary'
    arbitrary.mkdir()
    with pytest.raises(ValueError, match='stage0_prepared_inventory_required'):
        stage0._inventory(prepared(arbitrary, plan))
    with pytest.raises(NativeBundleError, match='native_bundle_root_invalid'):
        native._open_root(arbitrary)
    with pytest.raises(ValueError, match='bounded prepared Kaggle inventory'):
        replace(spec(), artifact_root=str(arbitrary))
    link = root.parent/'link'
    link.symlink_to(root, target_is_directory=True)
    monkeypatch.setattr(stage0, 'TEMP_ARTIFACT_ROOT', link)
    monkeypatch.setattr(native, 'TEMP_ARTIFACT_ROOT', link)
    with pytest.raises(Stage0Error):
        prepare_stage0(prepared(link, plan), plan.wire())
    with pytest.raises(NativeBundleError, match='native_bundle_root_invalid'):
        native._open_root(link)
    monkeypatch.setattr(stage0, 'TEMP_ARTIFACT_ROOT', root)
    monkeypatch.setattr(native, 'TEMP_ARTIFACT_ROOT', root)
    root.parent.chmod(0o777)
    try:
        with pytest.raises(Stage0Error):
            prepare_stage0(prepared(root, plan), plan.wire())
        with pytest.raises(NativeBundleError, match='native_bundle_root_invalid'):
            native._open_root(root)
    finally:
        root.parent.chmod(0o711)


def generated_guard(source, provider):
    # Exercise the exact stdlib guard shipped inside the generated notebook,
    # substituting only a fixture provider prefix (no /kaggle touched on Dev).
    start = source.index('    _root = _Path(')
    end = source.index('    try:\n        _root.mkdir', start)
    return textwrap.dedent(source[start:end]).replace('/kaggle', str(provider))


@pytest.mark.parametrize('root_name', ['working', 'temp/rkb-artifacts'])
def test_run_config_and_cold_root_bootstrap(tmp_path, root_name):
    provider = tmp_path/'kaggle'
    provider.mkdir(mode=0o711)
    provider.chmod(0o711)
    root = '/kaggle/'+root_name
    queue, run = queue_run(tmp_path)
    target = replace(spec(), artifact_root=root)
    source = generate(queue, run, target)
    compile(source, 'generated-notebook-fixture', 'exec')
    config = ast.literal_eval(next(node.value for node in ast.parse(source).body
        if isinstance(node, ast.Assign) and any(isinstance(name, ast.Name) and name.id=='RUN_CONFIG'
            for name in node.targets)))
    assert config['prepared_model']['root'] == root
    assert config['target_runtime']['artifact_root'] == root
    assert config['target_runtime']['sql_root'].startswith(root+'/r-')
    if root_name == 'working':
        (provider/'working').mkdir()
    fixture_root = provider/root_name
    exec(generated_guard(source, provider),
         {'_Path':Path, 'RUN_CONFIG':{'prepared_model':{'root':str(fixture_root)}}})
    assert fixture_root.is_dir()
    assert (provider/'temp').exists() == (root_name!='working')
    if root_name!='working':
        assert fixture_root.stat().st_mode&0o777 == 0o711


@pytest.mark.parametrize('escape', ['temp-link', 'artifact-link', 'writable-parent'])
def test_generated_temp_root_rejects_symlink_escape_and_untrusted_parent(tmp_path, escape):
    provider = tmp_path/'kaggle'
    provider.mkdir(mode=0o711)
    provider.chmod(0o711)
    outside = tmp_path/'outside'
    outside.mkdir()
    if escape == 'temp-link':
        (provider/'temp').symlink_to(outside, target_is_directory=True)
    else:
        temp = provider/'temp'
        temp.mkdir(mode=0o711)
        temp.chmod(0o711)
        if escape == 'artifact-link':
            (temp/'rkb-artifacts').symlink_to(outside, target_is_directory=True)
        else:
            temp.chmod(0o777)
    queue, run = queue_run(tmp_path)
    source = generate(queue, run, replace(spec(), artifact_root='/kaggle/temp/rkb-artifacts'))
    with pytest.raises((OSError, RuntimeError)):
        exec(generated_guard(source, provider),
             {'_Path':Path, 'RUN_CONFIG':{'prepared_model':{'root':str(provider/'temp/rkb-artifacts')}}})
    assert not (outside/'r-anything').exists()
