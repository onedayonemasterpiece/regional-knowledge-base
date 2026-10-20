"""Pinned synthetic native libraries only; no native executable is run."""
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_native_env import verified_native_environment
from regional_knowledge.kaggle_native_bundle import prepare_native_bundle,NativeBundleError
from test_kaggle_native_bundle import fixture as native_fixture,sha


def test_native_environment_uses_only_retained_verified_library_directories(native_fixture,monkeypatch):
    name='usr/lib/x86_64-linux-gnu/libfixture.so.1'
    root,spec,inventory,_=native_fixture(entries=[(name,'file',b'library')],files=[dict(
        path=name,size=7,sha256=sha(b'library'),package='fixture',member=name,executable=False)])
    cap=prepare_native_bundle(root,spec.manifest_name,spec.manifest_sha256,inventory=inventory)
    monkeypatch.setenv('LD_PRELOAD','untrusted.so');monkeypatch.setenv('LD_LIBRARY_PATH','/untrusted')
    env=verified_native_environment(cap)
    assert env==dict(PATH='/usr/bin:/bin',LANG='C',LC_ALL='C',
        LD_LIBRARY_PATH=str(cap.prefix/'usr/lib/x86_64-linux-gnu'))
    with pytest.raises(NativeBundleError):verified_native_environment(SimpleNamespace(prefix=cap.prefix,current=lambda:True))
    Path(cap.pins[0][0]).chmod(0o600);Path(cap.pins[0][0]).write_bytes(b'changed')
    with pytest.raises(NativeBundleError):verified_native_environment(cap)
