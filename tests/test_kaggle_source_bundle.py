"""The shipped notebook package imports without the Dev checkout or inference."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

from regional_knowledge.kaggle_target_launch import MODULES, source_bundle


def test_shipped_package_import_closure_is_self_contained(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    _, files, _ = source_bundle(repository)
    for relative, encoded in files.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(encoded, validate=True))
    # -I excludes cwd/PYTHONPATH. The package's own path must resolve only to
    # the shipped bundle; installed third-party dependencies remain available.
    script = """
import importlib,json,pathlib,socket,sys
root=pathlib.Path(sys.argv[1]).resolve()
def denied(*args,**kwargs):raise AssertionError('bundle import attempted network')
socket.socket.connect=denied
socket.create_connection=denied
sys.path.insert(0,str(root))
package=importlib.import_module('regional_knowledge')
assert list(package.__path__)==[str(root/'regional_knowledge')]
for name in json.loads(sys.argv[2]):
    loaded=importlib.import_module('regional_knowledge.'+name)
    assert pathlib.Path(loaded.__file__).resolve().is_relative_to(root)
assert not {'torch','transformers','tensorflow','flax'} & sys.modules.keys()
"""
    completed = subprocess.run(
        [sys.executable, '-I', '-c', script, str(tmp_path), json.dumps(MODULES)],
        cwd=tmp_path, env={'PATH': os.defpath}, capture_output=True,
        timeout=20, text=True,
    )
    assert completed.returncode == 0, completed.stderr[-4000:]
