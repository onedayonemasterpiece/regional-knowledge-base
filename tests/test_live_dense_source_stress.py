"""Safety and accounting tests for the private-fixture live dense stress runner."""
import importlib.util
import struct
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location(
    "live_dense_source_stress",
    ROOT/"scripts/benchmarks/live_dense_source_stress.py",
)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_percentiles_and_source_proof_hit_cutoffs():
    rows=[
        {"rank":1,"ready":True,"wrong_doc":False},
        {"rank":5,"ready":True,"wrong_doc":False},
        {"rank":10,"ready":True,"wrong_doc":True},
        {"rank":None,"ready":False,"wrong_doc":False},
    ]
    assert module.percentile([0,100,200],.5)==100
    assert module.hits(rows)=={
        "n":4,"hit1":.25,"hit5":.5,"hit10":.75,
        "ready_fraction":.75,"wrong_document_top1":.25,
    }
    assert module.hits([])=={"n":0}


def test_private_fixture_and_results_are_forbidden_inside_git(tmp_path):
    with pytest.raises(ValueError,match="may not be stored in Git checkout"):
        module.authorized_private_path(ROOT/"tests"/"test_live_dense_source_stress.py")
    assert module.authorized_private_path(tmp_path/"benchmark.json")==(tmp_path/"benchmark.json").resolve()


def _fixture_npy(path,rows):
    shape=(rows,1024)
    header=repr({"descr":"<f4","fortran_order":False,"shape":shape})
    # NPY v1 header must make the whole prefix a multiple of 16.
    pad=(-(10+len(header)+1))%16
    packed=(header+" "*pad+"\n").encode("ascii")
    path.write_bytes(b"\x93NUMPY"+bytes([1,0])+struct.pack("<H",len(packed))
        +packed+struct.pack("<"+str(rows*1024)+"f",*([0.0]*(rows*1024))))


def test_frozen_matrix_rejects_wrong_shape_and_incomplete_input(tmp_path):
    name=tmp_path/"matrix.npy"
    _fixture_npy(name,2)
    raw,offset,digest=module.frozen_matrix(name,2)
    assert len(raw)==offset+2*1024*4
    assert len(digest)==64
    with pytest.raises(ValueError,match="shape or dtype"):
        module.frozen_matrix(name,3)
    name.write_bytes(name.read_bytes()[:-4])
    with pytest.raises(ValueError,match="incomplete"):
        module.frozen_matrix(name,2)
