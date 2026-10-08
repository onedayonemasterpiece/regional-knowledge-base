"""Public runner accepts private source-grounded cases without committing them."""
import json
import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"/"benchmarks"))
from live_retrieval_followup import load_fixture

SOURCE="00000000-0000-4000-8000-000000000001"
UNAUTHORIZED="00000000-0000-4000-8000-000000000002"

def _write_fixture(tmp_path,overrides=None):
    data={
        "version":1,
        "positives":[{"query":"synthetic historical test","document_id":SOURCE}],
        "negative_identifiers":["notfound99999"],
        "scoped":[{"query":"synthetic historical test","document_ids":[SOURCE]}],
        "unauthorized_doc_id":UNAUTHORIZED,
    }
    data.update(overrides or {})
    path=tmp_path/"private-fixture.json"
    path.write_text(json.dumps(data))
    return path

def test_private_fixture_loads_and_is_hash_identified(tmp_path):
    positives,negatives,scoped,unauthorized,digest=load_fixture(_write_fixture(tmp_path))
    assert positives==[("synthetic historical test",SOURCE)]
    assert negatives==["notfound99999"]
    assert scoped==[("synthetic historical test",[SOURCE])]
    assert unauthorized==UNAUTHORIZED
    assert len(digest)==64

@pytest.mark.parametrize("override",[
    {"version":2},
    {"positives":[]},
    {"positives":[{"query":"x","document_id":"invalid"}]},
    {"scoped":[{"query":"x","document_ids":[]}]},
    {"scoped":[{"query":"x","document_ids":[SOURCE,SOURCE]}]},
])
def test_private_fixture_rejects_unsafe_or_invalid_shapes(tmp_path,override):
    with pytest.raises((ValueError,TypeError)):
        load_fixture(_write_fixture(tmp_path,override))

def test_fixture_must_not_be_checked_into_public_repository():
    with pytest.raises(ValueError,match="outside the public repository"):
        load_fixture(Path(__file__).resolve())
