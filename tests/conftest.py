import pytest

@pytest.fixture(autouse=True)
def isolate_private_caches(tmp_path,monkeypatch):
    monkeypatch.setenv('RKB_ORIGINAL_CACHE_DIR',str(tmp_path/'originals'))
    monkeypatch.setenv('RKB_PROOF_CACHE_DIR',str(tmp_path/'proofs'))
    monkeypatch.setenv('RKB_PROOF_WORK_DIR',str(tmp_path/'proof-work'))
