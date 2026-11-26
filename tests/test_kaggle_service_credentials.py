import os
import sys
from pathlib import Path
from types import ModuleType

import pytest

from regional_knowledge.kaggle_dataset_publisher import authenticated_service_api


def fake_sdk(monkeypatch):
    calls = []

    class Api:
        def authenticate(self):
            calls.append((os.environ.get('KAGGLE_USERNAME'), os.environ.get('KAGGLE_KEY')))

    module = ModuleType('kaggle.api.kaggle_api_extended')
    module.KaggleApi = Api
    for name in ('kaggle', 'kaggle.api'):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return calls


def test_service_uses_dedicated_pair_when_shared_account_is_different(monkeypatch):
    calls = fake_sdk(monkeypatch)
    monkeypatch.setattr(Path, 'read_text', lambda path: (
        'KAGGLE_USERNAME=other-owner\nKAGGLE_KEY=other-key\n'
        'KAGGLE_RKB_USERNAME=rkb-owner\nKAGGLE_RKB_KEY="rkb-key"\n'))
    authenticated_service_api()
    assert calls == [('rkb-owner', 'rkb-key')]


@pytest.mark.parametrize('pair', [
    'KAGGLE_RKB_USERNAME=rkb-owner\n',
    'KAGGLE_RKB_KEY=rkb-key\n',
])
def test_incomplete_dedicated_pair_never_uses_shared_credentials(monkeypatch, pair):
    calls = fake_sdk(monkeypatch)
    monkeypatch.setattr(Path, 'read_text', lambda path: (
        'KAGGLE_USERNAME=other-owner\nKAGGLE_KEY=other-key\n' + pair))
    with pytest.raises(ValueError, match='rkb_kaggle_credentials_missing'):
        authenticated_service_api()
    assert calls == []
