import base64
import json
from pathlib import Path

import pytest

from regional_knowledge.telegram_runtime_owners import existing_rkb1_credentials
from regional_knowledge.telegram_runtime_binding import TelegramBindingUnavailable
from test_telegram_runtime_binding import DEVICE,SYNTHETIC_SESSION


def test_supplier_uses_existing_named_bundle_and_preserves_device_metadata(monkeypatch):
    bundle=base64.b64encode(json.dumps(dict(session=SYNTHETIC_SESSION,**DEVICE)).encode()).decode()
    def read(path,*args,**kwargs):
        assert path==Path('/home/dev/.env')
        return f"UNRELATED=ignored\nTELEGRAM_RKB1='{bundle}'\nTELEGRAM_API_ID=12345\nTELEGRAM_API_HASH={'a'*32}\n"
    monkeypatch.setattr(Path,'read_text',read)
    assert existing_rkb1_credentials()==dict(bundle_b64=bundle,api_id=12345,api_hash='a'*32)


def test_supplier_never_substitutes_e2e_or_exposes_invalid_input(monkeypatch):
    monkeypatch.setattr(Path,'read_text',lambda *a,**k:'TELEGRAM_E2E=private_input_must_not_escape\n')
    with pytest.raises(TelegramBindingUnavailable) as error:existing_rkb1_credentials()
    assert str(error.value)=='telegram_credential_supplier_unavailable'
