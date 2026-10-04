import importlib.util
from pathlib import Path
from enum import Enum

def test_kaggle_status_accepts_current_sdk_enum_and_legacy_dictionary():
    path=Path(__file__).resolve().parents[1]/'scripts/production/bge_kaggle_controller.py'
    spec=importlib.util.spec_from_file_location('bge_controller_test',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    class KernelWorkerStatus(Enum):ERROR=1;RUNNING=2
    class Response:status=KernelWorkerStatus.ERROR
    assert module.provider_phase(Response())=='ERROR'
    assert module.provider_phase({'status':'failed'})=='FAILED'
    assert module.provider_phase({'status':KernelWorkerStatus.RUNNING})=='RUNNING'
