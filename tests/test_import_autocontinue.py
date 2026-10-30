"""Autocontinue decision: continue only after a finished turn; reconcile an unconfirmed turn; bounded retry after blocked."""
import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'import_autocontinue.py'
spec = importlib.util.spec_from_file_location('rkb_import_autocontinue', SCRIPT)
auto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auto)


def test_continue_only_after_finished_turn_and_bounded_blocked_retry():
    idle = 10_000
    assert auto.decide(None, 0, idle) == 'continue'
    assert auto.decide({'model_status': 'progress'}, 0, idle) == 'wait'
    # an unconfirmed turn is never continued blindly: read-only reconcile, rate-limited
    assert auto.decide({'model_status': 'progress', 'execution': 'unconfirmed'}, 0, idle) == 'reconcile'
    assert auto.decide({'model_status': 'progress', 'execution': 'unconfirmed'}, 0, 60) == 'wait'
    assert auto.decide({'model_status': 'progress', 'execution': 'page_completed'}, 0, idle) == 'continue'
    assert auto.decide({'model_status': 'done'}, 0, idle) == 'continue'
    assert auto.decide({'model_status': 'done'}, 0, 30) == 'wait'
    assert auto.decide({'model_status': 'blocked'}, 0, idle) == 'continue'
    assert auto.decide({'model_status': 'blocked'}, 0, 600) == 'wait'
    assert auto.decide({'model_status': 'blocked'}, 3, idle) == 'wait'
