"""Operator audit safety tests; no live credentials, network or corpus data."""
import importlib.util
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1]


@pytest.fixture
def audit(monkeypatch):
    monkeypatch.setitem(__import__('sys').modules, 'operator_env',
                        SimpleNamespace(load_service_env=lambda: None))
    spec = importlib.util.spec_from_file_location(
        'storage_budget_audit_under_test',
        ROOT / 'scripts/production/audit_storage_budget.py',
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_configuration_never_connects(audit, monkeypatch, capsys):
    monkeypatch.delenv('KB_SUPABASE_SESSION_CONNECTION', raising=False)
    def forbidden(*args, **kwargs):
        raise AssertionError('must not connect')
    monkeypatch.setattr(audit.psycopg, 'connect', forbidden)
    assert audit.main() == 2
    assert json.loads(capsys.readouterr().out)['error'] == 'configured_database_connection_missing'


def test_explicit_readonly_before_queries(audit, monkeypatch, capsys):
    seen = []
    class Connection:
        read_only = False
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def transaction(self): return nullcontext()
        def execute(self, query):
            assert self.read_only is True
            seen.append(query)
            return SimpleNamespace(fetchall=lambda: [{'aggregate': 1}])
    def connect(*args, **kwargs):
        assert kwargs['connect_timeout'] == 10
        assert 'statement_timeout=8000' in kwargs['options']
        return Connection()
    monkeypatch.setenv('KB_SUPABASE_SESSION_CONNECTION', 'fixture-connection-not-a-secret')
    monkeypatch.setattr(audit.psycopg, 'connect', connect)
    assert audit.main() == 0
    output = json.loads(capsys.readouterr().out)
    assert 'database' in output and 'chunks' in output
    assert seen[0].startswith('SET LOCAL statement_timeout')
    assert len(seen) == len(audit.QUERIES) + 1
    assert all(query.lstrip().lower().startswith('select') for query in seen[1:])


def test_connection_error_does_not_export_connection_details(audit, monkeypatch, capsys):
    sentinel = 'fixture-credential-must-not-be-logged'
    monkeypatch.setenv('KB_SUPABASE_SESSION_CONNECTION', sentinel)
    def fail(*args, **kwargs): raise RuntimeError(sentinel)
    monkeypatch.setattr(audit.psycopg, 'connect', fail)
    assert audit.main() == 1
    output = capsys.readouterr().out
    assert sentinel not in output
    assert json.loads(output) == {'error_type': 'RuntimeError'}


def test_target_documents_and_navigation_exist():
    prompt=ROOT/'docs/prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md'
    text=prompt.read_text()
    for number in range(1,11):assert f'D{number:02d}' in text
    for name in ('mcp.md','ingestion.md','storage.md','architecture.md'):
        assert prompt.name in (ROOT/'docs'/name).read_text()
