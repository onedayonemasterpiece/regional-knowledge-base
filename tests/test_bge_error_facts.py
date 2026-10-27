import json
from pathlib import Path
import sqlite3

import pytest

from regional_knowledge.bge_error_facts import failure_facts
from regional_knowledge.bge_queue import BgeQueue


def test_real_queue_write_contention_reports_busy_and_source_without_payload(tmp_path):
    queue = BgeQueue(tmp_path / 'private-database.sqlite')
    with sqlite3.connect(queue.path) as holder:
        holder.execute('BEGIN IMMEDIATE')
        with pytest.raises(sqlite3.OperationalError) as caught:
            queue.maintenance()
    facts = failure_facts(caught.value)
    assert facts['sqlite_errorcode'] == sqlite3.SQLITE_BUSY
    assert facts['sqlite_errorname'] == 'SQLITE_BUSY'
    assert facts['error_frames'][-1]['file'] == 'bge_queue.py'
    assert facts['error_frames'][-1]['function'] == 'connect'
    assert all(type(frame['line']) is int and frame['line'] > 0 for frame in facts['error_frames'])
    assert 1 <= len(facts['error_frames']) <= 3
    assert str(tmp_path) not in json.dumps(facts)
    assert 'private-database' not in json.dumps(facts)
    assert 'database is locked' not in json.dumps(facts)


def test_real_shared_cache_lock_retains_extended_sqlite_code():
    uri = 'file:diagnostic-fixture?mode=memory&cache=shared'
    with sqlite3.connect(uri, uri=True) as holder, sqlite3.connect(uri, uri=True) as reader:
        holder.execute('CREATE TABLE fixture (value INTEGER)')
        holder.commit()
        holder.execute('INSERT INTO fixture VALUES (1)')
        with pytest.raises(sqlite3.OperationalError) as caught:
            reader.execute('SELECT value FROM fixture').fetchall()
    assert failure_facts(caught.value) == dict(
        sqlite_errorcode=sqlite3.SQLITE_LOCKED_SHAREDCACHE,
        sqlite_errorname='SQLITE_LOCKED_SHAREDCACHE')


def test_non_sqlite_operational_error_and_object_cannot_supply_diagnostics():
    class OperationalError(RuntimeError):
        @property
        def sqlite_errorcode(self):
            raise AssertionError('must not inspect non-SQLite attributes')

    private = 'SECRET https://private.invalid/?token=PRIVATE SELECT private_payload'
    assert failure_facts(OperationalError(private)) == {}
    assert failure_facts(object()) == {}
    error = sqlite3.OperationalError(private)
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    error.sqlite_errorname = private
    assert failure_facts(error) == {}


def test_traceback_locations_are_bounded_and_exclude_untrusted_paths():
    import regional_knowledge.bge_error_facts as diagnostics

    namespace = {}
    exec(compile('def recurse(n):\n if n: return recurse(n-1)\n raise RuntimeError("PRIVATE")',
                 diagnostics.__file__, 'exec'), namespace)
    with pytest.raises(RuntimeError) as caught:
        namespace['recurse'](8)
    facts = failure_facts(caught.value)
    assert len(facts['error_frames']) == 3
    assert all(set(frame) == {'file', 'function', 'line'} for frame in facts['error_frames'])
    assert all(frame['file'] == Path(diagnostics.__file__).name for frame in facts['error_frames'])
    assert 'PRIVATE' not in json.dumps(facts)

    with pytest.raises(RuntimeError) as external:
        exec(compile('raise RuntimeError("PRIVATE")', '/private/SECRET.py', 'exec'), {})
    assert failure_facts(external.value) == {}
