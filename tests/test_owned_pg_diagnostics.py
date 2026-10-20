"""Bounded synthetic native startup stderr; no subprocess/service effects."""
import asyncio
from dataclasses import replace
import json
import stat

import pytest

from regional_knowledge.kaggle_owned_pg import OwnedPostgres,BootstrapUnavailable
from test_kaggle_target_runtime import spec


@pytest.mark.asyncio
async def test_native_failure_retains_bounded_private_bytes_and_logs_only_metadata(tmp_path,caplog,monkeypatch):
    database=spec(fresh=True).database
    object.__setattr__(database,'cluster',str(tmp_path/'owned'))
    owner=OwnedPostgres(database)
    monkeypatch.setattr(owner,'_identity',lambda:{})
    payload=b'postgres: error while loading shared libraries: libfixture.so.1: private diagnostic payload\n'
    class Process:
        returncode=127
        stdout=asyncio.StreamReader()
        stderr=asyncio.StreamReader()
        async def wait(self):return self.returncode
    process=Process();process.stdout.feed_eof();process.stderr.feed_data(payload);process.stderr.feed_eof()
    async def spawn(*args,**kwargs):
        assert 'LD_PRELOAD' not in kwargs['env'] and kwargs['stderr']==asyncio.subprocess.PIPE
        return process
    owner.spawn=spawn
    with pytest.raises(BootstrapUnavailable,match='native_pg_command_failed'):
        await owner._command('/pinned/pg_config','--sharedir')
    files=list(tmp_path.glob('.rkb-native-*.log'))
    assert len(files)==1 and files[0].read_bytes()==payload
    assert stat.S_IMODE(files[0].stat().st_mode)==0o600
    record=json.loads(caplog.records[-1].message)
    assert record['tool']=='pg_config' and record['exit_code']==127
    assert record['missing_library']=='libfixture.so.1' and record['diagnostic_retained']
    assert 'private diagnostic payload' not in caplog.text
    owner._retain_failure('pg_config',127,b'cannot overwrite')
    assert files[0].read_bytes()==payload


@pytest.mark.asyncio
async def test_native_stderr_budget_kills_only_owned_child_and_drops_post_startup():
    owner=OwnedPostgres(spec(fresh=True).database);events=[]
    class Process:
        returncode=None
        stderr=asyncio.StreamReader()
        def kill(self):events.append('kill');self.returncode=-9
    process=Process();process.stderr.feed_data(b'x'*9000);process.stderr.feed_eof()
    buffer=bytearray()
    await owner._capture_stderr(process,buffer)
    assert events==['kill'] and len(buffer)==8192
    owner._startup_complete=True
    process.stderr=asyncio.StreamReader();process.stderr.feed_data(b'accepted private query');process.stderr.feed_eof()
    await owner._capture_stderr(process,buffer,startup_only=True)
    assert buffer==b'' and events==['kill']


def test_directory_read_failure_is_classified_without_private_paths(tmp_path,caplog):
    database=spec(fresh=True).database
    object.__setattr__(database,'cluster',str(tmp_path/'owned'))
    owner=OwnedPostgres(database)
    owner._retain_failure('postgres',1,
        b'FATAL:  could not open directory "/private-opaque-path/native/lib": Permission denied\n')
    record=json.loads(caplog.records[-1].message)
    assert record['startup_reason']=='directory_read_denied'
    assert 'private-opaque-path' not in caplog.text and 'native/lib' not in caplog.text
