"""Targeted in-notebook PG backup diagnostic. No shell, provider calls or Saved promotion.

Invoke run(factory, binding, workspace) from the current trusted notebook owner.
The factory supplies pinned binaries, its PG socket and its private scratch path.
"""
import asyncio
import shutil
import os
import signal
import time
from pathlib import Path

from .kaggle_pg_snapshot_native import SnapshotError, applied_prefix, pack_tree
from .kaggle_snapshot_io import _SNAPSHOT_FAILURE_CODES


async def _tail(stream):
    tail=b''
    while chunk:=await stream.read(4096):tail=(tail+chunk)[-4096:]
    return tail


async def run(factory, binding, workspace):
    result = {'stage': 'database', 'success': False, 'provider_calls': 0,
              'saved_promoted': False, 'commands': []}
    root = None
    started = time.monotonic()
    try:
        factory._current(binding)
        async with factory.acquire() as connection:
            cursor = await connection.execute("""SELECT
                current_setting('server_version_num')::integer,
                current_setting('max_connections')::integer,
                (SELECT count(*) FROM pg_stat_activity),
                applied_seq,writer_generation FROM rkb_v4.recovery_state WHERE singleton""")
            row = await cursor.fetchone()
            result['database'] = dict(zip(('server_version_num', 'max_connections',
                'connections', 'applied_sequence', 'writer_generation'), row))
            result['stage'] = 'prefix'
            await applied_prefix(connection,row[3],factory.limits)
        result['stage'] = 'native_probe'
        await factory.probe(acquire=factory.acquire)
        result['stage'] = 'private_workspace'
        root = await asyncio.to_thread(factory._workspace, workspace,
                                      factory.limits.max_unpacked_bytes * 2)
        commands = (
            ('pg_basebackup', factory.tools.pg_basebackup.path,
             ('--pgdata', str(root / 'base'), '--wal-method=stream', '--checkpoint=fast',
              '--manifest-checksums=SHA256', '--no-password', '--host',
              factory.database.socket, '--port', str(factory.database.port),
              '--username', 'rkb_bootstrap')),
            ('pg_verifybackup', factory.tools.pg_verifybackup.path, (str(root / 'base'),)),
        )
        for label, program, arguments in commands:
            factory._current(binding)
            result['stage'] = label
            process = await asyncio.create_subprocess_exec(
                program, *arguments, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=factory.native_env, start_new_session=True, **factory._identity())
            try:
                async with asyncio.timeout(factory.limits.timeout_seconds):
                    _, stderr = await asyncio.gather(_tail(process.stdout), _tail(process.stderr))
                    await process.wait()
            finally:
                if process.returncode is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
            result['commands'].append({'program': label, 'exit_code': process.returncode,
                'stderr': stderr.decode('utf-8', errors='replace')[-4096:]})
            if process.returncode:
                return result
        result['stage'] = 'archive'
        _, unpacked, _, size, _ = await asyncio.to_thread(pack_tree,
            root/'base', root/'base.tar', factory.limits)
        result.update(stage='complete', success=True, archive_bytes=size, unpacked_bytes=unpacked)
        return result
    except Exception as error:
        result['error_type'] = type(error).__name__
        if type(error) is SnapshotError and len(error.args)==1 and type(error.args[0]) is str and error.args[0] in _SNAPSHOT_FAILURE_CODES:
            result['error_code']=error.args[0]
        result['os_errno'] = getattr(error, 'errno', None)
        result['sqlstate'] = getattr(error, 'sqlstate', None)
        return result
    finally:
        result['elapsed_seconds'] = round(time.monotonic() - started, 3)
        if root is not None:
            # Only the freshly allocated private diagnostic tree; never PGDATA.
            await asyncio.to_thread(shutil.rmtree, Path(root))
