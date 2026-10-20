"""Explicit owned empty-generation PG bootstrap. Import performs no effects.

Existing clusters and accepted history are never initialized or overwritten.
Native binaries/extension files must already exist in the verified inventory.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
from collections.abc import Mapping
from types import MappingProxyType

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from .kaggle_apply import initialize_database, Revisions
from .kaggle_bootstrap import BootstrapUnavailable, _file_hash

SQL_ARTIFACTS = ('001_search.sql', '002_recovery.sql', '003_search_scope.sql',
                 '004_read_fences.sql', '005_source_withdraw.sql')
READER_FUNCTIONS = ('lexical_candidates(text,integer)', 'dense_candidates(public.vector,integer)',
    'hydrate(uuid[])', 'checked_source_scope(uuid[])', 'lexical_candidates_scoped(text,integer,uuid[])',
    'dense_candidates_scoped(public.vector,integer,uuid[])', 'hydrate_scoped(uuid[],uuid[])')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class OwnedDatabase:
    mode: str
    database_generation: str
    writer_generation: str
    schema_revision: str
    function_revision: str
    executor_revision: str
    socket: str
    dbname: str
    port: int = 5432
    cluster: str | None = None
    uid: int | None = None
    gid: int | None = None
    native_paths: object = None
    ownership_evidence: str = ''

    def __post_init__(self):
        if self.mode not in ('NEW_EMPTY_GENERATION', 'VERIFIED_INITIALIZED'):
            raise ValueError('explicit initialized or new empty generation required')
        for key in ('database_generation','writer_generation','schema_revision','function_revision',
                    'executor_revision','ownership_evidence'):
            value = getattr(self,key)
            if not isinstance(value,str) or not 1 <= len(value.encode()) <= 256:
                raise ValueError('explicit bounded database ownership required')
        if (not isinstance(self.dbname,str) or not re.fullmatch('[a-z][a-z0-9_]{0,62}',self.dbname)
                or type(self.port) is not int or not 1<=self.port<=65535
                or not isinstance(self.socket,str) or len(self.socket.encode())>90
                or not Path(self.socket).is_relative_to('/kaggle/working') or '..' in Path(self.socket).parts):
            raise ValueError('explicit private native PG endpoint required')
        if self.mode == 'NEW_EMPTY_GENERATION':
            if (not isinstance(self.cluster,str) or not Path(self.cluster).is_absolute()
                    or '..' in Path(self.cluster).parts
                    or not Path(self.cluster).is_relative_to('/kaggle/working')
                    or Path(self.socket) != Path(self.cluster)/'socket'
                    or Path(self.cluster) == Path('/kaggle/working')
                    or any(type(v) is not int or v<1 for v in (self.uid,self.gid))
                    or not isinstance(self.native_paths,Mapping)
                    or set(self.native_paths) != {'initdb','postgres','pg_config','vector_library','vector_control','vector_sql'}
                    or any(not isinstance(p,str) or not Path(p).is_absolute() for p in self.native_paths.values())):
                raise ValueError('exact new owned native PG inventory required')
            object.__setattr__(self,'native_paths',MappingProxyType(dict(self.native_paths)))
        elif any(v is not None for v in (self.cluster,self.uid,self.gid,self.native_paths)):
            raise ValueError('initialized adoption cannot start or alter a cluster')

    def wire(self):
        return dict(vars(self)) | {'native_paths':dict(self.native_paths) if self.native_paths else None}

    def connection(self, *, reader=True, database=None):
        return dict(host=self.socket, port=self.port, dbname=database or self.dbname,
                    user='rkb_reader' if reader else 'rkb_bootstrap', password='',
                    connect_timeout=2, sslmode='disable', row_factory=tuple_row,
                    application_name='rkb_owned_target')


class OwnedPostgres:
    def __init__(self, spec, *, connect=psycopg.AsyncConnection.connect, spawn=asyncio.create_subprocess_exec):
        if type(spec) is not OwnedDatabase:
            raise TypeError('typed owned PG specification required')
        self.spec, self.connect, self.spawn = spec, connect, spawn
        self.process = None

    def _identity(self):
        spec = self.spec
        if os.geteuid() == 0:
            return dict(user=spec.uid,group=spec.gid,extra_groups=[])
        if (os.geteuid()!=spec.uid or os.getegid()!=spec.gid
                or not set(os.getgroups()) <= {spec.gid}):
            raise BootstrapUnavailable('native_pg_identity_mismatch')
        # A Stage0-verified nonroot notebook already has the correct identity.
        # setgroups(), even to [], would require root and fail with EPERM.
        return {}

    async def _command(self, *args, timeout=30):
        spec = self.spec
        process = await self.spawn(*args, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            **self._identity())
        try:
            async with asyncio.timeout(timeout):
                # pg_config/initdb output is bounded; drain at most 64KiB.
                output = bytearray()
                while chunk := await process.stdout.read(4096):
                    output.extend(chunk)
                    if len(output)>65536:
                        raise BootstrapUnavailable('native_pg_output_bound')
                if await process.wait() != 0:
                    raise BootstrapUnavailable('native_pg_command_failed')
                return output.decode('utf-8').strip()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def start_empty(self, *, intent, artifacts, sql_root):
        spec = self.spec
        if spec.mode != 'NEW_EMPTY_GENERATION':
            return
        if (type(intent) is not dict or intent.get('codec') != 'rkb.empty-generation.v1'
                or intent.get('database') != spec.wire() or intent.get('journal_head') is not None
                or not intent.get('journal_stream') or not intent.get('run_id') or not intent.get('launch_nonce')):
            raise BootstrapUnavailable('native_pg_initialization_intent_missing')
        verified = {str(artifacts.root/e.requirement.name):e.requirement for e in artifacts.entries if e.role=='code'}
        if not set(spec.native_paths.values()) <= verified.keys():
            raise BootstrapUnavailable('native_pg_artifact_binding_missing')
        for path in spec.native_paths.values():
            requirement = verified[path]
            if _file_hash(path,requirement.size) != requirement.sha256:
                raise BootstrapUnavailable('native_pg_artifact_hash_mismatch')
        files = {name:(Path(sql_root)/name).read_bytes() for name in SQL_ARTIFACTS}
        if intent.get('sql_hashes') != {name:digest(data) for name,data in files.items()}:
            raise BootstrapUnavailable('native_pg_sql_binding_mismatch')
        paths = spec.native_paths
        library = await self._command(paths['pg_config'],'--pkglibdir',timeout=5)
        shared = await self._command(paths['pg_config'],'--sharedir',timeout=5)
        if (Path(paths['vector_library']) != Path(library)/'vector.so'
                or Path(paths['vector_control']) != Path(shared)/'extension/vector.control'
                or Path(paths['vector_sql']) != Path(shared)/'extension/vector--0.8.2.sql'):
            raise BootstrapUnavailable('native_pg_extension_binding_mismatch')
        cluster = Path(spec.cluster)
        if cluster.parent.resolve(strict=True) != cluster.parent:
            raise BootstrapUnavailable('native_pg_parent_unverified')
        try:
            cluster.mkdir(mode=0o700)  # O_EXCL directory creation; never adopt partial state.
        except FileExistsError:
            raise BootstrapUnavailable('native_pg_existing_cluster_refused') from None
        os.chown(cluster,spec.uid,spec.gid)
        marker = cluster/'initialization.json'
        with marker.open('xb') as handle:
            handle.write(canonical(intent))
        marker.chmod(0o400)
        socket = Path(spec.socket)
        socket.mkdir(mode=0o700)
        os.chown(socket,spec.uid,spec.gid)
        data = cluster/'data'
        await self._command(paths['initdb'],'-D',str(data),'--username=rkb_bootstrap',
                            '--encoding=UTF8','--locale=C','--auth-local=trust','--auth-host=reject')
        self.process = await self.spawn(paths['postgres'],'-D',str(data),'-k',str(socket),'-h','',
            '-p',str(spec.port),'-c','shared_buffers=32MB','-c','max_connections=8',
            '-c','work_mem=4MB','-c','maintenance_work_mem=16MB','-c','max_wal_size=64MB',
            '-c','min_wal_size=32MB',stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL,
            **self._identity())
        async with asyncio.timeout(20):
            while True:
                if self.process.returncode is not None:
                    raise BootstrapUnavailable('native_pg_start_failed')
                try:
                    admin = await self.connect(**spec.connection(reader=False,database='postgres'))
                    break
                except psycopg.OperationalError:
                    await asyncio.sleep(.1)
        async with admin:
            await admin.set_autocommit(True)
            await admin.execute("SET statement_timeout='10s'")
            await admin.execute(sql.SQL('CREATE DATABASE {} OWNER rkb_bootstrap').format(sql.Identifier(spec.dbname)))
        admin = await self.connect(**spec.connection(reader=False))
        async with admin:
            await admin.execute("SET statement_timeout='10s'; SET lock_timeout='2s'",prepare=False)
            await admin.commit()
            async with admin.transaction():
                await admin.execute('CREATE EXTENSION vector WITH SCHEMA public')
                for name in SQL_ARTIFACTS[:2]:
                    await admin.execute(files[name].decode(),prepare=False)
            await initialize_database(admin,database_generation=spec.database_generation,
                writer_generation=spec.writer_generation,
                revisions=Revisions(spec.schema_revision,spec.function_revision,spec.executor_revision))
            async with admin.transaction():
                for name in SQL_ARTIFACTS[2:]:
                    await admin.execute(files[name].decode(),prepare=False)
                await admin.execute('''CREATE ROLE rkb_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
                    CREATE ROLE rkb_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
                    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
                    REVOKE ALL ON SCHEMA rkb_v4 FROM PUBLIC;
                    REVOKE ALL ON ALL TABLES IN SCHEMA rkb_v4 FROM PUBLIC;
                    REVOKE ALL ON ALL FUNCTIONS IN SCHEMA rkb_v4 FROM PUBLIC;
                    ALTER DEFAULT PRIVILEGES FOR ROLE rkb_bootstrap IN SCHEMA rkb_v4 REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
                    GRANT USAGE ON SCHEMA rkb_v4 TO rkb_writer,rkb_reader;
                    GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA rkb_v4 TO rkb_writer;
                    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rkb_v4 TO rkb_writer;
                    GRANT SELECT ON rkb_v4.sources,rkb_v4.revisions,rkb_v4.active_revision,
                        rkb_v4.passage_search,rkb_v4.recovery_state,rkb_v4.read_frontier,
                        rkb_v4.source_visibility TO rkb_reader;''',prepare=False)
                for signature in READER_FUNCTIONS:
                    await admin.execute('GRANT EXECUTE ON FUNCTION rkb_v4.'+signature+' TO rkb_reader')
                await admin.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(spec.dbname)))
                await admin.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO rkb_writer,rkb_reader').format(sql.Identifier(spec.dbname)))
        # Fresh empty projections contain no accepted/source data; explicitly
        # publish only cursor zero under the same trusted writer lock.
        admin = await self.connect(**spec.connection(reader=False))
        async with admin:
            await admin.execute("SET statement_timeout='10s'; SET lock_timeout='2s'",prepare=False)
            await admin.commit()
            async with admin.transaction():
                await admin.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',
                                    (spec.database_generation,spec.writer_generation))

    async def aclose(self):
        if self.process is not None and self.process.returncode is None:
            self.process.send_signal(signal.SIGINT)  # PG fast shutdown; owned postmaster only.
            async with asyncio.timeout(30):
                await self.process.wait()
