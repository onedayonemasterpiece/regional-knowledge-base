"""Explicit owned empty-generation PG bootstrap. Import performs no effects.

Existing clusters and accepted history are never initialized or overwritten.
Native binaries/extension files must already exist in the verified inventory.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import pwd
import logging
import secrets
from collections.abc import Mapping
from types import MappingProxyType

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from .kaggle_apply import initialize_database
from .kaggle_schema_migrate import latest_schema_version
from .kaggle_bootstrap import BootstrapUnavailable, _file_hash

# Kaggle saves /kaggle/working outputs; /kaggle/temp is session-only.
PRIVATE_RUNTIME_ROOT = Path('/kaggle/temp')
_LOG=logging.getLogger(__name__)

SQL_ARTIFACTS = ('001_search.sql', '002_recovery.sql', '003_search_scope.sql',
                 '004_read_fences.sql', '005_source_withdraw.sql', '006_source_import.sql')
STORY_SQL_ARTIFACTS=SQL_ARTIFACTS+('007_source_stories.sql',)
FACT_SQL_ARTIFACTS=STORY_SQL_ARTIFACTS+('008_source_facts.sql',)
EDITORIAL_SQL_ARTIFACTS=FACT_SQL_ARTIFACTS+('009_story_editorial.sql',)
CASE_SQL_ARTIFACTS=EDITORIAL_SQL_ARTIFACTS+('010_disagreement_cases.sql',)
DOMAIN_SQL_ARTIFACTS=CASE_SQL_ARTIFACTS+('011_geo_identity_manifest.sql',)
DOMAIN_V2_SQL_ARTIFACTS=DOMAIN_SQL_ARTIFACTS+('012_geo_dependency_bound.sql',)
IMAGE_SOURCE_SQL_ARTIFACTS=DOMAIN_V2_SQL_ARTIFACTS+('013_source_stage.sql','014_source_stage_activation.sql')
NATIVE_SOURCE_SQL_ARTIFACTS=IMAGE_SOURCE_SQL_ARTIFACTS

STORY_READER_FUNCTIONS=('read_story_v1(uuid,text,bigint,bigint,integer)',
    'story_lexical_candidates_v1(text,integer,uuid[])','story_dense_candidates_v1(public.vector,integer,uuid[])',
    'story_hydrate_v1(uuid[],uuid[])')

READER_FUNCTIONS = ('lexical_candidates(text,integer)', 'dense_candidates(public.vector,integer)',
    'hydrate(uuid[])', 'checked_source_scope(uuid[])', 'lexical_candidates_scoped(text,integer,uuid[])',
    'dense_candidates_scoped(public.vector,integer,uuid[])', 'hydrate_scoped(uuid[],uuid[])')


async def create_native_roles(admin):
    """Fixed fresh-cluster roles, before SQL files grant privileges to them."""
    await admin.execute('''CREATE ROLE rkb_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
        CREATE ROLE rkb_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;''',prepare=False)


async def configure_native_roles(admin,*,selected_sql,dbname,create_roles=True):
    """Harden a newly owned native cluster, using the exact bootstrap grant path.

    The caller owns the transaction. Roles must be absent; this function never
    adopts existing users or extends privileges from caller-provided SQL names.
    """
    selected_sql=tuple(selected_sql)
    if (not selected_sql or any(not re.fullmatch(r'[0-9]{3}_[a-z0-9_]+\.sql',name) for name in selected_sql)
            or [int(name[:3]) for name in selected_sql]!=list(range(1,len(selected_sql)+1))):
        raise ValueError('complete verified native SQL prefix required')
    if type(dbname) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',dbname):
        raise ValueError('exact owned native database name required')
    if create_roles:await create_native_roles(admin)
    await admin.execute('''REVOKE CREATE ON SCHEMA public FROM PUBLIC;
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
    if len(selected_sql)>=7:
        await admin.execute('GRANT SELECT ON rkb_v4.source_regions,rkb_v4.story_assertion_versions,'
            'rkb_v4.stories,rkb_v4.story_versions,rkb_v4.story_variant_versions,rkb_v4.story_search,'
            'rkb_v4.source_story_pending,rkb_v4.story_history_receipts TO rkb_reader')
    fact_functions=()
    if len(selected_sql)>=8:
        from .kaggle_fact_read import FACT_READER_FUNCTIONS
        fact_functions=FACT_READER_FUNCTIONS
        await admin.execute('GRANT SELECT ON rkb_v4.source_event_dates,rkb_v4.source_observations TO rkb_reader')
    domain_functions=()
    if len(selected_sql)>=9:
        await admin.execute('GRANT SELECT ON rkb_v4.editorial_story_heads,rkb_v4.editorial_story_versions,'
            'rkb_v4.editorial_variant_versions,rkb_v4.editorial_variant_heads,rkb_v4.editorial_story_receipts,'
            'rkb_v4.editorial_story_search,rkb_v4.story_editor_grants,rkb_v4.source_catalog,'
            'rkb_v4.source_passage_provenance TO rkb_reader')
        await admin.execute('GRANT SELECT(source_id,source_revision,manifest) ON rkb_v4.source_manifests TO rkb_reader')
        await admin.execute('GRANT SELECT(source_id,revision,original_sha256) ON rkb_v4.source_import_origins TO rkb_reader')
        domain_functions=('read_story_edit_context_v1(uuid,uuid)','story_editor_role_v1(uuid,uuid)','read_editorial_story_v1(uuid,bigint,text,bigint,integer,uuid)',
            'editorial_lexical_candidates_v1(text,integer,uuid[])',
            'editorial_dense_candidates_v1(public.vector,integer,uuid[])','editorial_hydrate_v1(uuid[],uuid[])',
            'read_source_manifest_v1(uuid)','find_source_catalog_v1(text,integer)','source_passage_metadata_v1(uuid[])')
    if len(selected_sql)>=10:
        from .kaggle_domain_schema import TABLES_10
        await admin.execute('GRANT SELECT ON '+','.join('rkb_v4.'+name for name in sorted(TABLES_10))+' TO rkb_reader')
        await admin.execute('GRANT SELECT(operation_key,sequence,status,command_bytes,command_sha256,outcome_bytes) ON rkb_v4.applied_ops TO rkb_reader')
        domain_functions+=('verify_disagreement_origins_v1(uuid[])','case_role_v1(uuid,uuid)','case_sources_v1(uuid)',
            'read_disagreement_v1(uuid,uuid,text,bigint,integer,bigint,uuid,bigint,uuid)',
            'read_disagreement_operation_v1(uuid,uuid,text)')
    if len(selected_sql)>=11:
        from .kaggle_domain_schema import TABLES_11
        await admin.execute('GRANT SELECT ON '+','.join('rkb_v4.'+name for name in sorted(TABLES_11))+' TO rkb_reader')
        domain_functions+=('read_source_graph_v1(uuid,uuid,text,uuid,text,integer)','read_geo_identity_v1(uuid,uuid,text,text,text,integer,uuid,integer)',
            'read_geo_members_v1(uuid,uuid,text,text,integer)')
    if len(selected_sql)>=30:
        # Story evidence needs only page identity/index, never raw model or image bytes.
        await admin.execute('GRANT SELECT(source_id,page_id,physical_page_index) '
            'ON rkb_v4.source_stage_pages TO rkb_reader')
    if len(selected_sql)>=37:
        await admin.execute('GRANT SELECT ON rkb_v4.source_illustrations TO rkb_reader')
        await admin.execute('GRANT SELECT(source_id,ingestion_id) ON rkb_v4.source_stage_sessions TO rkb_reader')
        await admin.execute('GRANT SELECT(source_id,batch_index,component,manifest,media,figure_ids) '
            'ON rkb_v4.source_batch_publications TO rkb_reader')
    # Schema14 reuses the same accepted-source metadata/projection readers.
    # Raw model batches, private candidates and activation mutation functions
    # are writer-only; never grant them through the public reader profile.
    for signature in READER_FUNCTIONS+(STORY_READER_FUNCTIONS if len(selected_sql)>=7 else ())+fact_functions+domain_functions:
        await admin.execute('GRANT EXECUTE ON FUNCTION rkb_v4.'+signature+' TO rkb_reader')
    await admin.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(dbname)))
    await admin.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO rkb_writer,rkb_reader').format(sql.Identifier(dbname)))


async def configure_migration_defaults(connection):
    """Carry the existing technical writer profile onto additive SQL objects.

    Product participant ACL/eligibility tables and public reader grants remain
    unchanged. The migration ledger explicitly revokes this technical default.
    """
    await connection.execute("""ALTER DEFAULT PRIVILEGES IN SCHEMA rkb_v4
        GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO rkb_writer;
        ALTER DEFAULT PRIVILEGES IN SCHEMA rkb_v4
        GRANT EXECUTE ON FUNCTIONS TO rkb_writer""",prepare=False)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def _can_access(info,uid,gid,bits):
    shift=6 if info.st_uid==uid else 3 if info.st_gid==gid else 0
    return ((stat.S_IMODE(info.st_mode)>>shift)&bits)==bits


def ensure_private_runtime_root(*, native_uid=None, native_gid=None):
    """Create only the fixed session-only parent, never caller directories.

    Kaggle images need not precreate /kaggle/temp. The trusted provider parent
    must already exist. A root supervisor leaves the new parent traversable by
    its nonroot PG child, without giving other identities write permission;
    each actual cluster/workspace is still an exclusive private directory.
    """
    root=PRIVATE_RUNTIME_ROOT
    parent_fd=root_fd=None;created=False
    try:
        if root.parent.resolve(strict=True)!=root.parent:
            raise OSError('untrusted provider parent')
        parent_fd=os.open(root.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        parent=os.fstat(parent_fd)
        if parent.st_uid not in (0,os.geteuid()) or stat.S_IMODE(parent.st_mode)&0o022:
            raise OSError('untrusted provider parent')
        if native_uid is not None and (native_gid is None or not _can_access(parent,native_uid,native_gid,1)):
            raise OSError('native identity cannot traverse provider parent')
        mode=0o711 if native_uid is not None and native_uid!=os.geteuid() else 0o700
        try:
            os.mkdir(root.name,mode=mode,dir_fd=parent_fd)
            created=True
        except FileExistsError:pass
        root_fd=os.open(root.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
        info=os.fstat(root_fd)
        if info.st_uid not in (0,os.geteuid()) or stat.S_IMODE(info.st_mode)&0o022:
            raise OSError('untrusted private root')
        if created:
            os.fchmod(root_fd,mode)  # Explicit traversal survives a restrictive umask.
            info=os.fstat(root_fd)
        if native_uid is not None and (native_gid is None or not _can_access(info,native_uid,native_gid,1)):
            raise OSError('native identity cannot traverse private root')
        # Probe the actual supervisor write permission before native children.
        probe='.rkb-private-root-probe-'+secrets.token_hex(16)
        fd=os.open(probe,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=root_fd)
        os.close(fd);os.unlink(probe,dir_fd=root_fd)
        observed=os.stat(root.name,dir_fd=parent_fd,follow_symlinks=False)
        if (observed.st_dev,observed.st_ino)!=(info.st_dev,info.st_ino):
            raise OSError('private root changed')
    except OSError:
        _LOG.warning('native_private_root state=unavailable')
        raise BootstrapUnavailable('native_pg_private_root_unavailable') from None
    finally:
        if root_fd is not None:os.close(root_fd)
        if parent_fd is not None:os.close(parent_fd)
    _LOG.info('native_private_root state=ready created=%s native_uid=%s',created,native_uid)
    return root


def grant_native_code_access(root,pins,*,uid,gid):
    """Give only the owned native identity access to exact verified public code.

    Give read/search on code directories: PostgreSQL enumerates its native
    library directory during startup. No recursion, model/credential access,
    owner changes, root chmod or new group-write/world permissions.
    Descriptors reject symlink parent swaps.
    """
    root=Path(root)
    if root.resolve(strict=True)!=root:raise BootstrapUnavailable('native_pg_permission_root_invalid')
    if os.geteuid()!=0 and (os.geteuid()!=uid or os.getegid()!=gid or not set(os.getgroups())<={gid}):
        raise BootstrapUnavailable('native_pg_identity_mismatch')
    root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        if not _can_access(os.fstat(root_fd),uid,gid,1):
            raise BootstrapUnavailable('native_pg_working_root_not_traversable')
        if not 1<=len(pins)<=16384:raise BootstrapUnavailable('native_pg_permission_inventory_bound')
        seen=set()
        for absolute,size,expected,executable in pins:
            path=Path(absolute)
            if not path.is_relative_to(root) or path==root or '..' in path.parts or str(path) in seen:
                raise BootstrapUnavailable('native_pg_permission_path_invalid')
            seen.add(str(path))
            parts=path.relative_to(root).parts
            if len(parts)>32:raise BootstrapUnavailable('native_pg_permission_inventory_bound')
            parent=os.dup(root_fd)
            try:
                for part in parts[:-1]:
                    child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
                    os.close(parent);parent=child
                    info=os.fstat(parent)
                    if info.st_uid not in (os.geteuid(),uid):
                        raise BootstrapUnavailable('native_pg_permission_parent_unowned')
                    if os.geteuid()==0:
                        mode=(stat.S_IMODE(info.st_mode)|0o050)&~0o020
                        if info.st_gid!=gid:os.fchown(parent,-1,gid)
                        if mode!=stat.S_IMODE(info.st_mode):os.fchmod(parent,mode)
                    elif not _can_access(info,uid,gid,5):
                        raise BootstrapUnavailable('native_pg_permission_parent_denied')
                fd=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
                try:
                    before=os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode) or before.st_size!=size or before.st_uid not in (os.geteuid(),uid):
                        raise BootstrapUnavailable('native_pg_permission_file_invalid')
                    actual=hashlib.sha256();remaining=size
                    while remaining:
                        chunk=os.read(fd,min(1024*1024,remaining))
                        if not chunk:raise BootstrapUnavailable('native_pg_permission_file_invalid')
                        actual.update(chunk);remaining-=len(chunk)
                    after=os.fstat(fd)
                    if (os.read(fd,1) or actual.hexdigest()!=expected
                            or (before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns)):
                        raise BootstrapUnavailable('native_pg_artifact_hash_mismatch')
                    if os.geteuid()==0:
                        mode=(stat.S_IMODE(after.st_mode)|0o040)&~0o030
                        if executable:mode|=0o010
                        if after.st_gid!=gid:os.fchown(fd,-1,gid)
                        if mode!=stat.S_IMODE(after.st_mode):os.fchmod(fd,mode)
                    elif not _can_access(after,uid,gid,5 if executable else 4):
                        raise BootstrapUnavailable('native_pg_permission_file_denied')
                finally:os.close(fd)
            finally:os.close(parent)
    except OSError:
        raise BootstrapUnavailable('native_pg_permission_verification_failed') from None
    finally:os.close(root_fd)


@dataclass(frozen=True)
class OwnedDatabase:
    mode: str
    database_generation: str
    writer_generation: str
    socket: str
    dbname: str
    port: int = 5432
    cluster: str | None = None
    uid: int | None = None
    gid: int | None = None
    native_paths: object = None
    ownership_evidence: str = ''
    schema_version: int = field(default_factory=latest_schema_version)

    def __post_init__(self):
        if type(self.schema_version) is not int or self.schema_version<1:
            raise ValueError('integer database schema version required')
        if self.mode not in ('NEW_EMPTY_GENERATION', 'VERIFIED_INITIALIZED'):
            raise ValueError('explicit initialized or new empty generation required')
        for key in ('database_generation','writer_generation','ownership_evidence'):
            value = getattr(self,key)
            if not isinstance(value,str) or not 1 <= len(value.encode()) <= 256:
                raise ValueError('explicit bounded database ownership required')
        if (not isinstance(self.dbname,str) or not re.fullmatch('[a-z][a-z0-9_]{0,62}',self.dbname)
                or type(self.port) is not int or not 1<=self.port<=65535
                or not isinstance(self.socket,str) or len(self.socket.encode())>90
                or not Path(self.socket).is_relative_to(PRIVATE_RUNTIME_ROOT) or Path(self.socket)==PRIVATE_RUNTIME_ROOT or '..' in Path(self.socket).parts):
            raise ValueError('explicit private native PG endpoint required')
        if self.mode == 'NEW_EMPTY_GENERATION':
            if (not isinstance(self.cluster,str) or not Path(self.cluster).is_absolute()
                    or '..' in Path(self.cluster).parts
                    or not Path(self.cluster).is_relative_to(PRIVATE_RUNTIME_ROOT)
                    or Path(self.socket) != Path(self.cluster)/'socket'
                    or Path(self.cluster) == PRIVATE_RUNTIME_ROOT
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
        self.native_env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'}
        self._startup_complete=False
        self._postgres_stderr=None
        self._startup_stderr=bytearray()

    async def _capture_stderr(self,process,buffer,*,startup_only=False):
        while chunk:=await process.stderr.read(1024):
            if startup_only and self._startup_complete:
                buffer.clear();continue
            remaining=8192-len(buffer)
            buffer.extend(chunk[:remaining])
            if len(chunk)>remaining and process.returncode is None:
                process.kill()  # Finite startup output budget, owned child only.
                return

    def _retain_failure(self,tool,exit_code,data):
        tool=Path(tool).name
        if tool not in {'initdb','pg_config','postgres'}:tool='native_tool'
        retained=False
        if data:
            # This exact private session root is excluded from provider outputs.
            parent=Path(self.spec.cluster).parent
            path=parent/('.rkb-native-'+digest(self.spec.database_generation.encode())[:20]+'.log')
            try:
                if parent.resolve(strict=True)!=parent:raise OSError('private parent')
                fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                with os.fdopen(fd,'wb') as handle:handle.write(bytes(data[:8192]))
                retained=True
            except OSError:pass  # Never overwrite retained diagnostic evidence.
        metadata=dict(event='native_pg_bootstrap_failed',tool=tool,exit_code=exit_code,
            diagnostic_bytes=min(8192,len(data)),diagnostic_retained=retained)
        missing=re.search(rb'error while loading shared libraries: ([A-Za-z0-9_.+-]{1,128}):',bytes(data))
        if missing:metadata['missing_library']=missing.group(1).decode('ascii')
        if re.search(rb'could not open directory "[^"\r\n]*": Permission denied',bytes(data)):
            metadata['startup_reason']='directory_read_denied'
        _LOG.warning(json.dumps(metadata,sort_keys=True))

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
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=self.native_env,**self._identity())
        errors=bytearray()
        capture=(asyncio.create_task(self._capture_stderr(process,errors))
            if getattr(process,'stderr',None) is not None else None)
        try:
            async with asyncio.timeout(timeout):
                # pg_config/initdb output is bounded; drain at most 64KiB.
                output = bytearray()
                while chunk := await process.stdout.read(4096):
                    output.extend(chunk)
                    if len(output)>65536:
                        raise BootstrapUnavailable('native_pg_output_bound')
                if await process.wait() != 0:
                    if capture is not None:await capture
                    self._retain_failure(args[0],process.returncode,errors)
                    raise BootstrapUnavailable('native_pg_command_failed')
                return output.decode('utf-8').strip()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            if capture is not None:
                if not capture.done():capture.cancel()
                await asyncio.gather(capture,return_exceptions=True)

    async def start_empty(self, *, intent, artifacts, sql_root, native_bundle=None):
        spec = self.spec
        if spec.mode != 'NEW_EMPTY_GENERATION':
            return
        cluster=Path(spec.cluster)
        if cluster.exists() or cluster.is_symlink():
            raise BootstrapUnavailable('native_pg_existing_cluster_refused')
        if (type(intent) is not dict or intent.get('codec') != 'rkb.empty-generation.v1'
                or intent.get('database') != spec.wire() or intent.get('journal_head') is not None
                or not intent.get('journal_stream') or not intent.get('run_id') or not intent.get('launch_nonce')):
            raise BootstrapUnavailable('native_pg_initialization_intent_missing')
        executable={spec.native_paths[name] for name in ('initdb','postgres','pg_config')}
        verified = {str(artifacts.root/e.requirement.name):(e.requirement.size,e.requirement.sha256,
            str(artifacts.root/e.requirement.name) in executable) for e in artifacts.entries if e.role=='code'}
        if native_bundle is not None:
            from .kaggle_native_bundle import VerifiedNativeBundle,NativeBundleSpec
            if (type(native_bundle) is not VerifiedNativeBundle or native_bundle.root!=artifacts.root
                    or intent.get('native_bundle')!=NativeBundleSpec(native_bundle.manifest_name,native_bundle.manifest_sha256).wire()
                    or not await asyncio.to_thread(native_bundle.current)):
                raise BootstrapUnavailable('native_pg_verified_bundle_required')
            manifest=verified.get(str(artifacts.root/native_bundle.manifest_name))
            if manifest is None or manifest[1]!=native_bundle.manifest_sha256:
                raise BootstrapUnavailable('native_pg_bundle_manifest_binding_missing')
            for path,size,sha,execute in native_bundle.pins:
                if path in verified or not Path(path).is_relative_to(native_bundle.prefix):
                    raise BootstrapUnavailable('native_pg_bundle_path_binding_invalid')
                verified[path]=(size,sha,execute)
            if any(path not in verified or not verified[path][2] for path in executable):
                raise BootstrapUnavailable('native_pg_bundle_executable_binding_invalid')
        elif intent.get('native_bundle') is not None:
            raise BootstrapUnavailable('native_pg_verified_bundle_required')
        if not set(spec.native_paths.values()) <= verified.keys():
            raise BootstrapUnavailable('native_pg_artifact_binding_missing')
        for path in spec.native_paths.values():
            size,sha,_ = verified[path]
            if _file_hash(path,size) != sha:
                raise BootstrapUnavailable('native_pg_artifact_hash_mismatch')
        pins=[(path,*entry) for path,entry in verified.items()]
        await asyncio.to_thread(grant_native_code_access,artifacts.root,pins,uid=spec.uid,gid=spec.gid)
        if native_bundle is not None:
            from .kaggle_native_env import verified_native_environment
            self.native_env=await asyncio.to_thread(verified_native_environment,native_bundle)
        try:
            account=pwd.getpwuid(spec.uid)
            if account.pw_gid!=spec.gid:raise KeyError
        except KeyError:
            raise BootstrapUnavailable('native_pg_service_account_unverified') from None
        from .kaggle_schema_migrate import bundle_migrations,migrate
        migrations=bundle_migrations({'sql/'+p.name:p.read_bytes()
            for p in Path(sql_root).glob('[0-9][0-9][0-9]_*.sql')})
        selected_sql=tuple(m.name for m in migrations)
        files={m.name:m.body for m in migrations}
        if len(migrations)!=spec.schema_version:
            raise BootstrapUnavailable('native_pg_bundle_version_mismatch')
        if intent.get('sql_hashes') != {name:digest(data) for name,data in files.items()}:
            raise BootstrapUnavailable('native_pg_sql_binding_mismatch')
        await asyncio.to_thread(ensure_private_runtime_root,native_uid=spec.uid,native_gid=spec.gid)
        paths = spec.native_paths
        library = await self._command(paths['pg_config'],'--pkglibdir',timeout=5)
        shared = await self._command(paths['pg_config'],'--sharedir',timeout=5)
        if (Path(paths['vector_library']) != Path(library)/'vector.so'
                or Path(paths['vector_control']) != Path(shared)/'extension/vector.control'
                or Path(paths['vector_sql']) != Path(shared)/'extension/vector--0.8.2.sql'):
            raise BootstrapUnavailable('native_pg_extension_binding_mismatch')
        cluster = Path(spec.cluster)
        try:
            if cluster.parent.resolve(strict=True) != cluster.parent:
                raise OSError('private parent symlink')
            for parent in (cluster.parent,*cluster.parent.parents):
                if not parent.is_relative_to(PRIVATE_RUNTIME_ROOT):break
                info=parent.stat()
                if info.st_uid not in (0,os.geteuid()) or stat.S_IMODE(info.st_mode)&0o022:
                    raise OSError('untrusted private parent')
        except OSError:
            raise BootstrapUnavailable('native_pg_parent_unverified') from None
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
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.PIPE,
            env=self.native_env,**self._identity())
        self._postgres_stderr=asyncio.create_task(self._capture_stderr(self.process,self._startup_stderr,startup_only=True))
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
            await self._initialize_schema(admin,files)
        # Fresh empty projections contain no accepted/source data; explicitly
        # publish only cursor zero under the same trusted writer lock.
        admin = await self.connect(**spec.connection(reader=False))
        async with admin:
            await admin.execute("SET statement_timeout='10s'; SET lock_timeout='2s'",prepare=False)
            await admin.commit()
            async with admin.transaction():
                await admin.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',
                                    (spec.database_generation,spec.writer_generation))
        self._startup_complete=True
        self._startup_stderr.clear()  # Accepted/query diagnostics are never retained.

    async def _initialize_schema(self,admin,files):
        """Production fresh-schema sequence, independently testable on CI PG17."""
        from .kaggle_schema_migrate import migrate
        selected_sql=tuple(files)
        spec=self.spec
        async with admin.transaction():
            await admin.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            await create_native_roles(admin)
        await migrate(admin,{'sql/'+name:files[name] for name in selected_sql})
        await initialize_database(admin,database_generation=spec.database_generation,
            writer_generation=spec.writer_generation)
        async with admin.transaction():
            await configure_native_roles(admin,selected_sql=selected_sql,dbname=spec.dbname,create_roles=False)
            await admin.execute('GRANT SELECT ON rkb_v4.schema_migrations TO rkb_reader')
            await admin.execute('REVOKE ALL ON rkb_v4.schema_migrations FROM rkb_writer')

    async def aclose(self):
        try:
            if self.process is not None and self.process.returncode is None:
                self.process.send_signal(signal.SIGINT)  # PG fast shutdown; owned postmaster only.
                async with asyncio.timeout(30):
                    await self.process.wait()
        finally:
            if self._postgres_stderr is not None:
                if not self._postgres_stderr.done():self._postgres_stderr.cancel()
                await asyncio.gather(self._postgres_stderr,return_exceptions=True)
                if not self._startup_complete:
                    self._retain_failure('postgres',self.process.returncode,self._startup_stderr)
                self._postgres_stderr=None
