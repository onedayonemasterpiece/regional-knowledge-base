"""Production Dev owner composition from existing auth, queue and S3 client.

No environment credential discovery, legacy corpus/backend, new service or
alternate dispatcher. Construction performs no provider operation; the MCP
lifespan activates only verified initial genesis or exact persisted restart.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

from .bge_queue import BgeQueue
from .control_spool import ControlSpool
from .kaggle_apply import HandlerRegistry
from .kaggle_service_startup import TargetServiceConfig,TargetServiceStartup
from .oauth_provider import RegionalOAuthProvider
from .participants import Participants
from .read_authority import ReadAuthority
from .recovery_journal import RecoveryJournal
from .recovery_s3 import S3ConditionalRecoveryStore
from .queue_writer import QueueWriterAuthority


def compose_source_service(*,queue,oauth_provider,participants,object_store,stream,registry,
                           history_adapter,run_id,database_generation,writer_generation,
                           destination_ref='rkb-source-http-response',source_import=None,geo_manifest_operation_keys=(),source_restore=None):
    """Reuse the actual owner objects; returns the real server.main input.

    object_store is the already configured S3ObjectStore authority/client, not
    a new credential or bucket configuration. Registry/history pins come from
    the existing independently verified native/loaded version observations.
    """
    if (not isinstance(queue,BgeQueue) or not isinstance(oauth_provider,RegionalOAuthProvider)
            or not isinstance(participants,Participants) or not isinstance(registry,HandlerRegistry)
            or oauth_provider.personal_policy is None
            or oauth_provider.personal_policy.participants is not participants):
        raise TypeError('actual canonical OAuth/participants/queue/registry required')
    admission=getattr(queue,'_shared_reads',None);created=admission is None
    writer=spool=None
    try:
        if created:
            authority=ReadAuthority(participants,oauth_provider=oauth_provider,
                issuer=oauth_provider.issuer,resource=oauth_provider.resource)
            admission=queue.configure_shared_reads(authority)
        writer=QueueWriterAuthority(queue,run_id=run_id,database_generation=database_generation,
            writer_generation=writer_generation,stream=stream)
        store=S3ConditionalRecoveryStore(client=object_store.client,bucket=object_store.bucket,
            prefix='recovery/v1/'+hashlib.sha256(stream.encode()).hexdigest()+'/',writer_guard=writer)
        journal=RecoveryJournal(store,stream=stream)
        spool=ControlSpool(queue,participants,journal,registry)
        import_options={}
        if source_import is not None:
            from .kaggle_import_composition import SourceImportOwners
            if type(source_import) is not SourceImportOwners:
                raise TypeError('explicit typed source import owners required')
            import_options=source_import.compose(queue=queue,spool=spool,participants=participants,
                object_store=object_store,writer=writer)
        return TargetServiceStartup(TargetServiceConfig(oauth_provider,participants,queue,spool,history_adapter,
            writer_generation,writer.current,destination_ref=destination_ref,writer_owner=writer,
            geo_manifest_operation_keys=geo_manifest_operation_keys,source_restore=source_restore,**import_options))
    except BaseException:
        if spool is not None:spool.close()
        if writer is not None:writer.close()
        if created and admission is not None:
            admission.close();queue._shared_reads=None
        raise


def configured_source_service():
    """Fixed production entrypoint over the application's existing owner config."""
    path=os.getenv('RKB_KAGGLE_SOURCE_CONFIG','').strip()
    if not path:raise RuntimeError('RKB_KAGGLE_SOURCE_CONFIG is required for target startup')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode&0o077 or not 1<=info.st_size<=65536:
            raise ValueError('private bounded source owner config required')
        data=os.read(fd,65537)
        if len(data)!=info.st_size or os.read(fd,1):raise ValueError('source owner config changed')
    finally:os.close(fd)
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError('duplicate source owner config field')
            result[key]=value
        return result
    value=json.loads(data,object_pairs_hook=unique)
    fields={'codec','run_id','stream','database','command_handlers','catalog_sha256','acl_sha256','observer_sha256'}
    if (type(value) is not dict or not fields<=value.keys() or set(value)-fields-{'source_import','geo_manifest_operation_keys','source_restore'}
            or value['codec']!='rkb.source-service.v1'):
        raise ValueError('exact source owner configuration required')
    if 'geo_manifest_operation_keys' in value and type(value['geo_manifest_operation_keys']) is not list:
        raise ValueError('exact GEO manifest operation key list required')
    queue_path=os.getenv('RKB_BGE_QUEUE_PATH','').strip()
    if not queue_path or not Path(queue_path).is_file():raise ValueError('existing configured queue required')
    # Existing fixed app constructors require existing OAuth state/key and
    # canonical participant DB. They never mint a credential or create a user.
    from .personal_auth_cli import _runtime
    from .supabase_backend import _object_store_from_env
    from .object_store import S3ObjectStore
    from .kaggle_owned_pg import OwnedDatabase
    from .kaggle_source_commands import fixed_registry
    from .source_history_wss import WssNativeHistoryProofAdapter
    provider,_owner=_runtime()
    people=provider.personal_policy.participants
    object_store=_object_store_from_env()
    if type(object_store) is not S3ObjectStore:raise ValueError('existing configured native S3 authority required')
    database=OwnedDatabase(**value['database'])
    registry=fixed_registry(database,value['command_handlers'])
    history=WssNativeHistoryProofAdapter(database=database,expected_catalog_sha256=value['catalog_sha256'],
        expected_acl_sha256=value['acl_sha256'],expected_observer_sha256=value['observer_sha256'])
    import_owners=None
    if 'source_import' in value:
        from .kaggle_import_composition import SourceImportOwners
        import_owners=SourceImportOwners.from_wire(value['source_import'])
    restore=None
    if 'source_restore' in value:
        from .kaggle_restore_startup import SourceRestoreConfig
        restore=SourceRestoreConfig.from_wire(value['source_restore'])
    return compose_source_service(queue=BgeQueue(queue_path),oauth_provider=provider,participants=people,
        object_store=object_store,stream=value['stream'],registry=registry,history_adapter=history,
        run_id=value['run_id'],database_generation=database.database_generation,writer_generation=database.writer_generation,
        source_import=import_owners,geo_manifest_operation_keys=tuple(value.get('geo_manifest_operation_keys',())),source_restore=restore)
