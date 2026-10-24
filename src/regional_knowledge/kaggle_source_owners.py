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
                           destination_ref='rkb-source-http-response',source_import=None,geo_manifest_operation_keys=(),source_restore=None,source_snapshot=None,geo_bootstrap=None,dataset_seed=None,telegram_runtime_source=None):
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
        if source_restore is not None and source_restore.dataset_checkpoint is not None:
            from .dataset_recovery_store import DatasetRecoveryStore
            from .kaggle_dataset_publisher import KaggleDatasetPublisher,authenticated_service_api
            checkpoint=source_restore.dataset_checkpoint
            api=authenticated_service_api()
            publisher=KaggleDatasetPublisher(api,provider_ref=checkpoint.owner_slug+'/'+checkpoint.dataset_slug,
                owner=api.get_config_value(api.CONFIG_NAME_USER))
            store=DatasetRecoveryStore(queue,checkpoint=checkpoint,manifest_ref=source_restore.manifest,
                publisher=publisher,writer_guard=writer,stream=stream)
        else:
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
            geo_manifest_operation_keys=geo_manifest_operation_keys,source_restore=source_restore,source_snapshot=source_snapshot,geo_bootstrap=geo_bootstrap,
            dataset_seed=dataset_seed,telegram_runtime_source=telegram_runtime_source,**import_options))
    except BaseException:
        if spool is not None:spool.close()
        if writer is not None:writer.close()
        if created and admission is not None:
            admission.close();queue._shared_reads=None
        raise


def configured_expert_slack_ingress(target_startup, config_path):
    """Opt-in signed Slack route using the existing Dev person and spool owners.

    The installed signing secret is read only for an explicitly configured
    route. Neither this factory nor the HTTP ingress seals expert requests:
    until a separate remote-intent reconciler exists, valid messages remain
    received/503 and cannot be dispatched or counted as Saved.
    """
    import re
    from .expert_request_control import TargetExpertRequestControl
    from .expert_slack_ingress import ExpertSlackIngress

    if type(target_startup) is not TargetServiceStartup:
        raise TypeError('Slack ingress requires the exact target startup')

    def private_bytes(value, maximum):
        if (type(value) is not str or not 1 <= len(value) <= 1024
                or not Path(value).is_absolute()):
            raise ValueError('absolute private Slack config path required')
        fd = os.open(value, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077
                    or not 1 <= before.st_size <= maximum):
                raise ValueError('private Slack config file required')
            raw = os.read(fd, maximum + 1)
            after = os.fstat(fd)
            if (len(raw) != before.st_size or os.read(fd, 1)
                    or (after.st_size, after.st_mtime_ns) !=
                    (before.st_size, before.st_mtime_ns)):
                raise ValueError('Slack config changed during read')
            return raw
        finally:
            os.close(fd)

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('duplicate Slack config field')
            value[key] = item
        return value

    value = json.loads(private_bytes(config_path, 4096), object_pairs_hook=unique)
    # The trusted Socket Mode SDK uses a preauthenticated WebSocket and does
    # not need an HTTP signing secret. The original HTTP v1 schema is unchanged.
    socket_only=(type(value) is dict
                 and value.get('codec')=='rkb.expert-slack-socket.v1')
    required = {'codec', 'app_id', 'team_id', 'bot_user_id', 'allowed_channels'}
    if not socket_only:
        required.add('signing_secret_file')
    if (type(value) is not dict or set(value) != required
            or value['codec'] not in ('rkb.expert-slack-ingress.v1',
                                      'rkb.expert-slack-socket.v1')
            or type(value['app_id']) is not str
            or re.fullmatch(r'A[A-Z0-9]+', value['app_id']) is None
            or type(value['team_id']) is not str
            or re.fullmatch(r'T[A-Z0-9]+', value['team_id']) is None
            or type(value['bot_user_id']) is not str
            or re.fullmatch(r'[UW][A-Z0-9]+', value['bot_user_id']) is None
            or type(value['allowed_channels']) is not list
            or len(value['allowed_channels']) > 32
            or any(type(channel) is not str
                   or re.fullmatch(r'[CG][A-Z0-9]+', channel) is None
                   for channel in value['allowed_channels'])
            or len(set(value['allowed_channels'])) != len(value['allowed_channels'])):
        raise ValueError('exact Slack ingress config required')
    if socket_only and not value['allowed_channels']:
        raise ValueError('explicit Socket Mode channel allowlist required')
    signing_secret = (None if socket_only else
        private_bytes(value['signing_secret_file'], 257).decode('ascii').removesuffix('\n'))
    control = TargetExpertRequestControl(
        target_startup.config.participants, target_startup.config.spool,
        app_id=value['app_id'], team_id=value['team_id'],
        bot_user_id=value['bot_user_id'],
        allowed_channels=tuple(value['allowed_channels']))
    return ExpertSlackIngress(control, signing_secret=signing_secret,
                              socket_only=socket_only)


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
    fields={'codec','run_id','stream','database','registry_sha256','observer_sha256'}
    if (type(value) is not dict or not fields<=value.keys() or set(value)-fields-{'source_import','geo_manifest_operation_keys','source_restore','source_snapshot','geo_bootstrap','dataset_seed','telegram_original_locator_file'}
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
    from .kaggle_current_registry import current_registry
    from .kaggle_source_history_native import observer_fingerprint
    from .source_history_wss import WssNativeHistoryProofAdapter
    provider,_owner=_runtime()
    people=provider.personal_policy.participants
    object_store=_object_store_from_env()
    if type(object_store) is not S3ObjectStore:raise ValueError('existing configured native S3 authority required')
    database=OwnedDatabase(**value['database'])
    queue=BgeQueue(queue_path)
    with queue.connect(write=False) as db:
        runtime=queue._runtime_row(db,value['run_id'])
        observed=json.loads(runtime['observed_json']) if runtime is not None and runtime['observed_json'] else None
    if observed is not None and observed.get('database_generation')==database.database_generation:
        from dataclasses import replace
        database=replace(database,schema_version=observed.get('schema_version',database.schema_version))
    registry=current_registry()
    # Code pins belong to the installed main release, not a former run's
    # template. Native observations still prove the actual loaded code.
    history=WssNativeHistoryProofAdapter(database=database,expected_observer_sha256=observer_fingerprint(),
        geo_bootstrap_operation_keys=tuple(value.get('geo_manifest_operation_keys',())))
    import_owners=None
    if 'source_import' in value:
        from .kaggle_import_composition import SourceImportOwners
        import_owners=SourceImportOwners.from_wire(value['source_import'])
    restore=None
    if 'source_restore' in value:
        from .kaggle_restore_startup import SourceRestoreConfig
        restore=SourceRestoreConfig.from_wire(value['source_restore'])
    snapshot=None
    if 'source_snapshot' in value:
        from .kaggle_snapshot_startup import SourceSnapshotConfig
        snapshot=SourceSnapshotConfig.from_wire(value['source_snapshot'])
    geo_bootstrap=None
    if 'geo_bootstrap' in value:
        from .kaggle_geo_install import SourceGeoInstallConfig
        geo_bootstrap=SourceGeoInstallConfig.from_wire(value['geo_bootstrap'])
    dataset_seed=None
    if 'dataset_seed' in value:
        from .kaggle_dataset_seed import DatasetSeedConfig
        dataset_seed=DatasetSeedConfig.from_wire(value['dataset_seed'])
    telegram_source=None
    if 'telegram_original_locator_file' in value:
        if import_owners is None:raise ValueError('Telegram original requires configured source import')
        from .telegram_runtime_owners import configured_rkb1_source
        telegram_source=configured_rkb1_source(queue,value['telegram_original_locator_file'])
    return compose_source_service(queue=queue,oauth_provider=provider,participants=people,
        object_store=object_store,stream=value['stream'],registry=registry,history_adapter=history,
        run_id=value['run_id'],database_generation=database.database_generation,writer_generation=database.writer_generation,
        source_import=import_owners,geo_manifest_operation_keys=tuple(value.get('geo_manifest_operation_keys',())),source_restore=restore,source_snapshot=snapshot,geo_bootstrap=geo_bootstrap,
        dataset_seed=dataset_seed,telegram_runtime_source=telegram_source)
