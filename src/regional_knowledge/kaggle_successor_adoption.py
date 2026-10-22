"""Rebind retained Dev source owners to an exact controller-prepared restore.

The public backend, spool, canonical auth/admission and route callables survive.
Only run-bound history/recovery/import capabilities are replaced, while closed.
"""
from dataclasses import replace
import json
import logging

from .kaggle_cold_successor import successor_source_config

_LOG = logging.getLogger(__name__)


async def adopt_prepared_successor(startup):
    original_json = startup.config.successor_source_json
    if original_json is None:
        return False
    old = startup.config
    queue, spool = old.queue, old.spool
    original = json.loads(original_json)
    value = successor_source_config(queue, original)
    if old.writer_owner is None or value['run_id'] == old.writer_owner.run_id:
        return False
    from .source_history_wss import WssNativeHistoryProofAdapter
    from .kaggle_owned_pg import OwnedDatabase
    from .kaggle_restore_startup import SourceRestoreConfig, SourceRestoreCoordinator
    from .queue_writer import QueueWriterAuthority
    from .recovery_journal import RecoveryJournal
    from .dataset_recovery_store import DatasetRecoveryStore
    from .recovery_s3 import S3ConditionalRecoveryStore
    adapter = old.history_adapter
    if type(adapter) is not WssNativeHistoryProofAdapter or startup.snapshot is not None:
        raise ValueError('existing verified restore history owner required for successor')
    database = OwnedDatabase(**value['database'])
    restore = SourceRestoreConfig.from_wire(value['source_restore'])
    # This transition cannot upgrade schema or alter registry/catalog authority.
    if (replace(database, writer_generation=adapter.database.writer_generation) != adapter.database
            or old.source_restore is None):
        raise ValueError('successor cannot change source database profile')
    if (spool._writing or spool._lock.locked()
            or startup.importer is not None and startup.importer._lock.locked()
            or startup.dataset_checkpoints is not None and startup.dataset_checkpoints._refresh_lock.locked()):
        raise ValueError('retained source operation must settle before successor adoption')
    # Close admission through the retained gate before ANY await/owner change.
    startup.coverage = startup.binding = None
    startup.gate.invalidate(); startup.case_gate.invalidate()
    if startup.geo_gate is not None: startup.geo_gate.invalidate()
    spool.invalidate_source_history()
    if startup.geo_installer is not None:
        await startup.geo_installer.aclose()
    if startup.restorer is not None:
        await startup.restorer.aclose()
    async with spool._lock:
        with queue.connect(write=False) as db:
            prior = db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
            current = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]
            retained = (prior is not None and prior['run_id'] == value['run_id']
                and prior['writer_generation'] == database.writer_generation
                and prior['database_generation'] == database.database_generation
                and prior['stream'] == original['stream'])
            if (current != value['run_id'] or prior is None or not retained and
                    (prior['active'] or prior['publication_json'] is not None
                     or prior['run_id'] != old.writer_owner.run_id)):
                raise ValueError('retained retired writer and current successor required')
        writer = QueueWriterAuthority(queue, run_id=value['run_id'],
            database_generation=database.database_generation, writer_generation=database.writer_generation,
            stream=original['stream'])
        old_journal = spool.journal
        try:
            old_store = old_journal.store
            if type(old_store) is DatasetRecoveryStore:
                if restore.dataset_checkpoint is None:
                    raise ValueError('Dataset successor must retain Dataset recovery')
                store = DatasetRecoveryStore(queue, checkpoint=restore.dataset_checkpoint,
                    manifest_ref=restore.manifest, publisher=old_store.publisher,
                    writer_guard=writer, stream=old_journal.stream)
            elif type(old_store) is S3ConditionalRecoveryStore and restore.dataset_checkpoint is None:
                store = S3ConditionalRecoveryStore(client=old_store.client, bucket=old_store.bucket,
                    prefix=old_store.prefix, writer_guard=writer)
            else:
                raise ValueError('same retained recovery storage authority required')
            journal = RecoveryJournal(store, stream=old_journal.stream)
            history_adapter = WssNativeHistoryProofAdapter(database=database,
                expected_catalog_sha256=adapter.catalog, expected_acl_sha256=adapter.acl,
                expected_observer_sha256=adapter.observer, coverage_seconds=adapter.coverage_seconds,
                timeout_seconds=adapter.timeout, max_commands=adapter.max_commands,
                max_history_bytes=adapter.max_bytes, geo_bootstrap_operation_keys=adapter.geo_bootstrap_operation_keys,
                geo_upgrade_pins=adapter.geo_upgrade_pins, native_source_upgrade_pins=adapter.native_source_upgrade_pins)
            spool.journal = journal
            imports = {}
            if old.import_originals is not None:
                from .original_intake_controller import OriginalIntakeController
                originals = old.import_originals
                def current(binding):
                    return startup.binding_current(binding)
                imports = dict(import_originals=OriginalIntakeController(queue=queue, spool=spool,
                    participants=old.participants, object_store=originals.store, binding_current=current,
                    current_binding=originals.current_binding, allowed_origins=originals.allowed_origins,
                    max_bytes=originals.max_bytes),
                    import_launch_config_sha256=value['source_import']['launch_config_sha256'])
            config = replace(old, history_adapter=history_adapter,
                writer_generation=database.writer_generation, writer_owner=writer,
                writer_current=writer.current, source_restore=restore, **imports)
            startup.config = config
            history = history_adapter.bind(spool=spool, routes=startup.routes,
                binding_current=startup.binding_current, writer_generation=database.writer_generation)
            restorer = SourceRestoreCoordinator(startup, restore)
            importer = None
            if imports:
                from .kaggle_importer import KaggleSourceImporter
                importer = KaggleSourceImporter(startup, originals=config.import_originals,
                    tokenizer=config.import_tokenizer)
            checkpoints = None
            if restore.dataset_checkpoint is not None:
                from .kaggle_dataset_checkpoint import SourceDatasetCheckpoints
                checkpoints = SourceDatasetCheckpoints(startup)
            geo = None
            if config.geo_bootstrap is not None:
                from .kaggle_geo_install import SourceGeoInstaller
                geo = SourceGeoInstaller(startup, config.geo_bootstrap, upgrade_pins=config.geo_dependency_upgrade)
            startup.history, startup.restorer = history, restorer
            startup.importer, startup.dataset_checkpoints = importer, checkpoints
            startup.geo_installer = geo
            startup._geo_denied = None
        except BaseException:
            startup.config = old
            spool.journal = old_journal
            writer.close()
            raise
        old.writer_owner.close()
    _LOG.info('target_successor state=recovery_pending run_id=%s', value['run_id'])
    return True
