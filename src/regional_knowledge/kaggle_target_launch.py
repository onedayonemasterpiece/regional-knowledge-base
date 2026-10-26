"""Controller-only immutable source assembly and pre-attachment launch intent."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import zlib

from .bge_contract import REVISION, SPACE
from .kaggle_owned_pg import canonical,digest
from .kaggle_readiness import SearchRuntimeExpectation
from .kaggle_target_runtime import TargetLaunchSpec
from .recovery_journal import RecoveryJournal

MODULES = ('__init__','bge_contract','kaggle_protocol','kaggle_query','kaggle_search',
    'kaggle_readiness','kaggle_worker_query','kaggle_warm_read','kaggle_warm_maintenance','kaggle_database_probe','kaggle_bootstrap',
    'kaggle_owned_pg','kaggle_schema_migrate','kaggle_schema_live','kaggle_warm_maintenance','kaggle_target_runtime','kaggle_apply','recovery_journal','query_telemetry',
    'source_import','kaggle_source_commands','kaggle_current_registry','kaggle_stage0','kaggle_restored_database','kaggle_native_bundle',
    'kaggle_environment','kaggle_pg_snapshot_native','kaggle_recovery_phases','kaggle_snapshot_channel','kaggle_snapshot_io',
    'native_snapshot_transfer','kaggle_bootstrap_pins','kaggle_native_env','kaggle_source_acl','kaggle_source_history_native',
    'original_intake','telegram_original','telegram_runtime_binding','telegram_runtime_channel','kaggle_original_native','kaggle_original_parser','kaggle_djvu_inspect','kaggle_djvu_pixels','kaggle_djvu_elf','kaggle_djvu_decoder_child','file_ingress','source_adapter','ingestion',
    'source_import_v2','story_contracts','kaggle_story_read','source_import_v3','kaggle_fact_contracts','kaggle_fact_read','kaggle_source_read','source_import_v4','source_manifest',
    'story_editorial','disagreement_contracts','disagreement_native','geo_migration','geo_manifest_commands','kaggle_dataset_base',
    'kaggle_case_contracts','kaggle_case_read','kaggle_geo_contracts','kaggle_geo_read',
    'kaggle_domain_bindings','kaggle_domain_schema','kaggle_editorial_contracts','kaggle_editorial_read','source_graph','entity_graph','contracts','kaggle_source_graph_read','kaggle_source_geo_mvp','stage_graph','poi_reference','kaggle_recovery_composition','kaggle_snapshot_drill','kaggle_acceptance_contracts','source_stage','source_activation','source_graph_native','source_catalog_command','kaggle_native_source_bindings',
    'kaggle_source_materializer','kaggle_source_batch','source_batch','source_batch_selection','source_batch_context','source_packet_tails','kaggle_import_contracts','rights','search_material','region_policy','continuation_context',
    'kaggle_passage_illustrations')
MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_BUNDLE_JSON_BYTES = 3 * 1024 * 1024
MAX_NOTEBOOK_SOURCE_BYTES = 1_000_000
def source_bundle(repository):
    repository = Path(repository)
    files = {f'regional_knowledge/{name}.py':(repository/'src/regional_knowledge'/f'{name}.py').read_bytes()
             for name in MODULES}
    from .kaggle_schema_migrate import bundle_migrations
    sql_files = {f'sql/{path.name}': path.read_bytes()
                 for path in (repository/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql')}
    bundle_migrations(sql_files)
    files.update(sql_files)
    if sum(map(len,files.values())) > MAX_SOURCE_BYTES:
        raise ValueError('immutable target source byte budget exceeded')
    manifest = {name:digest(data) for name,data in sorted(files.items())}
    bundle_id = digest(canonical(manifest))
    # A short directory label avoids repeating the complete manifest digest in
    # every inventory path. Integrity still checks each file's full SHA-256;
    # a label collision fails that check instead of accepting different bytes.
    relative = 'r-'+bundle_id[:16]
    # Source members are UTF-8 Python/SQL. Compress their text directly; the
    # outer base64 still transports the compressed bytes in the notebook.
    encoded = {name:data.decode('utf-8') for name,data in sorted(files.items())}
    entries = [dict(name=relative+'/'+name,size=len(data),sha256=digest(data),role='code')
               for name,data in sorted(files.items())]
    return relative,encoded,entries


async def preview_target_history(target, recovery_journal):
    """Read-only controller preflight using the SAME exact recovery authority.

    It cannot initiate a notebook, create local SQLite rows, promote a
    snapshot, or classify an initialized PG runtime as ready. A nonempty
    accepted head only proves that NEW_EMPTY cannot safely run again;
    verified restore still needs independent exact manifest/receipt proof.
    """
    if type(target) is not TargetLaunchSpec or not isinstance(recovery_journal,RecoveryJournal):
        raise TypeError('typed target and existing real RecoveryJournal required')
    head=await recovery_journal.head()  # UNKNOWN does not mean empty.
    sequence=head.reference.last_sequence if head is not None else 0
    mode=target.database.mode
    status=('restore_required' if head is not None else 'empty_genesis_permitted') \
        if mode=='NEW_EMPTY_GENERATION' else 'verify_restore_contract'
    return dict(status=status,target_mode=mode,accepted_head_sequence=sequence,can_push=False)


async def persist_empty_intent(queue, run_id, target, *, launch_nonce, recovery_journal, sql_hashes):
    """Only an actual read from the injected existing remote authority proves empty.

    The owner must hold writer/intake fencing while creating/initializing genesis.
    This operation never creates a journal/store or replays/restores any history.
    """
    if type(target) is not TargetLaunchSpec or target.database.mode != 'NEW_EMPTY_GENERATION':
        raise ValueError('explicit empty-generation initialization required')
    if not isinstance(recovery_journal,RecoveryJournal):
        raise TypeError('existing real RecoveryJournal required')
    preflight=await preview_target_history(target,recovery_journal)
    if preflight['status']!='empty_genesis_permitted':
        raise ValueError('accepted history requires verified restore; initialization refused')
    intent = dict(codec='rkb.empty-generation.v1',run_id=run_id,launch_nonce=launch_nonce,
        database=target.database.wire(),journal_stream=recovery_journal.stream,journal_head=None,
        stage0_receipt=target.stage0_receipt,sql_hashes=dict(sql_hashes),
        native_bundle=target.native_bundle.wire() if target.native_bundle is not None else None)
    data = canonical(intent).decode()
    with queue.connect() as db:
        run = db.execute('SELECT status,connection_generation,channel_active,launch_nonce FROM runs WHERE id=?',
                         (run_id,)).fetchone()
        if (run is None or run['status']!='starting' or run['connection_generation']
                or run['channel_active'] or run['launch_nonce']!=launch_nonce):
            raise ValueError('initialization intent must precede first attachment')
        db.execute('CREATE TABLE IF NOT EXISTS bge_empty_generation_intents(run_id TEXT PRIMARY KEY,intent_json TEXT NOT NULL)')
        prior = db.execute('SELECT intent_json FROM bge_empty_generation_intents WHERE run_id=?',(run_id,)).fetchone()
        if prior is not None and prior['intent_json'] != data:
            raise ValueError('initialization intent is immutable')
        if prior is None:
            db.execute('INSERT INTO bge_empty_generation_intents VALUES(?,?)',(run_id,data))
    return intent


def read_empty_intent(queue,run_id):
    with queue.connect() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bge_empty_generation_intents'").fetchone():
            return None
        row = db.execute('SELECT intent_json FROM bge_empty_generation_intents WHERE run_id=?',(run_id,)).fetchone()
        return json.loads(row['intent_json']) if row else None


def compose_target(template,config,target,*,queue,repository):
    if type(target) is not TargetLaunchSpec or config.get('transport')!='wss':
        raise ValueError('typed target requires existing WSS transport')
    relative,files,code_entries = source_bundle(repository)
    root = Path(target.artifact_root)
    entries = [dict(e) for e in target.artifacts] + code_entries
    if len({e['name'] for e in entries}) != len(entries):
        raise ValueError('unique bounded final artifact inventory required')
    intent = read_empty_intent(queue,config['run_id'])
    if target.database.mode == 'NEW_EMPTY_GENERATION':
        sql_hashes = {name[4:]:digest(data.encode('utf-8')) for name,data in files.items() if name.startswith('sql/')}
        if (intent is None or intent['database']!=target.database.wire()
                or intent['run_id']!=config['run_id'] or intent['launch_nonce']!=config['launch_nonce']
                or intent.get('native_bundle')!=(target.native_bundle.wire() if target.native_bundle is not None else None)
                or intent['sql_hashes']!=sql_hashes):
            raise ValueError('persist actual empty-journal initialization intent first')
    runtime_wire=target.wire()
    runtime_wire.pop('stage0',None)  # One copy, consumed only by load_encoder.
    # The complete external+source inventory is verified once in prepared_model.
    # Runtime consumes that retained VerifiedArtifacts object, never this second
    # copy.
    runtime_wire.pop('artifacts',None)
    config.update(shared_reads=True,prepared_model=dict(root=str(root),entries=entries,
        max_total_bytes=sum(e['size'] for e in entries)),target_runtime=dict(
        **runtime_wire,sql_root=str(root/relative/'sql'),initialization_intent=intent))
    if target.stage0 is not None:
        config['stage0']=target.stage0.wire()
    # This extractor only writes predetermined bounded source files. It runs
    # before control; model imports, pool creation and initdb occur after ATTACH.
    entry = '''
if __name__ == '__main__':
    import base64 as _b64, hashlib as _hashlib, sys as _sys, json as _json, zlib as _zlib, time as _time
    from pathlib import Path as _Path
    def _rkb_stage(stage):
        print(_json.dumps(dict(event='worker_bootstrap',run_id=RUN_CONFIG.get('run_id'),
            stage=stage,timestamp=_time.time())),flush=True)
    _rkb_stage('python_start')
    _rkb_stage('bundle_unpack_start')
    _decoder = _zlib.decompressobj()
    _raw = _decoder.decompress(_b64.b64decode(BUNDLE_PACKED, validate=True), BUNDLE_LIMIT + 1)
    if len(_raw) > BUNDLE_LIMIT or not _decoder.eof or _decoder.unused_data or _decoder.unconsumed_tail:
        raise RuntimeError('target_bundle_transport_invalid')
    _bundle = _json.loads(_raw)
    _root = _Path(BUNDLE_ROOT)
    _artifact_root = _root.parent
    if _artifact_root != _Path(RUN_CONFIG['prepared_model']['root']):
        raise RuntimeError('target_bundle_artifact_root_mismatch')
    if _artifact_root == _Path('/kaggle/temp/rkb-artifacts'):
        # The only new parent permitted at cold start; no arbitrary mkdir -p.
        import os as _os, stat as _stat
        _fd = _os.open('/kaggle', _os.O_RDONLY|_os.O_DIRECTORY|_os.O_NOFOLLOW)
        try:
            for _part in (None, 'temp', 'rkb-artifacts'):
                _created = False
                if _part is not None:
                    try:
                        _os.mkdir(_part, mode=0o711, dir_fd=_fd)
                        _created = True
                    except FileExistsError:
                        pass
                    _next = _os.open(_part, _os.O_RDONLY|_os.O_DIRECTORY|_os.O_NOFOLLOW, dir_fd=_fd)
                    _os.close(_fd)
                    _fd = _next
                    if _created: _os.fchmod(_fd, 0o711)
                _info = _os.fstat(_fd)
                if (_info.st_uid not in (0, _os.geteuid()) or
                        _stat.S_IMODE(_info.st_mode)&0o022 or not _info.st_mode&_stat.S_IXOTH):
                    raise RuntimeError('target_bundle_parent_invalid')
        finally:
            _os.close(_fd)
    elif _artifact_root != _Path('/kaggle/working'):
        raise RuntimeError('target_bundle_artifact_root_invalid')
    if _artifact_root.resolve(strict=True) != _artifact_root:
        raise RuntimeError('target_bundle_parent_invalid')
    try:
        _root.mkdir(mode=0o700)
        _new = True
    except FileExistsError:
        _new = False
    if _root.is_symlink() or not _root.is_dir():
        raise RuntimeError('target_bundle_invalid')
    for _name, _encoded in _bundle.items():
        _file = _root / _name
        _data = _encoded.encode('utf-8')
        if _new:
            _file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with _file.open('xb') as _handle:
                _handle.write(_data)
            _file.chmod(0o400)
        if (_file.is_symlink() or _file.parent.is_symlink()
                or _file.stat().st_size != len(_data)
                or _hashlib.sha256(_file.read_bytes()).digest() != _hashlib.sha256(_data).digest()):
            raise RuntimeError('target_bundle_bytes_mismatch')
    _sys.path.insert(0, str(_root))
    _rkb_stage('bundle_unpack_complete')
    class _DeferredTargetRuntime:
        def __init__(self):
            self.instance = None
        async def prepare(self, encoder, **identity):
            # load_encoder has completed Stage0 only AFTER ATTACH/WELCOME.
            # PostgreSQL/pydantic/pool imports cannot precede that preparation.
            from regional_knowledge.kaggle_target_runtime import TargetRuntime
            if self.instance is None:
                self.instance = TargetRuntime(RUN_CONFIG, executed_source=str(_Path(__file__).resolve()))
            return await self.instance.prepare(encoder, **identity)
        async def aclose(self):
            if self.instance is not None:
                await self.instance.aclose()
    if RUN_CONFIG['target_runtime'].get('recovery') is not None:
        _rkb_stage('recovery_import_start')
        from regional_knowledge.kaggle_recovery_composition import DeferredRecoveryRuntime
        _rkb_stage('recovery_import_complete')
        _rkb_stage('recovery_construct_start')
        runtime = DeferredRecoveryRuntime(RUN_CONFIG, executed_source=str(_Path(__file__).resolve()))
        _rkb_stage('recovery_construct_complete')
        _rkb_stage('wss_worker_start')
        main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose,
             recovery_handler=runtime, snapshot_channel=runtime.ticket_channel)
    else:
        runtime = _DeferredTargetRuntime()
        _rkb_stage('wss_worker_start')
        main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose)
'''.replace('BUNDLE_PACKED',repr(base64.b64encode(zlib.compress(canonical(files),9)).decode()))\
    .replace('BUNDLE_LIMIT',str(MAX_BUNDLE_JSON_BYTES)).replace('BUNDLE_ROOT',repr(str(root/relative)))
    if template.count("if __name__=='__main__':main()") != 1:
        raise ValueError('target entrypoint slot missing')
    template = template.replace("if __name__=='__main__':main()",entry)
    source = template.replace('RUN_CONFIG = {}','RUN_CONFIG = '+repr(config),1)
    if len(source.encode())>MAX_NOTEBOOK_SOURCE_BYTES:
        raise ValueError('provider notebook source byte budget exceeded')
    def role_hash(roles):
        return digest(canonical(sorted((e['role'],e['name'],e['size'],e['sha256'])
                                      for e in entries if e['role'] in roles)))
    hashes = dict(code=role_hash({'code'}),launch_config=digest(canonical(config)),
        executed_source=digest(source.encode()),dependency=target.dependency_sha256,
        model=digest(canonical(dict(revision=REVISION,space=SPACE,
            artifacts=role_hash({'weight','model_config','tokenizer'})))),schema=digest(canonical(target.database.schema_version)))
    database = target.database
    # Final source hash is persisted outside its own source bytes: no self hash.
    if target.bootstrap_only:
        from .kaggle_bootstrap_pins import BootstrapRuntimeExpectation,ANCHORS
        if target.stage0 is None or target.native_bundle is None:
            raise ValueError('bootstrap requires complete pinned native and package plan')
        queue.expect_bootstrap_runtime(config['run_id'],BootstrapRuntimeExpectation({key: hashes[key] for key in ANCHORS}, database.database_generation, target.stage0.packages, target.native_bundle.manifest_sha256, intent['sql_hashes'], target.bootstrap_seconds, target.min_data_seq))
    else:
        queue.expect_search_runtime(config['run_id'],SearchRuntimeExpectation(hashes, database.database_generation, target.min_data_seq, schema_version=database.schema_version))
    return source
