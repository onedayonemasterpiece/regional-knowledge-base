"""Controller-only immutable source assembly and pre-attachment launch intent."""
from __future__ import annotations

import base64
import json
from pathlib import Path

from .bge_contract import REVISION, SPACE
from .kaggle_owned_pg import SQL_ARTIFACTS, canonical, digest
from .kaggle_readiness import SearchRuntimeExpectation
from .kaggle_target_runtime import TargetLaunchSpec
from .recovery_journal import RecoveryJournal

MODULES = ('__init__','bge_contract','kaggle_protocol','kaggle_query','kaggle_search',
    'kaggle_readiness','kaggle_worker_query','kaggle_database_probe','kaggle_bootstrap',
    'kaggle_owned_pg','kaggle_target_runtime','kaggle_apply','recovery_journal','query_telemetry',
    'source_import','kaggle_source_commands','kaggle_stage0','kaggle_restored_database','kaggle_native_bundle',
    'kaggle_environment','kaggle_pg_snapshot_native','kaggle_recovery_phases','kaggle_snapshot_channel',
    'native_snapshot_transfer','kaggle_bootstrap_pins','kaggle_native_env','kaggle_source_acl','kaggle_source_history_native',
    'original_intake','kaggle_original_native','kaggle_original_parser','file_ingress','source_adapter','ingestion')
MAX_SOURCE_BYTES = 2 * 1024 * 1024


def source_bundle(repository):
    repository = Path(repository)
    files = {f'regional_knowledge/{name}.py':(repository/'src/regional_knowledge'/f'{name}.py').read_bytes()
             for name in MODULES}
    files.update({f'sql/{name}':(repository/'sql/kaggle'/name).read_bytes() for name in SQL_ARTIFACTS})
    if sum(map(len,files.values())) > MAX_SOURCE_BYTES:
        raise ValueError('immutable target source byte budget exceeded')
    manifest = {name:digest(data) for name,data in sorted(files.items())}
    bundle_id = digest(canonical(manifest))
    relative = 'rkb-target-'+bundle_id
    encoded = {name:base64.b64encode(data).decode('ascii') for name,data in sorted(files.items())}
    entries = [dict(name=relative+'/'+name,size=len(data),sha256=digest(data),role='code')
               for name,data in sorted(files.items())]
    return relative,encoded,entries


async def persist_empty_intent(queue, run_id, target, *, launch_nonce, recovery_journal, sql_hashes):
    """Only an actual read from the injected existing remote authority proves empty.

    The owner must hold writer/intake fencing while creating/initializing genesis.
    This operation never creates a journal/store or replays/restores any history.
    """
    if type(target) is not TargetLaunchSpec or target.database.mode != 'NEW_EMPTY_GENERATION':
        raise ValueError('explicit empty-generation initialization required')
    if not isinstance(recovery_journal,RecoveryJournal):
        raise TypeError('existing real RecoveryJournal required')
    head = await recovery_journal.head()  # UNKNOWN and any history fail closed.
    if head is not None:
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
    if len({e['name'] for e in entries}) != len(entries) or len(entries)>128:
        raise ValueError('unique bounded final artifact inventory required')
    intent = read_empty_intent(queue,config['run_id'])
    if target.database.mode == 'NEW_EMPTY_GENERATION':
        sql_hashes = {name:digest(base64.b64decode(files['sql/'+name])) for name in SQL_ARTIFACTS}
        if (intent is None or intent['database']!=target.database.wire()
                or intent['run_id']!=config['run_id'] or intent['launch_nonce']!=config['launch_nonce']
                or intent.get('native_bundle')!=(target.native_bundle.wire() if target.native_bundle is not None else None)
                or intent['sql_hashes']!=sql_hashes):
            raise ValueError('persist actual empty-journal initialization intent first')
    runtime_wire=target.wire()
    runtime_wire.pop('stage0',None)  # One copy, consumed only by load_encoder.
    config.update(shared_reads=True,prepared_model=dict(root=str(root),entries=entries,
        max_total_bytes=sum(e['size'] for e in entries)),target_runtime=dict(
        **runtime_wire,sql_root=str(root/relative/'sql'),initialization_intent=intent))
    if target.stage0 is not None:
        config['stage0']=target.stage0.wire()
    if len(canonical(config))>65536:
        raise ValueError('bounded complete target launch configuration required')
    # This extractor only writes predetermined bounded source files. It runs
    # before control; model imports, pool creation and initdb occur after ATTACH.
    entry = '''
if __name__ == '__main__':
    import base64 as _b64, hashlib as _hashlib, sys as _sys
    from pathlib import Path as _Path
    _bundle = BUNDLE_FILES
    _root = _Path(BUNDLE_ROOT)
    if _root.parent.resolve(strict=True) != _root.parent:
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
        _data = _b64.b64decode(_encoded, validate=True)
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
    runtime = _DeferredTargetRuntime()
    main(prepare_runtime=runtime.prepare, close_runtime=runtime.aclose)
'''.replace('BUNDLE_FILES',repr(files)).replace('BUNDLE_ROOT',repr(str(root/relative)))
    if template.count("if __name__=='__main__':main()") != 1:
        raise ValueError('target entrypoint slot missing')
    template = template.replace("if __name__=='__main__':main()",entry)
    source = template.replace('RUN_CONFIG = {}','RUN_CONFIG = '+repr(config),1)
    def role_hash(roles):
        return digest(canonical(sorted((e['role'],e['name'],e['size'],e['sha256'])
                                      for e in entries if e['role'] in roles)))
    hashes = dict(code=role_hash({'code'}),launch_config=digest(canonical(config)),
        executed_source=digest(source.encode()),dependency=target.dependency_sha256,
        model=digest(canonical(dict(revision=REVISION,space=SPACE,
            artifacts=role_hash({'weight','model_config','tokenizer'})))),schema=target.schema_sha256)
    database = target.database
    # Final source hash is persisted outside its own source bytes: no self hash.
    if target.bootstrap_only:
        from .kaggle_bootstrap_pins import BootstrapRuntimeExpectation,ANCHORS
        if target.stage0 is None or target.native_bundle is None:
            raise ValueError('bootstrap requires complete pinned native and package plan')
        queue.expect_bootstrap_runtime(config['run_id'],BootstrapRuntimeExpectation(
            {key:hashes[key] for key in ANCHORS},database.database_generation,database.schema_revision,
            database.function_revision,database.executor_revision,target.stage0.packages,
            target.native_bundle.manifest_sha256,intent['sql_hashes'],target.bootstrap_seconds,target.min_data_seq))
    else:
        queue.expect_search_runtime(config['run_id'],SearchRuntimeExpectation(hashes,
            database.database_generation,database.schema_revision,database.function_revision,
            database.executor_revision,target.min_data_seq))
    return source
