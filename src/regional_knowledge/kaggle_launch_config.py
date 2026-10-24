"""Bounded typed target metadata loader; grants no journal/provider authority."""
from __future__ import annotations

from dataclasses import fields
import json
import os
import stat

from .kaggle_native_bundle import NativeBundleSpec
from .kaggle_owned_pg import OwnedDatabase
from .kaggle_stage0 import Stage0Spec
from .kaggle_target_runtime import TargetLaunchSpec

MAX_CONFIG_BYTES=65536


def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate target configuration field')
        result[key]=value
    return result


def target_launch_from_wire(value):
    optional={'recovery','dataset_checkpoint','schema_sha256'}
    required={field.name for field in fields(TargetLaunchSpec)}-optional
    if type(value) is not dict or not required<=set(value) or set(value)-required-optional:
        raise ValueError('exact target launch configuration fields required')
    options=dict(value)
    if options.get('dataset_checkpoint') is not None:
        from .kaggle_dataset_base import DatasetCheckpoint
        options['dataset_checkpoint']=DatasetCheckpoint.from_wire(options['dataset_checkpoint'])
    if options.get('recovery') is not None:
        from .kaggle_recovery_composition import SnapshotRecoveryConfig
        options['recovery']=SnapshotRecoveryConfig.from_wire(options['recovery'])
    options['database']=OwnedDatabase(**options['database'])
    for name in ('artifacts','dependency_packages'):
        if type(options[name]) is not list:raise ValueError('target configuration arrays required')
        options[name]=tuple(options[name])
    options['stage0']=Stage0Spec.from_wire(options['stage0']) if options['stage0'] is not None else None
    options['native_bundle']=NativeBundleSpec.from_wire(options['native_bundle']) if options['native_bundle'] is not None else None
    if options['original_intake'] is not None:
        from .kaggle_original_native import OriginalIntakeConfig
        options['original_intake']=OriginalIntakeConfig.from_wire(options['original_intake'])
    return TargetLaunchSpec(**options)


def read_target_launch(path):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        before=os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or not 1<=before.st_size<=MAX_CONFIG_BYTES:
            raise ValueError('bounded regular target launch configuration required')
        data=b''
        while len(data)<=MAX_CONFIG_BYTES:
            chunk=os.read(fd,min(8192,MAX_CONFIG_BYTES+1-len(data)))
            if not chunk:break
            data+=chunk
        after=os.fstat(fd)
        if len(data)!=before.st_size or (after.st_size,after.st_mtime_ns)!=(before.st_size,before.st_mtime_ns):
            raise ValueError('target launch configuration changed during read')
    finally:os.close(fd)
    value=json.loads(data,object_pairs_hook=_object,
        parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite target configuration')))
    return value


def load_target_launch(path):
    return target_launch_from_wire(read_target_launch(path))


def successor_target(queue, value, run_id):
    """Freeze the verified Saved and current run in its existing launch folder.

    A restart reuses these exact pins; a later Saved cannot change a submitted
    restore. This is launch metadata, not a second recovery pointer.
    """
    from .kaggle_current_registry import registry_sha256
    from .kaggle_schema_migrate import latest_schema_version
    folder=queue.path.parent/'bge-launches'/run_id
    path=folder/'target.json'
    if path.exists():return load_target_launch(path)
    value=json.loads(json.dumps(value))
    if value['database']['mode']!='VERIFIED_INITIALIZED' or value.get('dataset_checkpoint') is None:
        return target_launch_from_wire(value)
    with queue.connect(write=False) as db:
        run=db.execute('SELECT current_run,successor FROM control WHERE id=1').fetchone()
        if run is None or run_id not in tuple(run):raise ValueError('current successor required')
        rows=db.execute('SELECT checkpoint_json,latest_json FROM rkb_dataset_recovery_store').fetchall()
    if len(rows)!=1:raise ValueError('one verified Dataset recovery store required')
    checkpoint=json.loads(rows[0]['latest_json'] or rows[0]['checkpoint_json'])
    previous=value['dataset_checkpoint']
    if (checkpoint['database_generation']!=value['database']['database_generation']
            or checkpoint['base_sequence']<previous['base_sequence']
            or (checkpoint['owner_slug'],checkpoint['dataset_slug'])!=(previous['owner_slug'],previous['dataset_slug'])):
        raise ValueError('verified Saved must extend the same restore base')
    value['database'].update(writer_generation=run_id,schema_version=latest_schema_version())
    value.update(dataset_checkpoint=checkpoint,min_data_seq=checkpoint['base_sequence'],registry_sha256=registry_sha256())
    target=target_launch_from_wire(value)
    folder.mkdir(mode=0o700,parents=True,exist_ok=True)
    # Controller is the only producer. Atomic visibility also covers an API
    # restart while notebook composition is still in progress.
    temporary=path.with_suffix('.pending')
    with open(temporary,'w',opener=lambda name,flags:os.open(name,flags,0o600)) as handle:
        json.dump(target.wire(),handle,sort_keys=True,separators=(',',':'))
    temporary.replace(path)
    return target
