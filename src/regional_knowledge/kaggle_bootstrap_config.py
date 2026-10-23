"""Concrete first PG+BGE configuration from exact model/wheel/native receipts.

Metadata only: no assets, credentials or provider effects are acquired here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .kaggle_model_manifest import bge_model_manifest
from .kaggle_native_bundle import NativeBundleSpec
from .kaggle_owned_pg import OwnedDatabase
from .kaggle_stage0 import Stage0Spec,Stage0Download
from .kaggle_target_runtime import TargetLaunchSpec



def build_bootstrap_target(*,wheel_lock,native_receipt,native_manifest_bytes,native_downloads,
                           database_generation,writer_generation,uid,gid,identity_evidence,
                           seconds=1800,original_intake=None):
    """Build full load_target_launch-compatible wire metadata with no guessed pins.

    uid/gid are explicit candidate numeric identities. Actual early observation
    and native account lookup must verify them; no account is created. Native
    helper checks the actual OS/ABI before extraction, pip checks wheel ABI.
    original_intake is an explicit typed deployment, checked against the exact
    supplied wheel closure by TargetLaunchSpec. It never installs a parser or
    widens supported formats merely because an import is requested.
    """
    if (type(wheel_lock) is not dict or wheel_lock.get('codec')!='rkb.candidate_wheel_lock.v1'
            or type(native_receipt) is not dict or native_receipt.get('codec')!='rkb.native_collection_artifacts.v1'
            or type(native_manifest_bytes) is not bytes or not 1<=len(native_manifest_bytes)<=1536*1024):
        raise ValueError('exact native and wheel input receipts required')
    bundle=NativeBundleSpec.from_wire(native_receipt['spec'])
    if hashlib.sha256(native_manifest_bytes).hexdigest()!=bundle.manifest_sha256:
        raise ValueError('actual native manifest differs from receipt')
    manifest=json.loads(native_manifest_bytes)
    pins={item['path']:item for item in manifest['files']}
    relative=dict(initdb='usr/lib/postgresql/17/bin/initdb',postgres='usr/lib/postgresql/17/bin/postgres',
        pg_config='usr/lib/postgresql/17/bin/pg_config',vector_library='usr/lib/postgresql/17/lib/vector.so',
        vector_control='usr/share/postgresql/17/extension/vector.control',
        vector_sql='usr/share/postgresql/17/extension/vector--0.8.2.sql')
    if (not set(relative.values())<=pins.keys()
            or any(not pins[relative[key]]['executable'] for key in ('initdb','postgres','pg_config'))):
        raise ValueError('complete native PostgreSQL/pgvector path pins required')
    root=Path('/kaggle/working');prefix=bundle.prefix(root)
    leaf='rkb-'+hashlib.sha256(database_generation.encode()).hexdigest()[:20]
    cluster=Path('/kaggle/temp')/leaf
    database=OwnedDatabase(mode='NEW_EMPTY_GENERATION', database_generation=database_generation, writer_generation=writer_generation, cluster=str(cluster), socket=str(cluster / 'socket'), dbname='rkb', uid=uid, gid=gid, native_paths={key: str(prefix / value) for key, value in relative.items()}, ownership_evidence=identity_evidence)
    model_entries,model_downloads=bge_model_manifest()
    packages={};wheels=[];wheel_entries=[];wheel_downloads=[]
    for item in wheel_lock['packages']:
        if item['package'] in packages:raise ValueError('one exact wheel per pinned package required')
        packages[item['package']]=item['version'];wheels.append(item['name'])
        row={key:item[key] for key in ('name','size','sha256')}
        wheel_entries.append(dict(row,role='code'))
        wheel_downloads.append(dict(row,url=item['url'],executable=False))
    native_entries=tuple(dict(item) for item in native_receipt['artifacts'])
    declared={item['name']:(item['size'],item['sha256']) for item in native_entries}
    supplied={item['name']:(item['size'],item['sha256']) for item in native_downloads}
    if supplied!=declared or len(native_downloads)!=len(declared):
        raise ValueError('native hosted inputs must exactly match published receipt')
    downloads=tuple(Stage0Download.from_wire(dict(item)) for item in
        (*model_downloads,*wheel_downloads,*native_downloads))
    plan=Stage0Spec(downloads,tuple(wheels),packages,
        install_bytes=max(1024*1024,int(wheel_lock['wheel_bytes'])*6),total_seconds=min(seconds,900))
    return TargetLaunchSpec(database,str(root),tuple((*model_entries,*wheel_entries,*native_entries)),
        tuple(sorted(packages)),None,None,identity_evidence,stage0=plan,native_bundle=bundle,
        bootstrap_only=True,bootstrap_seconds=seconds,original_intake=original_intake)
