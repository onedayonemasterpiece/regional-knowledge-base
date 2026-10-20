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
    if type(value) is not dict or set(value)!={field.name for field in fields(TargetLaunchSpec)}:
        raise ValueError('exact target launch configuration fields required')
    options=dict(value)
    options['database']=OwnedDatabase(**options['database'])
    for name in ('artifacts','dependency_packages','command_handlers'):
        if type(options[name]) is not list:raise ValueError('target configuration arrays required')
        options[name]=tuple(options[name])
    options['stage0']=Stage0Spec.from_wire(options['stage0']) if options['stage0'] is not None else None
    options['native_bundle']=NativeBundleSpec.from_wire(options['native_bundle']) if options['native_bundle'] is not None else None
    if options['original_intake'] is not None:
        from .kaggle_original_native import OriginalIntakeConfig
        options['original_intake']=OriginalIntakeConfig.from_wire(options['original_intake'])
    return TargetLaunchSpec(**options)


def load_target_launch(path):
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
    return target_launch_from_wire(value)
