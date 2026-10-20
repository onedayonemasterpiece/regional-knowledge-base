"""Small pre-install environment observation, never a readiness capability."""
from __future__ import annotations

import os
from pathlib import Path
import sys

_FIELDS = frozenset({'python', 'cache_tag', 'machine', 'libc_name', 'libc_version',
                    'os_id', 'os_version_id'})


def validate_environment(value):
    if type(value) is not dict or set(value) != _FIELDS:
        raise ValueError('runtime_environment_fields')
    for name, text in value.items():
        limit = 1024 if name == 'python' else 128
        if (type(text) is not str or not 1 <= len(text.encode('utf-8')) <= limit
                or any(ord(char) < 32 or ord(char) == 127 for char in text)):
            raise ValueError('runtime_environment_value')
    return dict(value)


def observe_environment():
    """Read bounded nonsecret Linux metadata before optional package imports."""
    release = {}
    try:
        with Path('/etc/os-release').open('rb') as stream:
            raw = stream.read(4097)
        if len(raw) <= 4096:
            for line in raw.decode('utf-8').splitlines():
                key, separator, value = line.partition('=')
                if separator and key in ('ID', 'VERSION_ID'):
                    release[key] = value.strip('"\'')
    except (OSError, UnicodeError):
        pass
    try:
        libc = os.confstr('CS_GNU_LIBC_VERSION') or 'unknown'
    except (OSError, ValueError):
        libc = 'unknown'
    libc_name, _, libc_version = libc.partition(' ')
    return validate_environment(dict(python=sys.version.replace('\n', ' '),
        cache_tag=sys.implementation.cache_tag or 'unknown', machine=os.uname().machine,
        libc_name=libc_name, libc_version=libc_version or 'unknown',
        os_id=release.get('ID') or 'unknown', os_version_id=release.get('VERSION_ID') or 'unknown'))
