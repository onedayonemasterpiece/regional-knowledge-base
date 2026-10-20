"""Small pre-install environment observation, never a readiness capability."""
from __future__ import annotations

import os
from pathlib import Path
import pwd
import re
import sys

_FIELDS = frozenset({'python', 'cache_tag', 'machine', 'libc_name', 'libc_version',
                    'os_id', 'os_version_id'})
_PASSWD=Path('/etc/passwd')


def _numeric_id(value):
    return type(value) is int and 0<=value<2**32-1


def _validate_identity(value):
    if (type(value) is not dict or set(value)!={'uid','gid','groups','accounts','accounts_source'}
            or not _numeric_id(value['uid']) or not _numeric_id(value['gid'])
            or value['accounts_source'] not in {'local_passwd','unavailable'}
            or type(value['groups']) is not list or len(value['groups'])>64
            or any(not _numeric_id(group) for group in value['groups'])
            or value['groups']!=sorted(set(value['groups']))
            or type(value['accounts']) is not list or len(value['accounts'])>128):
        raise ValueError('runtime_environment_identity')
    seen=set()
    for account in value['accounts']:
        if (type(account) is not dict or set(account)!={'name','uid','gid'}
                or type(account['name']) is not str
                or re.fullmatch(r'[A-Za-z0-9_.-]{1,63}\$?',account['name']) is None
                or not _numeric_id(account['uid']) or account['uid']==0
                or not _numeric_id(account['gid']) or account['uid'] in seen):
            raise ValueError('runtime_environment_account')
        seen.add(account['uid'])
    if (value['accounts']!=sorted(value['accounts'],key=lambda account:(account['uid'],account['name']))
            or value['accounts_source']=='unavailable' and value['accounts']):
        raise ValueError('runtime_environment_account_order')
    return dict(value,groups=list(value['groups']),accounts=[dict(account) for account in value['accounts']])


def _observe_identity():
    """Bound local passwd bytes before resolving candidate numeric identities.

    No password/gecos/home/shell fields leave the observer. getpwuid confirms
    the account lookup initdb needs; no nonroot account is selected or granted.
    """
    accounts={};source='unavailable'
    try:
        with _PASSWD.open('rb') as stream:raw=stream.read(65537)
        if len(raw)<=65536:
            lines=raw.decode('utf-8').splitlines()
            if len(lines)<=128:
                source='local_passwd'
                for line in lines:
                    fields=line.split(':')
                    if len(fields)!=7:continue
                    try:
                        uid,gid=int(fields[2]),int(fields[3])
                        if not _numeric_id(uid) or uid==0 or not _numeric_id(gid):continue
                        account=pwd.getpwuid(uid)
                        if (account.pw_name!=fields[0] or account.pw_gid!=gid
                                or re.fullmatch(r'[A-Za-z0-9_.-]{1,63}\$?',account.pw_name) is None):continue
                        accounts[uid]=dict(name=account.pw_name,uid=uid,gid=gid)
                    except (ValueError,KeyError,OverflowError):continue
    except (OSError,UnicodeError):pass
    return _validate_identity(dict(uid=os.geteuid(),gid=os.getegid(),groups=sorted(set(os.getgroups())),
        accounts=sorted(accounts.values(),key=lambda account:(account['uid'],account['name'])),accounts_source=source))


def validate_environment(value):
    if type(value) is not dict or set(value) not in (_FIELDS,_FIELDS|{'identity'}):
        raise ValueError('runtime_environment_fields')
    for name, text in value.items():
        if name=='identity':continue
        limit = 1024 if name == 'python' else 128
        if (type(text) is not str or not 1 <= len(text.encode('utf-8')) <= limit
                or any(ord(char) < 32 or ord(char) == 127 for char in text)):
            raise ValueError('runtime_environment_value')
    return dict(value,identity=_validate_identity(value['identity'])) if 'identity' in value else dict(value)


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
        os_id=release.get('ID') or 'unknown', os_version_id=release.get('VERSION_ID') or 'unknown',
        identity=_observe_identity()))
