"""Controller-side Debian metadata locking; no installation or ambient libraries.

Consumes explicitly captured same-release Packages snapshots. Its lock is a
candidate download closure, not proof of target ABI, ELF loading or PostgreSQL.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import cmp_to_key
import hashlib
from pathlib import Path
import re
from urllib.parse import urljoin, urlsplit

MAX_INDEX_BYTES=256*1024*1024
MAX_RECORDS=300000
MAX_PACKAGES=256


class NativeRecipeError(RuntimeError):
    pass


def debian_version_compare(left,right):
    def split(value):
        epoch,separator,rest=value.partition(':')
        if not separator:epoch,rest='0',value
        upstream,separator,revision=rest.rpartition('-')
        if not separator:upstream,revision=rest,'0'
        return int(epoch),upstream,revision
    def order(char):
        if char=='~':return -1
        if not char:return 0
        if char.isalpha():return ord(char)
        return ord(char)+256
    def part(a,b):
        x=y=0
        while x<len(a) or y<len(b):
            while x<len(a) and not a[x].isdigit() or y<len(b) and not b[y].isdigit():
                ca=a[x] if x<len(a) and not a[x].isdigit() else ''
                cb=b[y] if y<len(b) and not b[y].isdigit() else ''
                if order(ca)!=order(cb):return -1 if order(ca)<order(cb) else 1
                if ca:x+=1
                if cb:y+=1
            while x<len(a) and a[x]=='0':x+=1
            while y<len(b) and b[y]=='0':y+=1
            ax=x;by=y
            while x<len(a) and a[x].isdigit():x+=1
            while y<len(b) and b[y].isdigit():y+=1
            da,db=a[ax:x],b[by:y]
            if len(da)!=len(db):return -1 if len(da)<len(db) else 1
            if da!=db:return -1 if da<db else 1
        return 0
    ea,ua,ra=split(left);eb,ub,rb=split(right)
    if ea!=eb:return -1 if ea<eb else 1
    return part(ua,ub) or part(ra,rb)


@dataclass(frozen=True)
class PackageSnapshot:
    path:Path
    release:str
    base_url:str
    sha256:str

    def __post_init__(self):
        if (self.release not in ('jammy','noble') or urlsplit(self.base_url).scheme!='https'
                or urlsplit(self.base_url).username is not None
                or not re.fullmatch('[0-9a-f]{64}',self.sha256)):
            raise ValueError('pinned_same_release_metadata_required')


def _records(snapshot):
    path=Path(snapshot.path)
    if not 1<=path.stat().st_size<=MAX_INDEX_BYTES:
        raise NativeRecipeError('native_metadata_byte_bound')
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        while data:=stream.read(1024*1024):digest.update(data)
    if digest.hexdigest()!=snapshot.sha256:
        raise NativeRecipeError('native_metadata_hash_mismatch')
    fields={};last=None;count=0;size=0
    with path.open(encoding='utf-8') as stream:
        for line in stream:
            size+=len(line.encode())
            if size>256*1024:raise NativeRecipeError('native_metadata_paragraph_bound')
            if line=='\n':
                if fields:
                    count+=1
                    if count>MAX_RECORDS:raise NativeRecipeError('native_metadata_record_bound')
                    yield fields
                fields={};last=None;size=0
            elif line.startswith((' ','\t')):
                if last is None:raise NativeRecipeError('native_metadata_invalid')
                fields[last]+=' '+line.strip()
            else:
                key,separator,value=line.rstrip('\n').partition(':')
                value=value.lstrip(' ')
                if not separator or not re.fullmatch('[A-Za-z][A-Za-z0-9-]*',key) or key in fields:raise NativeRecipeError('native_metadata_invalid')
                fields[key]=value;last=key
        if fields:yield fields


def _dependencies(value):
    if not value:return ()
    result=[]
    for clause in value.split(','):
        alternatives=[]
        for term in clause.split('|'):
            match=re.fullmatch(r'\s*([a-z0-9][a-z0-9+.-]*)(?::(any|native|amd64))?\s*(?:\((<<|<=|=|>=|>>)\s*([^()\s]+)\))?\s*',term)
            if match is None:raise NativeRecipeError('native_dependency_syntax_unsupported')
            name,architecture,operator,version=match.groups()
            alternatives.append((name,operator,version))
        result.append(tuple(alternatives))
    return tuple(result)


def _satisfies(actual,operator,wanted):
    if operator is None:return True
    result=debian_version_compare(actual,wanted)
    return {'<<':result<0,'<=':result<=0,'=':result==0,'>=':result>=0,'>>':result>0}[operator]


def resolve_native_closure(snapshots,*,release,roots,architecture='amd64'):
    """Lock all Depends/Pre-Depends; treat no host package as already present.

    roots maps each deliberate root package to one exact version. Alternatives
    are resolved deterministically from captured metadata, never an apt index
    consulted during execution. Conflicts fail instead of silently backtracking
    into a different owner pin or claiming an ambient virtual provider.
    """
    if (release not in ('jammy','noble') or architecture!='amd64' or type(roots) is not dict
            or not roots or len(roots)>16 or not snapshots
            or any(type(snapshot) is not PackageSnapshot or snapshot.release!=release for snapshot in snapshots)):
        raise NativeRecipeError('native_same_release_plan_required')
    packages={};providers={};source_receipts=[]
    for snapshot in snapshots:
        source_receipts.append(dict(release=release,base_url=snapshot.base_url,sha256=snapshot.sha256))
        for fields in _records(snapshot):
            if fields.get('Architecture') not in (architecture,'all'):continue
            if not {'Package','Version','Filename','Size','SHA256'}<=fields.keys():continue
            filename=fields['Filename']
            if (filename.startswith('/') or '..' in filename.split('/')
                    or not re.fullmatch('[0-9a-f]{64}',fields['SHA256']) or not fields['Size'].isdigit()):
                raise NativeRecipeError('native_package_pin_invalid')
            candidate=dict(name=fields['Package'],version=fields['Version'],architecture=fields['Architecture'],
                filename=filename,url=urljoin(snapshot.base_url,filename),size=int(fields['Size']),
                sha256=fields['SHA256'],depends=(fields.get('Pre-Depends',''),fields.get('Depends','')),
                provides=_dependencies(fields.get('Provides','')),source_sha256=snapshot.sha256)
            packages.setdefault(candidate['name'],[]).append(candidate)
            for clause in candidate['provides']:
                if len(clause)!=1:raise NativeRecipeError('native_virtual_provider_invalid')
                provided,operator,version=clause[0]
                if operator not in (None,'='):raise NativeRecipeError('native_virtual_provider_invalid')
                providers.setdefault(provided,[]).append((candidate,version))
    for values in packages.values():
        values.sort(key=cmp_to_key(lambda left,right:-debian_version_compare(left['version'],right['version'])))
    selected={};pending=[];edges=[]
    def choose(name,operator=None,wanted=None):
        choices=[]
        for candidate in packages.get(name,[]):
            if name in roots and candidate['version']!=roots[name]:continue
            if _satisfies(candidate['version'],operator,wanted):choices.append(candidate)
        for candidate,provided_version in providers.get(name,[]):
            if candidate['name'] in roots and candidate['version']!=roots[candidate['name']]:continue
            if operator is None or provided_version is not None and _satisfies(provided_version,operator,wanted):
                choices.append(candidate)
        for candidate in choices:
            current=selected.get(candidate['name'])
            if current is not None and current['version']!=candidate['version']:continue
            if current is None:
                if len(selected)>=MAX_PACKAGES:raise NativeRecipeError('native_package_count_bound')
                selected[candidate['name']]=candidate;pending.append(candidate)
            return candidate
        return None
    for name,version in roots.items():
        if choose(name,'=',version) is None:raise NativeRecipeError('native_root_package_unavailable:'+name)
    while pending:
        candidate=pending.pop(0)
        for alternatives in tuple(clause for value in candidate['depends'] for clause in _dependencies(value)):
            picked=None
            for name,operator,wanted in alternatives:
                picked=choose(name,operator,wanted)
                if picked is not None:break
            if picked is None:
                raise NativeRecipeError('native_dependency_unavailable:'+candidate['name']+':'+ '|'.join(term[0] for term in alternatives))
            edges.append((candidate['name'],picked['name']))
    output=[]
    for name,candidate in sorted(selected.items()):
        output.append({key:candidate[key] for key in ('name','version','architecture','filename','url','size','sha256','source_sha256')})
    return dict(codec='rkb.native_package_lock.v1',release=release,architecture=architecture,roots=dict(roots),
        metadata=source_receipts,packages=output,dependency_edges=edges,total_download_bytes=sum(item['size'] for item in output),
        target_abi_observed=False,native_execution_verified=False)
