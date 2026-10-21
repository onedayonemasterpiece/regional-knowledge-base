"""Fail-closed ELF64/AMD64 DT_NEEDED closure for DjVuLibre native preview.

No external readelf/ldd, dynamic imports, shell, implicit host libraries or
remote software installation. The only filesystem inputs are byte-hashed files
in an existing VerifiedNativeBundle capability, which is reverified first.
"""
from __future__ import annotations

from collections import deque
from pathlib import Path
import re
import struct

MAX_ELF_BYTES = 64 * 1024 * 1024
MAX_DEPENDENCIES = 96
MAX_PROGRAM_HEADERS = 256
MAX_DYNAMIC_ENTRIES = 512
LIB_DIRS = frozenset((
    'usr/lib/x86_64-linux-gnu', 'lib/x86_64-linux-gnu',
    'usr/lib', 'lib', 'lib64',
))
INTERPRETER_BASENAME = 'ld-linux-x86-64.so.2'


class ElfClosureError(ValueError):
    pass


def _require(condition, code='djvu_elf_unverified'):
    if not condition:
        raise ElfClosureError(code)


def _str(table: bytes, offset: int):
    _require(type(offset) is int and 0 <= offset < len(table))
    end=table.find(b'\0',offset)
    _require(end != -1 and end-offset <= 255)
    try:value=table[offset:end].decode('ascii')
    except UnicodeDecodeError:
        raise ElfClosureError('djvu_elf_unverified') from None
    _require(bool(re.fullmatch(r'[A-Za-z0-9_.+\-]+',value))
             and '/' not in value and '..' not in value)
    return value


def _read_elf(path: Path):
    data=path.read_bytes()
    _require(64 <= len(data) <= MAX_ELF_BYTES)
    _require(data[:4]==b'\x7fELF' and data[4]==2 and data[5]==1
             and data[6]==1, 'djvu_elf_arch_unverified')
    typ,machine,version=struct.unpack_from('<HHI',data,16)
    _require(typ in (2,3) and machine==62 and version==1,
             'djvu_elf_arch_unverified')
    phoff=struct.unpack_from('<Q',data,32)[0]
    phentsize,phnum=struct.unpack_from('<HH',data,54)
    _require(phentsize==56 and 1<=phnum<=MAX_PROGRAM_HEADERS
             and phoff<=len(data)-phnum*phentsize)
    loads=[]
    dynamics=[]
    interpreter=None
    for i in range(phnum):
        ptype,flags,off,vaddr,paddr,filesz,memsz,align=struct.unpack_from(
            '<IIQQQQQQ',data,phoff+i*phentsize)
        _require(off<=len(data) and filesz<=len(data)-off and filesz<=memsz)
        if ptype==1:loads.append((off,vaddr,filesz))
        elif ptype==2:
            _require(not dynamics and filesz>0 and filesz%16==0
                     and filesz<=MAX_DYNAMIC_ENTRIES*16)
            dynamics.append((off,filesz))
        elif ptype==3:
            _require(interpreter is None and 1<filesz<=256)
            raw=data[off:off+filesz]
            _require(raw.endswith(b'\0') and b'\0' not in raw[:-1])
            try:interpreter=raw[:-1].decode('ascii')
            except UnicodeDecodeError:
                raise ElfClosureError('djvu_elf_interpreter_unverified') from None
    _require(len(dynamics)==1 and loads)
    entries=[]
    off,size=dynamics[0]
    finished=False
    for pos in range(off,off+size,16):
        tag,val=struct.unpack_from('<qQ',data,pos)
        if tag==0:
            finished=True
            break
        _require(tag not in (15,29), 'djvu_elf_rpath_forbidden')
        entries.append((tag,val))
    _require(finished)
    st=[value for tag,value in entries if tag==5]
    sz=[value for tag,value in entries if tag==10]
    _require(len(st)==1 and len(sz)==1 and 1<=sz[0]<=1024*1024)
    strtab_offset=None
    for load_off,load_vaddr,load_size in loads:
        if load_vaddr<=st[0] and st[0]-load_vaddr<=load_size:
            relative=st[0]-load_vaddr
            if sz[0]<=load_size-relative:
                strtab_offset=load_off+relative
                break
    _require(strtab_offset is not None and strtab_offset+sz[0]<=len(data))
    table=data[strtab_offset:strtab_offset+sz[0]]
    needed=tuple(_str(table,v) for tag,v in entries if tag==1)
    sonames=[_str(table,v) for tag,v in entries if tag==14]
    _require(len(needed)<=64 and len(sonames)<=1 and len(set(needed))==len(needed))
    return interpreter,needed,sonames[0] if sonames else None


def pinned_djvu_elf_closure(bundle):
    """Return trusted executable/loader/LD paths or reject the entire bundle.

    This checks static ELF dependencies. It cannot certify arbitrary libraries
    loaded through dlopen, or turn a fake signed manifest into authorization;
    the caller must use the owner-approved Stage0 native package lock.
    """
    from .kaggle_native_bundle import VerifiedNativeBundle
    if type(bundle) is not VerifiedNativeBundle or not bundle.current():
        raise ElfClosureError('djvu_decoder_bundle_not_verified')
    prefix=bundle.prefix
    pins={}
    byname={}
    for raw,size,sha,executable in bundle.pins:
        path=Path(raw)
        try:rel=path.relative_to(prefix)
        except ValueError:
            raise ElfClosureError('djvu_elf_path_outside_bundle') from None
        _require(type(size) is int and size>0 and type(sha) is str and len(sha)==64)
        pins[rel]=(path,size,sha,executable)
        if str(rel.parent) in LIB_DIRS:
            byname.setdefault(rel.name,[]).append((path,sha,executable))

    binary=prefix/'usr/bin/ddjvu'
    p=pins.get(Path('usr/bin/ddjvu'))
    _require(p is not None and p[3] is True and p[1]>=1024,
             'djvu_decoder_binary_not_in_verified_bundle')
    interp,needed,_=_read_elf(binary)
    _require(interp in ('/lib64/ld-linux-x86-64.so.2',
                       '/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2'),
             'djvu_elf_interpreter_unverified')

    loader_candidates=[p for p in byname.get(INTERPRETER_BASENAME,[]) if p[2] is True]
    _require(loader_candidates, 'djvu_elf_loader_unverified')
    hashes={sha for _,sha,_ in loader_candidates}
    _require(len(hashes)==1, 'djvu_elf_loader_ambiguous')
    loader=sorted(path for path,_,_ in loader_candidates)[0]
    loader_interp,loader_needed,_=_read_elf(loader)
    _require(loader_interp is None, 'djvu_elf_loader_unverified')

    closure=set()
    pending=deque((*needed,*loader_needed))
    paths=set()
    while pending:
        name=pending.popleft()
        if name in closure:continue
        _require(len(closure)<MAX_DEPENDENCIES)
        candidates=byname.get(name,[])
        _require(bool(candidates),'djvu_elf_dependency_missing')
        _require(len({sha for _,sha,_ in candidates})==1,
                 'djvu_elf_dependency_ambiguous')
        path,sha,execute=sorted(candidates,key=lambda v:str(v[0]))[0]
        shared_interp,dependencies,soname=_read_elf(path)
        _require(shared_interp is None and (soname is None or soname==name),
                 'djvu_elf_dependency_identity_invalid')
        closure.add(name)
        paths.add(path.parent)
        pending.extend(dependencies)
    _require('libdjvulibre.so.21' in closure,
             'djvu_elf_libdjvulibre_missing')
    # The already-verified loader is invoked DIRECTLY, not through ddjvu's
    # ambient PT_INTERP path. The absolute library path contains only verified
    # selected DT_NEEDED directories. No host loader/cache lookup is requested.
    return str(binary),str(loader),':'.join(map(str,sorted(paths)))
