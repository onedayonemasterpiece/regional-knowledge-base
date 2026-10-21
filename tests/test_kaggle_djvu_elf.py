"""Static-ELF dependency closure tests with synthetic, never-executable ELF files.

No target packages or real DjVuLibre are represented by these fixtures.
"""
import hashlib
import struct

import pytest

from regional_knowledge.kaggle_djvu_elf import (
    ElfClosureError, _read_elf, pinned_djvu_elf_closure,
)
from test_kaggle_native_bundle import fixture as native_fixture, prepare


def elf(*,needed=(),soname=None,interp=None,rpath=False):
    data=bytearray(2048)
    data[:16]=b"\x7fELF"+bytes([2,1,1])+bytes(9)
    phnum=3 if interp is not None else 2
    struct.pack_into("<HHIQQQIHHHHHH",data,16,3,62,1,0,64,0,0,64,56,phnum,0,0,0)
    phdr="<IIQQQQQQ"
    struct.pack_into(phdr,data,64,1,5,0,0x400000,0x400000,len(data),len(data),4096)
    names=bytearray(b"\0")
    offsets={}
    for name in (*needed,*( [soname] if soname else [])):
        if name not in offsets:
            offsets[name]=len(names)
            names.extend(name.encode()+b"\0")
    data[0x400:0x400+len(names)]=names
    entries=[(5,0x400400),(10,len(names))]
    entries.extend((1,offsets[n]) for n in needed)
    if soname:
        entries.append((14,offsets[soname]))
    if rpath:entries.append((29,offsets[needed[0]]))
    entries.append((0,0))
    for i,(tag,ptr) in enumerate(entries):
        struct.pack_into("<qQ",data,0x200+16*i,tag,ptr)
    struct.pack_into(phdr,data,120,2,4,0x200,0x400200,0x400200,16*len(entries),16*len(entries),8)
    if interp is not None:
        text=interp.encode()+b"\0"
        data[0x180:0x180+len(text)]=text
        struct.pack_into(phdr,data,176,3,4,0x180,0x400180,0x400180,len(text),len(text),1)
    return bytes(data)


def _bundle(native_fixture, *,omit=(),invalid_interp=False,extra_rpath=False,
            libc_foreign_interp=False):
    blobs={
        "usr/bin/ddjvu":elf(
            needed=("libdjvulibre.so.21","libc.so.6"),interp=
            "/lib64/ld-linux-x86-64.so.2" if not invalid_interp else "/host/mystery"),
        "lib/x86_64-linux-gnu/ld-linux-x86-64.so.2":elf(),
        "usr/lib/x86_64-linux-gnu/libdjvulibre.so.21":elf(
            needed=("libjpeg.so.8","libc.so.6"),soname="libdjvulibre.so.21",
            rpath=extra_rpath),
        "usr/lib/x86_64-linux-gnu/libc.so.6":elf(
            soname="libc.so.6",
            # Actual glibc2.39 libc is also executable and carries PT_INTERP.
            # Its interpreter must be the SAME pinned loader as ddjvu.
            interp="/lib64/ld-linux-x86-64.so.2" if not libc_foreign_interp
                   else "/untrusted/other-loader"),
        "usr/lib/x86_64-linux-gnu/libjpeg.so.8":elf(
            needed=("libc.so.6",),soname="libjpeg.so.8"),
    }
    for key in omit:del blobs[key]
    entries=[(name,"file",value) for name,value in blobs.items()]
    files=[dict(path=name,size=len(value),sha256=hashlib.sha256(value).hexdigest(),
            package="fixture",member=name,
            executable=name=="usr/bin/ddjvu" or name.endswith("/ld-linux-x86-64.so.2"))
           for name,value in blobs.items()]
    return prepare(native_fixture(entries=entries,files=files))


def test_exact_elf_interpreter_and_complete_transitive_library_graph(native_fixture):
    cap=_bundle(native_fixture)
    binary,loader,libpath=pinned_djvu_elf_closure(cap)
    assert binary.endswith("/usr/bin/ddjvu")
    assert loader.endswith("/ld-linux-x86-64.so.2")
    assert "usr/lib/x86_64-linux-gnu" in libpath
    assert "libdjvulibre" not in libpath  # path contains directories, not raw file identifiers
    assert _read_elf(cap.prefix/"usr/bin/ddjvu")[1]==("libdjvulibre.so.21","libc.so.6")


@pytest.mark.parametrize("missing",[
    "lib/x86_64-linux-gnu/ld-linux-x86-64.so.2",
    "usr/lib/x86_64-linux-gnu/libdjvulibre.so.21",
    "usr/lib/x86_64-linux-gnu/libjpeg.so.8",
    "usr/lib/x86_64-linux-gnu/libc.so.6",
])
def test_missing_pinned_dependency_or_interpreter_never_falls_back_to_host(native_fixture,missing):
    cap=_bundle(native_fixture,omit=(missing,))
    with pytest.raises(ElfClosureError):
        pinned_djvu_elf_closure(cap)


@pytest.mark.parametrize("case",["interp","rpath","corrupt","foreign_libc_interp"])
def test_forbidden_interpreter_rpath_and_non_elf_are_not_decoder_proof(native_fixture,case):
    cap=_bundle(native_fixture,invalid_interp=case=="interp",
                extra_rpath=case=="rpath",
                libc_foreign_interp=case=="foreign_libc_interp")
    if case=="corrupt":
        executable=cap.prefix/"usr/bin/ddjvu"
        executable.chmod(0o700)
        executable.write_bytes(b"this is not ELF")
    with pytest.raises(ElfClosureError):
        pinned_djvu_elf_closure(cap)
