"""B3: exact one-page child command and Kaggle immutable source closure."""
from pathlib import Path

import pytest

from regional_knowledge import kaggle_djvu_decoder_child as child
from regional_knowledge.kaggle_target_launch import MODULES, source_bundle


def test_decoder_launch_has_preexec_cpu_address_file_fd_limits_and_one_selected_page(monkeypatch):
    limited=[]
    executable=[]
    monkeypatch.setattr(child.resource, "setrlimit",
                        lambda limit, pair: limited.append((limit,pair)))
    def capture(exe,args,env):
        executable.append((exe,args,env))
    monkeypatch.setattr(child.os,"execve",capture)
    child.main(["child.py","/pinned/native/lib64/ld-linux-x86-64.so.2",
                "/pinned/native/usr/bin/ddjvu",
                "/kaggle/temp/verified/sha256.source","2","1280",
                "/pinned/native/usr/lib/x86_64-linux-gnu"])
    assert len(limited)==5
    assert limited[0]==(child.resource.RLIMIT_CPU,(16,16))
    assert limited[1]==(child.resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    assert limited[2]==(child.resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
    assert limited[3]==(child.resource.RLIMIT_NOFILE,(64,64))
    assert limited[4]==(child.resource.RLIMIT_CORE,(0,0))
    assert len(executable)==1
    command=executable[0][1]
    assert command==("/pinned/native/lib64/ld-linux-x86-64.so.2",
                     "--inhibit-cache","--library-path",
                     "/pinned/native/usr/lib/x86_64-linux-gnu",
                     "/pinned/native/usr/bin/ddjvu","-format=ppm","-page=3",
                     "-size=1280x1280","/kaggle/temp/verified/sha256.source","-")
    assert "LD_PRELOAD" not in executable[0][2]
    assert executable[0][2].keys() <= {"PATH","LANG","LC_ALL","LD_LIBRARY_PATH"}


@pytest.mark.parametrize("args",[
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","/kaggle/temp/x","2500","1280","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","/kaggle/temp/x","-1","1280","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","/kaggle/temp/x","0","4096","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/arbitrary","/kaggle/temp/x","0","1600","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","ddjvu","/kaggle/temp/x","0","1600","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","relative","0","1600","/native/lib"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","/kaggle/temp/x","0","1600","relative"],
    ["child.py","/native/ld-linux-x86-64.so.2","/native/ddjvu","/kaggle/temp/x","0","1600"],
])
def test_child_rejects_uncontrolled_paths_edges_and_missing_page(monkeypatch,args):
    monkeypatch.setattr(child.os,"execve",lambda *a: pytest.fail("untrusted exec"))
    with pytest.raises(SystemExit):
        child.main(args)


def test_b3_modules_are_in_exact_source_bundle_without_provider_execution():
    root=Path(__file__).resolve().parents[1]
    for name in ("kaggle_djvu_pixels","kaggle_djvu_elf","kaggle_djvu_decoder_child"):
        assert name in MODULES
    relative, files, manifest = source_bundle(root)
    for name in ("kaggle_djvu_pixels","kaggle_djvu_elf","kaggle_djvu_decoder_child"):
        member="regional_knowledge/"+name+".py"
        assert files[member].encode('utf-8')==(root/"src"/member).read_bytes()
