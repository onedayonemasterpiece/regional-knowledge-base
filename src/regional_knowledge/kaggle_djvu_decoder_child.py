"""Pinned DjVuLibre subprocess boundary: resource limits before binary exec.

Executed only from the verified Kaggle source bundle by python -I. It never
parses arbitrary shell lines, imports a model, opens archive/PG or accesses
network. The owning parent provides validated original path, page index and
pre-verified bundle executable. No independent authority is granted here.
"""
from __future__ import annotations

import os
from pathlib import Path
import resource
import runpy
import sys


def main(argv: list[str]) -> None:
    if len(argv)!=7:
        raise SystemExit(2)
    loader,executable,original,index_text,edge_text,library_path=argv[1:]
    try:
        index=int(index_text)
        edge=int(edge_text)
    except ValueError:
        raise SystemExit(2) from None
    libdirs=library_path.split(':')
    if (not os.path.isabs(loader) or Path(loader).name!='ld-linux-x86-64.so.2'
            or not os.path.isabs(executable) or Path(executable).name!='ddjvu'
            or not os.path.isabs(original) or not 0<=index<2500
            or not 1<=edge<=runpy.run_path(str(Path(__file__).with_name('kaggle_original_parser.py')))['MODEL_PAGE_MAX_EDGE'] or not 1<=len(libdirs)<=6
            or any(not os.path.isabs(p) or '..' in Path(p).parts for p in libdirs)):
        raise SystemExit(2)
    # Soft and hard ceilings survive execve: the native decoder cannot bypass
    # the trusted parent simply by allocating or forking. Never allow core dumps
    # or writing temporary output files beyond the selected pipe.
    resource.setrlimit(resource.RLIMIT_CPU,(30,30))
    resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    resource.setrlimit(resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
    resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    env={key:value for key,value in os.environ.items()
         if key in ('PATH','LANG','LC_ALL','LD_LIBRARY_PATH')}
    env['LD_LIBRARY_PATH']=library_path
    # Invoke the hashed loader itself with an explicit hashed-library search
    # path and no ambient ld.so.cache; the parent has walked DT_NEEDED.
    args=(loader,'--inhibit-cache','--library-path',library_path,
          executable,'-format=ppm',f'-page={index+1}',
          f'-size={edge}x{edge}',original,'-')
    os.execve(loader,args,env)


if __name__=='__main__':
    main(sys.argv)
