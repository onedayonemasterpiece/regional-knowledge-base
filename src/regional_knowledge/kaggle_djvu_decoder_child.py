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
import sys


def main(argv: list[str]) -> None:
    if len(argv)!=5:
        raise SystemExit(2)
    executable,original,index_text,edge_text=argv[1:]
    try:
        index=int(index_text)
        edge=int(edge_text)
    except ValueError:
        raise SystemExit(2) from None
    if (not os.path.isabs(executable) or not os.path.isabs(original)
            or not Path(executable).name=='ddjvu'
            or not 0<=index<2500 or edge not in (1600,1280)):
        raise SystemExit(2)
    # Soft and hard ceilings survive execve: the native decoder cannot bypass
    # the trusted parent simply by allocating or forking. Never allow core dumps
    # or writing temporary output files beyond the selected pipe.
    resource.setrlimit(resource.RLIMIT_CPU,(16,16))
    resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    resource.setrlimit(resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
    resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    env={key:value for key,value in os.environ.items()
         if key in ('PATH','LANG','LC_ALL','LD_LIBRARY_PATH')}
    args=(executable,'-format=ppm',f'-page={index+1}',
          f'-size={edge}x{edge}',original,'-')
    os.execve(executable,args,env)


if __name__=='__main__':
    main(sys.argv)
