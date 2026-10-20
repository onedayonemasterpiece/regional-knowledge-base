"""Native child environment from a retained exact code bundle, never ambient LD_* flags."""
from __future__ import annotations

import os
from pathlib import Path
import stat

LIBRARY_DIRS=('usr/lib/x86_64-linux-gnu','lib/x86_64-linux-gnu','usr/lib/postgresql/17/lib')


def verified_native_environment(bundle):
    from .kaggle_native_bundle import VerifiedNativeBundle,NativeBundleError
    if type(bundle) is not VerifiedNativeBundle or not bundle.current():
        raise NativeBundleError('native_environment_verified_bundle_required')
    pinned={Path(path) for path,_,_,_ in bundle.pins}
    directories=[]
    for relative in LIBRARY_DIRS:
        directory=bundle.prefix/relative
        if not any(path.parent==directory for path in pinned):continue
        parent=bundle.root
        for part in directory.relative_to(bundle.root).parts:
            parent=parent/part
            info=parent.lstat()
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_mode&0o022:
                raise NativeBundleError('native_environment_directory_invalid')
        directories.append(str(directory))
    if not directories:raise NativeBundleError('native_environment_library_closure_missing')
    return {'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','LD_LIBRARY_PATH':os.pathsep.join(directories)}
