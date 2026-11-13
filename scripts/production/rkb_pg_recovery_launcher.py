"""Bind one finite recovery part to its actual private Kaggle environment."""
import hashlib
import json
import os
from pathlib import Path
import platform
import pwd
import runpy
import shutil
import sys

RUN_CONFIG = {}  # Filled in the private notebook; contains no credentials.


def main():
    os.umask(0o077)
    config = RUN_CONFIG
    output = Path('/kaggle/working')
    phase = 'environment'
    try:
        release = platform.freedesktop_os_release()
        memory = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                          if line.startswith('MemTotal:'))) * 1024
        for path in ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'):
            if Path(path).exists() and Path(path).read_text().strip().isdigit():
                memory = min(memory, int(Path(path).read_text()))
        environment = dict(python=platform.python_version(), machine=platform.machine(),
            os_id=release['ID'], os_version_id=release['VERSION_ID'], libc=platform.libc_ver(),
            memory_bytes=memory, free_bytes=shutil.disk_usage('/kaggle').free, uid=os.geteuid())
        (output/'runtime-environment.json').write_text(json.dumps(environment, indent=2)+'\n')
        if (sys.version_info[:2] != (3, 13) or environment['machine'] != 'x86_64'
                or release['ID'] != 'ubuntu' or release['VERSION_ID'] != '24.04'
                or platform.libc_ver() != ('glibc', '2.39')):
            raise RuntimeError('retained native bundle and wheels require their original ABI')
        if memory < 12*1024**3 or environment['free_bytes'] < 32*1024**3:
            raise RuntimeError('insufficient room for input, PG, model, backup and actual restore')
        phase = 'input_binding'
        manifests = [p for p in Path('/kaggle/input').rglob('input-manifest.json')
                     if hashlib.sha256(p.read_bytes()).hexdigest() == config['input_manifest_sha256']]
        if len(manifests) != 1:
            raise RuntimeError('expected exactly one bound private input')
        inputs = manifests[0].parent
        config['input_root'] = str(inputs)
        temporary = Path('/kaggle/temp')
        temporary.mkdir(mode=0o711, exist_ok=True)
        identity = pwd.getpwnam('nobody') if os.geteuid() == 0 else pwd.getpwuid(os.geteuid())
        config['database'].update(uid=identity.pw_uid, gid=identity.pw_gid,
                                  ownership_evidence=config['run_id'])
        native = json.loads((inputs/config['native_manifest']['name']).read_bytes())
        prefix = Path(config['prepared_model']['root'])/('rkb-native-'+config['native_manifest']['sha256'])
        for key, suffix in config['database']['native_paths'].items():
            paths = [entry['path'] for entry in native['files'] if entry['path'].endswith(suffix)]
            if len(paths) != 1:
                raise RuntimeError('native executable or vector path is ambiguous')
            config['database']['native_paths'][key] = str(prefix/paths[0])
        # The mounted public weight has the original SHA/size; Stage0 verifies it.
        weight = next(e for e in config['prepared_model']['entries'] if e['role'] == 'weight')
        sources = list(Path('/kaggle/input').rglob('pytorch_model.bin'))
        if len(sources) != 1:
            raise RuntimeError('expected one pinned public model mount')
        target = Path(config['prepared_model']['root'])/weight['name']
        target.parent.mkdir(mode=0o700, parents=True)
        shutil.copyfile(sources[0], target)
        config_path = temporary/'recovery-part.json'
        config_path.write_text(json.dumps(config))
        phase = 'recovery'
        runpy.run_path(str(inputs/'rkb_pg_recovery_notebook.py'), run_name='_rkb_entry')['main'](config_path)
    except Exception as error:
        report = dict(status='failed', phase=phase, error_type=type(error).__name__, publishable=False)
        (output/'error-report.json').write_text(json.dumps(report)+'\n')
        print(json.dumps(report), flush=True)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
