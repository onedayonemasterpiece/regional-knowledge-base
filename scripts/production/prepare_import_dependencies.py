"""CI packaging adapter over the existing native resolver and bundle assembler.

No provider, credentials, package installation, PostgreSQL or model execution.
The four-wheel candidate stays a candidate until an explicit placement receipt
supplies the unpublished wheel. Missing hosting never becomes an invented URL.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import lzma
from pathlib import Path
import time
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

from regional_knowledge.kaggle_native_recipe import PackageSnapshot, resolve_native_closure
from regional_knowledge.kaggle_native_bundle import assemble_native_collection, MAX_PACKAGES

HERE = Path(__file__).resolve().parent
MANIFESTS = HERE / 'manifests'
DJVU_VERSION = '3.5.28-2ubuntu0.24.04.2'
PLATFORM = dict(os_id='ubuntu', os_version_id='24.04', machine='x86_64',
                libc_name='glibc', libc_version='2.39')
PREFIXES = ('usr/bin/ddjvu', 'usr/lib', 'lib', 'lib64', 'usr/lib64',
            'usr/share/postgresql', 'usr/share/djvu')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def secure_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        raise ValueError('permanent HTTPS URL without credentials/query required')
    return url


def merge_wheels(base, candidate, placement=None):
    """Produce the real Stage0 codec only with all four actual download URLs."""
    if base.get('codec') != 'rkb.candidate_wheel_lock.v1':
        raise ValueError('base Stage0 wheel lock required')
    if candidate.get('codec') != 'rkb.telethon_candidate_wheel_lock.v1':
        raise ValueError('reviewed four-wheel candidate required')
    packages = candidate['packages']
    if {p['package'] for p in packages} != {'telethon', 'pyaes', 'rsa', 'pyasn1'} or len(packages) != 4:
        raise ValueError('exact four-package Telegram closure required')
    pyaes = next(p for p in packages if p['package'] == 'pyaes')
    if placement is None:
        return dict(codec='rkb.import_dependency_inputs.v1', status='UNRESOLVED',
                    missing=['approved permanent pyaes HTTPS placement receipt'],
                    pyaes={k: pyaes[k] for k in ('filename', 'size', 'sha256', 'version')},
                    runtime_lock=None)
    if (placement.get('approved') is not True or not placement.get('receipt_ref')
            or any(placement.get(k) != pyaes[k] for k in ('size', 'sha256'))):
        raise ValueError('placement must bind approved exact pyaes bytes and receipt')
    url = secure_url(placement['url'])
    if pyaes['sha256'] not in urlsplit(url).path:
        raise ValueError('content-addressed pyaes placement required')
    result = json.loads(json.dumps(base))
    identities = {p['package'].lower().replace('_', '-') for p in base['packages']}
    if identities & {'telethon', 'pyaes', 'rsa', 'pyasn1'}:
        raise ValueError('duplicate dependency in base lock')
    for package in packages:
        row = {k: package[k] for k in ('version', 'size', 'sha256')}
        row.update(package='Telethon' if package['package'] == 'telethon' else package['package'],
                   name='wheels/' + package['filename'],
                   url=url if package['package'] == 'pyaes' else secure_url(package['url']))
        result['packages'].append(row)
    result['wheel_bytes'] = sum(p['size'] for p in result['packages'])
    result['telegram_placement_receipt'] = placement['receipt_ref']
    return result


def download(url, path, *, maximum, expected_sha=None, expected_size=None):
    secure_url(url)
    if path.exists():
        raise ValueError('download output already exists')
    partial = path.with_name(path.name + '.partial')
    for attempt in range(1, 4):
        h = hashlib.sha256()
        count = 0
        print(json.dumps(dict(event='dependency_download_start', file=path.name, attempt=attempt)), flush=True)
        try:
            with urlopen(url, timeout=60) as response, partial.open('xb') as target:
                secure_url(response.geturl())
                while data := response.read(65536):
                    count += len(data)
                    if count > maximum:
                        raise ValueError('download byte bound exceeded')
                    h.update(data)
                    target.write(data)
            if expected_sha is not None and h.hexdigest() != expected_sha:
                raise ValueError('download SHA mismatch')
            if expected_size is not None and count != expected_size:
                raise ValueError('download size mismatch')
            partial.rename(path)
            print(json.dumps(dict(event='dependency_download_verified', file=path.name, size=count)), flush=True)
            return dict(url=url, size=count, sha256=h.hexdigest())
        except (TimeoutError, URLError, ConnectionError) as error:
            partial.unlink(missing_ok=True)
            print(json.dumps(dict(event='dependency_download_retry', file=path.name,
                                  attempt=attempt, error_type=type(error).__name__)), flush=True)
            if attempt == 3:
                raise
            time.sleep(attempt * 2)


def merge_native(base, extra):
    """Preserve every existing pin; a dependency upgrade needs separate review."""
    if any(p.get('release') != 'noble' or p.get('architecture') != 'amd64' for p in (base, extra)):
        raise ValueError('one Noble amd64 closure required')
    packages = {p['name']: p for p in base['packages']}
    for package in extra['packages']:
        old = packages.get(package['name'])
        if old and any(old[k] != package[k] for k in ('version', 'architecture', 'size', 'sha256')):
            raise ValueError('existing native pin differs: ' + package['name'])
        packages.setdefault(package['name'], package)
    return dict(codec='rkb.native_package_lock.v1', release='noble', architecture='amd64',
                roots=base['roots'] | extra['roots'], packages=sorted(packages.values(), key=lambda p: p['name']),
                metadata=base['metadata'] + extra['metadata'],
                dependency_edges=base['dependency_edges'] + extra['dependency_edges'],
                total_download_bytes=sum(p['size'] for p in packages.values()),
                target_abi_observed=False, native_execution_verified=False)


def native_review(base, extra):
    """Retain both closures and report every conflict without blessing a union."""
    before = {p['name']: p for p in base['packages']}
    after = {p['name']: p for p in extra['packages']}
    conflicts = [dict(package=name, baseline=before[name], djvu=after[name])
                 for name in sorted(before.keys() & after.keys())
                 if any(before[name][key] != after[name][key]
                        for key in ('version', 'architecture', 'size', 'sha256'))]
    count = len(before.keys() | after.keys())
    return dict(codec='rkb.native_merge_review.v1',
                status='UNRESOLVED' if conflicts or count > MAX_PACKAGES else 'COMPATIBLE',
                conflicts=conflicts, union_package_count=count, current_max_packages=MAX_PACKAGES,
                package_bound_exceeded=count > MAX_PACKAGES, dependencies_removed=False,
                deployable=False, manifest=None, elf_receipt=None)


def preserved_native_closure(base):
    """Resolve captured dependency constraints with the original base pins intact."""
    metadata = json.loads((MANIFESTS / 'native-preserved-metadata-20261022.json').read_text())
    snapshots = [PackageSnapshot(MANIFESTS / item['name'], item['release'],
                                 item['base_url'], item['sha256']) for item in metadata['inputs']]
    lock = resolve_native_closure(snapshots, release='noble',
                                 roots=base['roots'] | {'djvulibre-bin': DJVU_VERSION})
    selected = {p['name']: p for p in lock['packages']}
    for previous in base['packages']:
        current = selected.get(previous['name'])
        if current is None or any(current[key] != previous[key]
                                 for key in ('version', 'architecture', 'size', 'sha256', 'url')):
            raise ValueError('preserved closure changed baseline pin: ' + previous['name'])
    return lock


def capture_djvu(root):
    snapshots = []
    receipts = []
    # Save exact index bytes and hashes in CI evidence. A subsequent rebuild can
    # download only the resulting immutable package lock; no moving apt install.
    for pocket in ('noble', 'noble-updates', 'noble-security'):
        for component in ('main', 'universe'):
            stem = pocket + '-' + component
            url = f'https://archive.ubuntu.com/ubuntu/dists/{pocket}/{component}/binary-amd64/Packages.xz'
            packed = root / (stem + '.xz')
            receipt = download(url, packed, maximum=64 * 1024**2)
            plain = root / (stem + '.Packages')
            size = 0
            with lzma.open(packed) as source, plain.open('xb') as target:
                while data := source.read(1024 * 1024):
                    size += len(data)
                    if size > 256 * 1024**2:
                        raise ValueError('metadata expansion bound exceeded')
                    target.write(data)
            sha = digest(plain.read_bytes())
            snapshots.append(PackageSnapshot(plain, 'noble', 'https://archive.ubuntu.com/ubuntu/', sha))
            receipts.append(receipt | dict(uncompressed_sha256=sha, uncompressed_size=size))
            save(root / 'metadata-receipts.json', receipts)
    save(root / 'metadata-receipts.json', receipts)
    return resolve_native_closure(snapshots, release='noble', roots={'djvulibre-bin': DJVU_VERSION})


def elf_closure(bundle):
    """Reuse the reviewed DjVu leaf validator against the actual verified bundle."""
    from regional_knowledge.kaggle_djvu_elf import pinned_djvu_elf_closure
    binary, loader, library_path = pinned_djvu_elf_closure(bundle)
    return dict(codec='rkb.native_elf_closure.v1',
                binary=str(Path(binary).relative_to(bundle.prefix)),
                loader=str(Path(loader).relative_to(bundle.prefix)),
                library_path=[str(Path(p).relative_to(bundle.prefix)) for p in library_path.split(':')],
                verifier_source_sha256='904b782aabd7af8a798c608655ed4671d60249ffc370ba0501d80081b2db96b9',
                recursive_dt_needed_verified=True, ambient_libraries_used=False,
                native_execution_verified=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--native-lock', type=Path, help='rebuild exact CI-produced lock, without index resolution')
    parser.add_argument('--refresh-djvu-metadata', action='store_true', help='explicitly capture new moving Ubuntu indexes')
    parser.add_argument('--pyaes-placement', type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    base = json.loads((MANIFESTS / 'kaggle-cp313-x86_64-pdf-wheels.json').read_text())
    candidate = json.loads((MANIFESTS / 'telethon-stage0-input-20261022.json').read_text())
    placement = json.loads(args.pyaes_placement.read_text()) if args.pyaes_placement else None
    save(root / 'stage0-wheel-lock.json', merge_wheels(base, candidate, placement))
    if args.native_lock:
        lock = json.loads(args.native_lock.read_text())
    else:
        base_native = json.loads((MANIFESTS / 'kaggle-native-noble-pg17.11-base-lock.json').read_text())
        extra = capture_djvu(root) if args.refresh_djvu_metadata else json.loads(
            (MANIFESTS / 'kaggle-native-djvu-noble-lock-20261022.json').read_text())
        save(root / 'djvu-package-lock.json', extra)
        save(root / 'native-base-lock.json', base_native)
        # Re-resolve actual Debian constraints from retained records, never
        # merely overwrite the three conflicting versions in the candidate.
        if not args.refresh_djvu_metadata:
            lock = preserved_native_closure(base_native)
            review = native_review(base_native, lock)
            save(root / 'native-package-lock.json', lock)
        else:
            review = native_review(base_native, extra)
            lock = None
        save(root / 'native-merge-review.json', review)
        if review['status'] == 'UNRESOLVED':
            print(json.dumps(review), flush=True)
            return
        if lock is None:
            lock = merge_native(base_native, extra)
    save(root / 'native-package-lock.json', lock)
    if len(lock['packages']) > MAX_PACKAGES:
        save(root / 'native-bound.json', dict(status='UNRESOLVED', actual=len(lock['packages']),
             current_max=MAX_PACKAGES, missing='integrator-owned native package bound review', dependencies_removed=False))
        return
    packages = []
    for package in lock['packages']:
        name = package['name'] + '.deb'
        download(package['url'], root / name, maximum=512 * 1024**2,
                 expected_sha=package['sha256'], expected_size=package['size'])
        packages.append(dict(name=package['name'], archive=name, size=package['size'], sha256=package['sha256']))
    receipt = assemble_native_collection(root, packages, identity=PLATFORM, include_prefixes=PREFIXES, total_seconds=900)
    save(root / 'native-receipt.json', receipt)
    # Existing extractor performs all nested hashes and safe link materialization.
    # CI has a disposable root; runtime paths remain entirely untouched.
    from regional_knowledge import kaggle_native_bundle as native
    native.TRUSTED_ROOT = root
    bundle = native.prepare_native_bundle(root, receipt['spec']['manifest_name'],
        receipt['spec']['manifest_sha256'], identity=PLATFORM, inventory=receipt['artifacts'])
    save(root / 'ddjvu-elf-closure.json', elf_closure(bundle))
    print(json.dumps(dict(status='PACKAGED_NOT_DEPLOYED', packages=len(packages),
                         pyaes_hosting='UNRESOLVED' if placement is None else 'SUPPLIED')))


if __name__ == '__main__':
    main()
