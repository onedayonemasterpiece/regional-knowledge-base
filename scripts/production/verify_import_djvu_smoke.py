"""GitHub-only synthetic pixel smoke over the exact assembled native bundle."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from regional_knowledge import kaggle_native_bundle as native
from regional_knowledge.kaggle_djvu_elf import pinned_djvu_elf_closure
from regional_knowledge.kaggle_djvu_pixels import _read_ppm


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while data := stream.read(1024 * 1024):
            value.update(data)
    return value.hexdigest()


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('synthetic decoder execution belongs on GitHub runner')
    root = Path(sys.argv[1]).resolve()
    expected = {
        'native-packages.tar': (96378880, 'ae4824f871ce47d4a15487b8c171515eee1c20da11611ad0bc7233b9a476c006'),
        'native-manifest.json': (955336, '84ab517abe83ca7f90b16290f30332415522eb4b355b6ef194c40df89cb264f5'),
    }
    for name, (size, digest) in expected.items():
        path = root / name
        if path.stat().st_size != size or sha(path) != digest:
            raise ValueError('exact assembled bundle bytes required')
    receipt = json.loads((root / 'native-receipt.json').read_text())
    native.TRUSTED_ROOT = root
    bundle = native.prepare_native_bundle(root, 'native-manifest.json',
        expected['native-manifest.json'][1], identity=receipt['platform'], inventory=receipt['artifacts'])
    binary, loader, libraries = pinned_djvu_elf_closure(bundle)
    fixture = root / 'synthetic-red-blue.ppm'
    fixture.write_bytes(b'P6\n64 48\n255\n' +
        ((bytes((224, 32, 32)) * 32 + bytes((32, 32, 224)) * 32) * 48))
    original = root / 'synthetic-red-blue.djvu'
    # System c44 creates test input only. The decoder below uses the pinned
    # bundle binary, explicit hashed loader and no ambient loader cache.
    subprocess.run(['/usr/bin/c44', str(fixture), str(original)], check=True,
                   capture_output=True, timeout=20)
    child = Path(__file__).resolve().parents[2] / 'src/regional_knowledge/kaggle_djvu_decoder_child.py'
    child_sha = sha(child)
    if child_sha != '4b29833eb25db1226bae5650113886a2f2be07f2bca38146cbd612bf7673489e':
        raise ValueError('reviewed decoder child bytes required')
    result = subprocess.run([sys.executable, '-I', str(child), loader, binary,
        str(original), '0', '1280', libraries], check=True, capture_output=True,
        timeout=20, env={'PATH': '/usr/bin:/bin', 'LANG': 'C'})
    width, height, pixels = _read_ppm(result.stdout, edge=1280)
    if width * 3 != height * 4 or width < 64 or height < 48:
        raise ValueError('synthetic page geometry mismatch')
    def pixel(x):
        offset = ((height // 2) * width + x) * 3
        return tuple(pixels[offset:offset + 3])
    left, right = pixel(width // 4), pixel(3 * width // 4)
    if not (left[0] > 150 and left[0] - left[2] > 100
            and right[2] > 150 and right[2] - right[0] > 100):
        raise ValueError('decoded synthetic pixels do not match source colors')
    proof = dict(codec='rkb.synthetic_djvu_smoke.v1', status='PASS',
        github_run=os.environ.get('GITHUB_RUN_ID'), source=os.environ.get('GITHUB_SHA'),
        input_bundle_run=39762610380, input_artifact=12419470365,
        manifest_sha256=expected['native-manifest.json'][1],
        native_archive_sha256=expected['native-packages.tar'][1],
        child_sha256=child_sha, original_sha256=sha(original),
        output_sha256=hashlib.sha256(result.stdout).hexdigest(),
        width=width, height=height, left_rgb=left, right_rgb=right,
        executor='pinned_ddjvu_via_reviewed_child', fixture_encoder='system_c44',
        explicit_pinned_loader=True, ambient_loader_cache=False,
        kaggle_execution_verified=False, book_quality_acceptance=False)
    (root / 'synthetic-djvu-smoke.json').write_text(json.dumps(proof, indent=2) + '\n')
    print(json.dumps(proof), flush=True)


if __name__ == '__main__':
    main()
