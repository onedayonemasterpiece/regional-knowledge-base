"""Notebook entrypoint: stdlib input staging, existing Stage0, finite PG part.

Run this trusted source with one private JSON config path. It sends no provider
requests and contains no account identity or default notebook/Dataset slug.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import shutil
import tarfile
import time


def main(config_path):
    os.umask(0o077)
    started = time.monotonic()
    config = json.loads(Path(config_path).read_bytes())
    if (not Path(config['output_root']).resolve().is_relative_to('/kaggle/working')
            or not any(Path(config['input_root']).resolve().is_relative_to(root)
                       for root in ('/kaggle/input', '/kaggle/temp'))
            or not all(Path(path).resolve().is_relative_to('/kaggle/temp') for path in (
                config['code_root'], config['prepared_model']['root'], config['database']['cluster']))):
        raise ValueError('private inputs and PG must stay outside captured notebook output')
    temporary = Path('/kaggle/temp')
    try:
        temporary.mkdir(mode=0o711)
    except FileExistsError:
        pass  # Existing parent permissions are checked by the owned PG helper.
    else:
        temporary.chmod(0o711)
    inputs = Path(config['input_root'])
    manifest = (inputs/'input-manifest.json').read_bytes()
    if hashlib.sha256(manifest).hexdigest() != config['input_manifest_sha256']:
        raise ValueError('input manifest differs from submitted binding')
    for entry in json.loads(manifest)['files']:
        digest = hashlib.sha256()
        with (inputs/entry['name']).open('rb') as source:
            while chunk := source.read(1024*1024):
                digest.update(chunk)
            if source.tell() != entry['size'] or digest.hexdigest() != entry['sha256']:
                raise ValueError('private input file differs from submitted inventory')
    code = Path(config['code_root'])
    with tarfile.open(inputs/'batch-code.tar.gz') as archive:
        archive.extractall(code, filter='data')
    sys.path.insert(0, str(code/'src'))
    private_names = {entry['name'] for entry in json.loads(manifest)['files']}
    downloads = {entry['name'] for entry in config['stage0']['downloads']}
    for entry in config['prepared_model']['entries']:
        if entry['name'] not in private_names and entry['name'] in downloads:
            continue  # Existing Stage0 fetches and verifies the pinned public asset.
        source_root = inputs if entry['name'] in private_names else Path(config['assets_input_root'])
        target = Path(config['prepared_model']['root'])/entry['name']
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (source_root/entry['name']).open('rb') as source, target.open('xb') as destination:
            shutil.copyfileobj(source, destination)
    from regional_knowledge.kaggle_stage0 import prepare_stage0
    prepare_stage0(config['prepared_model'], config['stage0'], run_id=config['run_id'])
    from regional_knowledge.pg_rebuild_part import execute_part
    return asyncio.run(execute_part(config, started))


if __name__ == '__main__':
    try:
        main(sys.argv[1])
    except Exception as error:
        # PG exceptions may contain accepted private text. Never print them.
        report = dict(status='failed', error_type=type(error).__name__, publishable=False)
        config = json.loads(Path(sys.argv[1]).read_bytes())
        output = Path(config['output_root'])
        output.mkdir(mode=0o700, exist_ok=True)
        (output/'error-report.json').write_text(json.dumps(report)+'\n')
        print(json.dumps(report), flush=True)
        raise SystemExit(1) from None
