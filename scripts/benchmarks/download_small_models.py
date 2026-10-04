"""Download only anonymous public static files, pinned to exact revisions."""
import hashlib
import json
import sys
import time
from pathlib import Path
import requests

MODELS = {
    'e5': ('Xenova/multilingual-e5-small', '761b726dd34fb83930e26aab4e9ac3899aa1fa78', ['onnx/model_quantized.onnx', 'tokenizer.json', 'tokenizer_config.json', 'special_tokens_map.json', 'config.json', 'README.md']),
    'gemma': ('onnx-community/embeddinggemma-300m-ONNX', '5090578d9565bb06545b4552f76e6bc2c93e4a66', ['onnx/model_q4.onnx', 'onnx/model_q4.onnx_data', 'tokenizer.json', 'tokenizer_config.json', 'special_tokens_map.json', 'config.json', 'README.md']),
    'potion': ('minishlab/potion-multilingual-128M', '73908c3438cf03b6a01bcb9611d62b23d0726f08', ['model.safetensors', 'tokenizer.json', 'tokenizer_config.json', 'special_tokens_map.json', 'config.json', 'README.md']),
}

def main():
    root = Path(sys.argv[1])
    session = requests.Session()
    session.trust_env = False  # Also disable implicit netrc credentials.
    for name, (repo, rev, files) in MODELS.items():
        dest = root / 'models' / name
        dest.mkdir(parents=True, exist_ok=True)
        manifest = {'repo': repo, 'revision': rev, 'files': [], 'anonymous': True}
        start = time.perf_counter()
        try:
            for file in files:
                target = dest / file
                target.parent.mkdir(parents=True, exist_ok=True)
                if not target.exists():
                    url = f'https://huggingface.co/{repo}/resolve/{rev}/{file}'
                    # No environment/API token is read or forwarded.
                    with session.get(url, stream=True, timeout=(20, 120)) as response:
                        response.raise_for_status()
                        digest = hashlib.sha256()
                        with target.with_suffix(target.suffix+'.partial').open('wb') as fh:
                            for block in response.iter_content(1024*1024):
                                fh.write(block)
                                digest.update(block)
                        target.with_suffix(target.suffix+'.partial').rename(target)
                digest = hashlib.sha256()
                with target.open('rb') as fh:
                    for block in iter(lambda: fh.read(1024*1024), b''): digest.update(block)
                manifest['files'].append({'path': file, 'bytes': target.stat().st_size, 'sha256': digest.hexdigest()})
                print(name, file, target.stat().st_size, flush=True)
            manifest['status'] = 'downloaded'
        except requests.HTTPError as exc:
            manifest['status'] = 'blocked'
            manifest['http_status'] = exc.response.status_code
            print(name, 'blocked', exc.response.status_code, flush=True)
        manifest['download_seconds'] = time.perf_counter()-start
        (dest/'manifest.json').write_text(json.dumps(manifest, indent=2))

if __name__ == '__main__': main()
